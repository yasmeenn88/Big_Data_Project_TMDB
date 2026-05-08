
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
import os

# CONFIGURATION
TMDB_API_KEY = Variable.get("tmdb_api_key", default_var=os.getenv("TMDB_API_KEY"))
SNOWFLAKE_CONN_ID = "snowflake_conn"
SPARK_YARN_CONN_ID = "spark_yarn"

HDFS_RAW = "hdfs://hadoop-namenode:9000/data/raw"
HDFS_PROCESSED = "hdfs://hadoop-namenode:9000/data/processed"
HDFS_WAREHOUSE = "hdfs://hadoop-namenode:9000/data/warehouse"

SPARK_CONF = {
    "spark.master": "yarn",
    "spark.deploy.mode": "client",
    "spark.submit.deployMode": "client",
    "spark.yarn.queue": "default",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.driver.memory": "512m",
    "spark.executor.memoryOverhead": "384m",
    "spark.driver.cores": "1",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.hadoop.fs.defaultFS": "hdfs://hadoop-namenode:9000",
    "spark.hadoop.yarn.resourcemanager.hostname": "resourcemanager",
    "spark.hadoop.user.name": "root",
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
}

# DAG DEFINITION
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["data-eng@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id="tmdb_data_warehouse_etl",
    default_args=default_args,
    description="TMDB ETL pipeline with Spark-on-YARN and Snowflake loading",
    schedule_interval="@daily",
    catchup=False,
    tags=["tmdb", "etl", "spark", "snowflake", "production"],
    params={
        "api_pages": Param(
            10,
            type="integer",
            description="Number of pages to fetch from TMDB API"
        ),
        "load_to_snowflake": Param(
            True,
            type="boolean",
            description="Load processed data to Snowflake"
        ),
    }
)

#  TASK GROUP: DATA INGESTION
with TaskGroup(
    group_id="data_ingestion",
    tooltip="Fetch TMDB data from API and upload to HDFS",
    dag=dag,
) as data_ingestion_group:

    ingest_task = SparkSubmitOperator(
        task_id="fetch_and_ingest_tmdb_data",
        application="/opt/airflow/spark_jobs/ingest_to_hdfs.py",
        conf=SPARK_CONF,
        total_executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "TMDB_API_KEY": TMDB_API_KEY,
            "HDFS_RAW_PATH": HDFS_RAW,
        },
        application_args=[
            "--pages", "{{ params.api_pages }}",
            "--hdfs-path", HDFS_RAW,
        ],
        name="tmdb_ingest_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

#  DATA CLEANING & TRANSFORMATION
with TaskGroup(
    group_id="data_processing",
    tooltip="Clean, deduplicate, and transform raw data into Parquet",
    dag=dag,
) as processing_group:

    clean_transform = SparkSubmitOperator(
        task_id="clean_and_transform",
        application="/opt/airflow/spark_jobs/clean_transform.py",
        conf=SPARK_CONF,
        total_executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
        env_vars={
            "HDFS_RAW_PATH": HDFS_RAW,
            "HDFS_PROCESSED_PATH": HDFS_PROCESSED,
        },
        application_args=[
            "--input", f"{HDFS_RAW}/tmdb_movies.json",
            "--output", HDFS_PROCESSED,
        ],
        name="tmdb_clean_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

#  STAR SCHEMA CREATION
with TaskGroup(
    group_id="warehouse_modeling",
    tooltip="Build dimensional and fact tables",
    dag=dag,
) as warehouse_group:

    star_schema = SparkSubmitOperator(
        task_id="build_star_schema",
        application="/opt/airflow/spark_jobs/star_schema.py",
        conf=SPARK_CONF,
        total_executor_cores=4,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "HDFS_PROCESSED": HDFS_PROCESSED,
            "HDFS_WAREHOUSE": HDFS_WAREHOUSE,
        },
        application_args=[
            "--processed-path", HDFS_PROCESSED,
            "--warehouse-path", HDFS_WAREHOUSE,
        ],
        name="tmdb_star_schema_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

#  SNOWFLAKE LOADING
with TaskGroup(
    group_id="snowflake_loading",
    tooltip="Load warehouse data to Snowflake",
    dag=dag,
) as snowflake_group:

    def check_snowflake_loading(**context):
        """Branching logic for conditional Snowflake loading"""
        load_flag = context["params"].get("load_to_snowflake", True)
        return "snowflake_loading.load_to_snowflake" if load_flag else "snowflake_loading.skip_snowflake_load"

    branch_snowflake = BranchPythonOperator(
        task_id="check_snowflake_loading",
        python_callable=check_snowflake_loading,
        provide_context=True,
        dag=dag,
    )

    load_to_snowflake_task = BashOperator(
        task_id="load_to_snowflake",
        bash_command="echo ' Loading data to Snowflake '",
        dag=dag,
    )

    # Load dimension tables to Snowflake
    load_dim_movie = SparkSubmitOperator(
        task_id="load_dim_movie_to_snowflake",
        application="/opt/airflow/spark_jobs/load_to_snowflake.py",
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "TMDB_DWH"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
        application_args=[
            "--table-name", "DIM_MOVIE",
            "--hdfs-path", f"{HDFS_WAREHOUSE}/dim_movie",
        ],
        name="load_dim_movie_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

    load_dim_genre = SparkSubmitOperator(
        task_id="load_dim_genre_to_snowflake",
        application="/opt/airflow/spark_jobs/load_to_snowflake.py",
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "TMDB_DWH"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
        application_args=[
            "--table-name", "DIM_GENRE",
            "--hdfs-path", f"{HDFS_WAREHOUSE}/dim_genre",
        ],
        name="load_dim_genre_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

    load_dim_date = SparkSubmitOperator(
        task_id="load_dim_date_to_snowflake",
        application="/opt/airflow/spark_jobs/load_to_snowflake.py",
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "TMDB_DWH"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
        application_args=[
            "--table-name", "DIM_DATE",
            "--hdfs-path", f"{HDFS_WAREHOUSE}/dim_date",
        ],
        name="load_dim_date_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

    load_fact_metrics = SparkSubmitOperator(
        task_id="load_fact_movie_metrics_to_snowflake",
        application="/opt/airflow/spark_jobs/load_to_snowflake.py",
        conf=SPARK_CONF,
        executor_memory="2g",
        driver_memory="1g",
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "TMDB_DWH"),
            "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
            "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        },
        application_args=[
            "--table-name", "FACT_MOVIE_METRICS",
            "--hdfs-path", f"{HDFS_WAREHOUSE}/fact_movie_metrics",
        ],
        name="load_fact_metrics_{{ ds }}",
        verbose=True,
        queue="default",
        dag=dag,
    )

    skip_load = BashOperator(
        task_id="skip_snowflake_load",
        bash_command="echo 'Snowflake loading skipped per configuration' ",
        dag=dag,
    )

    branch_snowflake >> load_to_snowflake_task >> [load_dim_movie, load_dim_genre, load_dim_date, load_fact_metrics]
    branch_snowflake >> skip_load

#  TASK: PIPELINE SUCCESS
notify_success = BashOperator(
    task_id="pipeline_success_notification",
    bash_command="""
    echo " TMDB ETL Pipeline Completed Successfully 'DONE' "
    echo " Execution Date: {{ ds }}"
    echo " Execution Time: {{ ts }}"
    echo " Airflow UI: http://localhost:18080"
    """,
    dag=dag,
)

# DAG DEPENDENCIES
data_ingestion_group >> processing_group >> warehouse_group >> snowflake_group >> notify_success


