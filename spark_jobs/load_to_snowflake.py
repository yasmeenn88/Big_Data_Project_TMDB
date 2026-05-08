import os
import argparse
import pandas as pd
import snowflake.connector
from pyspark.sql import SparkSession
from snowflake.connector.pandas_tools import write_pandas

# Configuration
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "TMDB_DWH")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# Primary keys for each table
TABLE_KEYS = {
    "DIM_MOVIE": ["movie_id"],
    "DIM_DATE": ["date_id"],
    "DIM_GENRE": ["genre_id"],
    "FACT_MOVIE_METRICS": ["movie_id", "genre_id"]
}


def create_spark_session():
    """Create Spark session on YARN"""
    spark = SparkSession.builder \
        .appName("LoadToSnowflake") \
        .master("yarn") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
        .config("spark.hadoop.user.name", "root") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.jars",
                "/opt/spark-3.4.0-bin-hadoop3/jars/snowflake-jdbc-3.13.29.jar,/opt/spark-3.4.0-bin-hadoop3/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_without_duplicates(df, table_name, conn):

    key_columns = TABLE_KEYS.get(table_name, [])

    if not key_columns:
        # No key defined → regular append
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name,
                                                  auto_create_table=True,
                                                  overwrite=False)
        return nrows, 0

    try:
        # Get existing IDs from Snowflake
        key_col_str = ", ".join(key_columns)
        existing_query = f"SELECT {key_col_str} FROM {table_name}"
        existing_df = pd.read_sql(existing_query, conn)

        if len(existing_df) == 0:
            # Table is empty → load everything
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name,
                                                      auto_create_table=True,
                                                      overwrite=False)
            return nrows, 0

        # Remove rows with existing IDs
        merged = df.merge(existing_df, on=key_columns, how='left', indicator=True)
        new_rows = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
        skipped_count = len(df) - len(new_rows)

        if len(new_rows) > 0:
            success, nchunks, nrows, _ = write_pandas(conn, new_rows, table_name,
                                                      auto_create_table=True,
                                                      overwrite=False)
            return nrows, skipped_count
        else:
            return 0, skipped_count

    except Exception as e:
        # Table doesn't exist yet → create and load everything
        print(f" Table {table_name} doesn't exist. Creating One to use")
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name,
                                                  auto_create_table=True,
                                                  overwrite=False)
        return nrows, 0


def load_to_snowflake(spark, hdfs_path, table_name):
    """
    Load data from HDFS Parquet to Snowflake
    Automatically skips duplicate rows based on primary keys
    """
    print(f"📖 Reading Parquet from: {hdfs_path}")
    df = spark.read.parquet(hdfs_path).toPandas()
    total_rows = len(df)
    print(f"✓ Loaded {total_rows} rows from HDFS")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    try:
        loaded_count, skipped_count = load_without_duplicates(df, table_name, conn)
        print(f" Loaded {loaded_count} new rows, skipped {skipped_count} duplicates to {table_name}")
    finally:
        conn.close()

    return True


def main():
    parser = argparse.ArgumentParser(description="Load data to Snowflake without duplicates")
    parser.add_argument("--table-name", required=True, help="Snowflake table name")
    parser.add_argument("--hdfs-path", required=True, help="HDFS path to Parquet data")

    args = parser.parse_args()


    print(" Snowflake Data Loading Started .... ")

    print(f"Table: {args.table_name}")
    print(f"HDFS Path: {args.hdfs_path}")
    print(f"Target: {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")

    spark = create_spark_session()

    try:
        load_to_snowflake(spark, args.hdfs_path, args.table_name)
        print(" Loading completed successfully!")

    except Exception as e:
        print(f" Pipeline failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
