<!--
# TMDB Data Warehouse ETL Pipeline

A production-grade ETL pipeline that ingests movie data from The Movie Database (TMDB) API, processes it with Apache Spark on YARN, builds a dimensional data warehouse, and loads it into Snowflake.

## 🏗️ Architecture Overview

```
TMDB API
   ↓
[Ingest] → HDFS Raw (/data/raw)
   ↓
[Clean & Transform] → HDFS Processed (/data/processed)
   ↓
[Star Schema] → HDFS Warehouse (/data/warehouse)
   ↓
[Snowflake Loading] → Snowflake DWH
```

### Technology Stack

- **Orchestration**: Apache Airflow
- **Distributed Processing**: Apache Spark on YARN
- **Data Lake**: Hadoop HDFS
- **Data Warehouse**: Snowflake
- **Data Source**: TMDB API v3
- **Language**: Python 3

## 📋 Project Structure

```
.
├── etl_pipeline.py           # Airflow DAG orchestration
├── ingest_to_hdfs.py         # API data ingestion
├── clean_transform.py        # Data cleaning & transformation
├── star_schema.py            # Dimensional modeling
└── load_to_snowflake.py      # Snowflake loading
```

## 🔧 Core Components

### 1. **ingest_to_hdfs.py** - Data Ingestion
Fetches movie data from TMDB API and uploads to HDFS.

**Key Features:**
- Paginated API requests with retry logic (max 2 attempts)
- Random sort selection to avoid duplicate data fetches
- Smart pagination logic based on current date
- Local file saving with UTF-8 encoding
- WebHDFS upload with datanode redirection

**Usage:**
```bash
python ingest_to_hdfs.py --pages 10 --hdfs-path hdfs://hadoop-namenode:9000/data/raw
```

**Environment Variables:**
- `TMDB_API_KEY` (required): TMDB API authentication key
- `HDFS_RAW_PATH` (optional): HDFS destination for raw data

**Output:**
- Local file: `/tmp/tmdb_data/tmdb_movies_YYYY-MM-DD.json`
- HDFS path: `/data/raw/tmdb_movies.json`

---

### 2. **clean_transform.py** - Data Processing
Cleans raw JSON data and transforms it into normalized Parquet format.

**Cleaning Steps:**
- Remove duplicate records (by movie ID)
- Filter out records missing `id` or `title`
- Trim whitespace from text columns
- Split into two normalized tables: movies and genres

**Output Structure:**
```
/data/processed/
├── movies_clean/      # Main movie dimensions
└── genres_clean/      # Movie-genre relationships
```

**Usage:**
```bash
python clean_transform.py \
  --input hdfs://hadoop-namenode:9000/data/raw/tmdb_movies.json \
  --output hdfs://hadoop-namenode:9000/data/processed
```

---

### 3. **star_schema.py** - Dimensional Modeling
Builds a star schema data warehouse with dimension and fact tables.

**Schema Design:**

#### Dimension Tables:
- **DIM_GENRE**: Movie genres (18 standard TMDB genres)
  - `genre_id`, `genre_name`

- **DIM_DATE**: Release dates extracted from movies
  - `date_id`, `year`, `month`, `day`

- **DIM_MOVIE**: Movie attributes
  - `movie_id`, `title`, `original_title`, `original_language`
  - `poster_url`, `poster_path`, `backdrop_path`
  - `release_date`, `adult`, `video`

#### Fact Table:
- **FACT_MOVIE_METRICS**: Movie metrics aggregated by genre and date
  - `movie_id`, `genre_id`, `date_id` (composite key)
  - `popularity`, `vote_average`, `vote_count`

**Usage:**
```bash
python star_schema.py \
  --processed-path hdfs://hadoop-namenode:9000/data/processed \
  --warehouse-path hdfs://hadoop-namenode:9000/data/warehouse
```

**Output:**
```
/data/warehouse/
├── dim_genre/
├── dim_date/
├── dim_movie/
└── fact_movie_metrics/
```

---

### 4. **load_to_snowflake.py** - Warehouse Loading
Loads warehouse data into Snowflake with automatic duplicate detection.

**Key Features:**
- Automatic table creation if missing
- Primary key-based duplicate detection
- Configurable loading strategy per table
- Pandas integration for optimized loading

**Duplicate Handling Logic:**
```
For each table:
  1. Read existing primary keys from Snowflake
  2. Compare new data against existing keys
  3. Load only new records
  4. Report load/skip counts
```

**Usage:**
```bash
python load_to_snowflake.py \
  --table-name DIM_MOVIE \
  --hdfs-path hdfs://hadoop-namenode:9000/data/warehouse/dim_movie
```

**Environment Variables:**
- `SNOWFLAKE_ACCOUNT` (required): Snowflake account identifier
- `SNOWFLAKE_USER` (required): Snowflake username
- `SNOWFLAKE_PASSWORD` (required): Snowflake password
- `SNOWFLAKE_DATABASE` (default: `TMDB_DWH`): Database name
- `SNOWFLAKE_SCHEMA` (default: `ANALYTICS`): Schema name
- `SNOWFLAKE_WAREHOUSE` (default: `COMPUTE_WH`): Warehouse name

---

### 5. **etl_pipeline.py** - Airflow Orchestration
Complete DAG orchestration with task groups and branching logic.

**Pipeline Flow:**
1. **Data Ingestion** → Fetch from API, upload to HDFS
2. **Data Processing** → Clean, deduplicate, transform to Parquet
3. **Warehouse Modeling** → Build star schema dimensions and facts
4. **Snowflake Loading** → Load tables with duplicate detection
5. **Notification** → Success/failure notifications

**DAG Parameters:**
- `api_pages` (int, default: 10): Number of API pages to fetch
- `load_to_snowflake` (bool, default: True): Enable/disable Snowflake loading

**Task Groups:**
- `data_ingestion`: API fetch and HDFS upload
- `data_processing`: Cleaning and transformation
- `warehouse_modeling`: Star schema creation
- `snowflake_loading`: Conditional loading with branching

**Schedule:** Daily (`@daily`)

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.4.0+
- Apache Hadoop 3.x
- Apache Airflow 2.x
- Snowflake account and credentials
- TMDB API key

### Installation

1. **Clone the repository:**
```bash
git clone <repo-url>
cd tmdb-etl-pipeline
```

2. **Install dependencies:**
```bash
pip install pyspark pandas requests snowflake-connector-python \
            apache-airflow apache-airflow-providers-apache-spark
```

3. **Set environment variables:**
```bash
export TMDB_API_KEY="your_tmdb_api_key"
export SNOWFLAKE_ACCOUNT="your_account_id"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_DATABASE="TMDB_DWH"
export SNOWFLAKE_SCHEMA="ANALYTICS"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export HDFS_RAW_PATH="hdfs://hadoop-namenode:9000/data/raw"
export HDFS_PROCESSED="hdfs://hadoop-namenode:9000/data/processed"
export HDFS_WAREHOUSE="hdfs://hadoop-namenode:9000/data/warehouse"
```

4. **Deploy to Airflow:**
```bash
cp etl_pipeline.py $AIRFLOW_HOME/dags/
cp ingest_to_hdfs.py /opt/airflow/spark_jobs/
cp clean_transform.py /opt/airflow/spark_jobs/
cp star_schema.py /opt/airflow/spark_jobs/
cp load_to_snowflake.py /opt/airflow/spark_jobs/
```

### Running Locally

**Single script execution:**
```bash
# Step 1: Ingest
python ingest_to_hdfs.py --pages 10

# Step 2: Clean & Transform
python clean_transform.py --input /tmp/tmdb_data/tmdb_movies_*.json --output /tmp/processed

# Step 3: Build Star Schema
python star_schema.py --processed-path /tmp/processed --warehouse-path /tmp/warehouse

# Step 4: Load to Snowflake
python load_to_snowflake.py --table-name DIM_MOVIE --hdfs-path /tmp/warehouse/dim_movie
```

**Via Airflow:**
```bash
# Initialize Airflow DB
airflow db init

# Start Airflow services
airflow webserver -p 8080 &
airflow scheduler &

# Access at http://localhost:8080
```

## 📊 Data Flow Details

### API Data Structure (Raw)
```json
{
  "id": 550,
  "title": "Fight Club",
  "original_title": "Fight Club",
  "overview": "An insomniac office worker...",
  "release_date": "1999-10-15",
  "popularity": 61.416,
  "vote_average": 8.4,
  "vote_count": 26280,
  "genre_ids": [18, 53],
  "poster_path": "/pB8BM7pdSp6B6Ih7QZ4DrQ3PchC.jpg",
  "backdrop_path": "/rr7E0NoGKxvbkXRVo6R50tLvnqD.jpg",
  "adult": false,
  "video": false
}
```

### Transformations
- **Raw JSON** → Parquet format with schema validation
- **Movies table**: Remove `genre_ids` column, keep all other fields
- **Genres table**: Explode `genre_ids` into separate rows
- **Date extraction**: Parse release_date into year, month, day components
- **Genre mapping**: Replace IDs with standard TMDB genre names

## 🔒 Configuration & Security

### Spark Configuration
```python
SPARK_CONF = {
    "spark.master": "yarn",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.driver.memory": "512m",
    "spark.sql.shuffle.partitions": "200",
}
```

### Retry Logic
- **Max retries**: 2 attempts per API request
- **Retry delay**: 2 seconds between attempts
- **Request timeout**: 30 seconds
- **Spark retry**: 2 retries with 5-minute delay in Airflow

### Data Validation
- Duplicate removal by primary keys
- Not-null constraints on critical fields
- Whitespace trimming on text columns
- Genre ID validation against known TMDB IDs

## 🐛 Error Handling

Each component includes:
- Try-catch exception handling
- Detailed error logging
- Graceful degradation where possible
- Airflow retry configuration
- Email notifications on failure

**Example error scenarios:**
```
API Rate Limiting → Retry with backoff
Missing API Key → ValueError with clear message
HDFS Connection Error → Exception logs + manual retry
Snowflake Auth Error → Credential validation at startup
Duplicate Records → Logged but not blocking
```

## 📈 Performance Optimization

- **Parquet compression**: Snappy codec for warehouse data
- **Spark partitioning**: Configurable shuffle partitions
- **YARN resource allocation**: Separate configs for memory/cores
- **Coalescing**: Single-partition output files for small datasets
- **Caching**: No unnecessary DF materializations

## 🔍 Monitoring & Debugging

### Airflow Logs
```bash
# View task logs
airflow logs tmdb_data_warehouse_etl fetch_and_ingest_tmdb_data 2024-01-01
```

### Spark Logs
```bash
# Yarn application logs
yarn logs -applicationId application_xxx
```

### HDFS Status
```bash
hdfs dfs -ls /data/raw
hdfs dfs -ls /data/processed
hdfs dfs -ls /data/warehouse
```

### Snowflake Status
```sql
SELECT * FROM TMDB_DWH.ANALYTICS.DIM_MOVIE LIMIT 5;
SELECT COUNT(*) FROM TMDB_DWH.ANALYTICS.FACT_MOVIE_METRICS;
```

-->

