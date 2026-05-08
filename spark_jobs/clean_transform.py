from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim
import os
import argparse

# Basic Configurations
HDFS_RAW = os.getenv("HDFS_RAW", "hdfs://hadoop-namenode:9000/data/raw")
HDFS_PROCESSED = os.getenv("HDFS_PROCESSED", "hdfs://hadoop-namenode:9000/data/processed")


def main():
    parser = argparse.ArgumentParser(description="Clean and transform TMDB data")
    parser.add_argument("--input", required=True, help="Input JSON path")
    parser.add_argument("--output", required=True, help="Output Parquet path")
    args = parser.parse_args()

    # Spark Session on YARN
    spark = SparkSession.builder \
        .appName("TMDB-Clean") \
        .master("yarn") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
        .config("spark.hadoop.user.name", "root") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.pyspark.python", "python3")\
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f" Reading raw data from: {args.input}")
        df = spark.read.option("multiline", "true").json(args.input)
        initial_count = df.count()
        print(f" Loaded: {initial_count} movies")

        # Cleaning STEPS
        # 1. Remove duplicates
        df = df.dropDuplicates(["id"])

        # 2. Remove rows without id or title
        df = df.filter(col("id").isNotNull() & col("title").isNotNull())

        # 3. Trim whitespace from text columns
        for c in ["title", "original_title", "overview"]:
            df = df.withColumn(c, trim(col(c)))

        cleaned_count = df.count()
        print(f" Cleaned: {cleaned_count} movies (removed {initial_count - cleaned_count})")

        #  Split
        movies_cols = [c for c in df.columns if c != "genre_ids"]
        movies_df = df.select(movies_cols)

        genres_df = df.select(
            col("id").alias("movie_id"),
            explode("genre_ids").alias("genre_id")
        ).filter(col("genre_id").isNotNull())

        print(f" Movies: {movies_df.count()} | Genres: {genres_df.count()}")

        # Save Parquet
        movies_path = f"{args.output}/movies_clean"
        genres_path = f"{args.output}/genres_clean"

        print(f" Saving movies to: {movies_path}")
        movies_df.write.mode("overwrite").parquet(movies_path)

        print(f" Saving genres to: {genres_path}")
        genres_df.write.mode("overwrite").parquet(genres_path)

        print(" Clean & Transform Complete!")

    except Exception as e:
        print(f" Error: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


