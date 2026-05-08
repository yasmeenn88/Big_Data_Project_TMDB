import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, coalesce, lit , to_date , concat
from pyspark.sql.types import IntegerType, DoubleType

#  Genre Mapping
#  دول ثوابت معروفين عملت عليهم سيرش وجيبتهم لان ال (API) بيجيبهم بال (ID) بس
GENRE_MAPPING = [
    (28, "Action"), (12, "Adventure"), (16, "Animation"),
    (35, "Comedy"), (80, "Crime"), (99, "Documentary"),
    (18, "Drama"), (14, "Fantasy"), (36, "History"),
    (27, "Horror"), (10402, "Music"), (9648, "Mystery"),
    (10749, "Romance"), (878, "Science Fiction"), (10770, "TV Movie"),
    (53, "Thriller"), (10752, "War"), (37, "Western")
]

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Build TMDB Star Schema")
    parser.add_argument("--processed-path",
                        default=os.getenv("HDFS_PROCESSED", "hdfs://hadoop-namenode:9000/data/processed"),
                        help="HDFS path to processed data")
    parser.add_argument("--warehouse-path",
                        default=os.getenv("HDFS_WAREHOUSE", "hdfs://hadoop-namenode:9000/data/warehouse"),
                        help="HDFS path to warehouse output")
    args = parser.parse_args()

    # Create Spark session on YARN
    spark = SparkSession.builder \
        .appName("TMDB-StarSchema") \
        .master("yarn") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
        .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
        .config("spark.hadoop.user.name", "root") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("⭐ Building Star Schema")

    try:

        print("\n  (1) Creating dim_genre ")
        dim_genre = spark.createDataFrame(GENRE_MAPPING, ["genre_id", "genre_name"])
        dim_genre.coalesce(1).write.mode("overwrite").parquet(f"{args.warehouse_path}/dim_genre")
        print(f" {dim_genre.count()} genres saved")


        print("\n  (2) Creating dim_date ")
        movies_df = spark.read.parquet(f"{args.processed_path}/movies_clean")

        dim_date = movies_df.select(
            to_date(col("release_date")).alias("date_id"),
            year(col("release_date")).cast("string").alias("year"),
            month(col("release_date")).cast("string").alias("month"),
            dayofmonth(col("release_date")).cast("string").alias("day")
        ).filter(col("date_id").isNotNull()).distinct()

        dim_date.coalesce(1).write.mode("overwrite").parquet(f"{args.warehouse_path}/dim_date")
        print(f" {dim_date.count()} dates saved")



        print("\n (3) Creating dim_movie ")
        dim_movie = movies_df.select(
            col("id").alias("movie_id"),
            col("title"),
            col("original_title"),
            col("original_language"),
            concat(lit(TMDB_IMAGE_BASE), col("poster_path")).alias("poster_url"),
            col("poster_path"),
            col("backdrop_path"),
            to_date(col("release_date")).alias("release_date"),
            col("adult"),
            col("video")
        )
        dim_movie.coalesce(1).write.mode("overwrite").parquet(f"{args.warehouse_path}/dim_movie")
        print(f" {dim_movie.count()} movies saved")



        print("\n (4) Creating fact_movie_metrics ")
        genres_df = spark.read.parquet(f"{args.processed_path}/genres_clean")

        fact = movies_df.alias("m").join(
            genres_df.alias("g"),
            col("m.id") == col("g.movie_id"),
            "left"
        ).select(
            col("m.id").alias("movie_id"),
            col("g.genre_id"),
            to_date(col("m.release_date")).alias("date_id"),
            col("m.popularity").cast(DoubleType()).alias("popularity"),
            col("m.vote_average").cast(DoubleType()).alias("vote_average"),
            col("m.vote_count").cast(IntegerType()).alias("vote_count")
        )
        fact.coalesce(1).write.mode("overwrite").parquet(f"{args.warehouse_path}/fact_movie_metrics")
        print(f"{fact.count()} metric records saved")

        print(" ⭐ Star Schema DONE ")

    except Exception as e:
        print(f"\n Error: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
