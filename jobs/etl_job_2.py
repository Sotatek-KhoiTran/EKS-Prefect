import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, upper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("prefect-spark-s3-demo-etl")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    raw_df = (
        spark.read.option('multiline', 'true').json(args.input)
    )

    transformed_df = (
        raw_df.withColumn("source_file", input_file_name())
        .withColumn("processed_at", current_timestamp())
    )

    transformed_df.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
