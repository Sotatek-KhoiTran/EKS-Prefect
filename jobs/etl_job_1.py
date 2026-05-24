import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, upper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-input-path")
    parser.add_argument("--target_data_base")
    parser.add_argument("--target_table")
    args, _ = parser.parse_known_args()
    return args


def main() -> None:
    args = parse_args()
    db = args.target_data_base
    table = args.target_table
    catalog_name = f"{db}_catalog"
    
    spark = (
        SparkSession.builder.appName("prefect-spark-s3-demo-etl")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"s3://prefect-demo-data/{db}")
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(args.source_input_path)
    )

    transformed_df = (
        raw_df.withColumn("source_file", input_file_name())
        .withColumn("processed_at", current_timestamp())
    )

    full_target_table = f"{catalog_name}.{db}.{table}"
    
    if "category" in transformed_df.columns:
        transformed_df = transformed_df.withColumn("category_normalized", upper(col("category")))

    transformed_df.writeTo(full_target_table).createOrReplace()
    
    spark.sql(f"""
    CALL {catalog_name}.system.expire_snapshots(
        table => '{db}.{table}',
        retain_last => 1
    )
    """)
    
    spark.stop()


if __name__ == "__main__":
    main()
