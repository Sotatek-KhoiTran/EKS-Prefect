import argparse
from typing import Any, Dict
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-data-base")
    parser.add_argument("--source-table")
    parser.add_argument("--dq-checks-rules")
    args, _ = parser.parse_known_args()
    return args

def load_rules(path: str, spark: SparkSession) -> Dict[str, Any]:
    if path.startswith(("s3://", "s3a://", "hdfs://")):
        content = "\n".join(row.value for row in spark.read.text(path).collect())
        rules = yaml.safe_load(content)
    else:
        with open(path, "r", encoding="utf-8") as f:
            rules = yaml.safe_load(f)
    
    if 'checks for listings' not in rules:
        raise KeyError("Missing 'checks for listings' section in DQ rules")
    
    flatten_rules = {}
    for rule in rules['checks for listings']:
        flatten_rules.update(rule)
    return flatten_rules

def run_dq_checks(df, rules: Dict[str, Any], table_name: str) -> None:
    missing_checks_cols = []
    duplicate_checks_cols = []
    invalid_value_checks_cols = []
    invalid_rule_name_mapping = {}
    
    for rule_key in rules.keys():
        if '(' not in rule_key or ')' not in rule_key:
            continue

        col = rule_key.split('(')[1].split(')')[0]

        if rule_key.startswith('missing_count'):
            missing_checks_cols.append(col)

        elif rule_key.startswith('duplicate_count'):
            duplicate_checks_cols.append(col)

        elif rule_key.startswith('invalid_count'):
            invalid_value_checks_cols.append(col)
            rule_info = rules.get(rule_key, {})
            invalid_rule_name_mapping[col] = rule_info.get('name', 'N/A')
    
    errors = []
    # Completeness checks
    if missing_checks_cols:
        df_null = df.select([
            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(col)
            for col in missing_checks_cols
        ])
        null_counts = df_null.collect()[0].asDict()
        for col, null_count in null_counts.items():
            if null_count > 0:
                errors.append(f"Completeness check failed for column '{col}': {null_count} missing values")
            else:
                logger.info(f"Completeness check passed for column '{col}'")
    
    # Uniqueness checks
    total_records = df.count()
    if duplicate_checks_cols:
        df_duplicate = df.select([
            (
                F.lit(total_records)
                - F.countDistinct(F.col(col))
            ).alias(col)
            for col in duplicate_checks_cols
        ])
        duplicate_count = df_duplicate.collect()[0].asDict()
        for col, dup_count in duplicate_count.items():
            if dup_count > 0:
                errors.append(f"Uniqueness check failed for column '{col}': {dup_count} duplicate values")
            else:
                logger.info(f"Uniqueness check passed for column '{col}'")
    
    # Validity checks
    if invalid_value_checks_cols:
        invalid_exprs = []
        
        for col in invalid_value_checks_cols:
            rule_key = f'invalid_count({col}) = 0'
            rule_info = rules.get(rule_key, {})
            
            invalid_conditions = []
            
            if 'valid values' in rule_info:
                valid_list = rule_info['valid values']
                invalid_conditions.append(~F.col(col).isin(valid_list))
                
            if 'valid min' in rule_info:
                valid_min = rule_info['valid min']
                invalid_conditions.append(F.col(col) < valid_min)
                
            if 'valid max' in rule_info:
                valid_max = rule_info['valid max']
                invalid_conditions.append(F.col(col) > valid_max)
                
            if invalid_conditions:
                combined_invalid_cond = invalid_conditions[0]
                for cond in invalid_conditions[1:]:
                    combined_invalid_cond = combined_invalid_cond | cond
                    
                final_cond = F.col(col).isNotNull() & combined_invalid_cond
                
                expr = F.sum(F.when(final_cond, 1).otherwise(0)).alias(col)
                invalid_exprs.append(expr)

        if invalid_exprs:
            df_invalid = df.select(invalid_exprs)
            invalid_counts = df_invalid.collect()[0].asDict()
            
            for col, inv_count in invalid_counts.items():
                if inv_count > 0:
                   errors.append(
                        f"Validity check failed for column '{col}': {inv_count} invalid values. "
                        f"Rule: '{invalid_rule_name_mapping.get(col, 'N/A')}'"
                    )
                else:
                    logger.info(f"Validity check passed for column '{col}'")
    
    # Custom checks
    failed_rows_rules = rules.get('failed rows')
    if failed_rows_rules:
        for rule in failed_rows_rules:
            fail_condition_str = rule.get('fail condition')
            rule_name = rule.get('name', 'Custom failed rows check')
            
            if fail_condition_str:
                failed_count = df.filter(F.expr(fail_condition_str)).count()
                
                if failed_count > 0:
                    errors.append(f"Failed rows check '{rule_name}': Found {failed_count} rows matching '{fail_condition_str}'")
                else:
                    logger.info(f"Custom check passed for '{rule_name}'")
                    
    # Schema checks
    schema_rule = rules.get('schema')
    if schema_rule:
        actual_cols = df.columns
        actual_dtypes = dict(df.dtypes)
        
        required_cols = schema_rule.get('when required column missing', [])
        if required_cols:
            for col in required_cols:
                if col not in actual_cols:
                    errors.append(f"Schema check failed: Required column '{col}' is missing")
                else:
                    logger.info(f"Schema check passed for required column '{col}'")
        
        expected_dtypes = schema_rule.get('when wrong column type', {}) 
        if expected_dtypes:
            type_mapping = {
                'integer': ['int', 'bigint', 'smallint', 'tinyint'],
                'float': ['float', 'double', 'decimal'],
                'string': ['string'],
                'boolean': ['boolean']
            }

            for col, expected_type in expected_dtypes.items():
                if col in actual_dtypes:
                    actual_type = actual_dtypes[col]
                    valid_types = type_mapping.get(expected_type, [])
                    
                    if not any(actual_type.startswith(valid) for valid in valid_types):
                        errors.append(f"Schema check failed: Column '{col}' has type '{actual_type}', expected '{expected_type}'")
                    else:
                        logger.info(f"Schema check passed for column '{col}' with type '{actual_type}'")
                else:
                    errors.append(f"Schema check failed: Column '{col}' is missing for type validation")
                    
    if errors:
        error_msg = "\n".join(f"- {err}" for err in errors)

        logger.error(
            f"Data Quality Checks Failed for table "
            f"'{table_name}':\n{error_msg}"
        )
        raise ValueError(f"Found {len(errors)} validation errors. See logs for details.")
    else:
        logger.info(f"All Data Quality Checks Passed for table '{table_name}'.")
        
            
def main() -> None:
    args = parse_args()
    db = args.source_data_base
    table = args.source_table   
    catalog_name = f"{db}_catalog"
    dq_rules_path = args.dq_checks_rules
    
    spark = (
        SparkSession.builder.appName("prefect-spark-s3-demo-etl")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"s3://prefect-demo-data/{db}")
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    
    try:   
        full_table_name = f"{catalog_name}.{db}.{table}"
        df = spark.read.table(full_table_name)
        dq_rules = load_rules(dq_rules_path, spark)
        
        run_dq_checks(df, dq_rules, full_table_name)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()  
