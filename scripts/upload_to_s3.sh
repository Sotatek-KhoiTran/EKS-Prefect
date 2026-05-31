#!/bin/bash
set -euo pipefail

SCRIPT_BUCKET="${SCRIPT_BUCKET:-prefect-demo-scripts}"
SPARK_JOBS_PREFIX="${SPARK_JOBS_PREFIX:-spark/jobs}"
SPARK_CONFIGS_PREFIX="${SPARK_CONFIGS_PREFIX:-spark/configs}"
PREFECT_FLOWS_PREFIX="${PREFECT_FLOWS_PREFIX:-prefect}"

aws s3 cp src/jobs/ "s3://${SCRIPT_BUCKET}/${SPARK_JOBS_PREFIX}/" --recursive --exclude "*" --include "*.py"
aws s3 cp configs/ "s3://${SCRIPT_BUCKET}/${SPARK_CONFIGS_PREFIX}/" --recursive --exclude "*" --include "*.yaml"
aws s3 cp pipelines "s3://${SCRIPT_BUCKET}/pipelines/" --recursive --exclude "*" --include "*.py"

