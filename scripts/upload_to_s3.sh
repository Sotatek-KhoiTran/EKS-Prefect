#!/bin/bash
set -euo pipefail

SCRIPT_BUCKET="${SCRIPT_BUCKET:-prefect-demo-scripts}"
SPARK_JOBS_PREFIX="${SPARK_JOBS_PREFIX:-spark/jobs}"
SPARK_CONFIGS_PREFIX="${SPARK_CONFIGS_PREFIX:-spark/configs}"
PREFECT_FLOWS_PREFIX="${PREFECT_FLOWS_PREFIX:-prefect/flows}"

aws s3 cp jobs/ "s3://${SCRIPT_BUCKET}/${SPARK_JOBS_PREFIX}/" --recursive --exclude "*" --include "*.py"
aws s3 cp jobs/configs/ "s3://${SCRIPT_BUCKET}/${SPARK_CONFIGS_PREFIX}/" --recursive --exclude "*" --include "*.yaml"
aws s3 cp flows/ "s3://${SCRIPT_BUCKET}/${PREFECT_FLOWS_PREFIX}/" --recursive --exclude "*" --include "*.py"

echo "Uploaded jobs/*.py to s3://${SCRIPT_BUCKET}/${SPARK_JOBS_PREFIX}/"
echo "Uploaded jobs/configs/*.yaml to s3://${SCRIPT_BUCKET}/${SPARK_CONFIGS_PREFIX}/"
echo "Uploaded flows/*.py to s3://${SCRIPT_BUCKET}/${PREFECT_FLOWS_PREFIX}/"
