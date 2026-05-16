#!/bin/bash
set -euo pipefail

DATA_BUCKET="${DATA_BUCKET:-<YOUR_BUCKET_NAME>}"
SPARK_JOBS_PREFIX="${SPARK_JOBS_PREFIX:-jobs}"
RAW_DATA_PREFIX="${RAW_DATA_PREFIX:-raw}"

aws s3 cp jobs/ "s3://${DATA_BUCKET}/${SPARK_JOBS_PREFIX}/" --recursive --exclude "*" --include "*.py"

echo "Uploaded jobs/*.py to s3://${DATA_BUCKET}/${SPARK_JOBS_PREFIX}/"
