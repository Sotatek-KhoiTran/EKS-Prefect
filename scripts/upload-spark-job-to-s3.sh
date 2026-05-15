#!/bin/bash
set -euo pipefail

DATA_BUCKET="${DATA_BUCKET:-<YOUR_BUCKET_NAME>}"
SPARK_JOB_KEY="${SPARK_JOB_KEY:-jobs/etl_job.py}"

aws s3 cp jobs/etl_job.py "s3://${DATA_BUCKET}/${SPARK_JOB_KEY}"

echo "Uploaded jobs/etl_job.py to s3://${DATA_BUCKET}/${SPARK_JOB_KEY}"
