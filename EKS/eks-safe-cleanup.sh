#!/bin/bash

echo "Deleting EKS cluster to stop billing..."
eksctl delete cluster --name prefect-spark-demo --region ap-southeast-1

echo "Done. EKS cluster deleted — no more $0.10/hr charges."
echo "S3 data is preserved (always-free tier up to 5GB)"