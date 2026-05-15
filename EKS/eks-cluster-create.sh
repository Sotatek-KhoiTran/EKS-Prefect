#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-prefect-spark-demo}"
REGION="${REGION:-ap-southeast-1}"
EKS_VERSION="${EKS_VERSION:-1.30}"

eksctl create cluster \
  --name "${CLUSTER_NAME}" \
  --region "${REGION}" \
  --fargate \
  --version "${EKS_VERSION}"

aws eks update-kubeconfig \
  --name "${CLUSTER_NAME}" \
  --region "${REGION}"

bash "${SCRIPT_DIR}/fargate-profiles-create.sh"

kubectl get nodes
