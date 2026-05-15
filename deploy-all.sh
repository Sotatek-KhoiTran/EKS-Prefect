#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

export CLUSTER_NAME="${CLUSTER_NAME:-prefect-spark-demo}"
export REGION="${REGION:-ap-southeast-1}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"

export DATA_BUCKET="${DATA_BUCKET:-prefect-demo-data}"
export PREFECT_FLOW_BUCKET="${PREFECT_FLOW_BUCKET:-prefect-demo-flow}"
export REPOSITORY_NAME="${REPOSITORY_NAME:-prefect-spark-runtime}"
export IMAGE_TAG="${IMAGE_TAG:-latest}"
export PREFECT_IMAGE_NAME="${PREFECT_IMAGE_NAME:-${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}}"
export PREFECT_IMAGE_TAG="${PREFECT_IMAGE_TAG:-${IMAGE_TAG}}"
export SPARK_IMAGE="${SPARK_IMAGE:-docker.io/library/spark:3.5.1-python3}"
export SPARK_JOB_KEY="${SPARK_JOB_KEY:-jobs/etl_job.py}"
export SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-false}"

export PREFECT_WORK_POOL_NAME="${PREFECT_WORK_POOL_NAME:-kubernetes-pool}"
export PREFECT_WORK_QUEUE_NAME="${PREFECT_WORK_QUEUE_NAME:-default}"
export PREFECT_WORK_POOL_TYPE="${PREFECT_WORK_POOL_TYPE:-kubernetes}"
export PREFECT_SERVER_API_URL="${PREFECT_SERVER_API_URL:-http://127.0.0.1:4200/api}"

required_commands=(
  aws
  eksctl
  helm
  kubectl
  prefect
)

if [[ "${SKIP_IMAGE_BUILD}" != "true" ]]; then
  required_commands+=(docker)
fi

for command_name in "${required_commands[@]}"; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${command_name}" >&2
    exit 1
  fi
done

required_vars=(
  AWS_ACCOUNT_ID
  DATA_BUCKET
  PREFECT_FLOW_BUCKET
)

for var_name in "${required_vars[@]}"; do
  var_value="${!var_name}"
  if [[ -z "${var_value}" || "${var_value}" == \<* ]]; then
    echo "Set ${var_name} before running this script." >&2
    exit 1
  fi
done

echo "Using cluster: ${CLUSTER_NAME}"
echo "Using region: ${REGION}"
echo "Using data bucket: s3://${DATA_BUCKET}"
echo "Using Prefect flow bucket: s3://${PREFECT_FLOW_BUCKET}"
echo "Using Prefect runtime image: ${PREFECT_IMAGE_NAME}:${PREFECT_IMAGE_TAG}"
echo "Using Spark image: ${SPARK_IMAGE}"
echo "Using self-hosted Prefect API URL in cluster: ${PREFECT_SERVER_API_URL}"
echo "Using Prefect work pool: ${PREFECT_WORK_POOL_NAME}"
echo "Using Prefect work pool type: ${PREFECT_WORK_POOL_TYPE}"
echo "Skip image build: ${SKIP_IMAGE_BUILD}"

aws eks update-kubeconfig \
  --name "${CLUSTER_NAME}" \
  --region "${REGION}"

bash "${ROOT_DIR}/EKS/fargate-profiles-create.sh"

bash "${ROOT_DIR}/prefect/prefect-server-create.sh"
bash "${ROOT_DIR}/spark/spark-operator-create.sh"
bash "${ROOT_DIR}/prefect/prefect-worker-create.sh"

bash "${ROOT_DIR}/prefect/prefect-irsa.sh"
bash "${ROOT_DIR}/spark/spark-pod-irsa.sh"

bash "${ROOT_DIR}/scripts/upload-spark-job-to-s3.sh"

if [[ "${SKIP_IMAGE_BUILD}" == "true" ]]; then
  echo "Skipping Docker image build/push. Using existing image ${PREFECT_IMAGE_NAME}:${PREFECT_IMAGE_TAG}"
else
  bash "${ROOT_DIR}/scripts/ecr-push-prefect-runtime.sh"
fi

bash "${ROOT_DIR}/scripts/prefect-selfhosted-deploy.sh"

echo "Deployment completed."
echo "Check Prefect worker pods: kubectl get pods -n prefect"
echo "Check Spark operator pods: kubectl get pods -n spark-operator"
