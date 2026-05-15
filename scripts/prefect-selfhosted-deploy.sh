#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

PREFECT_WORK_POOL_NAME="${PREFECT_WORK_POOL_NAME:-kubernetes-pool}"
PREFECT_WORK_QUEUE_NAME="${PREFECT_WORK_QUEUE_NAME:-default}"
PREFECT_WORK_POOL_TYPE="${PREFECT_WORK_POOL_TYPE:-kubernetes}"
PREFECT_LOCAL_API_URL="${PREFECT_LOCAL_API_URL:-http://127.0.0.1:4200/api}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
REGION="${REGION:-ap-southeast-1}"
REPOSITORY_NAME="${REPOSITORY_NAME:-prefect-spark-runtime}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
PREFECT_IMAGE_NAME="${PREFECT_IMAGE_NAME:-${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}}"
PREFECT_IMAGE_TAG="${PREFECT_IMAGE_TAG:-${IMAGE_TAG}}"
DATA_BUCKET="${DATA_BUCKET:-prefect-demo-data}"
SPARK_IMAGE="${SPARK_IMAGE:-docker.io/library/spark:3.5.1-python3}"

if [[ "${PREFECT_WORK_POOL_TYPE}" == "kubernetes" ]] && ! python -c "import prefect_kubernetes" >/dev/null 2>&1; then
  echo "Missing local Python package: prefect-kubernetes" >&2
  echo "Install it with: pip install prefect-kubernetes" >&2
  exit 1
fi

kubectl rollout status deployment/prefect-server -n prefect --timeout=300s

if ! curl -fsS "${PREFECT_LOCAL_API_URL}/health" >/dev/null 2>&1; then
  echo "Starting kubectl port-forward for Prefect Server on localhost:4200"
  kubectl port-forward -n prefect svc/prefect-server 4200:4200 >/tmp/prefect-server-port-forward.log 2>&1 &
  PORT_FORWARD_PID="$!"
  trap 'kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true' EXIT

  for _ in {1..30}; do
    if curl -fsS "${PREFECT_LOCAL_API_URL}/health" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
fi

if ! curl -fsS "${PREFECT_LOCAL_API_URL}/health" >/dev/null 2>&1; then
  echo "Prefect Server is not reachable at ${PREFECT_LOCAL_API_URL}" >&2
  echo "Port-forward log:" >&2
  cat /tmp/prefect-server-port-forward.log >&2 || true
  exit 1
fi

export PREFECT_API_URL="${PREFECT_LOCAL_API_URL}"
unset PREFECT_API_KEY
export PREFECT_WORK_POOL_NAME
export PREFECT_WORK_QUEUE_NAME
export PREFECT_IMAGE_NAME
export PREFECT_IMAGE_TAG
export DATA_BUCKET
export SPARK_IMAGE

echo "Using self-hosted Prefect API URL: ${PREFECT_API_URL}"
echo "Using Prefect work pool: ${PREFECT_WORK_POOL_NAME}"
echo "Using Prefect work queue: ${PREFECT_WORK_QUEUE_NAME}"
echo "Using Prefect runtime image: ${PREFECT_IMAGE_NAME}:${PREFECT_IMAGE_TAG}"

if prefect work-pool inspect "${PREFECT_WORK_POOL_NAME}" >/dev/null 2>&1; then
  echo "Prefect work pool already exists: ${PREFECT_WORK_POOL_NAME}"
else
  prefect work-pool create "${PREFECT_WORK_POOL_NAME}" \
    --type "${PREFECT_WORK_POOL_TYPE}" \
    --set-as-default
fi

cd "${ROOT_DIR}"
prefect deploy --all
