#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  source "${ROOT_DIR}/.env"
  set +a
fi

CLUSTER_NAME="${CLUSTER_NAME:-prefect-spark-demo}"
PREFECT_WORK_POOL_NAME="${PREFECT_WORK_POOL_NAME:-kubernetes-pool}"
PREFECT_SERVER_API_URL="${PREFECT_SERVER_API_URL:-http://127.0.0.1:4200/api}"

kubectl apply -f "${SCRIPT_DIR}/prefect-rbac.yaml"

helm repo add prefect https://prefecthq.github.io/prefect-helm --force-update
helm repo update

helm upgrade --install prefect-worker prefect/prefect-worker \
  --namespace prefect \
  --set serviceAccount.create=false \
  --set serviceAccount.name=prefect-worker \
  --set worker.config.workPool="${PREFECT_WORK_POOL_NAME}" \
  --set worker.apiConfig=selfHostedServer \
  --set worker.selfHostedServerApiConfig.apiUrl="${PREFECT_SERVER_API_URL}" \
  --set worker.clusterUid="${CLUSTER_NAME}"
