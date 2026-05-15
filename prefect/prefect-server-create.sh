#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

kubectl create namespace prefect \
  --dry-run=client -o yaml | kubectl apply -f -

helm repo add prefect https://prefecthq.github.io/prefect-helm --force-update
helm repo update

helm upgrade --install prefect-server prefect/prefect-server \
  --namespace prefect \
  --create-namespace \
  --set server.uiConfig.prefectUiApiUrl=http://localhost:4200/api \
  --set postgresql.primary.persistence.enabled=false \
  --set sqlite.enabled=false \
  --set service.type=ClusterIP

kubectl rollout status deployment/prefect-server -n prefect --timeout=300s
kubectl get pods -n prefect
