#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

kubectl apply -f "${SCRIPT_DIR}/spark-rbac.yaml"

helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
helm repo update

helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set controller.serviceAccount.create=false \
  --set controller.serviceAccount.name=spark-operator-controller \
  --set controller.rbac.create=false \
  --set spark.serviceAccount.create=false \
  --set spark.serviceAccount.name=spark-driver-sa \
  --set spark.rbac.create=false \
  --set "spark.jobNamespaces={spark-jobs}" \
  --set webhook.enable=true \
  --set prometheus.metrics.enable=true \
  --set controller.tolerations[0].key=eks.amazonaws.com/compute-type \
  --set controller.tolerations[0].operator=Equal \
  --set controller.tolerations[0].value=fargate \
  --set controller.tolerations[0].effect=NoSchedule \
  --set webhook.tolerations[0].key=eks.amazonaws.com/compute-type \
  --set webhook.tolerations[0].operator=Equal \
  --set webhook.tolerations[0].value=fargate \
  --set webhook.tolerations[0].effect=NoSchedule

kubectl get pods -n spark-operator
