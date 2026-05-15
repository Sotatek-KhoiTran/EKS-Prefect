#!/bin/bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-prefect-spark-demo}"
REGION="${REGION:-ap-southeast-1}"

create_fargate_profile() {
  local profile_name="$1"
  local namespace="$2"

  if aws eks describe-fargate-profile \
    --cluster-name "${CLUSTER_NAME}" \
    --fargate-profile-name "${profile_name}" \
    --region "${REGION}" \
    >/dev/null 2>&1; then
    echo "Fargate profile already exists: ${profile_name}"
    return
  fi

  eksctl create fargateprofile \
    --cluster "${CLUSTER_NAME}" \
    --region "${REGION}" \
    --name "${profile_name}" \
    --namespace "${namespace}"
}

create_fargate_profile fp-prefect prefect
create_fargate_profile fp-spark-operator spark-operator
create_fargate_profile fp-spark-jobs spark-jobs

eksctl get fargateprofile \
  --cluster "${CLUSTER_NAME}" \
  --region "${REGION}"
