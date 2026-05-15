#!/bin/bash
set -euo pipefail

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
REGION="${REGION:-ap-southeast-1}"
REPOSITORY_NAME="${REPOSITORY_NAME:-prefect-spark-runtime}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-false}"

if [[ "${SKIP_IMAGE_BUILD}" == "true" ]]; then
  echo "Skipping Docker build/push because SKIP_IMAGE_BUILD=true"
  echo "${IMAGE_URI}"
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable." >&2
  echo "Start Docker Desktop, then rerun this script." >&2
  echo "If the image already exists in ECR, rerun with SKIP_IMAGE_BUILD=true." >&2
  exit 1
fi

aws ecr describe-repositories \
  --repository-names "${REPOSITORY_NAME}" \
  --region "${REGION}" \
  >/dev/null 2>&1 || aws ecr create-repository \
    --repository-name "${REPOSITORY_NAME}" \
    --region "${REGION}" \
    >/dev/null

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

docker build -t "${IMAGE_URI}" .
docker push "${IMAGE_URI}"

echo "${IMAGE_URI}"
