#!/bin/bash
set -euo pipefail

AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
REGION="${REGION:-ap-southeast-1}"
REPOSITORY_NAME="${REPOSITORY_NAME:-prefect-spark-runtime}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-false}"

PREFECT_REPO="${PREFECT_REPO:-prefect-runtime}"
SPARK_REPO="${SPARK_REPO:-spark-runtime}"

PREFECT_IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${PREFECT_REPO}:${IMAGE_TAG}"
SPARK_IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${SPARK_REPO}:${IMAGE_TAG}"

if [[ "${SKIP_IMAGE_BUILD}" == "true" ]]; then
  echo "Skipping Docker build/push because SKIP_IMAGE_BUILD=true"
  echo "Prefect Image URI: ${PREFECT_IMAGE_URI}"
  echo "Spark Image URI  : ${SPARK_IMAGE_URI}"
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable." >&2
  echo "Start Docker Desktop, then rerun this script." >&2
  echo "If the image already exists in ECR, rerun with SKIP_IMAGE_BUILD=true." >&2
  exit 1
fi

echo "Ensuring ECR repositories exist..."
for REPO in "${PREFECT_REPO}" "${SPARK_REPO}"; do
  if ! aws ecr describe-repositories \
    --repository-names "${REPO}" \
    --region "${REGION}" \
    >/dev/null 2>&1; then
    echo "ECR repository '${REPO}' does not exist." >&2
    echo "Create it with Terraform first: cd terraform && terraform apply" >&2
    exit 1
  fi
  echo " - Repository '${REPO}' is ready."
done

echo "Logging into AWS ECR..."
aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

echo "----------------------------------------"
echo "Building Prefect image..."
docker build -f Dockerfile.prefect -t "${PREFECT_IMAGE_URI}" .
echo "Pushing Prefect image..."
docker push "${PREFECT_IMAGE_URI}"

echo "----------------------------------------"
echo "Building Spark image..."
docker build -f Dockerfile.spark -t "${SPARK_IMAGE_URI}" .
echo "Pushing Spark image..."
docker push "${SPARK_IMAGE_URI}"

echo "----------------------------------------"
echo "Successfully built and pushed both images!"
echo "Prefect Image URI: ${PREFECT_IMAGE_URI}"
echo "Spark Image URI  : ${SPARK_IMAGE_URI}"
