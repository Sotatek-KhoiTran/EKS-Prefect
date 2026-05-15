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
REGION="${REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
DATA_BUCKET="${DATA_BUCKET:-<YOUR_BUCKET_NAME>}"

SPARK_DATA_POLICY_NAME="${SPARK_DATA_POLICY_NAME:-SparkJobS3ReadWritePolicy}"
SPARK_DATA_POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${SPARK_DATA_POLICY_NAME}"
NAMESPACE="spark-jobs"
SERVICE_ACCOUNT_NAME="spark-driver-sa"
IRSA_STACK_NAME="eksctl-${CLUSTER_NAME}-addon-iamserviceaccount-${NAMESPACE}-${SERVICE_ACCOUNT_NAME}"

delete_stale_irsa_stack_if_needed() {
  local annotation
  annotation="$(kubectl get sa "${SERVICE_ACCOUNT_NAME}" -n "${NAMESPACE}" -o "jsonpath={.metadata.annotations.eks\.amazonaws\.com/role-arn}" 2>/dev/null || true)"

  if [[ -n "${annotation}" ]]; then
    echo "ServiceAccount ${NAMESPACE}/${SERVICE_ACCOUNT_NAME} already has IRSA role: ${annotation}"
    return
  fi

  local stack_status
  stack_status="$(aws cloudformation describe-stacks \
    --stack-name "${IRSA_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || true)"

  if [[ -n "${stack_status}" && "${stack_status}" != "None" ]]; then
    echo "Deleting stale IRSA CloudFormation stack ${IRSA_STACK_NAME} (${stack_status})"
    aws cloudformation update-termination-protection \
      --stack-name "${IRSA_STACK_NAME}" \
      --no-enable-termination-protection \
      --region "${REGION}" \
      >/dev/null 2>&1 || true
    aws cloudformation delete-stack \
      --stack-name "${IRSA_STACK_NAME}" \
      --region "${REGION}"
    aws cloudformation wait stack-delete-complete \
      --stack-name "${IRSA_STACK_NAME}" \
      --region "${REGION}"
  fi
}

eksctl utils associate-iam-oidc-provider \
  --cluster "${CLUSTER_NAME}" \
  --region "${REGION}" \
  --approve

kubectl create namespace "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

mkdir -p "${ROOT_DIR}/.generated"
POLICY_FILE="${ROOT_DIR}/.generated/spark-job-s3-read-write-policy.json"

cat > "${POLICY_FILE}" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListRawAndProcessedPrefixes",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${DATA_BUCKET}"
    },
    {
      "Sid": "ReadWriteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::prefect-demo-data/*"
    }
  ]
}
EOF

if aws iam get-policy --policy-arn "${SPARK_DATA_POLICY_ARN}" >/dev/null 2>&1; then
  echo "IAM policy already exists: ${SPARK_DATA_POLICY_ARN}"
else
  aws iam create-policy \
    --policy-name "${SPARK_DATA_POLICY_NAME}" \
    --policy-document "$(cat "${POLICY_FILE}")"
fi

delete_stale_irsa_stack_if_needed

eksctl create iamserviceaccount \
  --name "${SERVICE_ACCOUNT_NAME}" \
  --namespace "${NAMESPACE}" \
  --cluster "${CLUSTER_NAME}" \
  --region "${REGION}" \
  --attach-policy-arn "${SPARK_DATA_POLICY_ARN}" \
  --override-existing-serviceaccounts \
  --approve

ROLE_ARN="$(kubectl get sa "${SERVICE_ACCOUNT_NAME}" -n "${NAMESPACE}" -o "jsonpath={.metadata.annotations.eks\.amazonaws\.com/role-arn}" 2>/dev/null || true)"

if [[ -z "${ROLE_ARN}" ]]; then
  echo "IRSA annotation was not created on ${NAMESPACE}/${SERVICE_ACCOUNT_NAME}" >&2
  exit 1
fi

echo "IRSA role annotation: ${ROLE_ARN}"

echo "Use spec.driver.serviceAccount: spark-driver-sa in every SparkApplication."
