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
PREFECT_FLOW_BUCKET="${PREFECT_FLOW_BUCKET:-<YOUR_BUCKET_NAME>}"

PREFECT_FLOW_POLICY_NAME="${PREFECT_FLOW_POLICY_NAME:-PrefectFlowRunS3ReadPolicy}"
PREFECT_FLOW_POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${PREFECT_FLOW_POLICY_NAME}"
NAMESPACE="prefect"
SERVICE_ACCOUNT_NAME="prefect-flow-run"
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
POLICY_FILE="${ROOT_DIR}/.generated/prefect-flow-run-s3-read-policy.json"

cat > "${POLICY_FILE}" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListPrefectFlowPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${PREFECT_FLOW_BUCKET}",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["prefect/flows/*"]
        }
      }
    },
    {
      "Sid": "ReadPrefectFlowObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::${PREFECT_FLOW_BUCKET}/prefect/flows/*"
    }
  ]
}
EOF

if aws iam get-policy --policy-arn "${PREFECT_FLOW_POLICY_ARN}" >/dev/null 2>&1; then
  echo "IAM policy already exists: ${PREFECT_FLOW_POLICY_ARN}"
else
  aws iam create-policy \
    --policy-name "${PREFECT_FLOW_POLICY_NAME}" \
    --policy-document "$(cat "${POLICY_FILE}")"
fi

delete_stale_irsa_stack_if_needed

eksctl create iamserviceaccount \
  --name "${SERVICE_ACCOUNT_NAME}" \
  --namespace "${NAMESPACE}" \
  --cluster "${CLUSTER_NAME}" \
  --region "${REGION}" \
  --attach-policy-arn "${PREFECT_FLOW_POLICY_ARN}" \
  --override-existing-serviceaccounts \
  --approve

ROLE_ARN="$(kubectl get sa "${SERVICE_ACCOUNT_NAME}" -n "${NAMESPACE}" -o "jsonpath={.metadata.annotations.eks\.amazonaws\.com/role-arn}" 2>/dev/null || true)"

if [[ -z "${ROLE_ARN}" ]]; then
  echo "IRSA annotation was not created on ${NAMESPACE}/${SERVICE_ACCOUNT_NAME}" >&2
  exit 1
fi

echo "IRSA role annotation: ${ROLE_ARN}"

echo "Prefect Worker uses Kubernetes RBAC only for this flow."
echo "Configure the Prefect Kubernetes work pool job template to use serviceAccountName: prefect-flow-run."
echo "ECR image pull permission belongs to the node role or EKS Fargate pod execution role, not this pod IRSA role."
