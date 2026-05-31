# Terraform migration for Prefect + Spark on EKS

This folder replaces the current `eksctl`, IAM, S3/ECR, Kubernetes RBAC, and Helm provisioning scripts with Terraform-managed resources.

Terraform manages:

- VPC, public/private subnets, NAT gateway, and routes
- EKS cluster with Fargate profiles for `kube-system` CoreDNS, `prefect`, `spark-operator`, and `spark-jobs`
- S3 buckets for Spark data and Prefect flow storage
- ECR repository for the Prefect runtime image
- IRSA roles and policies for `prefect-flow-run` and `spark-driver-sa`
- Kubernetes namespaces, service accounts, roles, and role bindings
- Helm releases for Prefect Server, Prefect Worker, and Spark Operator
- Upload of `jobs/*.py` to `s3://<data_bucket>/jobs/`

Terraform does not build/push the Docker image or run `prefect deploy --all`. Keep those as explicit post-apply steps because they depend on your local Docker daemon and the Prefect API.

## Usage

For a clean migration, delete the old demo cluster first or change `cluster_name`/bucket names in `terraform.tfvars`. Terraform cannot create resources that already exist under the same names unless you import them into state.

Configure AWS credentials before running Terraform:

```bash
aws configure
aws sts get-caller-identity
```

The AWS identity must have permission to manage VPC, EKS, IAM, S3, ECR, CloudWatch logs, and EKS add-ons.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

If S3 buckets, the ECR repository, or IAM policies already exist from the old scripts, import them before running `terraform apply` again:

```bash
terraform import aws_s3_bucket.data prefect-demo-data
terraform import aws_s3_bucket.prefect_flow prefect-demo-flow
terraform import aws_ecr_repository.prefect_runtime prefect-spark-runtime
terraform import aws_iam_policy.prefect_flow_s3 arn:aws:iam::<AWS_ACCOUNT_ID>:policy/PrefectFlowRunS3ReadPolicy
terraform import aws_iam_policy.spark_data_s3 arn:aws:iam::<AWS_ACCOUNT_ID>:policy/SparkJobS3ReadWritePolicy
terraform apply
```

Then configure kubectl:

```bash
aws eks update-kubeconfig --name prefect-spark-demo --region ap-southeast-1
```

Build and push the runtime image:

```bash
cd ../..
bash scripts/ecr-push-prefect-runtime.sh
```

Deploy Prefect flows:

```bash
bash scripts/prefect-selfhosted-deploy.sh
```

The default Spark image is `docker.io/library/spark:3.5.1-python3`. If you override it, use an image tag that exists and includes Python support for PySpark jobs:

```bash
SPARK_IMAGE=docker.io/library/spark:3.5.1-python3 bash scripts/prefect-selfhosted-deploy.sh
```

Prefect UI port forwarding:

```bash
kubectl port-forward -n prefect svc/prefect-server 4200:4200
```

If a flow run fails with `User "system:anonymous" cannot create resource "sparkapplications"`,
verify the flow-run service account and RBAC:

```bash
kubectl auth can-i create sparkapplications.sparkoperator.k8s.io \
  -n spark-jobs \
  --as system:serviceaccount:prefect:prefect-flow-run

kubectl get job -n prefect -l prefect.io/flow-run-id -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.template.spec.serviceAccountName}{"\n"}{end}'
```

The permission check must return `yes`, and Prefect flow-run jobs must use
`prefect-flow-run` as their `serviceAccountName`. If either value is wrong, rerun
`terraform apply` and then `bash scripts/prefect-selfhosted-deploy.sh`.

Destroy the stack when finished:

```bash
cd terraform
terraform destroy
```

If destroy fails because ECR still contains images, keep `force_delete = true` on `aws_ecr_repository.prefect_runtime` and rerun:

```bash
terraform destroy
```

If old resources were created outside Terraform, or Spark created Glue tables after
Terraform created the Glue database, clean the demo leftovers explicitly:

```bash
cd ..
bash scripts/cleanup-aws-demo-resources.sh
```

The image push script expects Terraform to own the ECR repositories. If the
repositories were created by an older script, import them before destroy:

```bash
cd terraform
terraform import aws_ecr_repository.prefect_runtime prefect-runtime
terraform import aws_ecr_repository.spark_runtime spark-runtime
terraform import 'aws_glue_catalog_database.spark["processed"]' processed
terraform destroy
```

If a Kubernetes namespace is stuck in `Terminating`, inspect the remaining resources:

```bash
kubectl get all,configmap,secret,serviceaccount,role,rolebinding -n prefect
kubectl get all,configmap,secret,serviceaccount,role,rolebinding -n spark-operator
kubectl get all,configmap,secret,serviceaccount,role,rolebinding -n spark-jobs
```

## Notes

- `enable_nat_gateway = true` is the default because EKS Fargate pods in private subnets need outbound internet access to pull images.
- S3 buckets use `force_destroy = true` so `terraform destroy` can clean demo buckets. Change this before using production data.
- The old scripts can remain for reference, but avoid running `eksctl create cluster` against the same `cluster_name` after Terraform owns it.
- If you need to keep an existing `eksctl` cluster, import resources with `terraform import` before applying. For demo environments, recreating the stack is usually simpler and less error-prone.
- The Fargate event `LoggingDisabled` only means the optional `aws-logging` ConfigMap is missing. It does not block scheduling or image pulls.
