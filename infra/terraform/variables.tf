variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "prefect-spark-demo"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-southeast-1"
}

variable "eks_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.30"
}

variable "vpc_cidr" {
  description = "CIDR block for the EKS VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "enable_nat_gateway" {
  description = "Create one NAT gateway for private Fargate pod egress. Required for pulling public images and Helm workloads."
  type        = bool
  default     = true
}

variable "data_bucket" {
  description = "S3 bucket for data storage."
  type        = string
  default     = "prefect-demo-data"
}

variable "script_bucket" {
  description = "S3 bucket for scripts storage."
  type        = string
  default     = "prefect-demo-scripts"
}

variable "repository_name" {
  description = "Deprecated. ECR repository for the Prefect runtime image when prefect_repository_name is not set."
  type        = string
  default     = "prefect-runtime"
}

variable "prefect_repository_name" {
  description = "ECR repository for the Prefect runtime image."
  type        = string
  default     = null
}

variable "spark_repository_name" {
  description = "ECR repository for the Spark runtime image."
  type        = string
  default     = "spark-runtime"
}

variable "image_tag" {
  description = "Default image tag used by the Prefect and Spark runtime images."
  type        = string
  default     = "latest"
}

variable "spark_image" {
  description = "Spark container image used by flow parameters."
  type        = string
  default     = null
}

variable "glue_database_names" {
  description = "Glue databases used by Spark Iceberg jobs."
  type        = set(string)
  default     = ["processed"]
}

variable "prefect_work_pool_name" {
  description = "Prefect Kubernetes work pool name."
  type        = string
  default     = "kubernetes-pool"
}

variable "prefect_server_api_url" {
  description = "Prefect API URL used by in-cluster Prefect worker pods."
  type        = string
  default     = "http://prefect-server.prefect.svc.cluster.local:4200/api"
}

variable "prefect_api_url" {
  description = "Prefect API URL used by the Terraform Prefect provider. For the self-hosted cluster, run kubectl port-forward to expose the API locally before applying Prefect resources."
  type        = string
  default     = "http://127.0.0.1:4200/api"
}

variable "prefect_github_credentials_block_name" {
  description = "Name of the Prefect GitHub Credentials block referenced by prefect.yaml."
  type        = string
  default     = "github-token"
}

variable "create_prefect_github_credentials_block" {
  description = "Whether Terraform should create the Prefect GitHub Credentials block."
  type        = bool
  default     = false
}

variable "github_token" {
  description = "GitHub token for the Prefect GitHub Credentials block. Set with TF_VAR_github_token or a local tfvars file that is not committed."
  type        = string
  sensitive   = true
  default     = null
}

variable "common_tags" {
  description = "Tags applied to AWS resources."
  type        = map(string)
  default = {
    Project = "prefect-spark-eks"
  }
}
