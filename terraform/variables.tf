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
  description = "S3 bucket used by Spark jobs for input/output data."
  type        = string
  default     = "prefect-demo-data"
}

variable "prefect_flow_bucket" {
  description = "S3 bucket used by Prefect flow storage."
  type        = string
  default     = "prefect-demo-flow"
}

variable "repository_name" {
  description = "ECR repository for the Prefect runtime image."
  type        = string
  default     = "prefect-spark-runtime"
}

variable "image_tag" {
  description = "Default image tag used by the Prefect worker deployment."
  type        = string
  default     = "latest"
}

variable "spark_image" {
  description = "Spark container image used by flow parameters."
  type        = string
  default     = "docker.io/library/spark:3.5.1-python3"
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

variable "common_tags" {
  description = "Tags applied to AWS resources."
  type        = map(string)
  default = {
    Project = "prefect-spark-eks"
  }
}
