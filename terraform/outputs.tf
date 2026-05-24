output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "region" {
  value = var.region
}

output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "script_bucket" {
  value = aws_s3_bucket.script.bucket
}

output "ecr_repository_url" {
  description = "Deprecated. Prefect runtime ECR repository URL."
  value       = aws_ecr_repository.prefect_runtime.repository_url
}

output "prefect_ecr_repository_url" {
  value = aws_ecr_repository.prefect_runtime.repository_url
}

output "spark_ecr_repository_url" {
  value = aws_ecr_repository.spark_runtime.repository_url
}

output "prefect_runtime_image" {
  value = "${aws_ecr_repository.prefect_runtime.repository_url}:${var.image_tag}"
}

output "spark_runtime_image" {
  value = local.spark_image
}

output "glue_database_names" {
  value = sort(tolist(var.glue_database_names))
}

output "update_kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${var.region}"
}

output "prefect_port_forward_command" {
  value = "kubectl port-forward -n prefect svc/prefect-server 4200:4200"
}
