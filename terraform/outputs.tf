output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "region" {
  value = var.region
}

output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "prefect_flow_bucket" {
  value = aws_s3_bucket.prefect_flow.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.prefect_runtime.repository_url
}

output "prefect_runtime_image" {
  value = "${aws_ecr_repository.prefect_runtime.repository_url}:${var.image_tag}"
}

output "update_kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${var.region}"
}

output "prefect_port_forward_command" {
  value = "kubectl port-forward -n prefect svc/prefect-server 4200:4200"
}

