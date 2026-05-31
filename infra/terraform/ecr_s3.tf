resource "aws_s3_bucket" "data" {
  bucket        = var.data_bucket
  force_destroy = true

  tags = merge(var.common_tags, {
    Name = var.data_bucket
  })
}

resource "aws_s3_bucket" "script" {
  bucket        = var.script_bucket
  force_destroy = true

  tags = merge(var.common_tags, {
    Name = var.script_bucket
  })
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "script" {
  bucket = aws_s3_bucket.script.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

locals {
  prefect_repository_name = var.prefect_repository_name != null && var.prefect_repository_name != "" ? var.prefect_repository_name : var.repository_name
  spark_image             = var.spark_image != null && var.spark_image != "" ? var.spark_image : "${aws_ecr_repository.spark_runtime.repository_url}:${var.image_tag}"
}

resource "aws_ecr_repository" "prefect_runtime" {
  name                 = local.prefect_repository_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(var.common_tags, {
    Name = local.prefect_repository_name
  })
}

resource "aws_ecr_repository" "spark_runtime" {
  name                 = var.spark_repository_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(var.common_tags, {
    Name = var.spark_repository_name
  })
}

resource "aws_glue_catalog_database" "spark" {
  for_each = var.glue_database_names

  name         = each.value
  description  = "Spark Iceberg database managed by Terraform."
  location_uri = "s3://${aws_s3_bucket.data.bucket}/${each.value}/"

  parameters = {
    project = "prefect-spark-eks"
  }
}

resource "aws_s3_object" "spark_jobs" {
  for_each = fileset("${path.module}/../../src/jobs", "*.py")

  bucket       = aws_s3_bucket.script.id
  key          = "spark/jobs/${each.value}"
  source       = "${path.module}/../../src/jobs/${each.value}"
  etag         = filemd5("${path.module}/../../src/jobs/${each.value}")
  content_type = "text/x-python"
}

resource "aws_s3_object" "spark_configs" {
  for_each = fileset("${path.module}/../../configs", "**/*.yaml")

  bucket = aws_s3_bucket.script.id
  key    = "spark/configs/${each.value}"
  source = "${path.module}/../../configs/${each.value}"
  etag   = filemd5("${path.module}/../../configs/${each.value}")

  content_type = "application/x-yaml"
}

resource "aws_s3_object" "pipelines" {
  for_each = fileset("${path.module}/../../pipelines", "**/*.py")

  bucket = aws_s3_bucket.script.id
  key    = "pipelines/${each.value}"
  source = "${path.module}/../../pipelines/${each.value}"
  etag   = filemd5("${path.module}/../../pipelines/${each.value}")

  content_type = "text/x-python"
}
