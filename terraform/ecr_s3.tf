resource "aws_s3_bucket" "data" {
  bucket        = var.data_bucket
  force_destroy = true

  tags = merge(var.common_tags, {
    Name = var.data_bucket
  })
}

resource "aws_s3_bucket" "prefect_flow" {
  bucket        = var.prefect_flow_bucket
  force_destroy = true

  tags = merge(var.common_tags, {
    Name = var.prefect_flow_bucket
  })
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "prefect_flow" {
  bucket = aws_s3_bucket.prefect_flow.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_ecr_repository" "prefect_runtime" {
  name                 = var.repository_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(var.common_tags, {
    Name = var.repository_name
  })
}

resource "aws_s3_object" "spark_job" {
  bucket       = aws_s3_bucket.data.id
  key          = "jobs/etl_job.py"
  source       = "${path.module}/../jobs/etl_job.py"
  etag         = filemd5("${path.module}/../jobs/etl_job.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "sample_raw_sales" {
  bucket       = aws_s3_bucket.data.id
  key          = "raw/sample_sales.csv"
  source       = "${path.module}/../data/raw/sample_sales.csv"
  etag         = filemd5("${path.module}/../data/raw/sample_sales.csv")
  content_type = "text/csv"
}
