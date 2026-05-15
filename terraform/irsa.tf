data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer

  tags = var.common_tags
}

locals {
  oidc_provider_arn_suffix = replace(aws_iam_openid_connect_provider.eks.url, "https://", "")
}

data "aws_iam_policy_document" "prefect_flow_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_arn_suffix}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_arn_suffix}:sub"
      values   = ["system:serviceaccount:prefect:prefect-flow-run"]
    }
  }
}

data "aws_iam_policy_document" "spark_driver_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_arn_suffix}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider_arn_suffix}:sub"
      values   = ["system:serviceaccount:spark-jobs:spark-driver-sa"]
    }
  }
}

resource "aws_iam_role" "prefect_flow_run" {
  name               = "${var.cluster_name}-prefect-flow-run"
  assume_role_policy = data.aws_iam_policy_document.prefect_flow_assume_role.json

  tags = var.common_tags
}

resource "aws_iam_role" "spark_driver" {
  name               = "${var.cluster_name}-spark-driver"
  assume_role_policy = data.aws_iam_policy_document.spark_driver_assume_role.json

  tags = var.common_tags
}

data "aws_iam_policy_document" "prefect_flow_s3" {
  statement {
    sid       = "ListPrefectFlowPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.prefect_flow.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["prefect/flows/*"]
    }
  }

  statement {
    sid       = "ReadPrefectFlowObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.prefect_flow.arn}/prefect/flows/*"]
  }
}

data "aws_iam_policy_document" "spark_data_s3" {
  statement {
    sid       = "ListDataBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid    = "ReadWriteDataObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }
}

resource "aws_iam_policy" "prefect_flow_s3" {
  name   = "PrefectFlowRunS3ReadPolicy"
  policy = data.aws_iam_policy_document.prefect_flow_s3.json

  tags = var.common_tags
}

resource "aws_iam_policy" "spark_data_s3" {
  name   = "SparkJobS3ReadWritePolicy"
  policy = data.aws_iam_policy_document.spark_data_s3.json

  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "prefect_flow_s3" {
  role       = aws_iam_role.prefect_flow_run.name
  policy_arn = aws_iam_policy.prefect_flow_s3.arn
}

resource "aws_iam_role_policy_attachment" "spark_data_s3" {
  role       = aws_iam_role.spark_driver.name
  policy_arn = aws_iam_policy.spark_data_s3.arn
}

