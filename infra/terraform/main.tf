resource "aws_security_group" "prefect_rds_sg" {
  name        = "${var.cluster_name}-rds-sg"
  description = "Allow PostgreSQL inbound traffic from EKS VPC"
  vpc_id      = aws_vpc.this.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
}

resource "aws_db_subnet_group" "prefect_rds_subnet" {
  name       = "${var.cluster_name}-rds-subnet"
  subnet_ids = aws_subnet.private[*].id
  tags       = var.common_tags
}

resource "random_password" "db_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# Amazon RDS (PostgreSQL)
resource "aws_db_instance" "prefect_postgres" {
  identifier        = "prefect-flow-logs-db"
  engine            = "postgres"
  engine_version    = "15.18"
  instance_class    = "db.t3.micro"
  allocated_storage = 20

  db_name  = "prefectlogs"
  username = "prefect_admin"
  password = random_password.db_password.result

  skip_final_snapshot    = true
  publicly_accessible    = false
  db_subnet_group_name   = aws_db_subnet_group.prefect_rds_subnet.name
  vpc_security_group_ids = [aws_security_group.prefect_rds_sg.id]
}

# AWS Secrets Manager
resource "aws_secretsmanager_secret" "prefect_db_secret" {
  name                    = "prefect/postgres-credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "prefect_db_secret_val" {
  secret_id = aws_secretsmanager_secret.prefect_db_secret.id

  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.prefect_postgres.address
    port     = aws_db_instance.prefect_postgres.port
    dbname   = aws_db_instance.prefect_postgres.db_name
    username = aws_db_instance.prefect_postgres.username
    password = random_password.db_password.result
  })
}

output "secrets_manager_arn" {
  description = "Secrets Manager ARN"
  value       = aws_secretsmanager_secret.prefect_db_secret.arn
}
