# modules/rds/main.tf
# Creates the managed Postgres database — replaces the Docker postgres container.

variable "env"            {}
variable "vpc_id"         {}
variable "subnet_ids"     { type = list(string) }
variable "instance_class" { default = "db.t3.micro" }

# --- Subnet group (RDS needs to know which subnets it can use) ---

resource "aws_db_subnet_group" "main" {
  name       = "notiflow-${var.env}"
  subnet_ids = var.subnet_ids
  tags       = { Name = "notiflow-rds-subnet-group-${var.env}" }
}

# --- Security group (only EKS nodes can connect to Postgres) ---

resource "aws_security_group" "rds" {
  name   = "notiflow-rds-${var.env}"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]  # VPC only — never expose Postgres to the internet
    description = "Postgres from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "notiflow-rds-sg-${var.env}" }
}

# --- Random password (stored in AWS Secrets Manager automatically) ---

resource "random_password" "db" {
  length  = 32
  special = false  # avoid special chars that break connection strings
}

# --- RDS Postgres instance ---

resource "aws_db_instance" "main" {
  identifier        = "notiflow-${var.env}"
  engine            = "postgres"
  engine_version    = "16.3"
  instance_class    = var.instance_class
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = "notiflow"
  username = "notiflow"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # Backups — keep 7 days for dev, increase for prod
  backup_retention_period = var.env == "prod" ? 30 : 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  # Don't delete the DB if you run terraform destroy in prod
  deletion_protection = var.env == "prod"
  skip_final_snapshot = var.env != "prod"

  # Encrypt at rest
  storage_encrypted = true

  tags = { Env = var.env, Project = "notiflow" }
}

# --- Store the connection string in Secrets Manager ---
# Your K8s pods read from here via External Secrets Operator

resource "aws_secretsmanager_secret" "db_url" {
  name = "notiflow/${var.env}/database-url"
  tags = { Env = var.env }
}

resource "aws_secretsmanager_secret_version" "db_url" {
  secret_id = aws_secretsmanager_secret.db_url.id
  secret_string = "postgres://${aws_db_instance.main.username}:${random_password.db.result}@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}?sslmode=require"
}

# --- Outputs ---

output "db_endpoint"   { value = aws_db_instance.main.endpoint }
output "db_name"       { value = aws_db_instance.main.db_name }
output "db_secret_arn" { value = aws_secretsmanager_secret.db_url.arn }
output "db_url" {
  value     = "postgres://${aws_db_instance.main.username}:${random_password.db.result}@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}?sslmode=require"
  sensitive = true  # won't print in terraform output
}
