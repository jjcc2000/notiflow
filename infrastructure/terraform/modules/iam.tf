# modules/iam/main.tf
# Creates IAM roles for pods using IRSA (IAM Roles for Service Accounts).
# This is how your email-consumer pod calls SES without hardcoded AWS keys.
# The pod assumes a role → role has a policy → policy allows SES/Secrets Manager.

variable "env"          {}
variable "eks_oidc_url" {}
variable "eks_oidc_arn" { default = "" }

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  # Strip https:// from OIDC URL for the trust policy condition
  oidc_host  = replace(var.eks_oidc_url, "https://", "")
}

# --- Email consumer role (needs SES + Secrets Manager) ---

resource "aws_iam_role" "email_consumer" {
  name = "notiflow-email-consumer-${var.env}"

  # Trust policy: only the email-consumer service account in K8s can assume this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = "arn:aws:iam::${local.account_id}:oidc-provider/${local.oidc_host}"
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_host}:sub" = "system:serviceaccount:notiflow-services:notiflow-consumer"
          "${local.oidc_host}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Env = var.env }
}

resource "aws_iam_policy" "ses_send" {
  name        = "notiflow-ses-send-${var.env}"
  description = "Allow NotiFlow email consumer to send via SES"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ses:SendEmail", "ses:SendRawEmail"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "email_consumer_ses" {
  role       = aws_iam_role.email_consumer.name
  policy_arn = aws_iam_policy.ses_send.arn
}

# --- Notification service role (needs Secrets Manager to read DB URL) ---

resource "aws_iam_role" "notification_service" {
  name = "notiflow-notification-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = "arn:aws:iam::${local.account_id}:oidc-provider/${local.oidc_host}"
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_host}:sub" = "system:serviceaccount:notiflow-services:notiflow-notification"
          "${local.oidc_host}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_policy" "read_secrets" {
  name        = "notiflow-read-secrets-${var.env}"
  description = "Allow NotiFlow services to read secrets from Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ]
      Resource = "arn:aws:secretsmanager:*:${local.account_id}:secret:notiflow/${var.env}/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "notification_secrets" {
  role       = aws_iam_role.notification_service.name
  policy_arn = aws_iam_policy.read_secrets.arn
}

resource "aws_iam_role_policy_attachment" "email_consumer_secrets" {
  role       = aws_iam_role.email_consumer.name
  policy_arn = aws_iam_policy.read_secrets.arn
}

# --- Outputs — used in K8s ServiceAccount annotations ---

output "email_consumer_role_arn"      { value = aws_iam_role.email_consumer.arn }
output "notification_service_role_arn" { value = aws_iam_role.notification_service.arn }