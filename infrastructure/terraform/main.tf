    # infra/terraform/main.tf
# One terraform apply provisions the full NotiFlow stack.

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
  backend "s3" {
    bucket = "notiflow-tf-state"
    key    = "notiflow/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

module "vpc" {
  source = "./modules/vpc"
  env    = var.env
}

module "eks" {
  source       = "./modules/eks"
  cluster_name = "notiflow-${var.env}"
  vpc_id       = module.vpc.vpc_id
  subnet_ids   = module.vpc.private_subnet_ids
}

module "rds" {
  source         = "./modules/rds"
  env            = var.env
  vpc_id         = module.vpc.vpc_id
  subnet_ids     = module.vpc.private_subnet_ids
  instance_class = var.env == "prod" ? "db.t3.medium" : "db.t3.micro"
}

module "iam" {
  source      = "./modules/iam"
  env         = var.env
  eks_oidc_url = module.eks.oidc_provider_url
}

variable "env"        { default = "dev" }
variable "aws_region" { default = "us-east-1" }
