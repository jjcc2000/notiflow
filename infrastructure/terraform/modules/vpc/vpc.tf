# modules/vpc/main.tf
# Creates the private network where all NotiFlow resources live.
# Nothing is exposed to the internet except the EKS load balancer.

variable "env"    {}
variable "region" { default = "us-east-1" }

# Availability zones in the region
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true  # required for EKS

  tags = {
    Name    = "notiflow-${var.env}"
    Env     = var.env
    Project = "notiflow"
  }
}

# --- Public subnets (load balancers live here) ---

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                     = "notiflow-public-${count.index}-${var.env}"
    "kubernetes.io/role/elb" = "1"  # tells EKS to use these for load balancers
  }
}

# --- Private subnets (EKS nodes, RDS, Kafka, Redis live here) ---

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name                              = "notiflow-private-${count.index}-${var.env}"
    "kubernetes.io/role/internal-elb" = "1"
  }
}

# --- Internet Gateway (public subnets route through this) ---

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "notiflow-igw-${var.env}" }
}

# --- NAT Gateway (private subnets route outbound traffic through this) ---
# Needed so EKS nodes can pull Docker images from ECR

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "notiflow-nat-eip-${var.env}" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = "notiflow-nat-${var.env}" }
  depends_on    = [aws_internet_gateway.main]
}

# --- Route tables ---

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "notiflow-public-rt-${var.env}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "notiflow-private-rt-${var.env}" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- Outputs used by other modules ---

output "vpc_id"             { value = aws_vpc.main.id }
output "public_subnet_ids"  { value = aws_subnet.public[*].id }
output "private_subnet_ids" { value = aws_subnet.private[*].id }
