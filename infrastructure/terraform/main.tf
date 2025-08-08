# infrastructure/terraform/main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "databricks" {
  host     = var.databricks_host
  token    = var.databricks_token
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "multi-tenant-ingestion"
}

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "organizations" {
  description = "Organization configurations"
  type = map(object({
    name             = string
    display_name     = string
    cost_center      = string
    compliance_level = string
    resource_quotas = object({
      max_dbu_per_hour        = number
      max_storage_gb          = number
      max_api_calls_per_minute = number
    })
  }))
  default = {}
}

variable "enable_unity_catalog" {
  description = "Enable Unity Catalog features"
  type        = bool
  default     = true
}

variable "enable_iceberg" {
  description = "Enable Iceberg tables"
  type        = bool
  default     = true
}

# Local values
locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
  
  organization_names = keys(var.organizations)
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-vpc"
  })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-igw"
  })
}

# Public Subnets
resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-public-${count.index + 1}"
    Type = "public"
  })
}

# Private Subnets
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-private-${count.index + 1}"
    Type = "private"
  })
}

# NAT Gateways
resource "aws_eip" "nat" {
  count  = 2
  domain = "vpc"

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-nat-eip-${count.index + 1}"
  })
}

resource "aws_nat_gateway" "main" {
  count         = 2
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-nat-${count.index + 1}"
  })

  depends_on = [aws_internet_gateway.main]
}

# Route Tables
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-public-rt"
  })
}

resource "aws_route_table" "private" {
  count  = 2
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main[count.index].id
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-private-rt-${count.index + 1}"
  })
}

# Route Table Associations
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# Security Groups
resource "aws_security_group" "alb" {
  name        = "${var.project_name}-alb-sg"
  description = "Security group for Application Load Balancer"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-alb-sg"
  })
}

resource "aws_security_group" "ecs" {
  name        = "${var.project_name}-ecs-sg"
  description = "Security group for ECS services"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 8000
    to_port         = 8999
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-ecs-sg"
  })
}

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group for RDS instances"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs.id]
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-rds-sg"
  })
}

# RDS Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-db-subnet-group"
  })
}

# RDS Instance
resource "aws_db_instance" "main" {
  identifier             = "${var.project_name}-db"
  engine                 = "postgres"
  engine_version         = "14.9"
  instance_class         = "db.t3.medium"
  allocated_storage      = 100
  max_allocated_storage  = 1000
  storage_encrypted      = true
  
  db_name  = "multitenantdb"
  username = "dbadmin"
  password = aws_secretsmanager_secret_version.db_password.secret_string
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name
  
  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  skip_final_snapshot = false
  final_snapshot_identifier = "${var.project_name}-db-final-snapshot"
  
  tags = merge(local.common_tags, {
    Name = "${var.project_name}-db"
  })
}

# ElastiCache Subnet Group
resource "aws_elasticache_subnet_group" "main" {
  name       = "${var.project_name}-cache-subnet-group"
  subnet_ids = aws_subnet.private[*].id
}

# ElastiCache Redis Cluster
resource "aws_elasticache_replication_group" "main" {
  replication_group_id       = "${var.project_name}-redis"
  description                = "Redis cluster for multi-tenant ingestion"
  
  node_type                  = "cache.t3.micro"
  port                       = 6379
  parameter_group_name       = "default.redis7"
  
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled          = true
  
  subnet_group_name = aws_elasticache_subnet_group.main.name
  security_group_ids = [aws_security_group.ecs.id]
  
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  
  tags = merge(local.common_tags, {
    Name = "${var.project_name}-redis"
  })
}

# S3 Buckets for each organization
resource "aws_s3_bucket" "org_data" {
  for_each = var.organizations
  bucket   = "${var.project_name}-${each.key}-data"

  tags = merge(local.common_tags, {
    Name         = "${var.project_name}-${each.key}-data"
    Organization = each.key
  })
}

resource "aws_s3_bucket_versioning" "org_data" {
  for_each = var.organizations
  bucket   = aws_s3_bucket.org_data[each.key].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "org_data" {
  for_each = var.organizations
  bucket   = aws_s3_bucket.org_data[each.key].id

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

resource "aws_s3_bucket_public_access_block" "org_data" {
  for_each = var.organizations
  bucket   = aws_s3_bucket.org_data[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.common_tags
}

# ECS Cluster Capacity Providers
resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE"
  }
}

# Application Load Balancer
resource "aws_lb" "main" {
  name               = "${var.project_name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id

  enable_deletion_protection = false

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-alb"
  })
}

# Target Groups for each service
resource "aws_lb_target_group" "pipeline_service" {
  name     = "${var.project_name}-pipeline"
  port     = 8000
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 30
    interval            = 60
    path                = "/health"
    matcher             = "200"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-pipeline-tg"
  })
}

resource "aws_lb_target_group" "catalog_service" {
  name     = "${var.project_name}-catalog"
  port     = 8001
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 30
    interval            = 60
    path                = "/health"
    matcher             = "200"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-catalog-tg"
  })
}

resource "aws_lb_target_group" "connector_service" {
  name     = "${var.project_name}-connector"
  port     = 8002
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 30
    interval            = 60
    path                = "/health"
    matcher             = "200"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-connector-tg"
  })
}

resource "aws_lb_target_group" "dq_service" {
  name     = "${var.project_name}-dq"
  port     = 8003
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id
  target_type = "ip"

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    timeout             = 30
    interval            = 60
    path                = "/health"
    matcher             = "200"
  }

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-dq-tg"
  })
}

# ALB Listener
resource "aws_lb_listener" "main" {
  load_balancer_arn = aws_lb.main.arn
  port              = "80"
  protocol          = "HTTP"

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "text/plain"
      message_body = "Multi-Tenant Data Ingestion Platform"
      status_code  = "200"
    }
  }
}

# ALB Listener Rules
resource "aws_lb_listener_rule" "pipeline_service" {
  listener_arn = aws_lb_listener.main.arn
  priority     = 100

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.pipeline_service.arn
  }

  condition {
    path_pattern {
      values = ["/api/v1/pipelines*", "/api/v1/runs*"]
    }
  }
}

resource "aws_lb_listener_rule" "catalog_service" {
  listener_arn = aws_lb_listener.main.arn
  priority     = 200

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.catalog_service.arn
  }

  condition {
    path_pattern {
      values = ["/api/v1/catalogs*", "/api/v1/schemas*", "/api/v1/tables*"]
    }
  }
}

resource "aws_lb_listener_rule" "connector_service" {
  listener_arn = aws_lb_listener.main.arn
  priority     = 300

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.connector_service.arn
  }

  condition {
    path_pattern {
      values = ["/api/v1/connectors*", "/api/v1/connector-types*"]
    }
  }
}

resource "aws_lb_listener_rule" "dq_service" {
  listener_arn = aws_lb_listener.main.arn
  priority     = 400

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dq_service.arn
  }

  condition {
    path_pattern {
      values = ["/api/v1/rules*", "/api/v1/checks*", "/api/v1/reports*"]
    }
  }
}

# Secrets Manager
resource "aws_secretsmanager_secret" "db_password" {
  name        = "${var.project_name}/db/password"
  description = "Database password for multi-tenant ingestion"

  tags = local.common_tags
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = random_password.db_password.result
}

resource "random_password" "db_password" {
  length  = 32
  special = true
}

# Organization-specific secrets
resource "aws_secretsmanager_secret" "org_databricks_token" {
  for_each = var.organizations
  
  name        = "${var.project_name}/${each.key}/databricks-token"
  description = "Databricks token for organization ${each.key}"

  tags = merge(local.common_tags, {
    Organization = each.key
  })
}

# IAM Roles and Policies
resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.project_name}-ecs-task-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name = "${var.project_name}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "${var.project_name}-ecs-task-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          for bucket in aws_s3_bucket.org_data : "${bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          for bucket in aws_s3_bucket.org_data : bucket.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.db_password.arn,
          for secret in aws_secretsmanager_secret.org_databricks_token : secret.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:UpdateTable"
        ]
        Resource = "*"
      }
    ]
  })
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "services" {
  for_each = toset(["pipeline", "catalog", "connector", "dq"])
  
  name              = "/ecs/${var.project_name}-${each.key}"
  retention_in_days = 30

  tags = merge(local.common_tags, {
    Service = each.key
  })
}
