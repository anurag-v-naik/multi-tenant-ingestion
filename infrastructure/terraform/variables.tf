variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "multi-tenant-ingestion"
}

# Network Configuration
variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

variable "database_subnet_cidrs" {
  description = "Database subnet CIDR blocks"
  type        = list(string)
  default     = ["10.0.21.0/24", "10.0.22.0/24", "10.0.23.0/24"]
}

# Database Configuration
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage (GB)"
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "RDS max allocated storage (GB)"
  type        = number
  default     = 100
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "multi_tenant_ingestion"
}

variable "db_username" {
  description = "Database username"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

# Redis Configuration
variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
  default     = "cache.t3.micro"
}

variable "redis_num_cache_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 1
}

# Databricks Configuration
variable "databricks_host" {
  description = "Databricks workspace host"
  type        = string
}

variable "databricks_token" {
  description = "Databricks workspace token"
  type        = string
  sensitive   = true
}

variable "databricks_tokens" {
  description = "Organization-specific Databricks tokens"
  type        = map(string)
  sensitive   = true
  default     = {}
}

variable "databricks_workspaces" {
  description = "Organization-specific Databricks workspace URLs"
  type        = map(string)
  default     = {}
}

# Organizations Configuration
variable "organizations" {
  description = "Organization configuration"
  type = map(object({
    name             = string
    display_name     = string
    cost_center      = string
    compliance_level = string
    resource_quotas = object({
      max_dbu_per_hour         = number
      max_storage_gb           = number
      max_api_calls_per_minute = number
    })
  }))
  
  default = {
    "finance" = {
      name             = "finance"
      display_name     = "Finance Department"
      cost_center      = "FIN-001"
      compliance_level = "high"
      resource_quotas = {
        max_dbu_per_hour         = 100
        max_storage_gb           = 10000
        max_api_calls_per_minute = 1000
      }
    }
    "retail" = {
      name             = "retail"
      display_name     = "Retail Division"
      cost_center      = "RET-001"
      compliance_level = "medium"
      resource_quotas = {
        max_dbu_per_hour         = 200
        max_storage_gb           = 50000
        max_api_calls_per_minute = 2000
      }
    }
    "healthcare" = {
      name             = "healthcare"
      display_name     = "Healthcare Division"
      cost_center      = "HLT-001"
      compliance_level = "high"
      resource_quotas = {
        max_dbu_per_hour         = 150
        max_storage_gb           = 25000
        max_api_calls_per_minute = 1500
      }
    }
  }
}

# ECS Configuration
variable "ecs_cpu" {
  description = "ECS task CPU units"
  type        = number
  default     = 256
}

variable "ecs_memory" {
  description = "ECS task memory (MB)"
  type        = number
  default     = 512
}

variable "ecs_desired_count" {
  description = "Desired number of ECS tasks"
  type        = number
  default     = 2
}

variable "ecs_max_capacity" {
  description = "Maximum number of ECS tasks for auto scaling"
  type        = number
  default     = 10
}

variable "ecs_min_capacity" {
  description = "Minimum number of ECS tasks for auto scaling"
  type        = number
  default     = 1
}
