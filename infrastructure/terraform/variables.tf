# infrastructure/terraform/variables.tf
# Variable definitions for Multi-Tenant Ingestion Framework

# General Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "multi-tenant-ingestion"
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "terraform_state_bucket" {
  description = "S3 bucket for Terraform state"
  type        = string
}

# Network Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

# EKS Configuration
variable "kubernetes_version" {
  description = "Kubernetes version for EKS cluster"
  type        = string
  default     = "1.28"
}

variable "node_instance_types" {
  description = "Instance types for EKS worker nodes"
  type        = list(string)
  default     = ["t3.medium", "t3.large"]
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 10
}

# Database Configuration
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "db_allocated_storage" {
  description = "Initial allocated storage for RDS (GB)"
  type        = number
  default     = 100
}

variable "db_max_allocated_storage" {
  description = "Maximum allocated storage for RDS auto-scaling (GB)"
  type        = number
  default     = 1000
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "multi_tenant_ingestion"
}

variable "db_username" {
  description = "Database username"
  type        = string
  default     = "app_user"
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

# Databricks Configuration
variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}

variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks access token"
  type        = string
  sensitive   = true
}

variable "databricks_external_id" {
  description = "External ID for Databricks cross-account role"
  type        = string
  sensitive   = true
}

# Security Configuration
variable "jwt_secret_key" {
  description = "JWT secret key for authentication"
  type        = string
  sensitive   = true
}

# Multi-Tenant Organization Configuration
variable "organizations" {
  description = "Configuration for each tenant organization"
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
    finance = {
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
    retail = {
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
    healthcare = {
      name             = "healthcare"
      display_name     = "Healthcare Division"
      cost_center      = "HC-001"
      compliance_level = "high"
      resource_quotas = {
        max_dbu_per_hour         = 150
        max_storage_gb           = 25000
        max_api_calls_per_minute = 1500
      }
    }
  }
}

# Feature Flags
variable "enable_unity_catalog" {
  description = "Enable Unity Catalog integration"
  type        = bool
  default     = true
}

variable "enable_iceberg" {
  description = "Enable Apache Iceberg tables"
  type        = bool
  default     = true
}

variable "enable_data_quality" {
  description = "Enable data quality service"
  type        = bool
  default     = true
}

variable "enable_monitoring" {
  description = "Enable comprehensive monitoring and alerting"
  type        = bool
  default     = true
}

# Cost Management
variable "enable_cost_allocation_tags" {
  description = "Enable detailed cost allocation tags"
  type        = bool
  default     = true
}

variable "budget_alert_threshold" {
  description = "Budget alert threshold percentage"
  type        = number
  default     = 80
}

# Backup and Recovery
variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 30
}

variable "enable_point_in_time_recovery" {
  description = "Enable point-in-time recovery for RDS"
  type        = bool
  default     = true
}

# Monitoring and Alerting
variable "cloudwatch_retention_days" {
  description = "CloudWatch logs retention in days"
  type        = number
  default     = 30
}

variable "enable_detailed_monitoring" {
  description = "Enable detailed CloudWatch monitoring"
  type        = bool
  default     = true
}

# Security
variable "enable_encryption_at_rest" {
  description = "Enable encryption at rest for all storage services"
  type        = bool
  default     = true
}

variable "enable_encryption_in_transit" {
  description = "Enable encryption in transit"
  type        = bool
  default     = true
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the application"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Restrict this in production
}

# Scaling Configuration
variable "autoscaling_enabled" {
  description = "Enable auto-scaling for compute resources"
  type        = bool
  default     = true
}

variable "min_capacity" {
  description = "Minimum capacity for auto-scaling"
  type        = number
  default     = 2
}

variable "max_capacity" {
  description = "Maximum capacity for auto-scaling"
  type        = number
  default     = 20
}

variable "target_cpu_utilization" {
  description = "Target CPU utilization percentage for auto-scaling"
  type        = number
  default     = 70
}

# Data Lifecycle Management
variable "data_retention_policy" {
  description = "Data retention policies by organization"
  type = map(object({
    raw_data_retention_days       = number
    processed_data_retention_days = number
    archive_after_days           = number
  }))
  default = {
    finance = {
      raw_data_retention_days       = 2555  # 7 years
      processed_data_retention_days = 1825  # 5 years
      archive_after_days           = 365    # 1 year
    }
    retail = {
      raw_data_retention_days       = 1095  # 3 years
      processed_data_retention_days = 730   # 2 years
      archive_after_days           = 180    # 6 months
    }
    healthcare = {
      raw_data_retention_days       = 3650  # 10 years
      processed_data_retention_days = 2555  # 7 years
      archive_after_days           = 365    # 1 year
    }
  }
}

# Compliance and Governance
variable "compliance_requirements" {
  description = "Compliance requirements by organization"
  type = map(object({
    encryption_required     = bool
    audit_logging_required  = bool
    data_residency_required = bool
    pii_detection_required  = bool
  }))
  default = {
    finance = {
      encryption_required     = true
      audit_logging_required  = true
      data_residency_required = true
      pii_detection_required  = true
    }
    retail = {
      encryption_required     = true
      audit_logging_required  = false
      data_residency_required = false
      pii_detection_required  = true
    }
    healthcare = {
      encryption_required     = true
      audit_logging_required  = true
      data_residency_required = true
      pii_detection_required  = true
    }
  }
}