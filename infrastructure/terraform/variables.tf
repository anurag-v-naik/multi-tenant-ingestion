variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "development"
}

variable "availability_zones" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
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

variable "node_instance_type" {
  description = "EKS node instance type"
  type        = string
  default     = "t3.medium"
}

variable "node_desired_size" {
  description = "Desired number of EKS nodes"
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum number of EKS nodes"
  type        = number
  default     = 6
}

variable "node_min_size" {
  description = "Minimum number of EKS nodes"
  type        = number
  default     = 1
}

variable "organizations" {
  description = "Organization configurations"
  type = map(object({
    name         = string
    display_name = string
    cost_center  = string
    compliance_level = string
    resource_quotas = object({
      max_dbu_per_hour        = number
      max_storage_gb          = number
      max_api_calls_per_minute = number
    })
  }))
  default = {
    "finance" = {
      name         = "finance"
      display_name = "Finance Department"
      cost_center  = "FIN-001"
      compliance_level = "high"
      resource_quotas = {
        max_dbu_per_hour        = 100
        max_storage_gb          = 10000
        max_api_calls_per_minute = 1000
      }
    }
    "retail" = {
      name         = "retail"
      display_name = "Retail Division"
      cost_center  = "RET-001"
      compliance_level = "medium"
      resource_quotas = {
        max_dbu_per_hour        = 200
        max_storage_gb          = 50000
        max_api_calls_per_minute = 2000
      }
    }
  }
}
