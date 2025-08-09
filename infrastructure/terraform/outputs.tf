# infrastructure/terraform/outputs.tf
# Output values for Multi-Tenant Ingestion Framework

# VPC and Networking Outputs
output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "vpc_cidr_block" {
  description = "CIDR block of the VPC"
  value       = aws_vpc.main.cidr_block
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets"
  value       = aws_subnet.private[*].id
}

output "internet_gateway_id" {
  description = "ID of the Internet Gateway"
  value       = aws_internet_gateway.main.id
}

output "nat_gateway_ids" {
  description = "IDs of the NAT Gateways"
  value       = aws_nat_gateway.main[*].id
}

# EKS Cluster Outputs
output "eks_cluster_id" {
  description = "EKS cluster ID"
  value       = aws_eks_cluster.main.id
}

output "eks_cluster_arn" {
  description = "EKS cluster ARN"
  value       = aws_eks_cluster.main.arn
}

output "eks_cluster_endpoint" {
  description = "Endpoint for EKS control plane"
  value       = aws_eks_cluster.main.endpoint
}

output "eks_cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = aws_security_group.eks_cluster.id
}

output "eks_cluster_version" {
  description = "The Kubernetes version for the EKS cluster"
  value       = aws_eks_cluster.main.version
}

output "eks_cluster_certificate_authority_data" {
  description = "Base64 encoded certificate data required to communicate with the cluster"
  value       = aws_eks_cluster.main.certificate_authority[0].data
}

output "eks_node_group_arn" {
  description = "Amazon Resource Name (ARN) of the EKS Node Group"
  value       = aws_eks_node_group.main.arn
}

output "eks_node_group_status" {
  description = "Status of the EKS Node Group"
  value       = aws_eks_node_group.main.status
}

output "eks_nodes_security_group_id" {
  description = "Security group ID attached to the EKS nodes"
  value       = aws_security_group.eks_nodes.id
}

# Database Outputs
output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.main.endpoint
  sensitive   = true
}

output "rds_port" {
  description = "RDS instance port"
  value       = aws_db_instance.main.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.main.db_name
}

output "rds_username" {
  description = "RDS database username"
  value       = aws_db_instance.main.username
  sensitive   = true
}

output "rds_instance_id" {
  description = "RDS instance ID"
  value       = aws_db_instance.main.id
}

output "rds_instance_class" {
  description = "RDS instance class"
  value       = aws_db_instance.main.instance_class
}

output "rds_allocated_storage" {
  description = "RDS allocated storage"
  value       = aws_db_instance.main.allocated_storage
}

# Redis Outputs
output "redis_endpoint" {
  description = "Redis cluster endpoint"
  value       = aws_elasticache_replication_group.main.primary_endpoint_address
  sensitive   = true
}

output "redis_port" {
  description = "Redis cluster port"
  value       = aws_elasticache_replication_group.main.port
}

output "redis_cluster_id" {
  description = "Redis cluster ID"
  value       = aws_elasticache_replication_group.main.id
}

# Load Balancer Outputs
output "load_balancer_dns" {
  description = "DNS name of the load balancer"
  value       = aws_lb.main.dns_name
}

output "load_balancer_zone_id" {
  description = "Zone ID of the load balancer"
  value       = aws_lb.main.zone_id
}

output "load_balancer_arn" {
  description = "ARN of the load balancer"
  value       = aws_lb.main.arn
}

# S3 Bucket Outputs
output "tenant_s3_buckets" {
  description = "S3 bucket names for each tenant"
  value = {
    for org_name, bucket in aws_s3_bucket.tenant_data : org_name => bucket.id
  }
}

output "tenant_s3_bucket_arns" {
  description = "S3 bucket ARNs for each tenant"
  value = {
    for org_name, bucket in aws_s3_bucket.tenant_data : org_name => bucket.arn
  }
}

# Secrets Manager Outputs
output "app_secrets_arn" {
  description = "ARN of the application secrets"
  value       = aws_secretsmanager_secret.app_secrets.arn
}

output "tenant_secrets_arns" {
  description = "ARNs of tenant-specific secrets"
  value = {
    for org_name, secret in aws_secretsmanager_secret.tenant_secrets : org_name => secret.arn
  }
}

# IAM Outputs
output "eks_cluster_role_arn" {
  description = "ARN of the EKS cluster IAM role"
  value       = aws_iam_role.eks_cluster.arn
}

output "eks_node_group_role_arn" {
  description = "ARN of the EKS node group IAM role"
  value       = aws_iam_role.eks_nodes.arn
}

output "service_account_role_arn" {
  description = "ARN of the service account IAM role"
  value       = aws_iam_role.service_account.arn
}

output "databricks_tenant_role_arns" {
  description = "ARNs of Databricks tenant roles"
  value = {
    for org_name, role in aws_iam_role.databricks_tenant : org_name => role.arn
  }
}

output "databricks_instance_profile_arns" {
  description = "ARNs of Databricks instance profiles"
  value = {
    for org_name, profile in aws_iam_instance_profile.databricks_tenant : org_name => profile.arn
  }
}

# CloudWatch Outputs
output "app_log_group_name" {
  description = "Name of the application log group"
  value       = aws_cloudwatch_log_group.app_logs.name
}

output "tenant_log_group_names" {
  description = "Names of tenant-specific log groups"
  value = {
    for org_name, log_group in aws_cloudwatch_log_group.databricks_logs : org_name => log_group.name
  }
}

# Organization Configuration Outputs
output "organizations" {
  description = "Organization configuration"
  value = {
    for org_name, org_config in var.organizations : org_name => {
      name             = org_config.name
      display_name     = org_config.display_name
      cost_center      = org_config.cost_center
      compliance_level = org_config.compliance_level
      s3_bucket        = aws_s3_bucket.tenant_data[org_name].id
      databricks_role  = aws_iam_role.databricks_tenant[org_name].arn
      secrets_arn      = aws_secretsmanager_secret.tenant_secrets[org_name].arn
      log_group        = aws_cloudwatch_log_group.databricks_logs[org_name].name
    }
  }
}

# Environment Information
output "environment" {
  description = "Environment name"
  value       = var.environment
}

output "project_name" {
  description = "Project name"
  value       = var.project_name
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "aws_account_id" {
  description = "AWS account ID"
  value       = data.aws_caller_identity.current.account_id
}

# Kubernetes Configuration
output "kubectl_config" {
  description = "kubectl config command"
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${aws_eks_cluster.main.name}"
}

# Connection Strings (for application configuration)
output "database_url" {
  description = "Database connection URL"
  value       = "postgresql://${aws_db_instance.main.username}:${var.db_password}@${aws_db_instance.main.endpoint}/${aws_db_instance.main.db_name}"
  sensitive   = true
}

output "redis_url" {
  description = "Redis connection URL"
  value       = "redis://${aws_elasticache_replication_group.main.primary_endpoint_address}:${aws_elasticache_replication_group.main.port}"
  sensitive   = true
}

# Application URLs
output "application_url" {
  description = "Main application URL"
  value       = "https://${aws_lb.main.dns_name}"
}

output "api_base_url" {
  description = "API base URL"
  value       = "https://${aws_lb.main.dns_name}/api/v1"
}

# Monitoring URLs
output "grafana_url" {
  description = "Grafana dashboard URL (when deployed)"
  value       = "https://${aws_lb.main.dns_name}/grafana"
}

output "prometheus_url" {
  description = "Prometheus URL (when deployed)"
  value       = "https://${aws_lb.main.dns_name}/prometheus"
}

# Deployment Information
output "deployment_timestamp" {
  description = "Timestamp of deployment"
  value       = timestamp()
}

output "terraform_version" {
  description = "Terraform version used for deployment"
  value       = "1.6.0"
}

# Cost Allocation Tags
output "cost_allocation_tags" {
  description = "Tags for cost allocation"
  value = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Timestamp   = timestamp()
  }
}

# Security Information
output "security_groups" {
  description = "Security group IDs"
  value = {
    eks_cluster = aws_security_group.eks_cluster.id
    eks_nodes   = aws_security_group.eks_nodes.id
    rds         = aws_security_group.rds.id
    redis       = aws_security_group.redis.id
    alb         = aws_security_group.alb.id
  }
}

# OIDC Provider Information
output "eks_oidc_issuer_url" {
  description = "The URL on the EKS cluster OIDC Issuer"
  value       = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

output "eks_oidc_provider_arn" {
  description = "The ARN of the OIDC Provider for EKS"
  value       = aws_iam_openid_connect_provider.eks.arn
}

# Compliance and Governance
output "compliance_features" {
  description = "Enabled compliance features by organization"
  value = {
    for org_name, org_config in var.organizations : org_name => var.compliance_requirements[org_name]
  }
}

output "data_retention_policies" {
  description = "Data retention policies by organization"
  value = {
    for org_name, org_config in var.organizations : org_name => var.data_retention_policy[org_name]
  }
}

# Resource Quotas
output "resource_quotas" {
  description = "Resource quotas by organization"
  value = {
    for org_name, org_config in var.organizations : org_name => org_config.resource_quotas
  }
}

# Backup Information
output "backup_configuration" {
  description = "Backup configuration"
  value = {
    rds_backup_retention_period = aws_db_instance.main.backup_retention_period
    rds_backup_window          = aws_db_instance.main.backup_window
    rds_maintenance_window     = aws_db_instance.main.maintenance_window
    s3_versioning_enabled      = true
  }
}

# Networking Information
output "availability_zones" {
  description = "Availability zones used"
  value       = data.aws_availability_zones.available.names
}

output "nat_gateway_ips" {
  description = "Public IP addresses of NAT gateways"
  value       = aws_eip.nat[*].public_ip
}

# Feature Flags
output "feature_flags" {
  description = "Enabled feature flags"
  value = {
    unity_catalog_enabled = var.enable_unity_catalog
    iceberg_enabled      = var.enable_iceberg
    data_quality_enabled = var.enable_data_quality
    monitoring_enabled   = var.enable_monitoring
  }
}

# Service Discovery
output "service_discovery" {
  description = "Service discovery information"
  value = {
    cluster_name      = aws_eks_cluster.main.name
    namespace         = "multi-tenant-ingestion"
    service_mesh      = "istio"  # If using service mesh
    dns_domain        = "${var.project_name}.local"
  }
}