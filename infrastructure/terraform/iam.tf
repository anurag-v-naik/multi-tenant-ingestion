# infrastructure/terraform/iam.tf
# IAM Roles and Policies for Multi-Tenant Ingestion Framework

# EKS Cluster Service Role
resource "aws_iam_role" "eks_cluster" {
  name = "${var.project_name}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-eks-cluster-role"
  }
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

resource "aws_iam_role_policy_attachment" "eks_service_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
  role       = aws_iam_role.eks_cluster.name
}

# EKS Node Group Role
resource "aws_iam_role" "eks_nodes" {
  name = "${var.project_name}-eks-nodes-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-eks-nodes-role"
  }
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_container_registry_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes.name
}

# Service Account Role for Multi-Tenant Applications
resource "aws_iam_role" "service_account" {
  name = "${var.project_name}-service-account-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        }
        Condition = {
          StringEquals = {
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:default:multi-tenant-service-account"
            "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-service-account-role"
  }
}

# OIDC Provider for EKS
data "tls_certificate" "eks" {
  url = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.main.identity[0].oidc[0].issuer

  tags = {
    Name = "${var.project_name}-eks-oidc"
  }
}

# Multi-Tenant S3 Access Policy
resource "aws_iam_policy" "s3_multi_tenant_access" {
  name        = "${var.project_name}-s3-multi-tenant-access"
  description = "Policy for multi-tenant S3 access with tenant isolation"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          for bucket in aws_s3_bucket.tenant_data : bucket.arn
        ]
        Condition = {
          StringEquals = {
            "s3:prefix" = ["$${aws:userid}/"]
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutObjectAcl"
        ]
        Resource = [
          for bucket in aws_s3_bucket.tenant_data : "${bucket.arn}/*"
        ]
        Condition = {
          StringLike = {
            "s3:ExistingObjectTag/TenantId" = ["$${aws:userid}"]
          }
        }
      }
    ]
  })
}

# Secrets Manager Access Policy
resource "aws_iam_policy" "secrets_manager_access" {
  name        = "${var.project_name}-secrets-manager-access"
  description = "Policy for accessing application secrets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          aws_secretsmanager_secret.app_secrets.arn,
          "${aws_secretsmanager_secret.app_secrets.arn}:*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          for secret in aws_secretsmanager_secret.tenant_secrets : secret.arn
        ]
        Condition = {
          StringEquals = {
            "secretsmanager:ResourceTag/Organization" = ["$${aws:userid}"]
          }
        }
      }
    ]
  })
}

# CloudWatch Logs Policy
resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${var.project_name}-cloudwatch-logs"
  description = "Policy for CloudWatch logs access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          aws_cloudwatch_log_group.app_logs.arn,
          "${aws_cloudwatch_log_group.app_logs.arn}:*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          for log_group in aws_cloudwatch_log_group.databricks_logs : "${log_group.arn}:*"
        ]
        Condition = {
          StringEquals = {
            "logs:ResourceTag/Organization" = ["$${aws:userid}"]
          }
        }
      }
    ]
  })
}

# Databricks Integration Policy
resource "aws_iam_policy" "databricks_integration" {
  name        = "${var.project_name}-databricks-integration"
  description = "Policy for Databricks integration"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sts:AssumeRole"
        ]
        Resource = [
          for org in var.organizations : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-databricks-${org}-role"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:AssociateIamInstanceProfile",
          "ec2:AttachVolume",
          "ec2:CreateTags",
          "ec2:CreateVolume",
          "ec2:DeleteVolume",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeImages",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeVolumes",
          "ec2:DetachVolume",
          "ec2:DisassociateIamInstanceProfile",
          "ec2:ModifyInstanceAttribute",
          "ec2:ReplaceIamInstanceProfileAssociation",
          "ec2:RunInstances",
          "ec2:TerminateInstances"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "ec2:InstanceProfile" = [
              for org in var.organizations : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:instance-profile/${var.project_name}-databricks-${org}-instance-profile"
            ]
          }
        }
      }
    ]
  })
}

# Attach policies to service account role
resource "aws_iam_role_policy_attachment" "service_account_s3" {
  policy_arn = aws_iam_policy.s3_multi_tenant_access.arn
  role       = aws_iam_role.service_account.name
}

resource "aws_iam_role_policy_attachment" "service_account_secrets" {
  policy_arn = aws_iam_policy.secrets_manager_access.arn
  role       = aws_iam_role.service_account.name
}

resource "aws_iam_role_policy_attachment" "service_account_logs" {
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
  role       = aws_iam_role.service_account.name
}

resource "aws_iam_role_policy_attachment" "service_account_databricks" {
  policy_arn = aws_iam_policy.databricks_integration.arn
  role       = aws_iam_role.service_account.name
}

# Tenant-specific Databricks IAM Roles
resource "aws_iam_role" "databricks_tenant" {
  for_each = var.organizations

  name = "${var.project_name}-databricks-${each.key}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"  # Databricks AWS account
        }
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_external_id
          }
        }
      }
    ]
  })

  tags = {
    Name         = "${var.project_name}-databricks-${each.key}-role"
    Organization = each.key
  }
}

# Tenant-specific S3 access policy for Databricks
resource "aws_iam_policy" "databricks_tenant_s3" {
  for_each = var.organizations

  name        = "${var.project_name}-databricks-${each.key}-s3"
  description = "S3 access policy for ${each.key} Databricks workspace"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.tenant_data[each.key].arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${aws_s3_bucket.tenant_data[each.key].arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "databricks_tenant_s3" {
  for_each = var.organizations

  policy_arn = aws_iam_policy.databricks_tenant_s3[each.key].arn
  role       = aws_iam_role.databricks_tenant[each.key].name
}

# Instance profiles for Databricks clusters
resource "aws_iam_instance_profile" "databricks_tenant" {
  for_each = var.organizations

  name = "${var.project_name}-databricks-${each.key}-instance-profile"
  role = aws_iam_role.databricks_tenant[each.key].name

  tags = {
    Name         = "${var.project_name}-databricks-${each.key}-instance-profile"
    Organization = each.key
  }
}