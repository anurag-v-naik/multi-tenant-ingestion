# Multi-Tenant Data Ingestion Framework - Deployment Guide

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Infrastructure Setup](#infrastructure-setup)
3. [Application Deployment](#application-deployment)
4. [Tenant Configuration](#tenant-configuration)
5. [Verification and Testing](#verification-and-testing)
6. [Production Deployment](#production-deployment)
7. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Tools and Accounts

#### AWS Account Setup
- AWS Account with administrative access
- AWS CLI configured with appropriate credentials
- AWS region selected (recommended: us-east-1, us-west-2, eu-west-1)

#### Databricks Account
- Databricks Premium or Enterprise tier account
- Account ID and workspace URL
- Personal access token with cluster management permissions

#### Local Development Tools
```bash
# Install required tools
# Terraform
curl -O https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
unzip terraform_1.6.0_linux_amd64.zip
sudo mv terraform /usr/local/bin/

# AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Docker
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker $USER
```

#### Programming Languages and Runtimes
```bash
# Python 3.9+
sudo apt-get install python3.9 python3.9-pip python3.9-venv

# Node.js 16+
curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash -
sudo apt-get install -y nodejs

# Verify installations
terraform --version
aws --version
kubectl version --client
helm version
docker --version
python3.9 --version
node --version
```

### Environment Variables Setup

Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.example .env

# Edit with your specific values
nano .env
```

Required environment variables:
```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012
TERRAFORM_STATE_BUCKET=your-terraform-state-bucket

# Databricks Configuration
DATABRICKS_ACCOUNT_ID=your-databricks-account-id
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi-your-token-here
DATABRICKS_EXTERNAL_ID=your-external-id

# Database Configuration
DB_PASSWORD=your-secure-database-password
JWT_SECRET_KEY=your-jwt-secret-key-min-32-chars

# Environment
ENVIRONMENT=development  # or staging, production
PROJECT_NAME=multi-tenant-ingestion
```

## Infrastructure Setup

### Step 1: Prepare Terraform Backend

Create S3 bucket for Terraform state:
```bash
# Create bucket for Terraform state
aws s3 mb s3://your-terraform-state-bucket-name

# Enable versioning
aws s3api put-bucket-versioning \
    --bucket your-terraform-state-bucket-name \
    --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
    --bucket your-terraform-state-bucket-name \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }]
    }'
```

### Step 2: Configure Terraform Variables

```bash
# Navigate to terraform directory
cd infrastructure/terraform

# Copy example variables file
cp terraform.tfvars.example terraform.tfvars

# Edit with your configuration
nano terraform.tfvars
```

Example `terraform.tfvars`:
```hcl
# Basic Configuration
project_name = "multi-tenant-ingestion"
environment = "development"
aws_region = "us-east-1"
terraform_state_bucket = "your-terraform-state-bucket"

# Network Configuration
vpc_cidr = "10.0.0.0/16"
public_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
private_subnet_cidrs = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

# Database Configuration
db_password = "your-secure-password-here"
db_instance_class = "db.t3.medium"
db_allocated_storage = 100

# Databricks Configuration
databricks_account_id = "your-account-id"
databricks_workspace_url = "https://your-workspace.cloud.databricks.com"
databricks_token = "dapi-your-token-here"
databricks_external_id = "your-external-id"

# Security
jwt_secret_key = "your-jwt-secret-key-minimum-32-characters"

# Organizations Configuration
organizations = {
  finance = {
    name = "finance"
    display_name = "Finance Department"
    cost_center = "FIN-001"
    compliance_level = "high"
    resource_quotas = {
      max_dbu_per_hour = 100
      max_storage_gb = 10000
      max_api_calls_per_minute = 1000
    }
  }
  retail = {
    name = "retail"
    display_name = "Retail Division"
    cost_center = "RET-001"
    compliance_level = "medium"
    resource_quotas = {
      max_dbu_per_hour = 200
      max_storage_gb = 50000
      max_api_calls_per_minute = 2000
    }
  }
}
```

### Step 3: Deploy Infrastructure

```bash
# Initialize Terraform
terraform init

# Validate configuration
terraform validate

# Plan deployment
terraform plan -out=tfplan

# Review the plan carefully
# Apply infrastructure
terraform apply tfplan

# Save outputs for later use
terraform output > ../outputs.txt
```

### Step 4: Configure kubectl for EKS

```bash
# Update kubeconfig
aws eks update-kubeconfig --region $AWS_REGION --name multi-tenant-ingestion-cluster

# Verify connection
kubectl cluster-info
kubectl get nodes
```

## Application Deployment

### Step 1: Build Docker Images

```bash
# Return to project root
cd ../../

# Build all service images
make build-images

# Or build individually
docker build -t multi-tenant-ingestion/pipeline-service:latest services/pipeline-service/
docker build -t multi-tenant-ingestion/catalog-service:latest services/catalog-service/
docker build -t multi-tenant-ingestion/connector-service:latest services/connector-service/
docker build -t multi-tenant-ingestion/data-quality-service:latest services/data-quality-service/
docker build -t multi-tenant-ingestion/ui:latest services/ui/
```

### Step 2: Push Images to ECR

```bash
# Get ECR login token
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Create ECR repositories
for service in pipeline-service catalog-service connector-service data-quality-service ui; do
    aws ecr create-repository --repository-name multi-tenant-ingestion/$service --region $AWS_REGION || true
done

# Tag and push images
for service in pipeline-service catalog-service connector-service data-quality-service ui; do
    docker tag multi-tenant-ingestion/$service:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/multi-tenant-ingestion/$service:latest
    docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/multi-tenant-ingestion/$service:latest
done
```

### Step 3: Deploy with Helm

```bash
# Navigate to Helm charts
cd deployment/helm-charts

# Update dependencies
helm dependency update multi-tenant-ingestion/

# Create namespace
kubectl create namespace multi-tenant-ingestion

# Deploy application
helm install multi-tenant-ingestion ./multi-tenant-ingestion \
    --namespace multi-tenant-ingestion \
    --values values-development.yaml \
    --set image.registry=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com \
    --set database.host=$(terraform -chdir=../../infrastructure/terraform output -raw rds_endpoint) \
    --set redis.host=$(terraform -chdir=../../infrastructure/terraform output -raw redis_endpoint)
```

### Step 4: Setup Ingress and Load Balancer

```bash
# Install AWS Load Balancer Controller
kubectl apply -k "github.com/aws/eks-charts/stable/aws-load-balancer-controller//crds?ref=master"

helm repo add eks https://aws.github.io/eks-charts
helm repo update

helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
    -n kube-system \
    --set clusterName=multi-tenant-ingestion-cluster \
    --set serviceAccount.create=false \
    --set serviceAccount.name=aws-load-balancer-controller

# Deploy ingress
kubectl apply -f ../../infrastructure/kubernetes/ingress/
```

## Tenant Configuration

### Step 1: Initialize Database Schema

```bash
# Run database migrations
kubectl exec -it deployment/pipeline-service -n multi-tenant-ingestion -- python -m alembic upgrade head

# Seed initial data
kubectl exec -it deployment/pipeline-service -n multi-tenant-ingestion -- python scripts/seed_database.py
```

### Step 2: Setup Databricks Workspaces

```bash
# Run workspace setup script for each organization
./deployment/scripts/setup-organization.sh finance
./deployment/scripts/setup-organization.sh retail
./deployment/scripts/setup-organization.sh healthcare

# Verify workspace creation
curl -X GET https://$DATABRICKS_WORKSPACE_URL/api/2.0/clusters/list \
    -H "Authorization: Bearer $DATABRICKS_TOKEN"
```

### Step 3: Configure Unity Catalog

```bash
# Setup Unity Catalog for each tenant
kubectl exec -it deployment/catalog-service -n multi-tenant-ingestion -- python scripts/setup_unity_catalog.py --org finance
kubectl exec -it deployment/catalog-service -n multi-tenant-ingestion -- python scripts/setup_unity_catalog.py --org retail
```

### Step 4: Create Initial Pipelines

```bash
# Create sample pipelines from examples
kubectl exec -it deployment/pipeline-service -n multi-tenant-ingestion -- python scripts/create_sample_pipelines.py

# Verify pipeline creation
curl -X GET http://$(terraform -chdir=infrastructure/terraform output -raw load_balancer_dns)/api/v1/pipelines \
    -H "Authorization: Bearer $(./scripts/get-test-token.sh finance)"
```

## Verification and Testing

### Health Checks

```bash
# Check service health
kubectl get pods -n multi-tenant-ingestion

# Check application health endpoints
LB_DNS=$(terraform -chdir=infrastructure/terraform output -raw load_balancer_dns)

curl http://$LB_DNS/health
curl http://$LB_DNS/health/database
curl http://$LB_DNS/health/databricks
```

### Integration Tests

```bash
# Run integration test suite
cd tests/
python -m pytest integration/ -v

# Run tenant isolation tests
python -m pytest integration/test_tenant_isolation.py -v

# Run Databricks integration tests
python -m pytest integration/test_databricks.py -v
```

### Load Testing

```bash
# Install load testing tools
pip install locust

# Run load tests
cd tests/load/
locust -f pipeline_load_test.py --host http://$LB_DNS

# Monitor performance during load test
kubectl top pods -n multi-tenant-ingestion
```

### Data Flow Testing

```bash
# Test data ingestion pipeline
python tests/integration/test_data_flow.py

# Test cross-platform data access
python tests/integration/test_iceberg_integration.py

# Test data quality validation
python tests/integration/test_data_quality.py
```

## Production Deployment

### Pre-Production Checklist

- [ ] Security scan completed
- [ ] Performance testing passed
- [ ] Backup and recovery tested
- [ ] Monitoring and alerting configured
- [ ] Documentation updated
- [ ] Team training completed

### Production Configuration

```bash
# Update configuration for production
cp deployment/configs/production.env .env

# Update Terraform variables
cp infrastructure/terraform/terraform.tfvars.production infrastructure/terraform/terraform.tfvars

# Enable production features
terraform apply -var="environment=production" -var="enable_deletion_protection=true"
```

### Security Hardening

```bash
# Enable additional security features
kubectl apply -f infrastructure/kubernetes/security/

# Configure network policies
kubectl apply -f infrastructure/kubernetes/network-policies/

# Setup pod security standards
kubectl apply -f infrastructure/kubernetes/pod-security-standards/
```

### Monitoring Setup

```bash
# Deploy monitoring stack
helm install monitoring deployment/helm-charts/monitoring/

# Configure Grafana dashboards
kubectl apply -f infrastructure/kubernetes/monitoring/dashboards/

# Setup alerting rules
kubectl apply -f infrastructure/kubernetes/monitoring/alerts/
```

### Backup Configuration

```bash
# Enable automated backups
./deployment/scripts/setup-backups.sh

# Test backup restoration
./deployment/scripts/test-backup-restore.sh
```

## Troubleshooting

### Common Issues

#### 1. EKS Cluster Access Issues
```bash
# Update kubeconfig
aws eks update-kubeconfig --region $AWS_REGION --name multi-tenant-ingestion-cluster

# Check IAM permissions
aws sts get-caller-identity

# Verify cluster status
aws eks describe-cluster --name multi-tenant-ingestion-cluster
```

#### 2. Database Connection Issues
```bash
# Check RDS instance status
aws rds describe-db-instances --db-instance-identifier multi-tenant-ingestion-db

# Test database connectivity
kubectl exec -it deployment/pipeline-service -n multi-tenant-ingestion -- python -c "
import psycopg2
conn = psycopg2.connect(host='$DB_HOST', database='$DB_NAME', user='$DB_USER', password='$DB_PASSWORD')
print('Database connection successful')
"
```

#### 3. Databricks Integration Issues
```bash
# Verify Databricks token
curl -X GET https://$DATABRICKS_WORKSPACE_URL/api/2.0/clusters/list \
    -H "Authorization: Bearer $DATABRICKS_TOKEN"

# Check workspace permissions
curl -X GET https://$DATABRICKS_WORKSPACE_URL/api/2.0/permissions/clusters \
    -H "Authorization: Bearer $DATABRICKS_TOKEN"
```

#### 4. Service Discovery Issues
```bash
# Check DNS resolution
kubectl exec -it deployment/pipeline-service -n multi-tenant-ingestion -- nslookup catalog-service

# Check service endpoints
kubectl get endpoints -n multi-tenant-ingestion

# Check ingress configuration
kubectl describe ingress -n multi-tenant-ingestion
```

### Debugging Commands

```bash
# Check pod logs
kubectl logs -f deployment/pipeline-service -n multi-