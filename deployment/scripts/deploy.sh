#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="staging"
BUILD_IMAGES=false
DEPLOY_SERVICES=false
RUN_MIGRATIONS=false
SKIP_TESTS=false

# Usage function
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --environment ENV     Environment to deploy to (staging|production) [default: staging]"
    echo "  --build-images        Build Docker images before deployment"
    echo "  --deploy-services     Deploy services to ECS"
    echo "  --run-migrations      Run database migrations"
    echo "  --skip-tests          Skip running tests before deployment"
    echo "  --help                Show this help message"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --build-images)
            BUILD_IMAGES=true
            shift
            ;;
        --deploy-services)
            DEPLOY_SERVICES=true
            shift
            ;;
        --run-migrations)
            RUN_MIGRATIONS=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(staging|production)$ ]]; then
    echo -e "${RED}Error: Environment must be 'staging' or 'production'${NC}"
    exit 1
fi

echo -e "${BLUE}🚀 Starting deployment to ${ENVIRONMENT}${NC}"

# Check prerequisites
echo -e "${BLUE}🔍 Checking prerequisites...${NC}"

if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not found${NC}"
    exit 1
fi

if ! command -v terraform &> /dev/null; then
    echo -e "${RED}Error: Terraform not found${NC}"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker not found${NC}"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# Run tests (unless skipped)
if [[ "$SKIP_TESTS" == false ]]; then
    echo -e "${BLUE}🧪 Running tests...${NC}"
    
    # Run Python tests
    if ! pytest tests/ -v --tb=short; then
        echo -e "${RED}❌ Tests failed${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Tests passed${NC}"
fi

# Build Docker images
if [[ "$BUILD_IMAGES" == true ]]; then
    echo -e "${BLUE}🐳 Building Docker images...${NC}"
    
    # Get ECR registry URL
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION:-us-east-1}.amazonaws.com"
    
    # Login to ECR
    aws ecr get-login-password --region ${AWS_REGION:-us-east-1} | docker login --username AWS --password-stdin $ECR_REGISTRY
    
    # Build and push images
    services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service" "ui")
    
    for service in "${services[@]}"; do
        echo -e "${BLUE}Building ${service}...${NC}"
        
        # Build image
        docker build -t multi-tenant-${service}:latest services/${service}/
        
        # Tag for ECR
        docker tag multi-tenant-${service}:latest $ECR_REGISTRY/multi-tenant-${service}:latest
        docker tag multi-tenant-${service}:latest $ECR_REGISTRY/multi-tenant-${service}:$(git rev-parse --short HEAD)
        
        # Push to ECR
        docker push $ECR_REGISTRY/multi-tenant-${service}:latest
        docker push $ECR_REGISTRY/multi-tenant-${service}:$(git rev-parse --short HEAD)
        
        echo -e "${GREEN}✅ ${service} built and pushed${NC}"
    done
fi

# Deploy infrastructure with Terraform
echo -e "${BLUE}🏗️  Deploying infrastructure...${NC}"

cd infrastructure/terraform

# Initialize Terraform
terraform init

# Plan deployment
echo -e "${BLUE}Planning Terraform changes...${NC}"
terraform plan \
    -var="environment=${ENVIRONMENT}" \
    -var-file="${ENVIRONMENT}.tfvars" \
    -out=tfplan

# Apply changes
echo -e "${BLUE}Applying Terraform changes...${NC}"
terraform apply tfplan

# Get outputs
ECS_CLUSTER_NAME=$(terraform output -raw ecs_cluster_name)
LOAD_BALANCER_DNS=$(terraform output -raw load_balancer_dns)

cd ../..

# Run database migrations
if [[ "$RUN_MIGRATIONS" == true ]]; then
    echo -e "${BLUE}🗄️  Running database migrations...${NC}"
    
    # Get database connection details from Terraform outputs or AWS Secrets Manager
    DATABASE_SECRET_ARN=$(terraform output -raw database_secret_arn)
    DATABASE_CREDENTIALS=$(aws secretsmanager get-secret-value --secret-id $DATABASE_SECRET_ARN --query SecretString --output text)
    
    # Extract database URL
    DATABASE_URL=$(echo $DATABASE_CREDENTIALS | jq -r '.host + ":" + (.port | tostring)')
    
    # Run Alembic migrations
    alembic upgrade head
    
    echo -e "${GREEN}✅ Database migrations completed${NC}"
fi

# Deploy services to ECS
if [[ "$DEPLOY_SERVICES" == true ]]; then
    echo -e "${BLUE}🚢 Deploying services to ECS...${NC}"
    
    services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service")
    
    for service in "${services[@]}"; do
        echo -e "${BLUE}Deploying ${service}...${NC}"
        
        aws ecs update-service \
            --cluster $ECS_CLUSTER_NAME \
            --service $service \
            --force-new-deployment \
            --region ${AWS_REGION:-us-east-1}
    done
    
    # Wait for services to stabilize
    echo -e "${BLUE}Waiting for services to stabilize...${NC}"
    
    aws ecs wait services-stable \
        --cluster $ECS_CLUSTER_NAME \
        --services "${services[@]}" \
        --region ${AWS_REGION:-us-east-1}
    
    echo -e "${GREEN}✅ Services deployed successfully${NC}"
fi

# Health checks
echo -e "${BLUE}🏥 Running health checks...${NC}"

# Wait for services to be ready
sleep 30

# Check health endpoints
if curl -f "http://${LOAD_BALANCER_DNS}/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Main health check passed${NC}"
else
    echo -e "${RED}❌ Main health check failed${NC}"
    exit 1
fi

# Check individual service health
services=("pipelines" "catalog" "connectors" "data-quality")
for service in "${services[@]}"; do
    if curl -f "http://${LOAD_BALANCER_DNS}/api/v1/${service}/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ ${service} service health check passed${NC}"
    else
        echo -e "${YELLOW}⚠️  ${service} service health check failed${NC}"
    fi
done

# Deployment summary
echo -e "${BLUE}📋 Deployment Summary${NC}"
echo -e "Environment: ${ENVIRONMENT}"
echo -e "Load Balancer: http://${LOAD_BALANCER_DNS}"
echo -e "ECS Cluster: ${ECS_CLUSTER_NAME}"
echo -e "Git Commit: $(git rev-parse --short HEAD)"
echo -e "Deployed by: $(git config user.name)"
echo -e "Deployment time: $(date)"

echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"

# Save deployment info
cat > deployment-info.json << EOF
{
  "environment": "${ENVIRONMENT}",
  "load_balancer_dns": "${LOAD_BALANCER_DNS}",
  "ecs_cluster_name": "${ECS_CLUSTER_NAME}",
  "git_commit": "$(git rev-parse HEAD)",
  "git_commit_short": "$(git rev-parse --short HEAD)",
  "deployed_by": "$(git config user.name)",
  "deployment_time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "services_deployed": $(echo "${services[@]}" | jq -R 'split(" ")')
}
EOF

echo -e "${BLUE}💾 Deployment info saved to deployment-info.json${NC}"
