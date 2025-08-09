#!/bin/bash

set -e

# Multi-Tenant Ingestion Framework Deployment Script

# Default values
ENVIRONMENT="development"
BUILD_IMAGES=false
DEPLOY_SERVICES=false
RUN_MIGRATIONS=false
SKIP_TESTS=false
FORCE_DEPLOY=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat << EOF
Multi-Tenant Ingestion Framework Deployment Script

Usage: $0 [OPTIONS]

Options:
    --environment ENV          Set deployment environment (development|staging|production)
    --build-images             Build Docker images before deployment
    --deploy-services          Deploy services to Kubernetes
    --run-migrations           Run database migrations
    --skip-tests              Skip running tests
    --force                   Force deployment without confirmation
    --help                    Show this help message

Examples:
    $0 --environment=staging --build-images --deploy-services
    $0 --environment=production --run-migrations --deploy-services
    $0 --build-images --deploy-services --run-migrations

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --environment=*)
            ENVIRONMENT="${1#*=}"
            shift
            ;;
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
        --force)
            FORCE_DEPLOY=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|staging|production)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT"
    log_error "Valid environments: development, staging, production"
    exit 1
fi

log_info "Starting deployment for environment: $ENVIRONMENT"

# Load environment variables
if [[ -f "deployment/configs/${ENVIRONMENT}.env" ]]; then
    log_info "Loading environment configuration..."
    source "deployment/configs/${ENVIRONMENT}.env"
else
    log_warning "Environment config file not found: deployment/configs/${ENVIRONMENT}.env"
fi

# Validate required environment variables
required_vars=("AWS_REGION" "AWS_ACCOUNT_ID" "PROJECT_NAME")
for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        log_error "Required environment variable not set: $var"
        exit 1
    fi
done

# Check prerequisites
log_info "Checking prerequisites..."

command -v docker >/dev/null 2>&1 || { log_error "Docker is required but not installed."; exit 1; }
command -v kubectl >/dev/null 2>&1 || { log_error "kubectl is required but not installed."; exit 1; }
command -v aws >/dev/null 2>&1 || { log_error "AWS CLI is required but not installed."; exit 1; }

# Check AWS credentials
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    log_error "AWS credentials not configured or invalid"
    exit 1
fi

# Check kubectl cluster access
if ! kubectl cluster-info >/dev/null 2>&1; then
    log_error "Cannot connect to Kubernetes cluster"
    exit 1
fi

log_success "Prerequisites check passed"

# Confirmation for production
if [[ "$ENVIRONMENT" == "production" && "$FORCE_DEPLOY" != "true" ]]; then
    log_warning "You are about to deploy to PRODUCTION environment!"
    read -p "Are you sure? Type 'yes' to continue: " confirm
    if [[ "$confirm" != "yes" ]]; then
        log_info "Deployment cancelled"
        exit 0
    fi
fi

# Run tests unless skipped
if [[ "$SKIP_TESTS" != "true" ]]; then
    log_info "Running tests..."
    if ! make test; then
        log_error "Tests failed. Use --skip-tests to override."
        exit 1
    fi
    log_success "All tests passed"
fi

# Build images if requested
if [[ "$BUILD_IMAGES" == "true" ]]; then
    log_info "Building Docker images..."

    # Get ECR registry URL
    ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

    # Login to ECR
    log_info "Logging in to ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"

    # Create ECR repositories if they don't exist
    services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service" "ui")
    for service in "${services[@]}"; do
        if ! aws ecr describe-repositories --repository-names "${PROJECT_NAME}/${service}" --region "$AWS_REGION" >/dev/null 2>&1; then
            log_info "Creating ECR repository for ${service}..."
            aws ecr create-repository --repository-name "${PROJECT_NAME}/${service}" --region "$AWS_REGION"
        fi
    done

    # Build and push images
    if ! make build; then
        log_error "Failed to build images"
        exit 1
    fi

    if ! make push-images; then
        log_error "Failed to push images"
        exit 1
    fi

    log_success "Images built and pushed successfully"
fi

# Run database migrations if requested
if [[ "$RUN_MIGRATIONS" == "true" ]]; then
    log_info "Running database migrations..."

    # Get database connection details from Terraform outputs
    DB_ENDPOINT=$(terraform -chdir=infrastructure/terraform output -raw rds_endpoint 2>/dev/null || echo "localhost:5432")

    # Run migrations using kubectl exec if in cluster, otherwise locally
    if kubectl get pods -n multi-tenant-ingestion -l app=pipeline-service >/dev/null 2>&1; then
        log_info "Running migrations in cluster..."
        kubectl exec -n multi-tenant-ingestion deployment/pipeline-service -- python -m alembic upgrade head
    else
        log_info "Running migrations locally..."
        cd services/pipeline-service && python -m alembic upgrade head
        cd ../../
    fi

    log_success "Database migrations completed"
fi

# Deploy services if requested
if [[ "$DEPLOY_SERVICES" == "true" ]]; then
    log_info "Deploying services to Kubernetes..."

    # Update image tags in manifests
    ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

    # Create temporary directory for processed manifests
    TEMP_DIR=$(mktemp -d)
    cp -r infrastructure/kubernetes/* "$TEMP_DIR/"

    # Replace placeholders in manifests
    find "$TEMP_DIR" -name "*.yaml" -type f -exec sed -i.bak \
        -e "s/ACCOUNT_ID/${AWS_ACCOUNT_ID}/g" \
        -e "s/REGION/${AWS_REGION}/g" \
        -e "s/REPLACE_WITH_SERVICE_ACCOUNT_ROLE_ARN/$(terraform -chdir=infrastructure/terraform output -raw service_account_role_arn 2>/dev/null || echo 'PLACEHOLDER')/g" \
        {} \;

    # Deploy in order
    kubectl apply -f "$TEMP_DIR/namespaces/"
    kubectl apply -f "$TEMP_DIR/configmaps/"
    kubectl apply -f "$TEMP_DIR/secrets/"
    kubectl apply -f "$TEMP_DIR/rbac/"
    kubectl apply -f "$TEMP_DIR/deployments/"
    kubectl apply -f "$TEMP_DIR/services/"
    kubectl apply -f "$TEMP_DIR/hpa/"
    kubectl apply -f "$TEMP_DIR/pod-disruption-budgets/"
    kubectl apply -f "$TEMP_DIR/network-policies/"
    kubectl apply -f "$TEMP_DIR/ingress/"

    # Wait for rollout
    log_info "Waiting for deployment rollout..."
    kubectl rollout status deployment/pipeline-service -n multi-tenant-ingestion --timeout=300s
    kubectl rollout status deployment/catalog-service -n multi-tenant-ingestion --timeout=300s
    kubectl rollout status deployment/connector-service -n multi-tenant-ingestion --timeout=300s
    kubectl rollout status deployment/data-quality-service -n multi-tenant-ingestion --timeout=300s
    kubectl rollout status deployment/ui -n multi-tenant-ingestion --timeout=300s

    # Cleanup temporary directory
    rm -rf "$TEMP_DIR"

    log_success "Services deployed successfully"
fi

# Final status check
log_info "Checking deployment status..."
kubectl get pods -n multi-tenant-ingestion
kubectl get services -n multi-tenant-ingestion
kubectl get ingress -n multi-tenant-ingestion

# Get application URL
if kubectl get ingress multi-tenant-ingress -n multi-tenant-ingestion >/dev/null 2>&1; then
    APP_URL=$(kubectl get ingress multi-tenant-ingress -n multi-tenant-ingestion -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "pending")
    if [[ "$APP_URL" != "pending" && "$APP_URL" != "" ]]; then
        log_success "Application URL: https://$APP_URL"
    else
        log_info "Application URL is still being provisioned. Check again in a few minutes."
    fi
fi

log_success "Deployment completed successfully!"

log_info "Next steps:"
log_info "1. Verify all pods are running: kubectl get pods -n multi-tenant-ingestion"
log_info "2. Check service health: kubectl logs -f deployment/pipeline-service -n multi-tenant-ingestion"
log_info "3. Access the application through the load balancer URL"
