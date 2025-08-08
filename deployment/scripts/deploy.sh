#!/bin/bash

# Multi-Tenant Data Ingestion Framework - Deployment Script
# This script deploys the entire multi-tenant ingestion framework

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Default values
ENVIRONMENT="development"
BUILD_IMAGES=false
DEPLOY_SERVICES=false
RUN_MIGRATIONS=false
SKIP_INFRASTRUCTURE=false
SKIP_KUBERNETES=false
VERBOSE=false
DRY_RUN=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Multi-Tenant Data Ingestion Framework Deployment Script

Usage: $0 [OPTIONS]

OPTIONS:
    --environment ENV          Target environment (development|staging|production) [default: development]
    --build-images            Build Docker images before deployment
    --deploy-services         Deploy services to Kubernetes
    --run-migrations          Run database migrations
    --skip-infrastructure     Skip Terraform infrastructure deployment
    --skip-kubernetes         Skip Kubernetes resource deployment
    --verbose                 Enable verbose output
    --dry-run                 Show what would be deployed without executing
    --help                    Show this help message

EXAMPLES:
    # Full deployment for development
    $0 --environment development --build-images --deploy-services --run-migrations

    # Production deployment (skip infrastructure if already exists)
    $0 --environment production --skip-infrastructure --deploy-services

    # Dry run to see what would be deployed
    $0 --environment staging --dry-run --deploy-services

    # Build and deploy only
    $0 --build-images --deploy-services

EOF
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
        --skip-infrastructure)
            SKIP_INFRASTRUCTURE=true
            shift
            ;;
        --skip-kubernetes)
            SKIP_KUBERNETES=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|staging|production)$ ]]; then
    log_error "Invalid environment: $ENVIRONMENT. Must be development, staging, or production."
    exit 1
fi

# Set verbose mode
if [[ "$VERBOSE" == "true" ]]; then
    set -x
fi

# Load environment configuration
ENV_FILE="${ROOT_DIR}/deployment/configs/${ENVIRONMENT}.env"
if [[ -f "$ENV_FILE" ]]; then
    log_info "Loading environment configuration from $ENV_FILE"
    # shellcheck source=/dev/null
    source "$ENV_FILE"
else
    log_warn "Environment file not found: $ENV_FILE"
fi

# Validate required tools
check_dependencies() {
    log_info "Checking dependencies..."

    local missing_tools=()

    # Check for required tools
    for tool in aws kubectl terraform docker helm; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done

    if [[ ${#missing_tools[@]} -ne 0 ]]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again."
        exit 1
    fi

    log_success "All dependencies are available"
}

# Validate AWS credentials
check_aws_credentials() {
    log_info "Checking AWS credentials..."

    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or invalid"
        log_error "Please run 'aws configure' or set AWS environment variables"
        exit 1
    fi

    local account_id
    account_id=$(aws sts get-caller-identity --query Account --output text)
    log_success "AWS credentials valid (Account: $account_id)"
}

# Deploy infrastructure with Terraform
deploy_infrastructure() {
    if [[ "$SKIP_INFRASTRUCTURE" == "true" ]]; then
        log_info "Skipping infrastructure deployment"
        return 0
    fi

    log_info "Deploying infrastructure with Terraform..."

    local terraform_dir="${ROOT_DIR}/infrastructure/terraform"

    if [[ ! -d "$terraform_dir" ]]; then
        log_error "Terraform directory not found: $terraform_dir"
        exit 1
    fi

    cd "$terraform_dir"

    # Check if terraform.tfvars exists
    if [[ ! -f "terraform.tfvars" ]]; then
        log_error "terraform.tfvars not found. Please copy from terraform.tfvars.example and configure."
        exit 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run: terraform plan -var-file=terraform.tfvars"
        return 0
    fi

    # Initialize Terraform
    terraform init

    # Plan the deployment
    log_info "Planning Terraform deployment..."
    terraform plan -var="environment=$ENVIRONMENT" -out="tfplan"

    # Apply the plan
    log_info "Applying Terraform plan..."
    terraform apply "tfplan"

    log_success "Infrastructure deployment completed"

    cd "$ROOT_DIR"
}

# Configure kubectl
configure_kubectl() {
    log_info "Configuring kubectl..."

    local cluster_name
    cluster_name=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw cluster_name 2>/dev/null || echo "multi-tenant-ingestion-${ENVIRONMENT}")

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run: aws eks update-kubeconfig --name $cluster_name"
        return 0
    fi

    aws eks update-kubeconfig --name "$cluster_name" --region "${AWS_REGION:-us-east-1}"

    # Test kubectl connection
    if kubectl cluster-info &> /dev/null; then
        log_success "kubectl configured successfully"
    else
        log_error "Failed to configure kubectl"
        exit 1
    fi
}

# Build Docker images
build_images() {
    if [[ "$BUILD_IMAGES" != "true" ]]; then
        log_info "Skipping image build"
        return 0
    fi

    log_info "Building Docker images..."

    local services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service" "ui")
    local registry_url

    # Get ECR registry URL from Terraform output or use default
    registry_url=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw ecr_registry_url 2>/dev/null || echo "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION:-us-east-1}.amazonaws.com")

    if [[ "$DRY_RUN" == "true" ]]; then
        for service in "${services[@]}"; do
            log_info "[DRY RUN] Would build and push: $service"
        done
        return 0
    fi

    # Login to ECR
    aws ecr get-login-password --region "${AWS_REGION:-us-east-1}" | docker login --username AWS --password-stdin "$registry_url"

    for service in "${services[@]}"; do
        local service_dir="${ROOT_DIR}/services/${service}"

        if [[ ! -f "${service_dir}/Dockerfile" ]]; then
            log_warn "Dockerfile not found for service: $service, skipping..."
            continue
        fi

        log_info "Building $service..."

        local image_tag="${registry_url}/multi-tenant-ingestion/${service}:${ENVIRONMENT}-$(git rev-parse --short HEAD 2>/dev/null || echo 'latest')"

        # Build the image
        docker build -t "$image_tag" "$service_dir"

        # Push to registry
        docker push "$image_tag"

        log_success "Built and pushed $service"
    done

    log_success "All images built and pushed successfully"
}

# Deploy Kubernetes resources
deploy_kubernetes() {
    if [[ "$SKIP_KUBERNETES" == "true" ]]; then
        log_info "Skipping Kubernetes deployment"
        return 0
    fi

    log_info "Deploying Kubernetes resources..."

    local k8s_dir="${ROOT_DIR}/infrastructure/kubernetes"

    if [[ ! -d "$k8s_dir" ]]; then
        log_error "Kubernetes directory not found: $k8s_dir"
        exit 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would deploy Kubernetes resources from $k8s_dir"
        return 0
    fi

    # Deploy namespaces first
    if [[ -d "${k8s_dir}/namespaces" ]]; then
        log_info "Creating namespaces..."
        kubectl apply -f "${k8s_dir}/namespaces/" --recursive
    fi

    # Deploy RBAC resources
    if [[ -d "${k8s_dir}/rbac" ]]; then
        log_info "Creating RBAC resources..."
        kubectl apply -f "${k8s_dir}/rbac/" --recursive
    fi

    # Deploy secrets
    if [[ -d "${k8s_dir}/secrets" ]]; then
        log_info "Creating secrets..."
        kubectl apply -f "${k8s_dir}/secrets/" --recursive
    fi

    # Deploy configmaps
    if [[ -d "${k8s_dir}/configmaps" ]]; then
        log_info "Creating configmaps..."
        kubectl apply -f "${k8s_dir}/configmaps/" --recursive
    fi

    # Deploy monitoring stack
    if [[ -d "${k8s_dir}/monitoring" ]]; then
        log_info "Deploying monitoring stack..."
        kubectl apply -f "${k8s_dir}/monitoring/" --recursive
    fi

    log_success "Kubernetes resources deployed"
}

# Deploy services using Helm
deploy_services() {
    if [[ "$DEPLOY_SERVICES" != "true" ]]; then
        log_info "Skipping service deployment"
        return 0
    fi

    log_info "Deploying services with Helm..."

    local helm_dir="${ROOT_DIR}/deployment/helm-charts"

    if [[ ! -d "$helm_dir" ]]; then
        log_error "Helm charts directory not found: $helm_dir"
        exit 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would deploy services using Helm charts"
        return 0
    fi

    # Get image registry URL
    local registry_url
    registry_url=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw ecr_registry_url 2>/dev/null || echo "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION:-us-east-1}.amazonaws.com")

    # Deploy main application
    local chart_name="multi-tenant-ingestion"
    local release_name="multi-tenant-ingestion-${ENVIRONMENT}"

    if [[ -d "${helm_dir}/${chart_name}" ]]; then
        log_info "Deploying $chart_name..."

        helm upgrade --install "$release_name" "${helm_dir}/${chart_name}" \
            --namespace "multi-tenant-ingestion" \
            --create-namespace \
            --set "global.environment=${ENVIRONMENT}" \
            --set "global.imageRegistry=${registry_url}" \
            --set "global.imageTag=${ENVIRONMENT}-$(git rev-parse --short HEAD 2>/dev/null || echo 'latest')" \
            --values "${helm_dir}/${chart_name}/values-${ENVIRONMENT}.yaml" \
            --wait \
            --timeout 10m

        log_success "Deployed $chart_name"
    fi

    # Deploy monitoring if exists
    if [[ -d "${helm_dir}/monitoring" ]]; then
        log_info "Deploying monitoring stack..."

        helm upgrade --install "monitoring-${ENVIRONMENT}" "${helm_dir}/monitoring" \
            --namespace "monitoring" \
            --create-namespace \
            --set "global.environment=${ENVIRONMENT}" \
            --wait \
            --timeout 10m

        log_success "Deployed monitoring stack"
    fi

    log_success "All services deployed successfully"
}

# Run database migrations
run_migrations() {
    if [[ "$RUN_MIGRATIONS" != "true" ]]; then
        log_info "Skipping database migrations"
        return 0
    fi

    log_info "Running database migrations..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run database migrations"
        return 0
    fi

    # Get database connection details from Terraform outputs
    local db_host db_name db_user db_password

    db_host=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw rds_endpoint 2>/dev/null || echo "localhost")
    db_name=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw rds_database_name 2>/dev/null || echo "multi_tenant_ingestion")
    db_user=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw rds_username 2>/dev/null || echo "postgres")

    # Get password from AWS Secrets Manager
    local secret_name
    secret_name=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw rds_password_secret_name 2>/dev/null || echo "multi-tenant-ingestion-${ENVIRONMENT}-rds-password")

    db_password=$(aws secretsmanager get-secret-value --secret-id "$secret_name" --query SecretString --output text 2>/dev/null || echo "")

    if [[ -z "$db_password" ]]; then
        log_error "Could not retrieve database password from Secrets Manager"
        exit 1
    fi

    # Run migrations using kubectl job
    local migration_job_yaml=$(cat << EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migration-$(date +%s)
  namespace: multi-tenant-ingestion
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: migration
        image: ${registry_url}/multi-tenant-ingestion/pipeline-service:${ENVIRONMENT}-$(git rev-parse --short HEAD 2>/dev/null || echo 'latest')
        command: ["python", "-m", "alembic", "upgrade", "head"]
        env:
        - name: DATABASE_URL
          value: "postgresql://${db_user}:${db_password}@${db_host}:5432/${db_name}"
      backoffLimit: 3
EOF
)

    echo "$migration_job_yaml" | kubectl apply -f -

    # Wait for migration to complete
    log_info "Waiting for migration to complete..."
    kubectl wait --for=condition=complete job --selector=job-name --timeout=300s -n multi-tenant-ingestion

    log_success "Database migrations completed"
}

# Verify deployment
verify_deployment() {
    log_info "Verifying deployment..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would verify deployment"
        return 0
    fi

    # Check if pods are running
    log_info "Checking pod status..."
    kubectl get pods -n multi-tenant-ingestion

    # Check services
    log_info "Checking services..."
    kubectl get services -n multi-tenant-ingestion

    # Check ingress
    log_info "Checking ingress..."
    kubectl get ingress -n multi-tenant-ingestion

    # Health check endpoints
    local load_balancer_dns
    load_balancer_dns=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw load_balancer_dns 2>/dev/null || echo "")

    if [[ -n "$load_balancer_dns" ]]; then
        log_info "Testing health endpoints..."

        local services=("pipeline-service:8000" "catalog-service:8001" "connector-service:8002" "data-quality-service:8003")

        for service in "${services[@]}"; do
            local service_name service_port
            service_name=$(echo "$service" | cut -d: -f1)
            service_port=$(echo "$service" | cut -d: -f2)

            local health_url="https://${load_balancer_dns}/${service_name}/health"

            if curl -f -s "$health_url" &> /dev/null; then
                log_success "$service_name health check passed"
            else
                log_warn "$service_name health check failed"
            fi
        done
    fi

    log_success "Deployment verification completed"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up temporary files..."

    # Remove terraform plan files
    find "${ROOT_DIR}/infrastructure/terraform" -name "*.tfplan" -delete 2>/dev/null || true

    # Clean up Docker build cache if building images
    if [[ "$BUILD_IMAGES" == "true" ]]; then
        docker system prune -f --filter "until=24h" &> /dev/null || true
    fi
}

# Main execution function
main() {
    log_info "Starting deployment for environment: $ENVIRONMENT"

    # Set trap for cleanup
    trap cleanup EXIT

    # Pre-deployment checks
    check_dependencies
    check_aws_credentials

    # Deploy infrastructure
    deploy_infrastructure

    # Configure kubectl if infrastructure was deployed
    if [[ "$SKIP_INFRASTRUCTURE" != "true" ]]; then
        configure_kubectl
    fi

    # Build and push images
    build_images

    # Deploy Kubernetes resources
    deploy_kubernetes

    # Deploy services
    deploy_services

    # Run migrations
    run_migrations

    # Verify deployment
    verify_deployment

    log_success "Deployment completed successfully!"

    # Show access information
    if [[ "$DRY_RUN" != "true" ]]; then
        echo ""
        log_info "Access Information:"

        local load_balancer_dns
        load_balancer_dns=$(terraform -chdir="${ROOT_DIR}/infrastructure/terraform" output -raw load_balancer_dns 2>/dev/null || echo "Not available")

        echo "  Application URL: https://${load_balancer_dns}"
        echo "  Grafana Dashboard: https://${load_balancer_dns}/grafana"
        echo "  API Documentation: https://${load_balancer_dns}/docs"
        echo ""
        echo "  Kubernetes namespace: multi-tenant-ingestion"
        echo "  Monitoring namespace: monitoring"
        echo ""
        log_info "Run the following to check service status:"
        echo "  kubectl get pods -n multi-tenant-ingestion"
        echo "  kubectl get services -n multi-tenant-ingestion"
    fi
}

# Execute main function
main "$@"