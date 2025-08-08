#!/bin/bash
# deployment/scripts/deploy.sh
# Main deployment script for Multi-Tenant Data Ingestion Framework

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEPLOYMENT_DIR="${PROJECT_ROOT}/deployment"

# Default values
ENVIRONMENT="development"
BUILD_IMAGES=false
DEPLOY_SERVICES=false
RUN_MIGRATIONS=false
SKIP_HEALTH_CHECK=false
FORCE_RECREATE=false
VERBOSE=false

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

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${BLUE}[VERBOSE]${NC} $1"
    fi
}

# Help function
show_help() {
    cat << EOF
Multi-Tenant Data Ingestion Framework - Deployment Script

Usage: $0 [OPTIONS]

OPTIONS:
    --environment, -e       Target environment (development, staging, production)
    --build-images          Build Docker images before deployment
    --deploy-services       Deploy services to target environment
    --run-migrations        Run database migrations
    --skip-health-check     Skip health checks after deployment
    --force-recreate        Force recreate containers (Docker Compose only)
    --verbose, -v           Enable verbose logging
    --help, -h              Show this help message

EXAMPLES:
    # Local development deployment
    $0 --environment=development --build-images --deploy-services

    # Production deployment with migrations
    $0 --environment=production --build-images --deploy-services --run-migrations

    # Quick local restart
    $0 --environment=development --deploy-services --force-recreate

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --environment|-e)
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
            --skip-health-check)
                SKIP_HEALTH_CHECK=true
                shift
                ;;
            --force-recreate)
                FORCE_RECREATE=true
                shift
                ;;
            --verbose|-v)
                VERBOSE=true
                shift
                ;;
            --help|-h)
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
}

# Validate environment
validate_environment() {
    log_info "Validating environment: $ENVIRONMENT"
    
    case $ENVIRONMENT in
        development|staging|production)
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT"
            log_error "Valid environments: development, staging, production"
            exit 1
            ;;
    esac
    
    # Check if environment config exists
    local env_file="${DEPLOYMENT_DIR}/configs/${ENVIRONMENT}.env"
    if [ ! -f "$env_file" ]; then
        log_warning "Environment file not found: $env_file"
        log_info "Using default .env file"
    fi
}

# Load environment configuration
load_environment() {
    log_info "Loading environment configuration"
    
    # Load default .env if it exists
    if [ -f "${PROJECT_ROOT}/.env" ]; then
        log_verbose "Loading default .env file"
        set -a
        source "${PROJECT_ROOT}/.env"
        set +a
    fi
    
    # Load environment-specific config
    local env_file="${DEPLOYMENT_DIR}/configs/${ENVIRONMENT}.env"
    if [ -f "$env_file" ]; then
        log_verbose "Loading environment-specific config: $env_file"
        set -a
        source "$env_file"
        set +a
    fi
    
    # Export environment for child processes
    export ENVIRONMENT
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites"
    
    local missing_tools=()
    
    # Check required tools based on environment
    case $ENVIRONMENT in
        development)
            if ! command -v docker &> /dev/null; then
                missing_tools+=("docker")
            fi
            if ! command -v docker-compose &> /dev/null; then
                missing_tools+=("docker-compose")
            fi
            ;;
        staging|production)
            if ! command -v terraform &> /dev/null; then
                missing_tools+=("terraform")
            fi
            if ! command -v kubectl &> /dev/null; then
                missing_tools+=("kubectl")
            fi
            if ! command -v helm &> /dev/null; then
                missing_tools+=("helm")
            fi
            if ! command -v aws &> /dev/null; then
                missing_tools+=("aws")
            fi
            ;;
    esac
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again"
        exit 1
    fi
    
    log_success "All prerequisites satisfied"
}

# Build Docker images
build_images() {
    if [ "$BUILD_IMAGES" != true ]; then
        return 0
    fi
    
    log_info "Building Docker images"
    
    local services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service")
    
    for service in "${services[@]}"; do
        log_info "Building $service..."
        
        local dockerfile="${PROJECT_ROOT}/services/${service}/Dockerfile"
        if [ ! -f "$dockerfile" ]; then
            log_warning "Dockerfile not found for $service, creating basic Dockerfile"
            create_dockerfile "$service"
        fi
        
        if [ "$ENVIRONMENT" = "development" ]; then
            # Build with docker-compose for development
            docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" build "$service"
        else
            # Build and tag for registry push
            local image_tag="${DOCKER_REGISTRY:-multi-tenant}/${service}:${DOCKER_IMAGE_TAG:-latest}"
            docker build -t "$image_tag" "${PROJECT_ROOT}/services/${service}"
            
            if [ -n "${DOCKER_REGISTRY:-}" ]; then
                log_info "Pushing $service to registry..."
                docker push "$image_tag"
            fi
        fi
    done
    
    # Build UI if it exists
    if [ -d "${PROJECT_ROOT}/services/ui" ]; then
        log_info "Building UI service..."
        if [ "$ENVIRONMENT" = "development" ]; then
            docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" build ui
        else
            local ui_image_tag="${DOCKER_REGISTRY:-multi-tenant}/ui:${DOCKER_IMAGE_TAG:-latest}"
            docker build -t "$ui_image_tag" "${PROJECT_ROOT}/services/ui"
            
            if [ -n "${DOCKER_REGISTRY:-}" ]; then
                docker push "$ui_image_tag"
            fi
        fi
    fi
    
    log_success "Docker images built successfully"
}

# Create basic Dockerfile if missing
create_dockerfile() {
    local service="$1"
    local dockerfile="${PROJECT_ROOT}/services/${service}/Dockerfile"
    
    log_info "Creating basic Dockerfile for $service"
    
    cat > "$dockerfile" << EOF
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
  CMD curl -f http://localhost:8000/health || exit 1

# Run application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF
    
    # Create requirements.txt if missing
    local requirements="${PROJECT_ROOT}/services/${service}/requirements.txt"
    if [ ! -f "$requirements" ]; then
        cat > "$requirements" << EOF
fastapi==0.104.1
uvicorn[standard]==0.24.0
sqlalchemy==2.0.23
psycopg2-binary==2.9.9
redis==5.0.1
httpx==0.25.2
boto3==1.34.0
pandas==2.1.4
pydantic==2.5.0
python-multipart==0.0.6
EOF
    fi
}

# Run database migrations
run_migrations() {
    if [ "$RUN_MIGRATIONS" != true ]; then
        return 0
    fi
    
    log_info "Running database migrations"
    
    case $ENVIRONMENT in
        development)
            # Run migrations via docker-compose
            log_info "Running migrations in development environment"
            
            # Create databases if they don't exist
            docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" exec -T postgres psql -U dbadmin -d multitenantdb -c "
                CREATE DATABASE IF NOT EXISTS pipeline_db;
                CREATE DATABASE IF NOT EXISTS catalog_db;
                CREATE DATABASE IF NOT EXISTS connector_db;
                CREATE DATABASE IF NOT EXISTS dq_db;
                CREATE DATABASE IF NOT EXISTS airflow_db;
            " || true
            
            # Run service-specific migrations
            local services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service")
            for service in "${services[@]}"; do
                log_info "Running migrations for $service"
                docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" exec -T "$service" python -c "
from app.main import Base, engine
Base.metadata.create_all(bind=engine)
print('Migrations completed for $service')
" || log_warning "Migration failed for $service"
            done
            ;;
        staging|production)
            # Run migrations via kubectl or direct connection
            log_info "Running migrations in $ENVIRONMENT environment"
            
            # This would typically connect to the managed database
            # and run migrations using a job or init container
            local migration_job="${DEPLOYMENT_DIR}/kubernetes/migrations/migration-job.yaml"
            if [ -f "$migration_job" ]; then
                kubectl apply -f "$migration_job"
                kubectl wait --for=condition=complete --timeout=300s job/migration-job
            else
                log_warning "Migration job not found: $migration_job"
            fi
            ;;
    esac
    
    log_success "Database migrations completed"
}

# Deploy services
deploy_services() {
    if [ "$DEPLOY_SERVICES" != true ]; then
        return 0
    fi
    
    log_info "Deploying services to $ENVIRONMENT environment"
    
    case $ENVIRONMENT in
        development)
            deploy_development
            ;;
        staging)
            deploy_staging
            ;;
        production)
            deploy_production
            ;;
    esac
    
    log_success "Services deployed successfully"
}

# Deploy to development environment
deploy_development() {
    log_info "Deploying to development environment using Docker Compose"
    
    cd "$PROJECT_ROOT"
    
    local compose_args=()
    compose_args+=("-f" "docker-compose.yml")
    
    if [ "$FORCE_RECREATE" = true ]; then
        compose_args+=("--force-recreate")
    fi
    
    # Stop existing services
    docker-compose "${compose_args[@]}" down || true
    
    # Start services
    docker-compose "${compose_args[@]}" up -d
    
    log_info "Waiting for services to be ready..."
    sleep 30
}

# Deploy to staging environment
deploy_staging() {
    log_info "Deploying to staging environment using Kubernetes"
    
    # Deploy infrastructure with Terraform
    deploy_infrastructure_terraform "staging"
    
    # Deploy applications with Helm
    deploy_applications_helm "staging"
}

# Deploy to production environment
deploy_production() {
    log_info "Deploying to production environment using Kubernetes"
    
    # Deploy infrastructure with Terraform
    deploy_infrastructure_terraform "production"
    
    # Deploy applications with Helm
    deploy_applications_helm "production"
}

# Deploy infrastructure with Terraform
deploy_infrastructure_terraform() {
    local env="$1"
    log_info "Deploying infrastructure for $env environment"
    
    cd "${PROJECT_ROOT}/infrastructure/terraform"
    
    # Initialize Terraform
    terraform init -upgrade
    
    # Select or create workspace
    terraform workspace select "$env" || terraform workspace new "$env"
    
    # Plan deployment
    log_info "Planning Terraform deployment"
    terraform plan -var-file="terraform.tfvars" -var="environment=$env" -out="tfplan"
    
    # Apply deployment
    log_info "Applying Terraform deployment"
    terraform apply "tfplan"
    
    # Output important values
    log_info "Terraform outputs:"
    terraform output
}

# Deploy applications with Helm
deploy_applications_helm() {
    local env="$1"
    log_info "Deploying applications for $env environment using Helm"
    
    local helm_chart="${DEPLOYMENT_DIR}/helm-charts/multi-tenant-ingestion"
    local values_file="${helm_chart}/values-${env}.yaml"
    
    if [ ! -f "$values_file" ]; then
        log_warning "Values file not found: $values_file"
        values_file="${helm_chart}/values.yaml"
    fi
    
    # Add/update Helm repositories
    helm repo add stable https://charts.helm.sh/stable || true
    helm repo add bitnami https://charts.bitnami.com/bitnami || true
    helm repo update
    
    # Install or upgrade the application
    helm upgrade --install \
        "multi-tenant-ingestion" \
        "$helm_chart" \
        --namespace "multi-tenant-$env" \
        --create-namespace \
        --values "$values_file" \
        --set "environment=$env" \
        --set "image.tag=${DOCKER_IMAGE_TAG:-latest}" \
        --wait \
        --timeout 600s
    
    log_info "Application deployed successfully with Helm"
}

# Health check
health_check() {
    if [ "$SKIP_HEALTH_CHECK" = true ]; then
        return 0
    fi
    
    log_info "Running health checks"
    
    case $ENVIRONMENT in
        development)
            health_check_development
            ;;
        staging|production)
            health_check_kubernetes
            ;;
    esac
}

# Health check for development environment
health_check_development() {
    log_info "Checking service health in development environment"
    
    local services=(
        "pipeline-service:8000"
        "catalog-service:8001"
        "connector-service:8002"
        "data-quality-service:8003"
    )
    
    local max_attempts=30
    local attempt=1
    
    for service_port in "${services[@]}"; do
        local service=$(echo "$service_port" | cut -d: -f1)
        local port=$(echo "$service_port" | cut -d: -f2)
        local url="http://localhost:${port}/health"
        
        log_info "Checking health of $service..."
        
        while [ $attempt -le $max_attempts ]; do
            if curl -f -s "$url" > /dev/null 2>&1; then
                log_success "$service is healthy"
                break
            fi
            
            if [ $attempt -eq $max_attempts ]; then
                log_error "$service health check failed after $max_attempts attempts"
                return 1
            fi
            
            log_verbose "Attempt $attempt/$max_attempts for $service..."
            sleep 10
            ((attempt++))
        done
        
        attempt=1
    done
    
    # Check database connectivity
    if docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" exec -T postgres pg_isready -U dbadmin > /dev/null 2>&1; then
        log_success "Database is healthy"
    else
        log_error "Database health check failed"
        return 1
    fi
    
    # Check Redis connectivity
    if docker-compose -f "${PROJECT_ROOT}/docker-compose.yml" exec -T redis redis-cli ping > /dev/null 2>&1; then
        log_success "Redis is healthy"
    else
        log_error "Redis health check failed"
        return 1
    fi
    
    log_success "All health checks passed"
}

# Health check for Kubernetes environments
health_check_kubernetes() {
    log_info "Checking service health in Kubernetes environment"
    
    local namespace="multi-tenant-$ENVIRONMENT"
    
    # Check pod status
    log_info "Checking pod status..."
    kubectl get pods -n "$namespace"
    
    # Check service endpoints
    local services=("pipeline-service" "catalog-service" "connector-service" "data-quality-service")
    
    for service in "${services[@]}"; do
        log_info "Checking $service..."
        
        # Get service URL
        local service_url
        if [ "$ENVIRONMENT" = "production" ]; then
            service_url=$(kubectl get service "$service" -n "$namespace" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
        else
            # Port forward for staging
            kubectl port-forward "service/$service" 8080:80 -n "$namespace" &
            local pf_pid=$!
            service_url="http://localhost:8080"
            sleep 5
        fi
        
        # Health check
        if curl -f -s "${service_url}/health" > /dev/null 2>&1; then
            log_success "$service is healthy"
        else
            log_error "$service health check failed"
        fi
        
        # Clean up port forward
        if [ -n "${pf_pid:-}" ]; then
            kill $pf_pid 2>/dev/null || true
        fi
    done
}

# Cleanup function
cleanup() {
    log_info "Cleaning up temporary files"
    
    # Remove temporary files
    rm -f "${PROJECT_ROOT}/infrastructure/terraform/tfplan"
    
    # Kill any background processes
    jobs -p | xargs -r kill 2>/dev/null || true
}

# Setup organization
setup_organization() {
    local org_name="$1"
    log_info "Setting up organization: $org_name"
    
    case $ENVIRONMENT in
        development)
            # Create organization via API
            local api_url="http://localhost/api/v1"
            ;;
        staging|production)
            # Get API URL from Kubernetes service
            local api_url=$(kubectl get ingress -n "multi-tenant-$ENVIRONMENT" -o jsonpath='{.items[0].spec.rules[0].host}')
            api_url="https://$api_url/api/v1"
            ;;
    esac
    
    # Create organization (this would be a more complex operation in practice)
    log_info "Organization $org_name setup would be implemented here"
    log_info "This would include:"
    log_info "  - Creating Unity Catalog namespace"
    log_info "  - Setting up S3 buckets"
    log_info "  - Configuring RBAC"
    log_info "  - Creating default data quality rules"
}

# Show deployment summary
show_summary() {
    log_success "Deployment completed successfully!"
    
    case $ENVIRONMENT in
        development)
            cat << EOF

${GREEN}=== Development Environment Summary ===${NC}

Services:
  - Pipeline Service: http://localhost:8000
  - Catalog Service: http://localhost:8001  
  - Connector Service: http://localhost:8002
  - Data Quality Service: http://localhost:8003
  - UI Application: http://localhost:3000

Development Tools:
  - PgAdmin: http://localhost:5050
  - Redis Commander: http://localhost:8081
  - Grafana: http://localhost:3001
  - Prometheus: http://localhost:9090
  - Jupyter: http://localhost:8888
  - MinIO Console: http://localhost:9001

To view logs: docker-compose logs -f [service-name]
To stop services: docker-compose down
EOF
            ;;
        staging|production)
            local lb_dns=$(terraform -chdir="${PROJECT_ROOT}/infrastructure/terraform" output -raw load_balancer_dns 2>/dev/null || echo "Not available")
            cat << EOF

${GREEN}=== $ENVIRONMENT Environment Summary ===${NC}

Load Balancer: https://$lb_dns

Services deployed in namespace: multi-tenant-$ENVIRONMENT

To check status: kubectl get pods -n multi-tenant-$ENVIRONMENT
To view logs: kubectl logs -n multi-tenant-$ENVIRONMENT -l app=[service-name]
EOF
            ;;
    esac
}

# Main function
main() {
    log_info "Starting Multi-Tenant Data Ingestion Framework deployment"
    log_info "Environment: $ENVIRONMENT"
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Validate and load environment
    validate_environment
    load_environment
    
    # Check prerequisites
    check_prerequisites
    
    # Build images if requested
    build_images
    
    # Run migrations if requested
    run_migrations
    
    # Deploy services if requested
    deploy_services
    
    # Run health checks
    health_check
    
    # Show summary
    show_summary
    
    log_success "Deployment completed successfully!"
}

# Script entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    parse_args "$@"
    main
fi
