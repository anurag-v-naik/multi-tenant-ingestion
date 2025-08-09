#!/bin/bash

set -e

# Configuration
ENVIRONMENT=${1:-development}
BUILD_IMAGES=${2:-false}
DEPLOY_SERVICES=${3:-false}
RUN_MIGRATIONS=${4:-false}

echo "Deploying Multi-Tenant Data Ingestion Platform"
echo "Environment: $ENVIRONMENT"
echo "Build Images: $BUILD_IMAGES"
echo "Deploy Services: $DEPLOY_SERVICES"
echo "Run Migrations: $RUN_MIGRATIONS"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    echo "Checking prerequisites..."
    
    if ! command_exists docker; then
        echo "Error: Docker is required but not installed."
        exit 1
    fi
    
    if ! command_exists docker-compose; then
        echo "Error: Docker Compose is required but not installed."
        exit 1
    fi
    
    if ! command_exists terraform; then
        echo "Error: Terraform is required but not installed."
        exit 1
    fi
    
    echo "Prerequisites check passed."
}

# Build Docker images
build_images() {
    if [ "$BUILD_IMAGES" = "true" ]; then
        echo "Building Docker images..."
        
        # Build auth service
        echo "Building auth service..."
        docker build -t multi-tenant-auth:latest ./services/auth-service/
        
        # Build connector service
        echo "Building connector service..."
        docker build -t multi-tenant-connector:latest ./services/connector-service/
        
        # Build pipeline service
        echo "Building pipeline service..."
        docker build -t multi-tenant-pipeline:latest ./services/pipeline-service/
        
        # Build catalog service
        echo "Building catalog service..."
        docker build -t multi-tenant-catalog:latest ./services/catalog-service/
        
        # Build data quality service
        echo "Building data quality service..."
        docker build -t multi-tenant-data-quality:latest ./services/data-quality-service/
        
        # Build UI
        echo "Building UI..."
        docker build -t multi-tenant-ui:latest ./services/ui/
        
        echo "Docker images built successfully."
    fi
}

# Deploy infrastructure
deploy_infrastructure() {
    if [ "$ENVIRONMENT" != "development" ]; then
        echo "Deploying infrastructure with Terraform..."
        
        cd infrastructure/terraform
        
        # Initialize Terraform
        terraform init
        
        # Create workspace if it doesn't exist
        terraform workspace select $ENVIRONMENT || terraform workspace new $ENVIRONMENT
        
        # Plan deployment
        terraform plan -var-file="environments/$ENVIRONMENT.tfvars" -out=tfplan
        
        # Apply deployment
        terraform apply tfplan
        
        cd ../..
        
        echo "Infrastructure deployed successfully."
    fi
}

# Deploy services
deploy_services() {
    if [ "$DEPLOY_SERVICES" = "true" ]; then
        echo "Deploying services..."
        
        if [ "$ENVIRONMENT" = "development" ]; then
            # Use Docker Compose for development
            docker-compose -f docker-compose.yml up -d
        else
            # Use Kubernetes for production
            kubectl apply -f deployment/kubernetes/namespaces/
            kubectl apply -f deployment/kubernetes/configmaps/
            kubectl apply -f deployment/kubernetes/secrets/
            kubectl apply -f deployment/kubernetes/services/
            kubectl apply -f deployment/kubernetes/deployments/
            kubectl apply -f deployment/kubernetes/ingress/
        fi
        
        echo "Services deployed successfully."
    fi
}

# Run database migrations
run_migrations() {
    if [ "$RUN_MIGRATIONS" = "true" ]; then
        echo "Running database migrations..."
        
        # Wait for database to be ready
        sleep 30
        
        # Run migrations for each service
        docker-compose exec auth-service alembic upgrade head
        docker-compose exec connector-service alembic upgrade head
        docker-compose exec pipeline-service alembic upgrade head
        docker-compose exec catalog-service alembic upgrade head
        docker-compose exec data-quality-service alembic upgrade head
        
        echo "Database migrations completed successfully."
    fi
}

# Setup organizations
setup_organizations() {
    echo "Setting up organizations..."
    
    # This would call the setup-organization.sh script for each org
    ./deployment/scripts/setup-organization.sh finance
    ./deployment/scripts/setup-organization.sh retail
    
    echo "Organizations setup completed."
}

# Health checks
health_checks() {
    echo "Running health checks..."
    
    # Wait for services to start
    sleep 60
    
    # Check service health
    services=("auth" "connector" "pipeline" "catalog" "data-quality")
    
# Health checks
health_checks() {
    echo "Running health checks..."
    
    # Wait for services to start
    sleep 60
    
    # Check service health
    services=("auth" "connector" "pipeline" "catalog" "data-quality")
    
    for service in "${services[@]}"; do
        if [ "$ENVIRONMENT" = "development" ]; then
            url="http://localhost:8080/api/v1/${service}/health"
        else
            url="https://api.${ENVIRONMENT}.yourdomain.com/api/v1/${service}/health"
        fi
        
        echo "Checking health of $service service..."
        if curl -f "$url" > /dev/null 2>&1; then
            echo "✓ $service service is healthy"
        else
            echo "✗ $service service is not responding"
            exit 1
        fi
    done
    
    echo "All health checks passed."
}

# Main execution
main() {
    check_prerequisites
    build_images
    deploy_infrastructure
    deploy_services
    run_migrations
    setup_organizations
    health_checks
    
    echo ""
    echo "🎉 Deployment completed successfully!"
    echo ""
    echo "Access URLs:"
    if [ "$ENVIRONMENT" = "development" ]; then
        echo "  UI: http://localhost:3000"
        echo "  API Gateway: http://localhost:8080"
        echo "  Grafana: http://localhost:3001 (admin/admin)"
        echo "  Prometheus: http://localhost:9090"
    else
        echo "  UI: https://${ENVIRONMENT}.yourdomain.com"
        echo "  API: https://api.${ENVIRONMENT}.yourdomain.com"
    fi
    echo ""
}

# Execute main function
main