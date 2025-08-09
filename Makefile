# Makefile
.PHONY: help setup-dev build test format lint clean deploy-local

# Variables
DOCKER_REGISTRY ?= $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
PROJECT_NAME ?= multi-tenant-ingestion
ENVIRONMENT ?= development

# Help target
help: ## Show this help message
	@echo "Multi-Tenant Data Ingestion Framework"
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Development setup
setup-dev: ## Setup development environment
	@echo "Setting up development environment..."
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements-dev.txt
	pre-commit install
	@echo "Development environment setup complete!"

# Build Docker images
build: ## Build all Docker images
	@echo "Building Docker images..."
	docker build -t $(PROJECT_NAME)/pipeline-service:latest services/pipeline-service/
	docker build -t $(PROJECT_NAME)/catalog-service:latest services/catalog-service/
	docker build -t $(PROJECT_NAME)/connector-service:latest services/connector-service/
	docker build -t $(PROJECT_NAME)/data-quality-service:latest services/data-quality-service/
	docker build -t $(PROJECT_NAME)/ui:latest services/ui/
	@echo "All images built successfully!"

# Push images to ECR
push-images: ## Push Docker images to ECR
	@echo "Logging in to ECR..."
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(DOCKER_REGISTRY)

	@echo "Tagging and pushing images..."
	docker tag $(PROJECT_NAME)/pipeline-service:latest $(DOCKER_REGISTRY)/$(PROJECT_NAME)/pipeline-service:latest
	docker tag $(PROJECT_NAME)/catalog-service:latest $(DOCKER_REGISTRY)/$(PROJECT_NAME)/catalog-service:latest
	docker tag $(PROJECT_NAME)/connector-service:latest $(DOCKER_REGISTRY)/$(PROJECT_NAME)/connector-service:latest
	docker tag $(PROJECT_NAME)/data-quality-service:latest $(DOCKER_REGISTRY)/$(PROJECT_NAME)/data-quality-service:latest
	docker tag $(PROJECT_NAME)/ui:latest $(DOCKER_REGISTRY)/$(PROJECT_NAME)/ui:latest

	docker push $(DOCKER_REGISTRY)/$(PROJECT_NAME)/pipeline-service:latest
	docker push $(DOCKER_REGISTRY)/$(PROJECT_NAME)/catalog-service:latest
	docker push $(DOCKER_REGISTRY)/$(PROJECT_NAME)/connector-service:latest
	docker push $(DOCKER_REGISTRY)/$(PROJECT_NAME)/data-quality-service:latest
	docker push $(DOCKER_REGISTRY)/$(PROJECT_NAME)/ui:latest
	@echo "All images pushed successfully!"

# Run tests
test: ## Run all tests
	@echo "Running tests..."
	pytest tests/unit/ -v
	pytest tests/integration/ -v
	@echo "All tests completed!"

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	pytest tests/unit/ -v

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	pytest tests/integration/ -v

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	pytest --cov=services/ --cov-report=html --cov-report=term
	@echo "Coverage report generated in htmlcov/"

# Code formatting and linting
format: ## Format all code
	@echo "Formatting Python code..."
	black services/ tests/ scripts/
	isort services/ tests/ scripts/
	@echo "Formatting TypeScript code..."
	cd services/ui && npm run format
	@echo "Code formatting completed!"

format-python: ## Format Python code only
	@echo "Formatting Python code..."
	black services/ tests/ scripts/
	isort services/ tests/ scripts/

format-typescript: ## Format TypeScript code only
	@echo "Formatting TypeScript code..."
	cd services/ui && npm run format

lint: ## Lint all code
	@echo "Linting Python code..."
	pylint services/
	flake8 services/
	@echo "Linting TypeScript code..."
	cd services/ui && npm run lint
	@echo "Linting completed!"

lint-python: ## Lint Python code only
	@echo "Linting Python code..."
	pylint services/
	flake8 services/

lint-typescript: ## Lint TypeScript code only
	@echo "Linting TypeScript code..."
	cd services/ui && npm run lint

# Local deployment
dev-start: ## Start development environment
	@echo "Starting development environment..."
	docker-compose up -d
	@echo "Services are starting up. Check logs with: docker-compose logs -f"

dev-stop: ## Stop development environment
	@echo "Stopping development environment..."
	docker-compose down

dev-restart: ## Restart development environment
	@echo "Restarting development environment..."
	docker-compose down
	docker-compose up -d

dev-logs: ## Show development environment logs
	docker-compose logs -f

# Database operations
db-migrate: ## Run database migrations
	@echo "Running database migrations..."
	cd services/pipeline-service && python -m alembic upgrade head

db-reset: ## Reset database (WARNING: destructive)
	@echo "Resetting database..."
	@read -p "Are you sure? This will delete all data. Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ]
	docker-compose down -v
	docker-compose up -d postgres redis
	sleep 10
	$(MAKE) db-migrate

# Infrastructure operations
terraform-plan: ## Plan Terraform infrastructure changes
	@echo "Planning infrastructure changes..."
	cd infrastructure/terraform && terraform plan

terraform-apply: ## Apply Terraform infrastructure changes
	@echo "Applying infrastructure changes..."
	cd infrastructure/terraform && terraform apply

terraform-destroy: ## Destroy Terraform infrastructure (WARNING: destructive)
	@echo "Destroying infrastructure..."
	@read -p "Are you sure? This will destroy all infrastructure. Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ]
	cd infrastructure/terraform && terraform destroy

# Kubernetes operations
k8s-deploy: ## Deploy to Kubernetes
	@echo "Deploying to Kubernetes..."
	kubectl apply -f infrastructure/kubernetes/namespaces/
	kubectl apply -f infrastructure/kubernetes/configmaps/
	kubectl apply -f infrastructure/kubernetes/secrets/
	kubectl apply -f infrastructure/kubernetes/rbac/
	kubectl apply -f infrastructure/kubernetes/deployments/
	kubectl apply -f infrastructure/kubernetes/services/
	kubectl apply -f infrastructure/kubernetes/ingress/
	kubectl apply -f infrastructure/kubernetes/hpa/
	kubectl apply -f infrastructure/kubernetes/network-policies/
	kubectl apply -f infrastructure/kubernetes/pod-disruption-budgets/

k8s-status: ## Check Kubernetes deployment status
	@echo "Checking deployment status..."
	kubectl get pods -n multi-tenant-ingestion
	kubectl get services -n multi-tenant-ingestion
	kubectl get ingress -n multi-tenant-ingestion

k8s-logs: ## Show Kubernetes logs
	@echo "Showing logs for all services..."
	kubectl logs -l app=pipeline-service -n multi-tenant-ingestion --tail=100
	kubectl logs -l app=catalog-service -n multi-tenant-ingestion --tail=100
	kubectl logs -l app=connector-service -n multi-tenant-ingestion --tail=100
	kubectl logs -l app=data-quality-service -n multi-tenant-ingestion --tail=100

# Cleanup
clean: ## Clean up local artifacts
	@echo "Cleaning up..."
	docker system prune -f
	docker volume prune -f
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov/ .pytest_cache/
	@echo "Cleanup completed!"

# Security scans
security-scan: ## Run security scans
	@echo "Running security scans..."
	safety check --json || true
	bandit -r services/ || true
	@echo "Security scan completed!"

# Generate documentation
docs-build: ## Build documentation
	@echo "Building documentation..."
	cd docs && make html
	@echo "Documentation built successfully!"

docs-serve: ## Serve documentation locally
	@echo "Serving documentation at http://localhost:8000"
	cd docs/_build/html && python -m http.server 8000