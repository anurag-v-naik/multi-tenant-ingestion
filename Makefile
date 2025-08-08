.PHONY: help setup-dev clean test format lint build run deploy

# Default target
help:
	@echo "Available commands:"
	@echo "  setup-dev      - Setup development environment"
	@echo "  clean          - Clean build artifacts"
	@echo "  test           - Run all tests"
	@echo "  format         - Format code with black and isort"
	@echo "  lint           - Run linting checks"
	@echo "  build          - Build Docker images"
	@echo "  run            - Run services locally"
	@echo "  deploy-local   - Deploy to local environment"
	@echo "  deploy-staging - Deploy to staging"
	@echo "  deploy-prod    - Deploy to production"

# Setup development environment
setup-dev:
	@echo "Setting up development environment..."
	python -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt
	./venv/bin/pip install -e ".[dev,databricks,monitoring]"
	./venv/bin/pre-commit install
	@echo "Development environment setup complete!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf build/ dist/ htmlcov/ .coverage
	docker system prune -f
	@echo "Clean complete!"

# Run tests
test:
	@echo "Running tests..."
	pytest tests/ -v --cov=services --cov-report=term-missing

test-unit:
	@echo "Running unit tests..."
	pytest tests/unit/ -v

test-integration:
	@echo "Running integration tests..."
	pytest tests/integration/ -v

test-load:
	@echo "Running load tests..."
	pytest tests/load/ -v

# Format code
format:
	@echo "Formatting code..."
	black services/ tests/ scripts/
	isort services/ tests/ scripts/

# Run linting
lint:
	@echo "Running linting checks..."
	flake8 services/ tests/
	mypy services/
	black --check services/ tests/
	isort --check-only services/ tests/

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build

# Run services locally
run:
	@echo "Starting services locally..."
	docker-compose up -d

# Stop local services
stop:
	@echo "Stopping local services..."
	docker-compose down

# Deploy to local environment
deploy-local: build
	@echo "Deploying to local environment..."
	docker-compose up -d
	@echo "Services available at http://localhost:8000"

# Deploy to staging
deploy-staging:
	@echo "Deploying to staging..."
	./deployment/scripts/deploy.sh --environment=staging

# Deploy to production
deploy-prod:
	@echo "Deploying to production..."
	./deployment/scripts/deploy.sh --environment=production

# Setup organization
setup-org:
	@echo "Setting up organization: $(ORG)"
	./deployment/scripts/setup-organization.sh $(ORG)

# Generate test data
generate-test-data:
	@echo "Generating test data..."
	python scripts/generate-test-data.py

# Database migrations
migrate:
	@echo "Running database migrations..."
	alembic upgrade head

# Backup data
backup:
	@echo "Creating backup..."
	./deployment/scripts/backup.sh

# Install pre-commit hooks
install-hooks:
	pre-commit install
	pre-commit install --hook-type commit-msg

# Update dependencies
update-deps:
	pip-compile requirements.in
	pip-compile requirements-dev.in

# Security scan
security-scan:
	@echo "Running security scan..."
	bandit -r services/
	safety check

# Documentation
docs-serve:
	@echo "Serving documentation..."
	cd docs && python -m http.server 8080

# Monitor logs
logs:
	docker-compose logs -f

# Check health
health-check:
	@echo "Checking service health..."
	curl -f http://localhost:8000/health || exit 1
