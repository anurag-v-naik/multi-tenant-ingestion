.PHONY: help setup-dev build test lint format clean deploy-local deploy-staging deploy-production

# Default target
help:
	@echo "Available targets:"
	@echo "  setup-dev        - Setup development environment"
	@echo "  build           - Build all Docker images"
	@echo "  test            - Run all tests"
	@echo "  lint            - Run linting checks"
	@echo "  format          - Format code"
	@echo "  clean           - Clean up containers and images"
	@echo "  deploy-local    - Deploy to local development environment"
	@echo "  deploy-staging  - Deploy to staging environment"
	@echo "  deploy-prod     - Deploy to production environment"

# Development setup
setup-dev:
	@echo "Setting up development environment..."
	@chmod +x deployment/scripts/*.sh
	@cp .env.example .env
	@echo "Please edit .env file with your configuration"
	@docker-compose pull
	@echo "Development environment setup complete!"

# Build all services
build:
	@echo "Building all Docker images..."
	@docker-compose build
	@echo "Build complete!"

# Run tests
test:
	@echo "Running tests..."
	@docker-compose exec auth-service pytest tests/ -v
	@docker-compose exec connector-service pytest tests/ -v
	@docker-compose exec pipeline-service pytest tests/ -v
	@echo "Tests complete!"

# Linting
lint:
	@echo "Running linting..."
	@docker-compose exec auth-service flake8 app/
	@docker-compose exec connector-service flake8 app/
	@docker-compose exec pipeline-service flake8 app/
	@echo "Linting complete!"

# Code formatting
format:
	@echo "Formatting code..."
	@docker-compose exec auth-service black app/
	@docker-compose exec connector-service black app/
	@docker-compose exec pipeline-service black app/
	@echo "Formatting complete!"

# Clean up
clean:
	@echo "Cleaning up..."
	@docker-compose down -v
	@docker system prune -f
	@echo "Cleanup complete!"

# Local deployment
deploy-local:
	@echo "Deploying to local environment..."
	@./deployment/scripts/deploy.sh development true true true
	@echo "Local deployment complete!"

# Staging deployment
deploy-staging:
	@echo "Deploying to staging environment..."
	@./deployment/scripts/deploy.sh staging true true true
	@echo "Staging deployment complete!"

# Production deployment
deploy-production:
	@echo "Deploying to production environment..."
	@./deployment/scripts/deploy.sh production true true true
	@echo "Production deployment complete!"

# Database migrations
migrate:
	@echo "Running database migrations..."
	@docker-compose exec auth-service alembic upgrade head
	@docker-compose exec connector-service alembic upgrade head
	@docker-compose exec pipeline-service alembic upgrade head
	@echo "Migrations complete!"

# Monitoring
logs:
	@docker-compose logs -f

status:
	@docker-compose ps
	@echo ""
	@echo "Health checks:"
	@curl -f http://localhost:8080/health || echo "API Gateway: DOWN"

# Backup
backup:
	@echo "Creating backup..."
	@docker-compose exec postgres pg_dump -U postgres multi_tenant_ingestion > backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "Backup created!"
