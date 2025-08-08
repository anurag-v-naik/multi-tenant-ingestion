# 🏗️ Multi-Tenant Data Ingestion Framework

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Terraform](https://img.shields.io/badge/terraform-1.0+-purple.svg)](https://www.terraform.io/)
[![Databricks](https://img.shields.io/badge/databricks-enterprise-orange.svg)](https://databricks.com/)
[![AWS](https://img.shields.io/badge/AWS-cloud-orange.svg)](https://aws.amazon.com/)

A production-ready, enterprise-grade multi-tenant data ingestion framework built with Databricks, Unity Catalog, Apache Iceberg, and AWS. Designed for organizations that need secure, scalable, and cost-effective data processing with complete tenant isolation.

## 🎯 Key Features

- **🏢 Multi-Tenant Architecture**: Complete isolation between organizations with dedicated resources
- **🔥 Databricks Integration**: PySpark execution with organization-specific workspaces
- **📊 Unity Catalog**: Centralized metadata management with tenant governance
- **🧊 Iceberg Tables**: Cross-platform interoperability (Snowflake, Redshift, BigQuery)
- **🔒 Enterprise Security**: End-to-end encryption, RBAC, and compliance automation
- **💰 Cost Management**: Detailed chargeback, resource quotas, and optimization
- **🚀 Auto-Scaling**: Dynamic resource allocation based on workload patterns
- **📈 Monitoring**: Comprehensive observability and alerting

## 🏗️ Architecture Overview

```mermaid
graph TB
    subgraph "Multi-Tenant UI Layer"
        UI[React Multi-Tenant Dashboard]
        API[API Gateway with Tenant Routing]
    end
    
    subgraph "Microservices Layer"
        PS[Pipeline Service]
        CS[Connector Service]
        DQS[Data Quality Service]
        CAS[Catalog Service]
        NS[Notification Service]
    end
    
    subgraph "Processing Layer - Databricks"
        DB1[Finance Workspace]
        DB2[Retail Workspace]
        DB3[Healthcare Workspace]
    end
    
    subgraph "Data Layer"
        UC[Unity Catalog]
        ICE[Iceberg Tables]
        S3A[Finance S3 Bucket]
        S3B[Retail S3 Bucket]
        S3C[Healthcare S3 Bucket]
    end
    
    subgraph "Cross-Platform Integration"
        SF[Snowflake]
        RS[Redshift]
        BQ[BigQuery]
    end
    
    UI --> API
    API --> PS
    API --> CS
    API --> DQS
    API --> CAS
    
    PS --> DB1
    PS --> DB2
    PS --> DB3
    
    DB1 --> UC
    DB2 --> UC
    DB3 --> UC
    
    UC --> ICE
    ICE --> S3A
    ICE --> S3B
    ICE --> S3C
    
    ICE --> SF
    ICE --> RS
    ICE --> BQ
```

## 🚀 Quick Start

### Prerequisites

- **AWS Account** with administrative access
- **Databricks Account** (Premium or Enterprise tier)
- **Terraform** >= 1.0
- **Docker** >= 20.0
- **Python** >= 3.9
- **Node.js** >= 16.0

### 1. Clone Repository

```bash
git clone https://github.com/anurag-v-naik/multi-tenant-ingestion.git
cd multi-tenant-ingestion-framework
```

### 2. Infrastructure Setup

```bash
# Configure AWS credentials
aws configure

# Copy and edit Terraform variables
cp infrastructure/terraform/terraform.tfvars.example infrastructure/terraform/terraform.tfvars
nano infrastructure/terraform/terraform.tfvars

# Deploy infrastructure
cd infrastructure/terraform
terraform init
terraform plan
terraform apply -auto-approve
```

### 3. Application Deployment

```bash
# Return to root directory
cd ../../

# Build and deploy services
./deployment/scripts/deploy.sh --build-images --deploy-services

# Setup organizations
./deployment/scripts/setup-organization.sh finance
./deployment/scripts/setup-organization.sh retail
./deployment/scripts/setup-organization.sh healthcare
```

### 4. Verify Deployment

```bash
# Check service health
curl https://$(terraform output -raw load_balancer_dns)/health

# Access the UI
open https://$(terraform output -raw load_balancer_dns)
```

## 📁 Repository Structure

```
multi-tenant-ingestion-framework/
├── .github/
│   └── workflows/
│       ├── ci.yml                     # Continuous Integration
│       ├── cd.yml                     # Continuous Deployment
│       └── security-scan.yml          # Security scanning
├── docs/
│   ├── ARCHITECTURE.md                # Detailed architecture guide
│   ├── DEPLOYMENT.md                  # Deployment guide
│   ├── API_REFERENCE.md               # API documentation
│   ├── USER_GUIDE.md                  # End-user guide
│   └── CONTRIBUTING.md                # Contribution guidelines
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf                    # Complete AWS infrastructure
│   │   ├── variables.tf               # Configuration variables
│   │   ├── outputs.tf                 # Infrastructure outputs
│   │   └── terraform.tfvars.example   # Example configuration
│   ├── kubernetes/
│   │   ├── namespaces/                # Organization namespaces
│   │   ├── ingress/                   # Multi-tenant ingress
│   │   └── monitoring/                # Observability stack
│   └── databricks/
│       ├── workspace-config.py        # Workspace automation
│       └── unity-catalog-setup.sql    # Catalog initialization
├── services/
│   ├── pipeline-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── app/
│   │   │   ├── main.py                # FastAPI application
│   │   │   ├── models/                # Database models
│   │   │   ├── api/                   # API endpoints
│   │   │   └── core/                  # Business logic
│   │   └── notebooks/
│   │       └── pyspark_template.py    # Databricks notebook template
│   ├── catalog-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── main.py                # Unity Catalog & Iceberg service
│   │       ├── models/
│   │       ├── api/
│   │       └── core/
│   ├── connector-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── main.py                # Connector registry
│   │       ├── connectors/            # Built-in connectors
│   │       └── templates/             # Connector templates
│   ├── data-quality-service/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   │       ├── main.py                # DQ validation service
│   │       ├── rules/                 # Quality rules engine
│   │       └── reports/               # DQ reporting
│   └── ui/
│       ├── Dockerfile
│       ├── package.json
│       ├── src/
│       │   ├── components/            # React components
│       │   ├── pages/                 # Application pages
│       │   ├── hooks/                 # Custom hooks
│       │   └── utils/                 # Utility functions
│       └── public/
├── deployment/
│   ├── docker-compose.yml             # Local development
│   ├── helm-charts/                   # Kubernetes deployment
│   │   ├── multi-tenant-ingestion/
│   │   └── monitoring/
│   ├── scripts/
│   │   ├── deploy.sh                  # Main deployment script
│   │   ├── setup-organization.sh      # Organization setup
│   │   ├── backup.sh                  # Backup automation
│   │   └── rollback.sh                # Rollback procedures
│   └── configs/
│       ├── production.env             # Production configuration
│       ├── staging.env                # Staging configuration
│       └── development.env            # Development configuration
├── examples/
│   ├── pipeline-configs/              # Sample pipeline configurations
│   ├── connectors/                    # Custom connector examples
│   ├── notebooks/                     # Sample Databricks notebooks
│   └── data-quality-rules/            # DQ rule examples
├── tests/
│   ├── unit/                          # Unit tests
│   ├── integration/                   # Integration tests
│   ├── load/                          # Performance tests
│   └── fixtures/                      # Test data
├── scripts/
│   ├── setup-dev-environment.sh       # Development setup
│   ├── generate-test-data.py          # Test data generation
│   └── migrate-organization.py        # Organization migration
├── .env.example                       # Environment variables template
├── .gitignore                         # Git ignore rules
├── .pre-commit-config.yaml           # Pre-commit hooks
├── docker-compose.yml                # Local development stack
├── LICENSE                           # MIT License
├── Makefile                          # Development commands
├── pyproject.toml                    # Python project configuration
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012

# Databricks Configuration
DATABRICKS_ACCOUNT_ID=your-databricks-account-id
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com

# Database Configuration
DATABASE_URL=postgresql://user:pass@localhost:5432/multi_tenant_ingestion
REDIS_URL=redis://localhost:6379/0

# Security
JWT_SECRET_KEY=your-jwt-secret-key
ENCRYPTION_KEY=your-encryption-key

# Feature Flags
UNITY_CATALOG_ENABLED=true
ICEBERG_ENABLED=true
MULTI_TENANT_MODE=true
```

### Organization Configuration

```yaml
# terraform.tfvars
organizations = {
  "finance" = {
    name             = "finance"
    display_name     = "Finance Department"
    cost_center      = "FIN-001"
    compliance_level = "high"
    resource_quotas = {
      max_dbu_per_hour         = 100
      max_storage_gb           = 10000
      max_api_calls_per_minute = 1000
    }
  }
  "retail" = {
    name             = "retail"
    display_name     = "Retail Division"
    cost_center      = "RET-001"
    compliance_level = "medium"
    resource_quotas = {
      max_dbu_per_hour         = 200
      max_storage_gb           = 50000
      max_api_calls_per_minute = 2000
    }
  }
}
```

## 🐳 Local Development

### Using Docker Compose

```bash
# Start all services locally
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Using Make Commands

```bash
# Setup development environment
make setup-dev

# Run tests
make test

# Format code
make format

# Build images
make build

# Deploy locally
make deploy-local
```

## 🧪 Testing

### Run All Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Load tests
pytest tests/load/ -v

# Coverage report
pytest --cov=services/ --cov-report=html
```

### Specific Test Categories

```bash
# Test multi-tenant isolation
pytest tests/integration/test_tenant_isolation.py -v

# Test Databricks integration
pytest tests/integration/test_databricks.py -v

# Test Unity Catalog functionality
pytest tests/integration/test_unity_catalog.py -v

# Test Iceberg interoperability
pytest tests/integration/test_iceberg.py -v
```

## 📊 Monitoring & Observability

### Health Checks

```bash
# Service health
curl http://localhost:8000/health

# Database connectivity
curl http://localhost:8000/health/database

# Databricks connectivity
curl http://localhost:8000/health/databricks

# Unity Catalog connectivity
curl http://localhost:8000/health/unity-catalog
```

### Metrics & Dashboards

- **Grafana Dashboard**: `http://localhost:3000`
- **Prometheus Metrics**: `http://localhost:9090`
- **CloudWatch Dashboard**: Available in AWS Console
- **Application Logs**: Available in CloudWatch Logs

## 🔐 Security

### Authentication & Authorization

```bash
# Generate JWT token for testing
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin", "organization_id": "finance"}'

# Use token in API calls
curl -H "Authorization: Bearer $JWT_TOKEN" \
     -H "X-Organization-ID: finance" \
     http://localhost:8000/api/v1/pipelines
```

### Secrets Management

```bash
# Create organization secrets
aws secretsmanager create-secret \
  --name "finance/databricks-token" \
  --secret-string '{"token": "dapi-xxx", "workspace_url": "https://finance.databricks.com"}'

# Rotate secrets
./scripts/rotate-secrets.sh finance
```

## 💰 Cost Management

### Resource Monitoring

```bash
# Check organization costs
./scripts/cost-report.sh finance

# Set resource quotas
./scripts/set-quota.sh finance --max-dbu=100 --max-storage=10000

# Generate cost allocation report
./scripts/chargeback-report.sh --month=2024-01
```

## 🚀 Deployment

### Production Deployment

```bash
# Deploy to production
./deployment/scripts/deploy.sh \
  --environment=production \
  --build-images \
  --deploy-services \
  --run-migrations

# Verify deployment
./deployment/scripts/verify-deployment.sh production
```

### Staging Deployment

```bash
# Deploy to staging
./deployment/scripts/deploy.sh \
  --environment=staging \
  --skip-migrations

# Run integration tests
./deployment/scripts/run-integration-tests.sh staging
```

## 📚 Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) - Detailed system architecture
- [Deployment Guide](docs/DEPLOYMENT.md) - Step-by-step deployment instructions
- [API Reference](docs/API_REFERENCE.md) - Complete API documentation
- [User Guide](docs/USER_GUIDE.md) - End-user documentation
- [Contributing Guide](docs/CONTRIBUTING.md) - How to contribute

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](docs/CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `make test`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Submit a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/your-org/multi-tenant-ingestion-framework/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/multi-tenant-ingestion-framework/discussions)
- **Documentation**: [docs/](docs/)
- **Email**: data-engineering@yourcompany.com

## 🏷️ Tags

`data-engineering` `multi-tenant` `databricks` `unity-catalog` `iceberg` `aws` `terraform` `pyspark` `data-pipeline` `enterprise` `microservices` `cloud-native`

---

⭐ **Star this repository** if you find it helpful!

**Built with ❤️ by the Data Engineering Team**
