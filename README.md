# Multi-Tenant Data Ingestion Framework - Deployment Guide

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Terraform >= 1.0
- kubectl (for production)
- AWS CLI (for production)
- Node.js >= 16 (for UI development)
- Python >= 3.9 (for service development)

### Local Development Setup

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd multi-tenant-ingestion-framework
   make setup-dev
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Deploy Local Environment**
   ```bash
   make deploy-local
   ```

4. **Access Services**
   - UI: http://localhost:3000
   - API Gateway: http://localhost:8080
   - Grafana: http://localhost:3001 (admin/admin)
   - Prometheus: http://localhost:9090

### Production Deployment

1. **Configure Infrastructure**
   ```bash
   cd infrastructure/terraform
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your configuration
   ```

2. **Deploy Infrastructure**
   ```bash
   make deploy-production
   ```

3. **Verify Deployment**
   ```bash
   make status
   ```

## Architecture Overview

The framework consists of:

### Core Services
- **Auth Service** (Port 8001): Authentication and authorization
- **Connector Service** (Port 8002): Data connector management
- **Pipeline Service** (Port 8003): Pipeline orchestration
- **Catalog Service** (Port 8004): Metadata management
- **Data Quality Service** (Port 8005): Data quality validation

### Supporting Infrastructure
- **PostgreSQL**: Multi-tenant database
- **Redis**: Session and caching
- **Nginx**: API Gateway and load balancer
- **Prometheus + Grafana**: Monitoring and observability

### Available Connectors
- **RDBMS**: PostgreSQL, MySQL, Oracle
- **Cloud Storage**: AWS S3, Google Cloud Storage
- **Streaming**: Apache Kafka, AWS Kinesis
- **NoSQL**: MongoDB, DynamoDB
- **Data Warehouses**: Snowflake, Databricks
- **File Systems**: SharePoint

## Configuration

### Environment Variables
Key configuration options in `.env`:

```bash
# Core Settings
ENVIRONMENT=development|staging|production
DATABASE_URL=postgresql://user:pass@host:port/db
JWT_SECRET_KEY=your-secret-key

# Databricks Integration
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token

# Multi-tenant Settings
MULTI_TENANT_MODE=true
UNITY_CATALOG_ENABLED=true
ICEBERG_ENABLED=true
```

### Organization Setup
Organizations are configured in `terraform.tfvars`:

```hcl
organizations = {
  "finance" = {
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
}
```

## Operations

### Common Commands
```bash
# Development
make setup-dev          # Initial setup
make build              # Build all images
make deploy-local       # Deploy locally
make test               # Run tests
make logs               # View logs

# Production
make deploy-production  # Full production deployment
make status            # Check service status
make backup            # Create database backup

# Maintenance
make clean             # Clean up containers
make migrate           # Run database migrations
```

### Monitoring
- **Grafana Dashboards**: http://localhost:3001
- **Prometheus Metrics**: http://localhost:9090
- **Service Health**: http://localhost:8080/health

### Troubleshooting
1. **Check service health**: `make status`
2. **View logs**: `make logs`
3. **Restart services**: `docker-compose restart`
4. **Reset environment**: `make clean && make deploy-local`

## Security Considerations

### Multi-Tenant Isolation
- Database-level tenant isolation
- API-level tenant context enforcement
- Resource quotas and rate limiting
- Audit logging for all operations

### Authentication & Authorization
- JWT-based authentication
- Role-based access control (RBAC)
- Multi-factor authentication support
- Session management and timeout

### Data Encryption
- Encryption at rest (database, S3)
- Encryption in transit (TLS/SSL)
- Credential encryption in database
- Secure secret management

## Scaling and Performance

### Horizontal Scaling
- Kubernetes-based service scaling
- Load balancing with Nginx
- Database connection pooling
- Redis for distributed caching

### Resource Management
- Per-tenant resource quotas
- Cost allocation and chargeback
- Performance monitoring
- Auto-scaling based on workload

## Support and Maintenance

### Backup and Recovery
- Automated database backups
- S3 data versioning
- Disaster recovery procedures
- Point-in-time recovery

### Updates and Migrations
- Rolling updates with zero downtime
- Database migration scripts
- Backward compatibility
- Rollback procedures

### Monitoring and Alerting
- Service health monitoring
- Performance metrics collection
- Error tracking and alerting
- SLA monitoring and reporting