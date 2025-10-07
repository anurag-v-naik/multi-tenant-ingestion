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

## Health Check Endpoints and Status

### Service Health Endpoints

Each service exposes health check endpoints for monitoring:

- **API Gateway**: `http://localhost:8080/health`
- **Ingestion Service**: `http://localhost:8081/health`
- **Quality Service**: `http://localhost:8082/health`
- **Metadata Service**: `http://localhost:8083/health`
- **UI**: `http://localhost:3000/health`

### Health Check Response Format

```json
{
  "status": "healthy",
  "timestamp": "2025-10-07T18:51:00Z",
  "version": "1.0.0",
  "checks": {
    "database": "healthy",
    "redis": "healthy",
    "storage": "healthy"
  }
}
```

### Monitoring Service Status

```bash
# Check all services
make health-check

# Check individual service
curl http://localhost:8080/health

# View service logs
docker-compose logs -f [service-name]
```

## Troubleshooting FAQs

### Docker Compose Issues

**Q: Services fail to start with port conflicts**

A: Check if ports are already in use:
```bash
# Check port usage
lsof -i :8080
netstat -an | grep 8080

# Stop conflicting services or modify docker-compose.yml ports
```

**Q: Containers keep restarting**

A: Check container logs:
```bash
docker-compose logs [service-name]
docker-compose ps

# Restart specific service
docker-compose restart [service-name]
```

**Q: Out of memory errors**

A: Adjust Docker resource limits in `docker-compose.yml`:
```yaml
services:
  service-name:
    mem_limit: 2g
    mem_reservation: 1g
```

### Database Connectivity Issues

**Q: Cannot connect to PostgreSQL**

A: Verify database is running and accessible:
```bash
# Check database container
docker-compose ps postgres

# Test connection
docker-compose exec postgres psql -U postgres -c "SELECT 1;"

# Check connection string in .env
DATABASE_URL=postgresql://postgres:password@localhost:5432/ingestion_db
```

**Q: Database migrations fail**

A: Reset and re-run migrations:
```bash
# Check migration status
make db-status

# Rollback and retry
make db-rollback
make db-migrate

# Force reset (WARNING: destroys data)
make db-reset
```

**Q: Connection pool exhausted**

A: Adjust pool settings in service configuration:
```yaml
database:
  pool_size: 20
  max_overflow: 10
  pool_timeout: 30
```

### Service Startup Issues

**Q: Services start but return 500 errors**

A: Check service dependencies and configuration:
```bash
# Verify all dependencies are ready
make health-check

# Check environment variables
docker-compose config

# Review service logs
docker-compose logs -f [service-name]
```

**Q: Service initialization timeout**

A: Increase startup timeout and check dependencies:
```bash
# Increase healthcheck timeout in docker-compose.yml
healthcheck:
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 60s
```

**Q: Redis connection errors**

A: Verify Redis service:
```bash
# Check Redis container
docker-compose ps redis

# Test Redis connection
docker-compose exec redis redis-cli ping

# Verify Redis URL in .env
REDIS_URL=redis://localhost:6379/0
```

## Running Tests

### Unit Tests

Run unit tests for individual services:

```bash
# Run all unit tests
make test-unit

# Run tests for specific service
cd services/ingestion-service
pytest tests/unit/

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html
```

### Integration Tests

Run integration tests that test service interactions:

```bash
# Start test environment
make test-env-up

# Run all integration tests
make test-integration

# Run specific integration test suite
pytest tests/integration/test_ingestion_flow.py

# Cleanup test environment
make test-env-down
```

### Infrastructure Tests

Test infrastructure configuration and deployment:

```bash
# Validate Terraform configuration
cd infrastructure/terraform
terraform validate
terraform plan

# Run infrastructure tests
make test-infrastructure

# Test Kubernetes manifests
cd infrastructure/kubernetes
kubectl apply --dry-run=client -f .
```

### Running All Tests

Run the complete test suite:

```bash
# Run all tests (unit + integration + infrastructure)
make test-all

# Run with verbose output
make test-all VERBOSE=1

# Generate test report
make test-report
```

### Test Configuration

Configure test settings in `.env.test`:

```bash
# Copy test environment template
cp .env.test.example .env.test

# Edit test configuration
TEST_DATABASE_URL=postgresql://postgres:password@localhost:5433/test_db
TEST_REDIS_URL=redis://localhost:6380/0
```

## Extending the Framework

### Adding a New Connector

To add a new data source connector:

1. **Create Connector Class**

   Create a new file in `services/ingestion-service/src/connectors/`:

   ```python
   # src/connectors/my_connector.py
   from .base import BaseConnector
   
   class MyConnector(BaseConnector):
       """Connector for MyDataSource."""
       
       def __init__(self, config: dict):
           super().__init__(config)
           self.client = self._initialize_client()
       
       def connect(self) -> bool:
           """Establish connection to data source."""
           # Implementation
           pass
       
       def extract_data(self, **kwargs) -> Iterator[dict]:
           """Extract data from source."""
           # Implementation
           pass
       
       def validate_connection(self) -> bool:
           """Validate connection parameters."""
           # Implementation
           pass
   ```

2. **Register Connector**

   Add connector to registry in `src/connectors/__init__.py`:

   ```python
   from .my_connector import MyConnector
   
   CONNECTOR_REGISTRY = {
       'my_source': MyConnector,
       # ... other connectors
   }
   ```

3. **Add Configuration Schema**

   Define connector configuration in `src/schemas/connector_config.py`:

   ```python
   MY_CONNECTOR_SCHEMA = {
       'type': 'object',
       'properties': {
           'host': {'type': 'string'},
           'port': {'type': 'integer'},
           'credentials': {'type': 'object'}
       },
       'required': ['host', 'credentials']
   }
   ```

4. **Add Tests**

   Create tests in `tests/unit/connectors/test_my_connector.py`:

   ```python
   import pytest
   from src.connectors.my_connector import MyConnector
   
   def test_connector_initialization():
       config = {'host': 'localhost', 'credentials': {}}
       connector = MyConnector(config)
       assert connector is not None
   ```

5. **Update Documentation**

   Add connector documentation to `docs/connectors/my_connector.md`

### Adding a Quality Rule

To add a new data quality rule:

1. **Create Rule Class**

   Create a new file in `services/quality-service/src/rules/`:

   ```python
   # src/rules/my_rule.py
   from .base import BaseRule
   
   class MyQualityRule(BaseRule):
       """Custom data quality rule."""
       
       def __init__(self, config: dict):
           super().__init__(config)
           self.threshold = config.get('threshold', 0.95)
       
       def validate(self, data: pd.DataFrame) -> dict:
           """Validate data against rule."""
           # Implementation
           result = {
               'passed': True,
               'score': 0.98,
               'details': {}
           }
           return result
       
       def get_metadata(self) -> dict:
           """Return rule metadata."""
           return {
               'name': 'my_quality_rule',
               'description': 'Custom quality check',
               'category': 'accuracy'
           }
   ```

2. **Register Rule**

   Add rule to registry in `src/rules/__init__.py`:

   ```python
   from .my_rule import MyQualityRule
   
   RULE_REGISTRY = {
       'my_rule': MyQualityRule,
       # ... other rules
   }
   ```

3. **Add Rule Configuration**

   Define rule parameters in `src/schemas/rule_config.py`:

   ```python
   MY_RULE_SCHEMA = {
       'type': 'object',
       'properties': {
           'threshold': {'type': 'number', 'minimum': 0, 'maximum': 1},
           'strict_mode': {'type': 'boolean'}
       }
   }
   ```

4. **Add Tests**

   Create tests in `tests/unit/rules/test_my_rule.py`:

   ```python
   import pytest
   import pandas as pd
   from src.rules.my_rule import MyQualityRule
   
   def test_rule_validation():
       rule = MyQualityRule({'threshold': 0.9})
       data = pd.DataFrame({'col': [1, 2, 3]})
       result = rule.validate(data)
       assert result['passed'] is True
   ```

5. **Update Documentation**

   Add rule documentation to `docs/quality-rules/my_rule.md`

### Best Practices for Extensions

- Follow the existing code structure and naming conventions
- Implement comprehensive error handling
- Add logging for debugging and monitoring
- Write unit and integration tests
- Update documentation and examples
- Consider backward compatibility
- Add configuration validation
- Include performance considerations
