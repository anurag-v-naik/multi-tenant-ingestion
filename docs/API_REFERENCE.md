# Multi-Tenant Data Ingestion Framework - API Reference

## Table of Contents
1. [Authentication](#authentication)
2. [Pipeline Service API](#pipeline-service-api)
3. [Catalog Service API](#catalog-service-api)
4. [Connector Service API](#connector-service-api)
5. [Data Quality Service API](#data-quality-service-api)
6. [Notification Service API](#notification-service-api)
7. [Error Handling](#error-handling)
8. [Rate Limiting](#rate-limiting)
9. [Webhook Events](#webhook-events)

## Authentication

All API endpoints require authentication using JWT tokens. Include the token in the Authorization header and specify the organization ID.

### Headers
```http
Authorization: Bearer <jwt_token>
X-Organization-ID: <organization_id>
Content-Type: application/json
```

### Authentication Flow

#### Login
```http
POST /auth/login
```

**Request Body:**
```json
{
  "username": "user@example.com",
  "password": "password",
  "organization_id": "finance"
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "organization_id": "finance",
  "permissions": ["pipeline:read", "pipeline:write"]
}
```

#### Token Refresh
```http
POST /auth/refresh
```

**Request Body:**
```json
{
  "refresh_token": "refresh_token_here"
}
```

## Pipeline Service API

### Base URL
```
https://api.multi-tenant-ingestion.com/api/v1/pipelines
```

### Create Pipeline

```http
POST /
```

**Request Body:**
```json
{
  "name": "Customer Data Ingestion",
  "description": "Daily customer data import from CRM",
  "source_type": "postgresql",
  "target_type": "iceberg",
  "configuration": {
    "source": {
      "host": "crm-db.company.com",
      "port": 5432,
      "database": "crm",
      "schema": "public",
      "table": "customers",
      "connection_string": "postgresql://user:pass@host:5432/db"
    },
    "target": {
      "catalog": "finance_catalog",
      "schema": "raw",
      "table": "customers",
      "location": "s3://finance-data/raw/customers/",
      "file_format": "parquet"
    },
    "transformation": {
      "sql": "SELECT customer_id, name, email, created_at FROM customers WHERE created_at >= '${start_date}'",
      "parameters": {
        "start_date": "2024-01-01"
      }
    }
  },
  "schedule": {
    "cron": "0 2 * * *",
    "timezone": "UTC",
    "enabled": true
  },
  "data_quality_rules": [
    {
      "type": "not_null",
      "column": "customer_id"
    },
    {
      "type": "unique",
      "column": "email"
    }
  ]
}
```

**Response:**
```json
{
  "pipeline_id": "pip_123e4567-e89b-12d3-a456-426614174000",
  "status": "created",
  "message": "Pipeline created successfully",
  "deployment_status": "in_progress",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### List Pipelines

```http
GET /?status=active&limit=20&offset=0
```

**Query Parameters:**
- `status` (optional): Filter by status (created, active, paused, failed, deleted)
- `limit` (optional): Number of results (default: 20, max: 100)
- `offset` (optional): Pagination offset (default: 0)
- `sort` (optional): Sort field (name, created_at, updated_at)
- `order` (optional): Sort order (asc, desc)

**Response:**
```json
{
  "pipelines": [
    {
      "id": "pip_123e4567-e89b-12d3-a456-426614174000",
      "name": "Customer Data Ingestion",
      "description": "Daily customer data import from CRM",
      "source_type": "postgresql",
      "target_type": "iceberg",
      "status": "active",
      "schedule": {
        "cron": "0 2 * * *",
        "timezone": "UTC",
        "enabled": true
      },
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z",
      "last_execution": {
        "id": "exec_789",
        "status": "completed",
        "started_at": "2024-01-16T02:00:00Z",
        "completed_at": "2024-01-16T02:15:00Z",
        "records_processed": 15420
      }
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

### Get Pipeline Details

```http
GET /{pipeline_id}
```

**Response:**
```json
{
  "id": "pip_123e4567-e89b-12d3-a456-426614174000",
  "name": "Customer Data Ingestion",
  "description": "Daily customer data import from CRM",
  "source_type": "postgresql",
  "target_type": "iceberg",
  "configuration": {
    "source": {
      "host": "crm-db.company.com",
      "port": 5432,
      "database": "crm",
      "schema": "public",
      "table": "customers"
    },
    "target": {
      "catalog": "finance_catalog",
      "schema": "raw",
      "table": "customers",
      "location": "s3://finance-data/raw/customers/"
    }
  },
  "status": "active",
  "schedule": {
    "cron": "0 2 * * *",
    "timezone": "UTC",
    "enabled": true,
    "next_run": "2024-01-17T02:00:00Z"
  },
  "data_quality_rules": [
    {
      "type": "not_null",
      "column": "customer_id"
    }
  ],
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:00Z",
  "recent_executions": [
    {
      "id": "exec_789",
      "status": "completed",
      "started_at": "2024-01-16T02:00:00Z",
      "completed_at": "2024-01-16T02:15:00Z",
      "records_processed": 15420,
      "error_message": null
    }
  ],
  "metrics": {
    "total_executions": 30,
    "successful_executions": 28,
    "failed_executions": 2,
    "avg_execution_time_minutes": 12.5,
    "avg_records_per_execution": 15200
  }
}
```

### Update Pipeline

```http
PUT /{pipeline_id}
```

**Request Body:**
```json
{
  "name": "Updated Customer Data Ingestion",
  "description": "Updated description",
  "schedule": {
    "cron": "0 3 * * *",
    "timezone": "UTC",
    "enabled": true
  }
}
```

### Execute Pipeline

```http
POST /{pipeline_id}/execute
```

**Request Body (optional):**
```json
{
  "parameters": {
    "start_date": "2024-01-15",
    "end_date": "2024-01-16"
  },
  "force": false
}
```

**Response:**
```json
{
  "execution_id": "exec_987654321",
  "status": "started",
  "message": "Pipeline execution initiated",
  "started_at": "2024-01-16T15:30:00Z"
}
```

### Pause/Resume Pipeline

```http
POST /{pipeline_id}/pause
POST /{pipeline_id}/resume
```

### Delete Pipeline

```http
DELETE /{pipeline_id}
```

**Response:**
```json
{
  "message": "Pipeline deleted successfully",
  "deleted_at": "2024-01-16T15:30:00Z"
}
```

### Pipeline Executions

#### List Executions
```http
GET /{pipeline_id}/executions
```

#### Get Execution Details
```http
GET /{pipeline_id}/executions/{execution_id}
```

#### Cancel Execution
```http
POST /{pipeline_id}/executions/{execution_id}/cancel
```

## Catalog Service API

### Base URL
```
https://api.multi-tenant-ingestion.com/api/v1/catalog
```

### List Catalogs

```http
GET /catalogs
```

**Response:**
```json
{
  "catalogs": [
    {
      "name": "finance_catalog",
      "type": "unity_catalog",
      "provider": "databricks",
      "created_at": "2024-01-15T10:00:00Z",
      "schemas": [
        {
          "name": "raw",
          "tables_count": 25
        },
        {
          "name": "processed",
          "tables_count": 15
        }
      ]
    }
  ]
}
```

### List Schemas

```http
GET /catalogs/{catalog_name}/schemas
```

### List Tables

```http
GET /catalogs/{catalog_name}/schemas/{schema_name}/tables
```

**Response:**
```json
{
  "tables": [
    {
      "name": "customers",
      "type": "iceberg",
      "location": "s3://finance-data/raw/customers/",
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-16T02:15:00Z",
      "row_count": 15420,
      "size_bytes": 2048576,
      "columns": [
        {
          "name": "customer_id",
          "type": "bigint",
          "nullable": false
        },
        {
          "name": "name",
          "type": "string",
          "nullable": true
        },
        {
          "name": "email",
          "type": "string",
          "nullable": false
        }
      ],
      "partitions": [
        {
          "column": "created_date",
          "type": "date"
        }
      ]
    }
  ]
}
```

### Get Table Schema

```http
GET /catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/schema
```

### Get Table Statistics

```http
GET /catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/stats
```

**Response:**
```json
{
  "table_name": "customers",
  "row_count": 15420,
  "size_bytes": 2048576,
  "partition_count": 30,
  "last_updated": "2024-01-16T02:15:00Z",
  "column_stats": {
    "customer_id": {
      "null_count": 0,
      "distinct_count": 15420,
      "min_value": 1,
      "max_value": 15420
    },
    "email": {
      "null_count": 0,
      "distinct_count": 15420
    }
  }
}
```

## Connector Service API

### Base URL
```
https://api.multi-tenant-ingestion.com/api/v1/connectors
```

### List Available Connectors

```http
GET /types
```

**Response:**
```json
{
  "connectors": [
    {
      "type": "postgresql",
      "name": "PostgreSQL Database",
      "category": "database",
      "version": "1.0.0",
      "description": "Connect to PostgreSQL databases",
      "configuration_schema": {
        "type": "object",
        "properties": {
          "host": {"type": "string", "required": true},
          "port": {"type": "integer", "default": 5432},
          "database": {"type": "string", "required": true},
          "username": {"type": "string", "required": true},
          "password": {"type": "string", "required": true, "sensitive": true}
        }
      }
    }
  ]
}
```

### Create Connector Configuration

```http
POST /
```

**Request Body:**
```json
{
  "name": "CRM Database",
  "type": "postgresql",
  "description": "Primary CRM database connection",
  "configuration": {
    "host": "crm-db.company.com",
    "port": 5432,
    "database": "crm",
    "username": "readonly_user",
    "password": "secure_password",
    "ssl_mode": "require"
  },
  "tags": ["crm", "production"]
}
```

### Test Connector

```http
POST /{connector_id}/test
```

**Response:**
```json
{
  "status": "success",
  "message": "Connection successful",
  "latency_ms": 45,
  "metadata": {
    "database_version": "PostgreSQL 14.5",
    "available_schemas": ["public", "crm"]
  }
}
```

### List Connector Configurations

```http
GET /
```

### Get Connector Schema

```http
GET /{connector_id}/schema
```

**Response:**
```json
{
  "schemas": [
    {
      "name": "public",
      "tables": [
        {
          "name": "customers",
          "columns": [
            {
              "name": "id",
              "type": "integer",
              "nullable": false
            },
            {
              "name": "name",
              "type": "varchar",
              "nullable": true,
              "max_length": 255
            }
          ]
        }
      ]
    }
  ]
}
```

## Data Quality Service API

### Base URL
```
https://api.multi-tenant-ingestion.com/api/v1/data-quality
```

### Create Quality Rule

```http
POST /rules
```

**Request Body:**
```json
{
  "name": "Customer ID Not Null",
  "description": "Ensure customer_id is never null",
  "type": "not_null",
  "target": {
    "catalog": "finance_catalog",
    "schema": "raw",
    "table": "customers",
    "column": "customer_id"
  },
  "severity": "critical",
  "enabled": true
}
```

### List Quality Rules

```http
GET /rules
```

### Run Quality Check

```http
POST /checks
```

**Request Body:**
```json
{
  "target": {
    "catalog": "finance_catalog",
    "schema": "raw",
    "table": "customers"
  },
  "rules": ["rule_123", "rule_456"]
}
```

**Response:**
```json
{
  "check_id": "check_789",
  "status": "running",
  "started_at": "2024-01-16T15:00:00Z"
}
```

### Get Quality Report

```http
GET /checks/{check_id}/report
```

**Response:**
```json
{
  "check_id": "check_789",
  "status": "completed",
  "started_at": "2024-01-16T15:00:00Z",
  "completed_at": "2024-01-16T15:05:00Z",
  "overall_score": 0.95,
  "rules_results": [
    {
      "rule_id": "rule_123",
      "rule_name": "Customer ID Not Null",
      "status": "passed",
      "score": 1.0,
      "details": {
        "total_records": 15420,
        "passed_records": 15420,
        "failed_records": 0
      }
    },
    {
      "rule_id": "rule_456",
      "rule_name": "Email Format Valid",
      "status": "failed",
      "score": 0.92,
      "details": {
        "total_records": 15420,
        "passed_records": 14186,
        "failed_records": 1234,
        "sample_failures": [
          "invalid-email-1",
          "invalid-email-2"
        ]
      }
    }
  ]
}
```

## Notification Service API

### Base URL
```
https://api.multi-tenant-ingestion.com/api/v1/notifications
```

### Create Notification Channel

```http
POST /channels
```

**Request Body:**
```json
{
  "name": "Team Slack Channel",
  "type": "slack",
  "configuration": {
    "webhook_url": "https://hooks.slack.com/services/...",
    "channel": "#data-team",
    "username": "DataBot"
  },
  "enabled": true
}
```

### Create Alert Rule

```http
POST /alerts
```

**Request Body:**
```json
{
  "name": "Pipeline Failure Alert",
  "description": "Alert when any pipeline fails",
  "condition": {
    "event_type": "pipeline.execution.failed",
    "filters": {
      "severity": ["critical", "high"]
    }
  },
  "channels": ["channel_123"],
  "template": {
    "subject": "Pipeline Failure: {{pipeline_name}}",
    "body": "Pipeline {{pipeline_name}} failed at {{timestamp}}. Error: {{error_message}}"
  },
  "enabled": true
}
```

### Send Notification

```http
POST /send
```

**Request Body:**
```json
{
  "channel_id": "channel_123",
  "subject": "Test Notification",
  "message": "This is a test notification",
  "priority": "medium"
}
```

## Error Handling

### Error Response Format

All errors follow a consistent format:

```json
{
  "error": {
    "code": "PIPELINE_NOT_FOUND",
    "message": "Pipeline with ID pip_123 not found",
    "details": {
      "pipeline_id": "pip_123",
      "organization_id": "finance"
    },
    "timestamp": "2024-01-16T15:30:00Z",
    "request_id": "req_987654321"
  }
}
```

### HTTP Status Codes

- `200` - Success
- `201` - Created
- `400` - Bad Request
- `401` - Unauthorized
- `403` - Forbidden
- `404` - Not Found
- `409` - Conflict
- `422` - Validation Error
- `429` - Rate Limited
- `500` - Internal Server Error
- `503` - Service Unavailable

### Common Error Codes

- `INVALID_TOKEN` - JWT token is invalid or expired
- `TENANT_NOT_FOUND` - Organization ID not found
- `QUOTA_EXCEEDED` - Resource quota exceeded
- `PIPELINE_NOT_FOUND` - Pipeline not found
- `CONNECTOR_UNAVAILABLE` - External connector unreachable
- `VALIDATION_ERROR` - Request validation failed

## Rate Limiting

### Rate Limit Headers

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1642348800
```

### Rate Limits by Endpoint

- **Authentication**: 10 requests per minute
- **Pipeline CRUD**: 100 requests per minute
- **Pipeline Execution**: 50 requests per minute
- **Catalog Operations**: 200 requests per minute
- **Data Quality Checks**: 20 requests per minute

### Rate Limit Response

```http
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1642348860

{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded. Try again in 60 seconds.",
    "retry_after": 60
  }
}
```

## Webhook Events

### Event Types

- `pipeline.created`
- `pipeline.updated`
- `pipeline.deleted`
- `pipeline.execution.started`
- `pipeline.execution.completed`
- `pipeline.execution.failed`
- `data_quality.check.completed`
- `connector.connection.failed`

### Webhook Payload Format

```json
{
  "event_id": "evt_123456789",
  "event_type": "pipeline.execution.completed",
  "timestamp": "2024-01-16T15:30:00Z",
  "organization_id": "finance",
  "data": {
    "pipeline_id": "pip_123",
    "execution_id": "exec_789",
    "status": "completed",
    "records_processed": 15420,
    "duration_seconds": 900
  }
}
```

### Webhook Security

All webhooks include a signature header for verification:

```http
X-Webhook-Signature: sha256=1234567890abcdef...
```

Verify the signature using your webhook secret:

```python
import hmac
import hashlib

def verify_webhook(payload, signature, secret):
    expected = hmac.new(
        secret.encode('utf-8'),
        payload.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return f"sha256={expected}" == signature
```

This API reference provides comprehensive documentation for all endpoints in the multi-tenant data ingestion framework, including request/response examples, error handling, and security considerations.