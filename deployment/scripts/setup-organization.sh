#!/bin/bash

set -e

ORGANIZATION_NAME=$1

if [ -z "$ORGANIZATION_NAME" ]; then
    echo "Usage: $0 <organization_name>"
    exit 1
fi

echo "Setting up organization: $ORGANIZATION_NAME"

# API Base URL
API_BASE_URL=${API_BASE_URL:-"http://localhost:8080/api/v1"}

# Admin credentials (should be passed as environment variables in production)
ADMIN_EMAIL=${ADMIN_EMAIL:-"admin@example.com"}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-"admin123"}

# Login as admin and get token
echo "Authenticating as admin..."
AUTH_RESPONSE=$(curl -s -X POST "$API_BASE_URL/auth/login" \
    -H "Content-Type: application/json" \
    -d "{
        \"email\": \"$ADMIN_EMAIL\",
        \"password\": \"$ADMIN_PASSWORD\"
    }")

TOKEN=$(echo $AUTH_RESPONSE | jq -r '.access_token')

if [ "$TOKEN" = "null" ]; then
    echo "Failed to authenticate as admin"
    exit 1
fi

echo "Admin authenticated successfully"

# Create organization tenant
echo "Creating tenant for organization: $ORGANIZATION_NAME"

TENANT_DATA=$(cat <<EOF
{
    "name": "$ORGANIZATION_NAME",
    "display_name": "$(echo $ORGANIZATION_NAME | sed 's/.*/\u&/') Department",
    "description": "Tenant for $ORGANIZATION_NAME organization",
    "cost_center": "$(echo $ORGANIZATION_NAME | tr '[:lower:]' '[:upper:]')-001",
    "compliance_level": "medium",
    "max_dbu_per_hour": 100,
    "max_storage_gb": 10000,
    "max_api_calls_per_minute": 1000,
    "max_concurrent_jobs": 10
}
EOF
)

TENANT_RESPONSE=$(curl -s -X POST "$API_BASE_URL/tenants" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "$TENANT_DATA")

TENANT_ID=$(echo $TENANT_RESPONSE | jq -r '.id')

if [ "$TENANT_ID" = "null" ]; then
    echo "Failed to create tenant"
    echo "Response: $TENANT_RESPONSE"
    exit 1
fi

echo "Tenant created with ID: $TENANT_ID"

# Create organization admin user
echo "Creating admin user for organization..."

ADMIN_USER_DATA=$(cat <<EOF
{
    "email": "admin@${ORGANIZATION_NAME}.com",
    "username": "${ORGANIZATION_NAME}_admin",
    "first_name": "$(echo $ORGANIZATION_NAME | sed 's/.*/\u&/')",
    "last_name": "Admin",
    "password": "TempPassword123!",
    "role": "tenant_admin",
    "tenant_id": "$TENANT_ID",
    "tenant_role": "admin"
}
EOF
)

USER_RESPONSE=$(curl -s -X POST "$API_BASE_URL/users" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "$ADMIN_USER_DATA")

USER_ID=$(echo $USER_RESPONSE | jq -r '.id')

if [ "$USER_ID" = "null" ]; then
    echo "Failed to create admin user"
    echo "Response: $USER_RESPONSE"
    exit 1
fi

echo "Admin user created with ID: $USER_ID"

# Create sample connections for the organization
echo "Creating sample connections..."

# PostgreSQL connection
POSTGRES_CONNECTION=$(cat <<EOF
{
    "name": "${ORGANIZATION_NAME} PostgreSQL",
    "description": "Sample PostgreSQL connection for $ORGANIZATION_NAME",
    "connection_type": "postgresql",
    "config": {
        "host": "localhost",
        "port": 5432,
        "database": "${ORGANIZATION_NAME}_db",
        "schema": "public"
    },
    "credentials": {
        "username": "${ORGANIZATION_NAME}_user",
        "password": "sample_password"
    }
}
EOF
)

curl -s -X POST "$API_BASE_URL/connections" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-Tenant-ID: $TENANT_ID" \
    -d "$POSTGRES_CONNECTION"

# S3 connection
S3_CONNECTION=$(cat <<EOF
{
    "name": "${ORGANIZATION_NAME} S3 Bucket",
    "description": "Sample S3 connection for $ORGANIZATION_NAME",
    "connection_type": "s3",
    "config": {
        "bucket_name": "multi-tenant-ingestion-${ORGANIZATION_NAME}",
        "prefix": "data/",
        "region": "us-east-1"
    },
    "credentials": {
        "access_key_id": "sample_access_key",
        "secret_access_key": "sample_secret_key"
    }
}
EOF
)

curl -s -X POST "$API_BASE_URL/connections" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-Tenant-ID: $TENANT_ID" \
    -d "$S3_CONNECTION"

echo "Sample connections created"

# Create sample pipeline
echo "Creating sample pipeline..."

PIPELINE_DATA=$(cat <<EOF
{
    "name": "${ORGANIZATION_NAME} Data Pipeline",
    "description": "Sample data pipeline for $ORGANIZATION_NAME",
    "pipeline_type": "batch",
    "source_connection_id": "$(curl -s "$API_BASE_URL/connections" -H "Authorization: Bearer $TOKEN" -H "X-Tenant-ID: $TENANT_ID" | jq -r '.[0].id')",
    "target_connection_id": "$(curl -s "$API_BASE_URL/connections" -H "Authorization: Bearer $TOKEN" -H "X-Tenant-ID: $TENANT_ID" | jq -r '.[1].id')",
    "transformation_config": {
        "transforms": [
            {
                "type": "select",
                "columns": ["id", "name", "created_at"]
            },
            {
                "type": "filter",
                "condition": "created_at > '2024-01-01'"
            }
        ]
    },
    "schedule_config": {
        "cron_expression": "0 2 * * *",
        "timezone": "UTC"
    },
    "databricks_cluster_config": {
        "spark_version": "11.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
    }
}
EOF
)

curl -s -X POST "$API_BASE_URL/pipelines" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-Tenant-ID: $TENANT_ID" \
    -d "$PIPELINE_DATA"

echo "Sample pipeline created"

echo ""
echo "✅ Organization '$ORGANIZATION_NAME' setup completed successfully!"
echo ""
echo "Organization Details:"
echo "  Tenant ID: $TENANT_ID"
echo "  Admin User: admin@${ORGANIZATION_NAME}.com"
echo "  Temporary Password: TempPassword123!"
echo ""
echo "⚠️  Please change the admin password after first login!"
