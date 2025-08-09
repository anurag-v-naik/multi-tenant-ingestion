#!/bin/bash

set -e

# Setup script for new tenant organizations

# Check if organization name is provided
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <organization_name>"
    echo "Example: $0 finance"
    exit 1
fi

ORGANIZATION_NAME="$1"

echo "Setting up organization: $ORGANIZATION_NAME"

# Validate organization name
if [[ ! "$ORGANIZATION_NAME" =~ ^[a-z0-9_]+$ ]]; then
    echo "Error: Organization name must contain only lowercase letters, numbers, and underscores"
    exit 1
fi

# Check if kubectl is available and connected
if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Cannot connect to Kubernetes cluster"
    exit 1
fi

# Create organization-specific namespace
echo "Creating namespace for organization..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: multi-tenant-${ORGANIZATION_NAME}
  labels:
    organization: ${ORGANIZATION_NAME}
    app.kubernetes.io/name: multi-tenant-ingestion
    app.kubernetes.io/component: tenant-namespace
EOF

# Create organization-specific secrets
echo "Creating organization secrets..."
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: ${ORGANIZATION_NAME}-secrets
  namespace: multi-tenant-ingestion
type: Opaque
stringData:
  databricks_token: "REPLACE_WITH_ACTUAL_TOKEN"
  databricks_workspace_url: "https://${ORGANIZATION_NAME}.cloud.databricks.com"
  unity_catalog_name: "${ORGANIZATION_NAME}_catalog"
EOF

# Add organization to database
echo "Adding organization to database..."
kubectl exec -n multi-tenant-ingestion deployment/pipeline-service -- python -c "
import os
import sys
sys.path.append('/app')
from app.models.database import SessionLocal
from app.models.tenant import Tenant
from datetime import datetime

db = SessionLocal()
try:
    # Check if organization already exists
    existing = db.query(Tenant).filter(Tenant.organization_id == '${ORGANIZATION_NAME}').first()
    if existing:
        print('Organization ${ORGANIZATION_NAME} already exists')
    else:
        tenant = Tenant(
            organization_id='${ORGANIZATION_NAME}',
            display_name='${ORGANIZATION_NAME^} Department',
            description='Automatically created tenant organization',
            cost_center='${ORGANIZATION_NAME^^}-001',
            compliance_level='medium',
            databricks_workspace_url='https://${ORGANIZATION_NAME}.cloud.databricks.com',
            unity_catalog_name='${ORGANIZATION_NAME}_catalog',
            status='active',
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(tenant)
        db.commit()
        print('Organization ${ORGANIZATION_NAME} created successfully')
finally:
    db.close()
"

echo "Organization $ORGANIZATION_NAME setup completed!"
echo ""
echo "Next steps:"
echo "1. Update the ${ORGANIZATION_NAME}-secrets with actual Databricks credentials"
echo "2. Configure Unity Catalog for the organization"
echo "3. Set up Databricks workspace and clusters"
echo "4. Test the organization setup with sample pipelines"
