#!/usr/bin/env python3
"""
Seed development database with sample data
"""

import os
import sys
from datetime import datetime
import uuid

# Add the services directory to the path
sys.path.append('services/shared')

from models.database import SessionLocal, engine, Base
from models.tenant import Tenant
from models.pipeline import Pipeline, PipelineStatus
from models.connector import Connector


def create_tables():
    """Create all database tables"""
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")


def seed_tenants(db):
    """Seed sample tenant organizations"""
    print("Seeding tenant organizations...")

    tenants = [
        {
            "organization_id": "finance",
            "display_name": "Finance Department",
            "description": "Financial data and reporting",
            "cost_center": "FIN-001",
            "compliance_level": "high",
            "databricks_workspace_url": "https://finance.cloud.databricks.com",
            "unity_catalog_name": "finance_catalog",
            "max_dbu_per_hour": 100,
            "max_storage_gb": 10000,
            "max_api_calls_per_minute": 1000
        },
        {
            "organization_id": "retail",
            "display_name": "Retail Division",
            "description": "Retail operations and analytics",
            "cost_center": "RET-001",
            "compliance_level": "medium",
            "databricks_workspace_url": "https://retail.cloud.databricks.com",
            "unity_catalog_name": "retail_catalog",
            "max_dbu_per_hour": 200,
            "max_storage_gb": 50000,
            "max_api_calls_per_minute": 2000
        },
        {
            "organization_id": "healthcare",
            "display_name": "Healthcare Division",
            "description": "Healthcare data and compliance",
            "cost_center": "HC-001",
            "compliance_level": "high",
            "databricks_workspace_url": "https://healthcare.cloud.databricks.com",
            "unity_catalog_name": "healthcare_catalog",
            "max_dbu_per_hour": 150,
            "max_storage_gb": 25000,
            "max_api_calls_per_minute": 1500
        }
    ]

    for tenant_data in tenants:
        existing = db.query(Tenant).filter(
            Tenant.organization_id == tenant_data["organization_id"]
        ).first()

        if not existing:
            tenant = Tenant(**tenant_data)
            db.add(tenant)
            print(f"  Created tenant: {tenant_data['organization_id']}")

    db.commit()
    print("Tenants seeded successfully!")


def seed_connectors(db):
    """Seed sample connectors"""
    print("Seeding connectors...")

    connectors = [
        {
            "organization_id": "finance",
            "name": "Financial Database",
            "connector_type": "postgresql",
            "description": "Main financial database connection",
            "configuration": {
                "host": "finance-db.company.com",
                "port": 5432,
                "database": "finance",
                "username": "readonly_user"
            },
            "status": "healthy"
        },
        {
            "organization_id": "retail",
            "name": "Sales Database",
            "connector_type": "mysql",
            "description": "Retail sales database",
            "configuration": {
                "host": "sales-db.company.com",
                "port": 3306,
                "database": "sales",
                "username": "app_user"
            },
            "status": "healthy"
        },
        {
            "organization_id": "healthcare",
            "name": "Patient Data System",
            "connector_type": "postgresql",
            "description": "Healthcare patient management system",
            "configuration": {
                "host": "healthcare-db.company.com",
                "port": 5432,
                "database": "patients",
                "username": "healthcare_user"
            },
            "status": "healthy"
        }
    ]

    for connector_data in connectors:
        connector = Connector(
            id=uuid.uuid4(),
            **connector_data,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(connector)
        print(f"  Created connector: {connector_data['name']}")

    db.commit()
    print("Connectors seeded successfully!")


def seed_pipelines(db):
    """Seed sample pipelines"""
    print("Seeding pipelines...")

    pipelines = [
        {
            "organization_id": "finance",
            "name": "Daily Financial Reports",
            "description": "Extract daily financial data for reporting",
            "source_type": "postgresql",
            "target_type": "iceberg",
            "configuration": {
                "source": {
                    "connection": "finance-db",
                    "tables": ["transactions", "accounts", "budgets"],
                    "incremental_column": "updated_at"
                },
                "target": {
                    "catalog": "finance_catalog",
                    "schema": "raw",
                    "write_mode": "append"
                }
            },
            "schedule": "0 2 * * *",
            "status": PipelineStatus.ACTIVE
        },
        {
            "organization_id": "retail",
            "name": "Sales Data Ingestion",
            "description": "Hourly sales data ingestion",
            "source_type": "mysql",
            "target_type": "iceberg",
            "configuration": {
                "source": {
                    "connection": "sales-db",
                    "tables": ["orders", "customers", "products"],
                    "incremental_column": "created_at"
                },
                "target": {
                    "catalog": "retail_catalog",
                    "schema": "raw",
                    "write_mode": "append"
                }
            },
            "schedule": "0 * * * *",
            "status": PipelineStatus.ACTIVE
        },
        {
            "organization_id": "healthcare",
            "name": "Patient Data Sync",
            "description": "Secure patient data synchronization",
            "source_type": "postgresql",
            "target_type": "iceberg",
            "configuration": {
                "source": {
                    "connection": "healthcare-db",
                    "tables": ["patients", "visits", "treatments"],
                    "incremental_column": "last_modified"
                },
                "target": {
                    "catalog": "healthcare_catalog",
                    "schema": "secure",
                    "write_mode": "merge"
                }
            },
            "schedule": "0 6 * * *",
            "status": PipelineStatus.ACTIVE
        }
    ]

    for pipeline_data in pipelines:
        pipeline = Pipeline(
            id=uuid.uuid4(),
            **pipeline_data,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        db.add(pipeline)
        print(f"  Created pipeline: {pipeline_data['name']}")

    db.commit()
    print("Pipelines seeded successfully!")


def main():
    """Main function to seed development data"""
    print("Seeding development database...")

    # Create tables
    create_tables()

    # Create database session
    db = SessionLocal()

    try:
        # Seed data
        seed_tenants(db)
        seed_connectors(db)
        seed_pipelines(db)

        print("\nDevelopment data seeded successfully!")
        print("\nSample organizations created:")
        print("  - finance (Finance Department)")
        print("  - retail (Retail Division)")
        print("  - healthcare (Healthcare Division)")

    except Exception as e:
        print(f"Error seeding data: {e}")
        db.rollback()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()