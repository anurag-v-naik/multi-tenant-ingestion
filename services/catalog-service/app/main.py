"""
Multi-Tenant Catalog Service
FastAPI application for managing Unity Catalog and Iceberg table metadata
"""
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
import uuid
import logging
from datetime import datetime
import httpx
import asyncio
from pydantic import BaseModel

from .models.database import get_db
from .models.catalog import Catalog, Schema, Table, Column
from .models.tenant import Tenant
from .core.auth import get_current_tenant
from .core.unity_catalog_client import UnityCatalogClient
from .core.iceberg_client import IcebergClient
from .core.config import settings

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Multi-Tenant Catalog Service",
    description="Unity Catalog and Iceberg metadata management",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for API
class CatalogResponse(BaseModel):
    name: str
    description: Optional[str]
    schemas: List[Dict[str, Any]]
    created_at: datetime
    properties: Dict[str, Any]

class SchemaResponse(BaseModel):
    catalog: str
    schema: str
    tables: List[Dict[str, Any]]
    created_at: datetime
    properties: Dict[str, Any]

class TableResponse(BaseModel):
    catalog: str
    schema: str
    table: str
    columns: List[Dict[str, Any]]
    partition_columns: List[str]
    table_type: str
    location: Optional[str]
    properties: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

class CreateSchemaRequest(BaseModel):
    name: str
    description: Optional[str]
    properties: Optional[Dict[str, Any]] = {}

class CreateTableRequest(BaseModel):
    name: str
    columns: List[Dict[str, Any]]
    partition_columns: Optional[List[str]] = []
    table_type: str = "iceberg"
    properties: Optional[Dict[str, Any]] = {}

# Health check endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint for load balancer"""
    return {
        "status": "healthy",
        "service": "catalog-service",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

@app.get("/health/unity-catalog")
async def health_check_unity_catalog(
    tenant: Tenant = Depends(get_current_tenant)
):
    """Unity Catalog connectivity health check"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)
        catalogs = await unity_client.list_catalogs()
        return {
            "status": "healthy",
            "unity_catalog": "connected",
            "catalogs_available": len(catalogs.get("catalogs", []))
        }
    except Exception as e:
        logger.error(f"Unity Catalog health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Unity Catalog connection failed")

@app.get("/health/iceberg")
async def health_check_iceberg(
    tenant: Tenant = Depends(get_current_tenant)
):
    """Iceberg connectivity health check"""
    try:
        iceberg_client = IcebergClient(tenant.organization_id)
        tables = await iceberg_client.list_tables()
        return {
            "status": "healthy",
            "iceberg": "connected",
            "tables_available": len(tables)
        }
    except Exception as e:
        logger.error(f"Iceberg health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Iceberg connection failed")

# Catalog Management Endpoints

@app.get("/api/v1/catalogs", response_model=List[CatalogResponse])
async def list_catalogs(
    tenant: Tenant = Depends(get_current_tenant)
):
    """List all catalogs for the current tenant"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)
        catalogs_data = await unity_client.list_catalogs()

        catalogs = []
        for catalog_info in catalogs_data.get("catalogs", []):
            # Get schema information for each catalog
            schemas_data = await unity_client.list_schemas(catalog_info["name"])

            catalog = CatalogResponse(
                name=catalog_info["name"],
                description=catalog_info.get("comment", ""),
                schemas=[
                    {
                        "name": schema["name"],
                        "tables_count": schema.get("tables_count", 0)
                    }
                    for schema in schemas_data.get("schemas", [])
                ],
                created_at=datetime.fromisoformat(catalog_info.get("created_at", datetime.utcnow().isoformat())),
                properties=catalog_info.get("properties", {})
            )
            catalogs.append(catalog)

        return catalogs

    except Exception as e:
        logger.error(f"Failed to list catalogs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve catalogs")

@app.get("/api/v1/catalogs/{catalog_name}/schemas/{schema_name}", response_model=SchemaResponse)
async def get_schema_info(
    catalog_name: str,
    schema_name: str,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Get detailed schema information including tables"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)

        # Verify access to catalog and schema
        await verify_catalog_access(unity_client, catalog_name, tenant.organization_id)

        # Get tables in schema
        tables_data = await unity_client.list_tables(catalog_name, schema_name)

        tables = []
        for table_info in tables_data.get("tables", []):
            # Get detailed table information
            table_details = await unity_client.get_table(
                catalog_name, schema_name, table_info["name"]
            )

            table = {
                "name": table_info["name"],
                "type": table_details.get("table_type", "unknown"),
                "location": table_details.get("storage_location"),
                "columns": [
                    {
                        "name": col["name"],
                        "type": col["type_name"],
                        "nullable": col.get("nullable", True)
                    }
                    for col in table_details.get("columns", [])
                ],
                "partition_columns": table_details.get("partition_keys", []),
                "properties": table_details.get("properties", {}),
                "created_at": table_details.get("created_at"),
                "updated_at": table_details.get("updated_at")
            }
            tables.append(table)

        schema_info = SchemaResponse(
            catalog=catalog_name,
            schema=schema_name,
            tables=tables,
            created_at=datetime.utcnow(),  # Should get from Unity Catalog
            properties={}
        )

        return schema_info

    except Exception as e:
        logger.error(f"Failed to get schema info: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve schema information")

@app.post("/api/v1/catalogs/{catalog_name}/schemas", response_model=dict)
async def create_schema(
    catalog_name: str,
    schema_request: CreateSchemaRequest,
    background_tasks: BackgroundTasks,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Create a new schema in the catalog"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)

        # Verify access to catalog
        await verify_catalog_access(unity_client, catalog_name, tenant.organization_id)

        # Create schema in Unity Catalog
        schema_spec = {
            "name": schema_request.name,
            "catalog_name": catalog_name,
            "comment": schema_request.description or "",
            "properties": schema_request.properties
        }

        result = await unity_client.create_schema(schema_spec)

        # Create corresponding Iceberg namespace if needed
        if settings.ICEBERG_ENABLED:
            background_tasks.add_task(
                create_iceberg_namespace,
                catalog_name,
                schema_request.name,
                tenant.organization_id
            )

        logger.info(f"Schema created: {catalog_name}.{schema_request.name} for tenant: {tenant.organization_id}")

        return {
            "catalog": catalog_name,
            "schema": schema_request.name,
            "status": "created",
            "message": "Schema created successfully"
        }

    except Exception as e:
        logger.error(f"Schema creation failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/catalogs/{catalog_name}/schemas/{schema_name}/tables", response_model=dict)
async def create_table(
    catalog_name: str,
    schema_name: str,
    table_request: CreateTableRequest,
    background_tasks: BackgroundTasks,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Create a new table in the schema"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)

        # Verify access
        await verify_catalog_access(unity_client, catalog_name, tenant.organization_id)

        # Prepare table specification
        table_spec = {
            "name": table_request.name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_type": "MANAGED" if table_request.table_type == "iceberg" else "EXTERNAL",
            "data_source_format": "ICEBERG" if table_request.table_type == "iceberg" else "PARQUET",
            "columns": [
                {
                    "name": col["name"],
                    "type_name": col["type"],
                    "type_json": f'"{col["type"]}"',
                    "nullable": col.get("nullable", True),
                    "comment": col.get("description", "")
                }
                for col in table_request.columns
            ],
            "properties": table_request.properties
        }

        # Add partition information if specified
        if table_request.partition_columns:
            table_spec["partition_keys"] = table_request.partition_columns

        # Create table in Unity Catalog
        result = await unity_client.create_table(table_spec)

        # Create Iceberg table if specified
        if table_request.table_type == "iceberg" and settings.ICEBERG_ENABLED:
            background_tasks.add_task(
                create_iceberg_table,
                catalog_name,
                schema_name,
                table_request.name,
                table_request.columns,
                table_request.partition_columns,
                tenant.organization_id
            )

        logger.info(f"Table created: {catalog_name}.{schema_name}.{table_request.name} for tenant: {tenant.organization_id}")

        return {
            "catalog": catalog_name,
            "schema": schema_name,
            "table": table_request.name,
            "status": "created",
            "message": "Table created successfully",
            "table_type": table_request.table_type
        }

    except Exception as e:
        logger.error(f"Table creation failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/catalogs/search")
async def search_tables(
    q: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    table_type: Optional[str] = None,
    limit: int = 50,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Search for tables across catalogs"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)

        # Get all accessible catalogs
        catalogs_data = await unity_client.list_catalogs()
        results = []

        for catalog_info in catalogs_data.get("catalogs", []):
            catalog_name = catalog_info["name"]

            # Skip if catalog filter specified and doesn't match
            if catalog and catalog_name != catalog:
                continue

            # Get schemas in catalog
            schemas_data = await unity_client.list_schemas(catalog_name)

            for schema_info in schemas_data.get("schemas", []):
                schema_name = schema_info["name"]

                # Skip if schema filter specified and doesn't match
                if schema and schema_name != schema:
                    continue

                # Get tables in schema
                try:
                    tables_data = await unity_client.list_tables(catalog_name, schema_name)

                    for table_info in tables_data.get("tables", []):
                        table_name = table_info["name"]

                        # Search in table name and description
                        if (q.lower() in table_name.lower() or
                            q.lower() in table_info.get("comment", "").lower()):

                            # Apply table type filter if specified
                            if table_type and table_info.get("table_type", "").lower() != table_type.lower():
                                continue

                            # Calculate relevance score
                            score = calculate_search_score(q, table_name, table_info.get("comment", ""))

                            results.append({
                                "catalog": catalog_name,
                                "schema": schema_name,
                                "table": table_name,
                                "description": table_info.get("comment", ""),
                                "table_type": table_info.get("table_type", ""),
                                "score": score
                            })

                            # Limit results
                            if len(results) >= limit:
                                break

                    if len(results) >= limit:
                        break

                except Exception as e:
                    logger.warning(f"Failed to search in {catalog_name}.{schema_name}: {str(e)}")
                    continue

            if len(results) >= limit:
                break

        # Sort by relevance score
        results.sort(key=lambda x: x["score"], reverse=True)

        return {
            "results": results[:limit],
            "total": len(results),
            "query": q
        }

    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Search failed")

# Data Lineage Endpoints

@app.get("/api/v1/lineage/{catalog_name}/{schema_name}/{table_name}")
async def get_table_lineage(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    direction: str = "both",  # upstream, downstream, both
    tenant: Tenant = Depends(get_current_tenant)
):
    """Get data lineage for a specific table"""
    try:
        unity_client = UnityCatalogClient(tenant.organization_id)

        # Verify table exists and access
        await verify_table_access(unity_client, catalog_name, schema_name, table_name, tenant.organization_id)

        lineage_data = {
            "table": {
                "catalog": catalog_name,
                "schema": schema_name,
                "table": table_name
            },
            "upstream": [],
            "downstream": [],
            "transformations": []
        }

        # Get lineage information from Unity Catalog
        if direction in ["upstream", "both"]:
            upstream_info = await unity_client.get_lineage_upstream(catalog_name, schema_name, table_name)
            lineage_data["upstream"] = upstream_info.get("tables", [])

        if direction in ["downstream", "both"]:
            downstream_info = await unity_client.get_lineage_downstream(catalog_name, schema_name, table_name)
            lineage_data["downstream"] = downstream_info.get("tables", [])

        return lineage_data

    except Exception as e:
        logger.error(f"Failed to get lineage: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve lineage information")

# Iceberg Integration Endpoints

@app.get("/api/v1/iceberg/{catalog_name}/{schema_name}/{table_name}/snapshots")
async def get_table_snapshots(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Get Iceberg table snapshots"""
    try:
        iceberg_client = IcebergClient(tenant.organization_id)

        snapshots = await iceberg_client.get_table_snapshots(catalog_name, schema_name, table_name)

        return {
            "table": f"{catalog_name}.{schema_name}.{table_name}",
            "snapshots": snapshots
        }

    except Exception as e:
        logger.error(f"Failed to get snapshots: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve snapshots")

@app.post("/api/v1/iceberg/{catalog_name}/{schema_name}/{table_name}/optimize")
async def optimize_iceberg_table(
    catalog_name: str,
    schema_name: str,
    table_name: str,
    background_tasks: BackgroundTasks,
    tenant: Tenant = Depends(get_current_tenant)
):
    """Optimize Iceberg table (compact files, update statistics)"""
    try:
        # Add optimization task to background queue
        background_tasks.add_task(
            optimize_table_background,
            catalog_name,
            schema_name,
            table_name,
            tenant.organization_id
        )

        return {
            "table": f"{catalog_name}.{schema_name}.{table_name}",
            "status": "optimization_started",
            "message": "Table optimization initiated"
        }

    except Exception as e:
        logger.error(f"Failed to start optimization: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to start optimization")

# Background Tasks

async def verify_catalog_access(unity_client: UnityCatalogClient, catalog_name: str, organization_id: str):
    """Verify tenant has access to catalog"""
    catalogs = await unity_client.list_catalogs()
    catalog_names = [c["name"] for c in catalogs.get("catalogs", [])]

    if catalog_name not in catalog_names:
        raise HTTPException(status_code=404, detail=f"Catalog '{catalog_name}' not found or not accessible")

async def verify_table_access(unity_client: UnityCatalogClient, catalog_name: str, schema_name: str, table_name: str, organization_id: str):
    """Verify tenant has access to table"""
    try:
        table_info = await unity_client.get_table(catalog_name, schema_name, table_name)
        return table_info
    except Exception:
        raise HTTPException(status_code=404, detail=f"Table '{catalog_name}.{schema_name}.{table_name}' not found or not accessible")

async def create_iceberg_namespace(catalog_name: str, schema_name: str, organization_id: str):
    """Create Iceberg namespace in background"""
    try:
        iceberg_client = IcebergClient(organization_id)
        await iceberg_client.create_namespace(f"{catalog_name}.{schema_name}")
        logger.info(f"Iceberg namespace created: {catalog_name}.{schema_name}")
    except Exception as e:
        logger.error(f"Failed to create Iceberg namespace: {str(e)}")

async def create_iceberg_table(catalog_name: str, schema_name: str, table_name: str, columns: List[Dict], partition_columns: List[str], organization_id: str):
    """Create Iceberg table in background"""
    try:
        iceberg_client = IcebergClient(organization_id)
        await iceberg_client.create_table(
            f"{catalog_name}.{schema_name}.{table_name}",
            columns,
            partition_columns
        )
        logger.info(f"Iceberg table created: {catalog_name}.{schema_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to create Iceberg table: {str(e)}")

async def optimize_table_background(catalog_name: str, schema_name: str, table_name: str, organization_id: str):
    """Optimize Iceberg table in background"""
    try:
        iceberg_client = IcebergClient(organization_id)
        await iceberg_client.optimize_table(f"{catalog_name}.{schema_name}.{table_name}")
        logger.info(f"Table optimized: {catalog_name}.{schema_name}.{table_name}")
    except Exception as e:
        logger.error(f"Failed to optimize table: {str(e)}")

def calculate_search_score(query: str, table_name: str, description: str) -> float:
    """Calculate search relevance score"""
    score = 0.0
    query_lower = query.lower()

    # Exact match in table name gets highest score
    if query_lower == table_name.lower():
        score += 1.0
    elif query_lower in table_name.lower():
        score += 0.8

    # Match in description
    if query_lower in description.lower():
        score += 0.3

    # Prefix match
    if table_name.lower().startswith(query_lower):
        score += 0.5

    return min(score, 1.0)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)