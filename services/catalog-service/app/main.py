"""
Multi-Tenant Catalog Service
Unity Catalog and Apache Iceberg table management service
"""
import logging
import os
import json
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any
from datetime import datetime

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, DateTime, Text, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import requests
import boto3
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.unity_catalog.api import UnityCatalogApi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/multi_tenant_ingestion")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Security
security = HTTPBearer()


# Models
class Catalog(Base):
    __tablename__ = "catalogs"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    organization_id = Column(String, index=True)
    unity_catalog_id = Column(String, unique=True)
    storage_location = Column(String)
    metastore_id = Column(String)
    properties = Column(Text)  # JSON string
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Schema(Base):
    __tablename__ = "schemas"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    catalog_id = Column(String, index=True)
    organization_id = Column(String, index=True)
    unity_schema_id = Column(String)
    properties = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class IcebergTable(Base):
    __tablename__ = "iceberg_tables"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    schema_id = Column(String, index=True)
    catalog_id = Column(String, index=True)
    organization_id = Column(String, index=True)
    table_location = Column(String)
    metadata_location = Column(String)
    table_schema = Column(Text)  # JSON string
    partition_spec = Column(Text)  # JSON string
    properties = Column(Text)  # JSON string
    format_version = Column(Integer, default=2)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Pydantic models
class CatalogCreate(BaseModel):
    name: str
    storage_location: Optional[str] = None
    properties: Optional[Dict] = {}


class CatalogResponse(BaseModel):
    id: str
    name: str
    organization_id: str
    unity_catalog_id: str
    storage_location: Optional[str]
    metastore_id: str
    properties: Dict
    is_active: bool
    created_at: datetime
    updated_at: datetime


class SchemaCreate(BaseModel):
    name: str
    catalog_id: str
    properties: Optional[Dict] = {}


class SchemaResponse(BaseModel):
    id: str
    name: str
    catalog_id: str
    organization_id: str
    unity_schema_id: str
    properties: Dict
    is_active: bool
    created_at: datetime
    updated_at: datetime


class IcebergTableCreate(BaseModel):
    name: str
    schema_id: str
    table_schema: Dict  # Iceberg table schema
    partition_spec: Optional[List[Dict]] = []
    properties: Optional[Dict] = {}


class IcebergTableResponse(BaseModel):
    id: str
    name: str
    schema_id: str
    catalog_id: str
    organization_id: str
    table_location: str
    metadata_location: str
    table_schema: Dict
    partition_spec: List[Dict]
    properties: Dict
    format_version: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class TableMetrics(BaseModel):
    total_files: int
    total_size_bytes: int
    record_count: int
    last_updated: datetime


# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Authentication dependency
async def get_current_organization(
        authorization: HTTPAuthorizationCredentials = Depends(security),
        x_organization_id: Optional[str] = Header(None)
) -> str:
    if not x_organization_id:
        raise HTTPException(status_code=401, detail="Organization ID required")
    return x_organization_id


class UnityCatalogManager:
    """Manages Unity Catalog operations"""

    def __init__(self, organization_id: str):
        self.organization_id = organization_id
        self.workspace_url = self._get_workspace_url(organization_id)
        self.token = self._get_databricks_token(organization_id)
        self.api_client = ApiClient(host=self.workspace_url, token=self.token)
        self.unity_api = UnityCatalogApi(self.api_client)

    def _get_workspace_url(self, org_id: str) -> str:
        return f"https://{org_id}.databricks.com"

    def _get_databricks_token(self, org_id: str) -> str:
        return os.getenv(f"DATABRICKS_TOKEN_{org_id.upper()}", "dapi-default-token")

    async def create_catalog(self, name: str, storage_location: str = None) -> Dict:
        """Create a new Unity Catalog"""
        catalog_config = {
            "name": f"{self.organization_id}_{name}",
            "comment": f"Catalog for organization {self.organization_id}",
            "properties": {
                "organization_id": self.organization_id,
                "created_by": "multi-tenant-ingestion-service"
            }
        }

        if storage_location:
            catalog_config["storage_root"] = storage_location

        try:
            response = self.unity_api.create_catalog(catalog_config)
            return response
        except Exception as e:
            logger.error(f"Failed to create Unity Catalog: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Unity Catalog creation failed: {str(e)}")

    async def create_schema(self, catalog_name: str, schema_name: str) -> Dict:
        """Create a new schema in Unity Catalog"""
        full_catalog_name = f"{self.organization_id}_{catalog_name}"
        schema_config = {
            "name": schema_name,
            "catalog_name": full_catalog_name,
            "comment": f"Schema {schema_name} for organization {self.organization_id}",
            "properties": {
                "organization_id": self.organization_id
            }
        }

        try:
            response = self.unity_api.create_schema(schema_config)
            return response
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Schema creation failed: {str(e)}")

    async def list_catalogs(self) -> List[Dict]:
        """List all catalogs for the organization"""
        try:
            catalogs = self.unity_api.list_catalogs()
            # Filter catalogs for this organization
            org_catalogs = [
                cat for cat in catalogs.get("catalogs", [])
                if cat["name"].startswith(f"{self.organization_id}_")
            ]
            return org_catalogs
        except Exception as e:
            logger.error(f"Failed to list catalogs: {str(e)}")
            return []


class IcebergManager:
    """Manages Apache Iceberg table operations"""

    def __init__(self, organization_id: str):
        self.organization_id = organization_id
        self.s3_client = boto3.client('s3')
        self.bucket_name = f"iceberg-{organization_id}"

    async def create_table(self, catalog_name: str, schema_name: str, table_name: str,
                           table_schema: Dict, partition_spec: List[Dict] = None) -> Dict:
        """Create an Iceberg table"""
        table_location = f"s3://{self.bucket_name}/{catalog_name}/{schema_name}/{table_name}/"
        metadata_location = f"{table_location}metadata/"

        # Create table metadata
        table_metadata = {
            "format-version": 2,
            "table-uuid": self._generate_uuid(),
            "location": table_location,
            "last-sequence-number": 0,
            "last-updated-ms": int(datetime.utcnow().timestamp() * 1000),
            "last-column-id": len(table_schema.get("fields", [])),
            "schema": table_schema,
            "partition-spec": partition_spec or [],
            "default-spec-id": 0,
            "last-partition-id": len(partition_spec) if partition_spec else 0,
            "properties": {
                "organization_id": self.organization_id,
                "created_by": "multi-tenant-ingestion-service"
            },
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": []
        }

        # Store metadata in S3
        metadata_key = f"{metadata_location}v1.metadata.json"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=metadata_key,
            Body=json.dumps(table_metadata),
            ContentType="application/json"
        )

        return {
            "table_location": table_location,
            "metadata_location": f"s3://{self.bucket_name}/{metadata_key}",
            "table_metadata": table_metadata
        }

    async def get_table_metrics(self, table_location: str) -> Dict:
        """Get metrics for an Iceberg table"""
        try:
            # Parse S3 location
            bucket = table_location.replace("s3://", "").split("/")[0]
            prefix = "/".join(table_location.replace("s3://", "").split("/")[1:])

            # List objects in table location
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            total_files = 0
            total_size = 0

            for obj in response.get("Contents", []):
                if obj["Key"].endswith((".parquet", ".orc", ".avro")):
                    total_files += 1
                    total_size += obj["Size"]

            return {
                "total_files": total_files,
                "total_size_bytes": total_size,
                "record_count": 0,  # Would need to read metadata for exact count
                "last_updated": datetime.utcnow()
            }
        except Exception as e:
            logger.error(f"Failed to get table metrics: {str(e)}")
            return {
                "total_files": 0,
                "total_size_bytes": 0,
                "record_count": 0,
                "last_updated": datetime.utcnow()
            }

    def _generate_uuid(self) -> str:
        import uuid
        return str(uuid.uuid4())


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    logger.info("Catalog Service started")
    yield
    # Shutdown
    logger.info("Catalog Service shutting down")


# FastAPI app
app = FastAPI(
    title="Multi-Tenant Catalog Service",
    description="Unity Catalog and Apache Iceberg management service",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "catalog-service"}


@app.get("/health/unity-catalog")
async def unity_catalog_health(organization_id: str = Depends(get_current_organization)):
    try:
        unity_manager = UnityCatalogManager(organization_id)
        catalogs = await unity_manager.list_catalogs()
        return {"status": "healthy", "unity_catalog": "connected", "catalogs": len(catalogs)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unity Catalog unhealthy: {str(e)}")


@app.post("/api/v1/catalogs", response_model=CatalogResponse)
async def create_catalog(
        catalog: CatalogCreate,
        background_tasks: BackgroundTasks,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Create a new catalog"""
    import uuid

    catalog_id = str(uuid.uuid4())

    # Create in Unity Catalog
    unity_manager = UnityCatalogManager(organization_id)

    # Generate S3 storage location
    storage_location = catalog.storage_location or f"s3://iceberg-{organization_id}/{catalog.name}/"

    unity_catalog = await unity_manager.create_catalog(catalog.name, storage_location)

    # Store in database
    db_catalog = Catalog(
        id=catalog_id,
        name=catalog.name,
        organization_id=organization_id,
        unity_catalog_id=unity_catalog["name"],
        storage_location=storage_location,
        metastore_id=unity_catalog.get("metastore_id", "default"),
        properties=json.dumps(catalog.properties)
    )

    db.add(db_catalog)
    db.commit()
    db.refresh(db_catalog)

    return CatalogResponse(
        id=db_catalog.id,
        name=db_catalog.name,
        organization_id=db_catalog.organization_id,
        unity_catalog_id=db_catalog.unity_catalog_id,
        storage_location=db_catalog.storage_location,
        metastore_id=db_catalog.metastore_id,
        properties=json.loads(db_catalog.properties),
        is_active=db_catalog.is_active,
        created_at=db_catalog.created_at,
        updated_at=db_catalog.updated_at
    )


@app.get("/api/v1/catalogs", response_model=List[CatalogResponse])
async def list_catalogs(
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """List all catalogs for the organization"""
    catalogs = db.query(Catalog).filter(Catalog.organization_id == organization_id).all()

    return [
        CatalogResponse(
            id=cat.id,
            name=cat.name,
            organization_id=cat.organization_id,
            unity_catalog_id=cat.unity_catalog_id,
            storage_location=cat.storage_location,
            metastore_id=cat.metastore_id,
            properties=json.loads(cat.properties),
            is_active=cat.is_active,
            created_at=cat.created_at,
            updated_at=cat.updated_at
        )
        for cat in catalogs
    ]


@app.post("/api/v1/schemas", response_model=SchemaResponse)
async def create_schema(
        schema: SchemaCreate,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Create a new schema"""
    import uuid

    # Verify catalog exists and belongs to organization
    catalog = db.query(Catalog).filter(
        Catalog.id == schema.catalog_id,
        Catalog.organization_id == organization_id
    ).first()

    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found")

    schema_id = str(uuid.uuid4())

    # Create in Unity Catalog
    unity_manager = UnityCatalogManager(organization_id)
    unity_schema = await unity_manager.create_schema(catalog.name, schema.name)

    # Store in database
    db_schema = Schema(
        id=schema_id,
        name=schema.name,
        catalog_id=schema.catalog_id,
        organization_id=organization_id,
        unity_schema_id=f"{unity_schema['catalog_name']}.{unity_schema['name']}",
        properties=json.dumps(schema.properties)
    )

    db.add(db_schema)
    db.commit()
    db.refresh(db_schema)

    return SchemaResponse(
        id=db_schema.id,
        name=db_schema.name,
        catalog_id=db_schema.catalog_id,
        organization_id=db_schema.organization_id,
        unity_schema_id=db_schema.unity_schema_id,
        properties=json.loads(db_schema.properties),
        is_active=db_schema.is_active,
        created_at=db_schema.created_at,
        updated_at=db_schema.updated_at
    )


@app.get("/api/v1/catalogs/{catalog_id}/schemas", response_model=List[SchemaResponse])
async def list_schemas(
        catalog_id: str,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """List schemas in a catalog"""
    schemas = db.query(Schema).filter(
        Schema.catalog_id == catalog_id,
        Schema.organization_id == organization_id
    ).all()

    return [
        SchemaResponse(
            id=sch.id,
            name=sch.name,
            catalog_id=sch.catalog_id,
            organization_id=sch.organization_id,
            unity_schema_id=sch.unity_schema_id,
            properties=json.loads(sch.properties),
            is_active=sch.is_active,
            created_at=sch.created_at,
            updated_at=sch.updated_at
        )
        for sch in schemas
    ]


@app.post("/api/v1/tables", response_model=IcebergTableResponse)
async def create_iceberg_table(
        table: IcebergTableCreate,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Create a new Iceberg table"""
    import uuid

    # Verify schema exists and belongs to organization
    schema = db.query(Schema).filter(
        Schema.id == table.schema_id,
        Schema.organization_id == organization_id
    ).first()

    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")

    # Get catalog
    catalog = db.query(Catalog).filter(Catalog.id == schema.catalog_id).first()
    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found")

    table_id = str(uuid.uuid4())

    # Create Iceberg table
    iceberg_manager = IcebergManager(organization_id)
    iceberg_result = await iceberg_manager.create_table(
        catalog.name, schema.name, table.name,
        table.table_schema, table.partition_spec
    )

    # Store in database
    db_table = IcebergTable(
        id=table_id,
        name=table.name,
        schema_id=table.schema_id,
        catalog_id=schema.catalog_id,
        organization_id=organization_id,
        table_location=iceberg_result["table_location"],
        metadata_location=iceberg_result["metadata_location"],
        table_schema=json.dumps(table.table_schema),
        partition_spec=json.dumps(table.partition_spec),
        properties=json.dumps(table.properties)
    )

    db.add(db_table)
    db.commit()
    db.refresh(db_table)

    return IcebergTableResponse(
        id=db_table.id,
        name=db_table.name,
        schema_id=db_table.schema_id,
        catalog_id=db_table.catalog_id,
        organization_id=db_table.organization_id,
        table_location=db_table.table_location,
        metadata_location=db_table.metadata_location,
        table_schema=json.loads(db_table.table_schema),
        partition_spec=json.loads(db_table.partition_spec),
        properties=json.loads(db_table.properties),
        format_version=db_table.format_version,
        is_active=db_table.is_active,
        created_at=db_table.created_at,
        updated_at=db_table.updated_at
    )


@app.get("/api/v1/schemas/{schema_id}/tables", response_model=List[IcebergTableResponse])
async def list_tables(
        schema_id: str,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """List tables in a schema"""
    tables = db.query(IcebergTable).filter(
        IcebergTable.schema_id == schema_id,
        IcebergTable.organization_id == organization_id
    ).all()

    return [
        IcebergTableResponse(
            id=tbl.id,
            name=tbl.name,
            schema_id=tbl.schema_id,
            catalog_id=tbl.catalog_id,
            organization_id=tbl.organization_id,
            table_location=tbl.table_location,
            metadata_location=tbl.metadata_location,
            table_schema=json.loads(tbl.table_schema),
            partition_spec=json.loads(tbl.partition_spec),
            properties=json.loads(tbl.properties),
            format_version=tbl.format_version,
            is_active=tbl.is_active,
            created_at=tbl.created_at,
            updated_at=tbl.updated_at
        )
        for tbl in tables
    ]


@app.get("/api/v1/tables/{table_id}/metrics", response_model=TableMetrics)
async def get_table_metrics(
        table_id: str,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Get metrics for an Iceberg table"""
    table = db.query(IcebergTable).filter(
        IcebergTable.id == table_id,
        IcebergTable.organization_id == organization_id
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    iceberg_manager = IcebergManager(organization_id)
    metrics = await iceberg_manager.get_table_metrics(table.table_location)

    return TableMetrics(**metrics)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )