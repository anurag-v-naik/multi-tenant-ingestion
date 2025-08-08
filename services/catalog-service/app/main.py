# services/catalog-service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import httpx
import uvicorn
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import boto3

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/catalog_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Models
class Catalog(Base):
    __tablename__ = "catalogs"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    unity_catalog_name = Column(String, nullable=False)
    s3_location = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String)

class Schema(Base):
    __tablename__ = "schemas"
    
    id = Column(String, primary_key=True)
    catalog_id = Column(String, nullable=False, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    unity_schema_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Table(Base):
    __tablename__ = "tables"
    
    id = Column(String, primary_key=True)
    schema_id = Column(String, nullable=False, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    table_type = Column(String, default="iceberg")  # iceberg, delta, external
    unity_table_name = Column(String, nullable=False)
    s3_location = Column(String)
    partition_columns = Column(Text)  # JSON array
    schema_definition = Column(Text)  # JSON
    properties = Column(Text)  # JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow)

# Pydantic models
class CatalogCreate(BaseModel):
    name: str
    description: Optional[str] = None

class SchemaCreate(BaseModel):
    catalog_id: str
    name: str
    description: Optional[str] = None

class TableCreate(BaseModel):
    schema_id: str
    name: str
    description: Optional[str] = None
    table_type: str = "iceberg"
    partition_columns: Optional[List[str]] = []
    schema_definition: List[Dict[str, Any]]
    properties: Optional[Dict[str, Any]] = {}

class TableUpdate(BaseModel):
    description: Optional[str] = None
    schema_definition: Optional[List[Dict[str, Any]]] = None
    properties: Optional[Dict[str, Any]] = None

# FastAPI app
app = FastAPI(
    title="Multi-Tenant Catalog Service",
    description="Unity Catalog and Iceberg table management service",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Tenant extraction
async def get_tenant_id(x_tenant_id: str = Header(alias="X-Tenant-ID")):
    if not x_tenant_id:
        raise HTTPException(status_code=400, detail="X-Tenant-ID header required")
    return x_tenant_id

# Unity Catalog integration
class UnityCatalogClient:
    def __init__(self):
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    async def create_catalog(self, tenant_id: str, catalog_name: str, comment: str = None) -> Dict[str, Any]:
        """Create a Unity Catalog catalog for a tenant"""
        unity_catalog_name = f"{tenant_id}_{catalog_name}"
        
        payload = {
            "name": unity_catalog_name,
            "comment": comment or f"Catalog for tenant {tenant_id}",
            "properties": {
                "tenant_id": tenant_id,
                "managed_by": "multi-tenant-ingestion"
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.databricks_host}/api/2.1/unity-catalog/catalogs",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code not in [200, 201]:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Unity Catalog API error: {response.text}"
                )
            
            return response.json()
    
    async def create_schema(self, catalog_name: str, schema_name: str, comment: str = None) -> Dict[str, Any]:
        """Create a schema in Unity Catalog"""
        payload = {
            "name": f"{catalog_name}.{schema_name}",
            "catalog_name": catalog_name,
            "comment": comment or f"Schema {schema_name}",
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.databricks_host}/api/2.1/unity-catalog/schemas",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code not in [200, 201]:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Unity Catalog API error: {response.text}"
                )
            
            return response.json()
    
    async def create_table(self, catalog_name: str, schema_name: str, table_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create an Iceberg table in Unity Catalog"""
        table_name = table_spec["name"]
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        
        # Convert schema definition to Unity Catalog format
        columns = []
        for col in table_spec["schema_definition"]:
            columns.append({
                "name": col["name"],
                "type_text": col["type"],
                "comment": col.get("comment", ""),
                "nullable": col.get("nullable", True)
            })
        
        payload = {
            "name": full_table_name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_type": "MANAGED",
            "data_source_format": "ICEBERG",
            "columns": columns,
            "comment": table_spec.get("description", ""),
            "properties": table_spec.get("properties", {})
        }
        
        # Add partition information if provided
        if table_spec.get("partition_columns"):
            payload["partition_columns"] = table_spec["partition_columns"]
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.databricks_host}/api/2.1/unity-catalog/tables",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code not in [200, 201]:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Unity Catalog API error: {response.text}"
                )
            
            return response.json()
    
    async def get_table_info(self, full_table_name: str) -> Dict[str, Any]:
        """Get table information from Unity Catalog"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.databricks_host}/api/2.1/unity-catalog/tables/{full_table_name}",
                headers=self.headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Unity Catalog API error: {response.text}"
                )
            
            return response.json()

unity_client = UnityCatalogClient()

# Helper functions
def get_tenant_s3_location(tenant_id: str) -> str:
    """Generate S3 location for tenant data"""
    bucket = os.getenv("S3_DATA_BUCKET", "multi-tenant-data")
    return f"s3://{bucket}/{tenant_id}/"

def create_glue_table(tenant_id: str, table_spec: Dict[str, Any], s3_location: str):
    """Create Glue table for cross-platform compatibility"""
    database_name = f"{tenant_id}_catalog"
    table_name = table_spec["name"]
    
    # Ensure Glue database exists
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': f'Glue database for tenant {tenant_id}'
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        pass
    
    # Convert schema to Glue format
    columns = []
    for col in table_spec["schema_definition"]:
        glue_type = convert_to_glue_type(col["type"])
        columns.append({
            'Name': col["name"],
            'Type': glue_type,
            'Comment': col.get("comment", "")
        })
    
    # Create Glue table
    table_input = {
        'Name': table_name,
        'Description': table_spec.get("description", ""),
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergInputFormat',
            'OutputFormat': 'org.apache.iceberg.mr.mapreduce.IcebergOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
            }
        },
        'Parameters': {
            'table_type': 'ICEBERG',
            'tenant_id': tenant_id,
            **table_spec.get("properties", {})
        }
    }
    
    if table_spec.get("partition_columns"):
        table_input['PartitionKeys'] = [
            {'Name': col, 'Type': 'string'} for col in table_spec["partition_columns"]
        ]
    
    glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )

def convert_to_glue_type(unity_type: str) -> str:
    """Convert Unity Catalog types to Glue types"""
    type_mapping = {
        'string': 'string',
        'int': 'int',
        'bigint': 'bigint',
        'double': 'double',
        'float': 'float',
        'boolean': 'boolean',
        'timestamp': 'timestamp',
        'date': 'date',
        'decimal': 'decimal',
        'binary': 'binary'
    }
    return type_mapping.get(unity_type.lower(), 'string')

# API Routes
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "catalog-service", "timestamp": datetime.utcnow()}

@app.get("/health/unity-catalog")
async def health_unity_catalog():
    try:
        headers = {"Authorization": f"Bearer {unity_client.token}"}
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{unity_client.databricks_host}/api/2.1/unity-catalog/catalogs",
                headers=headers
            )
            if response.status_code == 200:
                return {"status": "healthy", "component": "unity-catalog"}
            else:
                raise HTTPException(status_code=503, detail="Unity Catalog API error")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unity Catalog unhealthy: {str(e)}")

@app.post("/api/v1/catalogs")
async def create_catalog(
    catalog: CatalogCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new catalog for the tenant"""
    catalog_id = f"catalog_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    unity_catalog_name = f"{tenant_id}_{catalog.name}"
    s3_location = get_tenant_s3_location(tenant_id)
    
    # Create Unity Catalog
    try:
        unity_response = await unity_client.create_catalog(
            tenant_id, catalog.name, catalog.description
        )
        
        # Store in local database
        db_catalog = Catalog(
            id=catalog_id,
            tenant_id=tenant_id,
            name=catalog.name,
            description=catalog.description,
            unity_catalog_name=unity_catalog_name,
            s3_location=s3_location
        )
        
        db.add(db_catalog)
        db.commit()
        db.refresh(db_catalog)
        
        return {
            "catalog_id": catalog_id,
            "unity_catalog_name": unity_catalog_name,
            "status": "created"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create catalog: {str(e)}")

@app.get("/api/v1/catalogs")
async def list_catalogs(
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all catalogs for the tenant"""
    catalogs = db.query(Catalog).filter(Catalog.tenant_id == tenant_id).all()
    
    result = []
    for catalog in catalogs:
        result.append({
            "id": catalog.id,
            "name": catalog.name,
            "description": catalog.description,
            "unity_catalog_name": catalog.unity_catalog_name,
            "s3_location": catalog.s3_location,
            "created_at": catalog.created_at
        })
    
    return {"catalogs": result}

@app.post("/api/v1/schemas")
async def create_schema(
    schema: SchemaCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new schema in a catalog"""
    # Verify catalog belongs to tenant
    catalog = db.query(Catalog).filter(
        Catalog.id == schema.catalog_id,
        Catalog.tenant_id == tenant_id
    ).first()
    
    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found")
    
    schema_id = f"schema_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    unity_schema_name = f"{tenant_id}_{schema.name}"
    
    try:
        # Create Unity Catalog schema
        unity_response = await unity_client.create_schema(
            catalog.unity_catalog_name, schema.name, schema.description
        )
        
        # Store in local database
        db_schema = Schema(
            id=schema_id,
            catalog_id=schema.catalog_id,
            tenant_id=tenant_id,
            name=schema.name,
            description=schema.description,
            unity_schema_name=unity_schema_name
        )
        
        db.add(db_schema)
        db.commit()
        db.refresh(db_schema)
        
        return {
            "schema_id": schema_id,
            "unity_schema_name": unity_schema_name,
            "status": "created"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create schema: {str(e)}")

@app.get("/api/v1/catalogs/{catalog_id}/schemas")
async def list_schemas(
    catalog_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all schemas in a catalog"""
    # Verify catalog belongs to tenant
    catalog = db.query(Catalog).filter(
        Catalog.id == catalog_id,
        Catalog.tenant_id == tenant_id
    ).first()
    
    if not catalog:
        raise HTTPException(status_code=404, detail="Catalog not found")
    
    schemas = db.query(Schema).filter(
        Schema.catalog_id == catalog_id,
        Schema.tenant_id == tenant_id
    ).all()
    
    result = []
    for schema in schemas:
        result.append({
            "id": schema.id,
            "name": schema.name,
            "description": schema.description,
            "unity_schema_name": schema.unity_schema_name,
            "created_at": schema.created_at
        })
    
    return {"schemas": result}

@app.post("/api/v1/tables")
async def create_table(
    table: TableCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new table in a schema"""
    # Verify schema belongs to tenant
    schema = db.query(Schema).filter(
        Schema.id == table.schema_id,
        Schema.tenant_id == tenant_id
    ).first()
    
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    
    # Get catalog info
    catalog = db.query(Catalog).filter(Catalog.id == schema.catalog_id).first()
    
    table_id = f"table_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    unity_table_name = f"{catalog.unity_catalog_name}.{schema.unity_schema_name}.{table.name}"
    s3_location = f"{catalog.s3_location}{schema.name}/{table.name}/"
    
    try:
        # Prepare table specification
        table_spec = {
            "name": table.name,
            "description": table.description,
            "schema_definition": table.schema_definition,
            "partition_columns": table.partition_columns,
            "properties": table.properties
        }
        
        # Create Unity Catalog table
        unity_response = await unity_client.create_table(
            catalog.unity_catalog_name, schema.unity_schema_name, table_spec
        )
        
        # Create Glue table for cross-platform access
        try:
            create_glue_table(tenant_id, table_spec, s3_location)
        except Exception as glue_error:
            print(f"Warning: Failed to create Glue table: {glue_error}")
        
        # Store in local database
        db_table = Table(
            id=table_id,
            schema_id=table.schema_id,
            tenant_id=tenant_id,
            name=table.name,
            description=table.description,
            table_type=table.table_type,
            unity_table_name=unity_table_name,
            s3_location=s3_location,
            partition_columns=json.dumps(table.partition_columns),
            schema_definition=json.dumps(table.schema_definition),
            properties=json.dumps(table.properties)
        )
        
        db.add(db_table)
        db.commit()
        db.refresh(db_table)
        
        return {
            "table_id": table_id,
            "unity_table_name": unity_table_name,
            "s3_location": s3_location,
            "status": "created"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create table: {str(e)}")

@app.get("/api/v1/schemas/{schema_id}/tables")
async def list_tables(
    schema_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all tables in a schema"""
    # Verify schema belongs to tenant
    schema = db.query(Schema).filter(
        Schema.id == schema_id,
        Schema.tenant_id == tenant_id
    ).first()
    
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    
    tables = db.query(Table).filter(
        Table.schema_id == schema_id,
        Table.tenant_id == tenant_id
    ).all()
    
    result = []
    for table in tables:
        result.append({
            "id": table.id,
            "name": table.name,
            "description": table.description,
            "table_type": table.table_type,
            "unity_table_name": table.unity_table_name,
            "s3_location": table.s3_location,
            "partition_columns": json.loads(table.partition_columns or "[]"),
            "created_at": table.created_at,
            "last_updated": table.last_updated
        })
    
    return {"tables": result}

@app.get("/api/v1/tables/{table_id}")
async def get_table(
    table_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get detailed table information"""
    table = db.query(Table).filter(
        Table.id == table_id,
        Table.tenant_id == tenant_id
    ).first()
    
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    
    # Get live information from Unity Catalog
    try:
        unity_info = await unity_client.get_table_info(table.unity_table_name)
        
        return {
            "id": table.id,
            "name": table.name,
            "description": table.description,
            "table_type": table.table_type,
            "unity_table_name": table.unity_table_name,
            "s3_location": table.s3_location,
            "partition_columns": json.loads(table.partition_columns or "[]"),
            "schema_definition": json.loads(table.schema_definition),
            "properties": json.loads(table.properties or "{}"),
            "created_at": table.created_at,
            "last_updated": table.last_updated,
            "unity_catalog_info": unity_info
        }
        
    except Exception as e:
        # Return local information if Unity Catalog is unavailable
        return {
            "id": table.id,
            "name": table.name,
            "description": table.description,
            "table_type": table.table_type,
            "unity_table_name": table.unity_table_name,
            "s3_location": table.s3_location,
            "partition_columns": json.loads(table.partition_columns or "[]"),
            "schema_definition": json.loads(table.schema_definition),
            "properties": json.loads(table.properties or "{}"),
            "created_at": table.created_at,
            "last_updated": table.last_updated,
            "error": f"Could not fetch live Unity Catalog info: {str(e)}"
        }

@app.put("/api/v1/tables/{table_id}")
async def update_table(
    table_id: str,
    table_update: TableUpdate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Update table metadata"""
    table = db.query(Table).filter(
        Table.id == table_id,
        Table.tenant_id == tenant_id
    ).first()
    
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    
    # Update local metadata
    update_data = table_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        if field in ["schema_definition", "properties"] and value:
            setattr(table, field, json.dumps(value))
        else:
            setattr(table, field, value)
    
    table.last_updated = datetime.utcnow()
    db.commit()
    
    return {"status": "updated"}

@app.get("/api/v1/tables/{table_id}/lineage")
async def get_table_lineage(
    table_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get data lineage for a table"""
    table = db.query(Table).filter(
        Table.id == table_id,
        Table.tenant_id == tenant_id
    ).first()
    
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    
    # This would integrate with Unity Catalog lineage APIs
    # For now, return placeholder lineage information
    return {
        "table_id": table_id,
        "upstream_tables": [],
        "downstream_tables": [],
        "transformations": [],
        "last_updated": datetime.utcnow()
    }

# Cross-platform compatibility endpoints
@app.get("/api/v1/tables/{table_id}/query-engines")
async def get_query_engines(
    table_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get connection information for different query engines"""
    table = db.query(Table).filter(
        Table.id == table_id,
        Table.tenant_id == tenant_id
    ).first()
    
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    
    # Return connection info for different platforms
    return {
        "table_id": table_id,
        "platforms": {
            "databricks": {
                "connection_string": table.unity_table_name,
                "query_example": f"SELECT * FROM {table.unity_table_name} LIMIT 10"
            },
            "snowflake": {
                "connection_string": f"iceberg.{tenant_id}.{table.name}",
                "query_example": f"SELECT * FROM iceberg.{tenant_id}.{table.name} LIMIT 10"
            },
            "redshift": {
                "connection_string": f"iceberg_{tenant_id}_{table.name}",
                "query_example": f"SELECT * FROM iceberg_{tenant_id}_{table.name} LIMIT 10"
            },
            "bigquery": {
                "connection_string": f"iceberg.{tenant_id}.{table.name}",
                "query_example": f"SELECT * FROM `iceberg.{tenant_id}.{table.name}` LIMIT 10"
            },
            "athena": {
                "connection_string": f"{tenant_id}_catalog.{table.name}",
                "query_example": f"SELECT * FROM {tenant_id}_catalog.{table.name} LIMIT 10"
            }
        }
    }

# Create tables
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
