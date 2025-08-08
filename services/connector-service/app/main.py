# services/connector-service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, validator
import httpx
import uvicorn
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import boto3
import pymongo
import psycopg2
from sqlalchemy import create_engine as sql_create_engine
import redis

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/connector_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class Connector(Base):
    __tablename__ = "connectors"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    connector_type = Column(String, nullable=False)  # postgres, mysql, mongodb, s3, etc.
    connection_config = Column(Text)  # Encrypted JSON
    schema_info = Column(Text)  # JSON
    status = Column(String, default="inactive")
    created_at = Column(DateTime, default=datetime.utcnow)
    last_tested = Column(DateTime)
    created_by = Column(String)

class ConnectorTemplate(Base):
    __tablename__ = "connector_templates"
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    connector_type = Column(String, nullable=False)
    description = Column(Text)
    config_schema = Column(Text)  # JSON schema for configuration
    default_config = Column(Text)  # JSON
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Pydantic models
class ConnectorCreate(BaseModel):
    name: str
    description: Optional[str] = None
    connector_type: str
    connection_config: Dict[str, Any]

class ConnectorUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    connection_config: Optional[Dict[str, Any]] = None
    status: Optional[str] = None

class ConnectorTest(BaseModel):
    connection_config: Dict[str, Any]
    connector_type: str

class DataPreview(BaseModel):
    connector_id: str
    table_name: Optional[str] = None
    query: Optional[str] = None
    limit: Optional[int] = 100

# FastAPI app
app = FastAPI(
    title="Multi-Tenant Connector Service",
    description="Data source connector and integration service",
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

# Encryption utilities
def encrypt_config(config: Dict[str, Any]) -> str:
    """Encrypt sensitive configuration data"""
    # In production, use proper encryption like AWS KMS
    import base64
    config_str = json.dumps(config)
    encoded = base64.b64encode(config_str.encode()).decode()
    return encoded

def decrypt_config(encrypted_config: str) -> Dict[str, Any]:
    """Decrypt configuration data"""
    import base64
    decoded = base64.b64decode(encrypted_config.encode()).decode()
    return json.loads(decoded)

# Connector implementations
class BaseConnector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    async def test_connection(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    async def get_schema(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        raise NotImplementedError

class PostgreSQLConnector(BaseConnector):
    async def test_connection(self) -> Dict[str, Any]:
        try:
            conn_str = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            engine = sql_create_engine(conn_str)
            with engine.connect() as conn:
                result = conn.execute("SELECT 1")
                return {"status": "error", "message": str(e)}
    
    async def get_schema(self) -> Dict[str, Any]:
        try:
            connection_string = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            client = pymongo.MongoClient(connection_string)
            db = client[self.config['database']]
            
            collections = {}
            for collection_name in db.list_collection_names():
                collection = db[collection_name]
                # Sample a few documents to infer schema
                sample_docs = list(collection.find().limit(10))
                
                if sample_docs:
                    # Extract field types from sample documents
                    fields = {}
                    for doc in sample_docs:
                        for key, value in doc.items():
                            if key not in fields:
                                fields[key] = type(value).__name__
                    
                    collections[collection_name] = [
                        {"name": field, "type": field_type, "nullable": True}
                        for field, field_type in fields.items()
                    ]
                else:
                    collections[collection_name] = []
            
            client.close()
            return {"status": "success", "collections": collections}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        try:
            connection_string = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            client = pymongo.MongoClient(connection_string)
            db = client[self.config['database']]
            
            if table_name:
                collection = db[table_name]
                if query:
                    # Parse MongoDB query (simplified)
                    import ast
                    mongo_query = ast.literal_eval(query) if query.startswith('{') else {}
                else:
                    mongo_query = {}
                
                docs = list(collection.find(mongo_query).limit(limit))
                
                # Convert ObjectId to string for JSON serialization
                for doc in docs:
                    if '_id' in doc:
                        doc['_id'] = str(doc['_id'])
                
                columns = list(docs[0].keys()) if docs else []
                
                client.close()
                return {
                    "status": "success",
                    "columns": columns,
                    "data": docs,
                    "row_count": len(docs)
                }
            else:
                raise ValueError("table_name (collection name) must be provided for MongoDB")
        except Exception as e:
            return {"status": "error", "message": str(e)}

class S3Connector(BaseConnector):
    async def test_connection(self) -> Dict[str, Any]:
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['access_key'],
                aws_secret_access_key=self.config['secret_key'],
                region_name=self.config.get('region', 'us-east-1')
            )
            
            # Test by listing buckets
            s3_client.list_buckets()
            return {"status": "success", "message": "Connection successful"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def get_schema(self) -> Dict[str, Any]:
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['access_key'],
                aws_secret_access_key=self.config['secret_key'],
                region_name=self.config.get('region', 'us-east-1')
            )
            
            bucket_name = self.config['bucket']
            prefix = self.config.get('prefix', '')
            
            # List objects in bucket
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=100
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        "key": obj['Key'],
                        "size": obj['Size'],
                        "last_modified": obj['LastModified'].isoformat(),
                        "storage_class": obj.get('StorageClass', 'STANDARD')
                    })
            
            return {"status": "success", "files": files}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.config['access_key'],
                aws_secret_access_key=self.config['secret_key'],
                region_name=self.config.get('region', 'us-east-1')
            )
            
            bucket_name = self.config['bucket']
            file_key = table_name or query  # Use table_name as file key
            
            if not file_key:
                raise ValueError("file key must be provided")
            
            # Get object
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read()
            
            # Try to parse as CSV/JSON
            if file_key.endswith('.json'):
                import json
                data = json.loads(content.decode())
                if isinstance(data, list):
                    preview_data = data[:limit]
                    columns = list(preview_data[0].keys()) if preview_data else []
                else:
                    preview_data = [data]
                    columns = list(data.keys())
            elif file_key.endswith('.csv'):
                import csv
                import io
                csv_data = csv.DictReader(io.StringIO(content.decode()))
                preview_data = list(csv_data)[:limit]
                columns = list(preview_data[0].keys()) if preview_data else []
            else:
                # Return raw content preview
                preview_data = [{"content": content.decode()[:1000]}]
                columns = ["content"]
            
            return {
                "status": "success",
                "columns": columns,
                "data": preview_data,
                "row_count": len(preview_data)
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

class APIConnector(BaseConnector):
    async def test_connection(self) -> Dict[str, Any]:
        try:
            headers = self.config.get('headers', {})
            auth = None
            
            if 'api_key' in self.config:
                headers['Authorization'] = f"Bearer {self.config['api_key']}"
            elif 'username' in self.config and 'password' in self.config:
                auth = (self.config['username'], self.config['password'])
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.config['base_url'],
                    headers=headers,
                    auth=auth,
                    timeout=30
                )
                
                if response.status_code == 200:
                    return {"status": "success", "message": "Connection successful"}
                else:
                    return {"status": "error", "message": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def get_schema(self) -> Dict[str, Any]:
        try:
            # For API connectors, schema discovery is endpoint-specific
            endpoints = self.config.get('endpoints', [])
            
            return {
                "status": "success",
                "endpoints": endpoints,
                "note": "API schema depends on specific endpoints"
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        try:
            endpoint = table_name or query
            if not endpoint:
                raise ValueError("endpoint must be provided")
            
            url = f"{self.config['base_url'].rstrip('/')}/{endpoint.lstrip('/')}"
            headers = self.config.get('headers', {})
            auth = None
            
            if 'api_key' in self.config:
                headers['Authorization'] = f"Bearer {self.config['api_key']}"
            elif 'username' in self.config and 'password' in self.config:
                auth = (self.config['username'], self.config['password'])
            
            # Add limit parameter if supported
            params = {}
            if limit and self.config.get('supports_limit', True):
                limit_param = self.config.get('limit_param', 'limit')
                params[limit_param] = limit
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    headers=headers,
                    auth=auth,
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Handle different response formats
                    if isinstance(data, list):
                        preview_data = data[:limit]
                        columns = list(preview_data[0].keys()) if preview_data else []
                    elif isinstance(data, dict):
                        # Look for common data keys
                        data_key = None
                        for key in ['data', 'results', 'items', 'records']:
                            if key in data and isinstance(data[key], list):
                                data_key = key
                                break
                        
                        if data_key:
                            preview_data = data[data_key][:limit]
                            columns = list(preview_data[0].keys()) if preview_data else []
                        else:
                            preview_data = [data]
                            columns = list(data.keys())
                    else:
                        preview_data = [{"response": str(data)}]
                        columns = ["response"]
                    
                    return {
                        "status": "success",
                        "columns": columns,
                        "data": preview_data,
                        "row_count": len(preview_data)
                    }
                else:
                    return {"status": "error", "message": f"HTTP {response.status_code}: {response.text}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

# Connector factory
def create_connector(connector_type: str, config: Dict[str, Any]) -> BaseConnector:
    connectors = {
        'postgresql': PostgreSQLConnector,
        'mysql': MySQLConnector,
        'mongodb': MongoDBConnector,
        's3': S3Connector,
        'api': APIConnector
    }
    
    if connector_type not in connectors:
        raise ValueError(f"Unsupported connector type: {connector_type}")
    
    return connectors[connector_type](config)

# API Routes
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "connector-service", "timestamp": datetime.utcnow()}

@app.get("/api/v1/connector-types")
async def list_connector_types():
    """List available connector types"""
    connector_types = [
        {
            "type": "postgresql",
            "name": "PostgreSQL",
            "description": "Connect to PostgreSQL databases",
            "config_fields": [
                {"name": "host", "type": "string", "required": True},
                {"name": "port", "type": "integer", "default": 5432},
                {"name": "database", "type": "string", "required": True},
                {"name": "username", "type": "string", "required": True},
                {"name": "password", "type": "password", "required": True}
            ]
        },
        {
            "type": "mysql",
            "name": "MySQL",
            "description": "Connect to MySQL databases",
            "config_fields": [
                {"name": "host", "type": "string", "required": True},
                {"name": "port", "type": "integer", "default": 3306},
                {"name": "database", "type": "string", "required": True},
                {"name": "username", "type": "string", "required": True},
                {"name": "password", "type": "password", "required": True}
            ]
        },
        {
            "type": "mongodb",
            "name": "MongoDB",
            "description": "Connect to MongoDB databases",
            "config_fields": [
                {"name": "host", "type": "string", "required": True},
                {"name": "port", "type": "integer", "default": 27017},
                {"name": "database", "type": "string", "required": True},
                {"name": "username", "type": "string", "required": True},
                {"name": "password", "type": "password", "required": True}
            ]
        },
        {
            "type": "s3",
            "name": "Amazon S3",
            "description": "Connect to Amazon S3 buckets",
            "config_fields": [
                {"name": "access_key", "type": "string", "required": True},
                {"name": "secret_key", "type": "password", "required": True},
                {"name": "region", "type": "string", "default": "us-east-1"},
                {"name": "bucket", "type": "string", "required": True},
                {"name": "prefix", "type": "string", "required": False}
            ]
        },
        {
            "type": "api",
            "name": "REST API",
            "description": "Connect to REST APIs",
            "config_fields": [
                {"name": "base_url", "type": "string", "required": True},
                {"name": "api_key", "type": "password", "required": False},
                {"name": "username", "type": "string", "required": False},
                {"name": "password", "type": "password", "required": False},
                {"name": "headers", "type": "object", "required": False}
            ]
        }
    ]
    
    return {"connector_types": connector_types}

@app.post("/api/v1/connectors/test")
async def test_connector(
    test_request: ConnectorTest,
    tenant_id: str = Depends(get_tenant_id)
):
    """Test a connector configuration"""
    try:
        connector = create_connector(test_request.connector_type, test_request.connection_config)
        result = await connector.test_connection()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/v1/connectors")
async def create_connector_endpoint(
    connector: ConnectorCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new connector"""
    connector_id = f"conn_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    
    # Test connection first
    try:
        conn_instance = create_connector(connector.connector_type, connector.connection_config)
        test_result = await conn_instance.test_connection()
        
        if test_result["status"] != "success":
            raise HTTPException(status_code=400, detail=f"Connection test failed: {test_result['message']}")
        
        # Get schema information
        schema_info = await conn_instance.get_schema()
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to create connector: {str(e)}")
    
    # Encrypt and store configuration
    encrypted_config = encrypt_config(connector.connection_config)
    
    db_connector = Connector(
        id=connector_id,
        tenant_id=tenant_id,
        name=connector.name,
        description=connector.description,
        connector_type=connector.connector_type,
        connection_config=encrypted_config,
        schema_info=json.dumps(schema_info),
        status="active",
        last_tested=datetime.utcnow()
    )
    
    db.add(db_connector)
    db.commit()
    db.refresh(db_connector)
    
    return {"connector_id": connector_id, "status": "created"}

@app.get("/api/v1/connectors")
async def list_connectors(
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all connectors for a tenant"""
    connectors = db.query(Connector).filter(Connector.tenant_id == tenant_id).all()
    
    result = []
    for connector in connectors:
        result.append({
            "id": connector.id,
            "name": connector.name,
            "description": connector.description,
            "connector_type": connector.connector_type,
            "status": connector.status,
            "created_at": connector.created_at,
            "last_tested": connector.last_tested
        })
    
    return {"connectors": result}

@app.get("/api/v1/connectors/{connector_id}")
async def get_connector(
    connector_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get a specific connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    # Don't return sensitive configuration data
    return {
        "id": connector.id,
        "name": connector.name,
        "description": connector.description,
        "connector_type": connector.connector_type,
        "status": connector.status,
        "schema_info": json.loads(connector.schema_info or "{}"),
        "created_at": connector.created_at,
        "last_tested": connector.last_tested
    }

@app.put("/api/v1/connectors/{connector_id}")
async def update_connector(
    connector_id: str,
    connector_update: ConnectorUpdate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Update a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    # If connection config is being updated, test the new configuration
    if connector_update.connection_config:
        try:
            conn_instance = create_connector(connector.connector_type, connector_update.connection_config)
            test_result = await conn_instance.test_connection()
            
            if test_result["status"] != "success":
                raise HTTPException(status_code=400, detail=f"Connection test failed: {test_result['message']}")
            
            # Update schema info
            schema_info = await conn_instance.get_schema()
            connector.schema_info = json.dumps(schema_info)
            connector.connection_config = encrypt_config(connector_update.connection_config)
            connector.last_tested = datetime.utcnow()
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to update connector: {str(e)}")
    
    # Update other fields
    update_data = connector_update.dict(exclude_unset=True, exclude={'connection_config'})
    for field, value in update_data.items():
        setattr(connector, field, value)
    
    db.commit()
    return {"status": "updated"}

@app.delete("/api/v1/connectors/{connector_id}")
async def delete_connector(
    connector_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Delete a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    db.delete(connector)
    db.commit()
    
    return {"status": "deleted"}

@app.post("/api/v1/connectors/{connector_id}/test")
async def test_existing_connector(
    connector_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Test an existing connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    try:
        config = decrypt_config(connector.connection_config)
        conn_instance = create_connector(connector.connector_type, config)
        result = await conn_instance.test_connection()
        
        # Update last tested timestamp
        connector.last_tested = datetime.utcnow()
        if result["status"] == "success":
            connector.status = "active"
        else:
            connector.status = "error"
        
        db.commit()
        
        return result
    except Exception as e:
        connector.status = "error"
        db.commit()
        return {"status": "error", "message": str(e)}

@app.post("/api/v1/connectors/{connector_id}/preview")
async def preview_connector_data(
    connector_id: str,
    preview_request: DataPreview,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Preview data from a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    try:
        config = decrypt_config(connector.connection_config)
        conn_instance = create_connector(connector.connector_type, config)
        
        result = await conn_instance.preview_data(
            table_name=preview_request.table_name,
            query=preview_request.query,
            limit=preview_request.limit
        )
        
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/connectors/{connector_id}/schema")
async def get_connector_schema(
    connector_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get schema information for a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.tenant_id == tenant_id
    ).first()
    
    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")
    
    try:
        # Return cached schema info or fetch fresh
        if connector.schema_info:
            return json.loads(connector.schema_info)
        
        config = decrypt_config(connector.connection_config)
        conn_instance = create_connector(connector.connector_type, config)
        schema_info = await conn_instance.get_schema()
        
        # Update cached schema info
        connector.schema_info = json.dumps(schema_info)
        db.commit()
        
        return schema_info
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Create tables
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002) "success", "message": "Connection successful"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def get_schema(self) -> Dict[str, Any]:
        try:
            conn_str = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            engine = sql_create_engine(conn_str)
            
            schema_query = """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
            
            with engine.connect() as conn:
                result = conn.execute(schema_query)
                rows = result.fetchall()
                
                tables = {}
                for row in rows:
                    table_name = row[0]
                    if table_name not in tables:
                        tables[table_name] = []
                    
                    tables[table_name].append({
                        "name": row[1],
                        "type": row[2],
                        "nullable": row[3] == "YES"
                    })
                
                return {"status": "success", "tables": tables}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        try:
            conn_str = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            engine = sql_create_engine(conn_str)
            
            if query:
                # For security, limit custom queries
                if not query.upper().strip().startswith('SELECT'):
                    raise ValueError("Only SELECT queries are allowed")
                sql_query = f"SELECT * FROM ({query}) subquery LIMIT {limit}"
            elif table_name:
                sql_query = f"SELECT * FROM {table_name} LIMIT {limit}"
            else:
                raise ValueError("Either table_name or query must be provided")
            
            with engine.connect() as conn:
                result = conn.execute(sql_query)
                rows = result.fetchall()
                columns = list(result.keys())
                
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    "status": "success",
                    "columns": columns,
                    "data": data,
                    "row_count": len(data)
                }
        except Exception as e:
            return {"status": "error", "message": str(e)}

class MySQLConnector(BaseConnector):
    async def test_connection(self) -> Dict[str, Any]:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['username'],
                password=self.config['password'],
                database=self.config['database']
            )
            conn.close()
            return {"status": "success", "message": "Connection successful"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def get_schema(self) -> Dict[str, Any]:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['username'],
                password=self.config['password'],
                database=self.config['database']
            )
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                ORDER BY table_name, ordinal_position
            """)
            
            rows = cursor.fetchall()
            tables = {}
            for row in rows:
                table_name = row[0]
                if table_name not in tables:
                    tables[table_name] = []
                
                tables[table_name].append({
                    "name": row[1],
                    "type": row[2],
                    "nullable": row[3] == "YES"
                })
            
            conn.close()
            return {"status": "success", "tables": tables}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    async def preview_data(self, table_name: str = None, query: str = None, limit: int = 100) -> Dict[str, Any]:
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['username'],
                password=self.config['password'],
                database=self.config['database']
            )
            
            cursor = conn.cursor(dictionary=True)
            
            if query:
                if not query.upper().strip().startswith('SELECT'):
                    raise ValueError("Only SELECT queries are allowed")
                sql_query = f"SELECT * FROM ({query}) subquery LIMIT {limit}"
            elif table_name:
                sql_query = f"SELECT * FROM {table_name} LIMIT {limit}"
            else:
                raise ValueError("Either table_name or query must be provided")
            
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            columns = list(rows[0].keys()) if rows else []
            
            conn.close()
            return {
                "status": "success",
                "columns": columns,
                "data": rows,
                "row_count": len(rows)
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

class MongoDBConnector(BaseConnector):
    async def test_connection(self) -> Dict[str, Any]:
        try:
            connection_string = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            client = pymongo.MongoClient(connection_string)
            client.server_info()  # Trigger connection
            client.close()
            return {"status": "success", "message": "Connection successful"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
