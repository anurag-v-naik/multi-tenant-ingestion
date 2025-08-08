"""
Multi-Tenant Connector Service
Manages data source connectors and connection templates
"""
import logging
import os
import json
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any
from datetime import datetime
from abc import ABC, abstractmethod

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import requests
import boto3
import pymysql
import psycopg2
import pyodbc
from azure.storage.blob import BlobServiceClient
from google.cloud import storage as gcs

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
class Connector(Base):
    __tablename__ = "connectors"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    connector_type = Column(String, index=True)  # database, file, api, stream
    organization_id = Column(String, index=True)
    configuration = Column(Text)  # Encrypted JSON string
    credentials = Column(Text)  # Encrypted credentials
    properties = Column(Text)  # JSON string
    is_active = Column(Boolean, default=True)
    last_tested = Column(DateTime)
    test_status = Column(String)  # success, failed, pending
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConnectorTemplate(Base):
    __tablename__ = "connector_templates"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    connector_type = Column(String, index=True)
    category = Column(String)  # databases, cloud_storage, saas, streaming
    description = Column(Text)
    configuration_schema = Column(Text)  # JSON schema
    credential_schema = Column(Text)  # JSON schema
    properties = Column(Text)  # JSON string
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# Pydantic models
class ConnectorCreate(BaseModel):
    name: str
    connector_type: str
    configuration: Dict
    credentials: Dict
    properties: Optional[Dict] = {}


class ConnectorUpdate(BaseModel):
    name: Optional[str] = None
    configuration: Optional[Dict] = None
    credentials: Optional[Dict] = None
    properties: Optional[Dict] = None
    is_active: Optional[bool] = None


class ConnectorResponse(BaseModel):
    id: str
    name: str
    connector_type: str
    organization_id: str
    configuration: Dict
    properties: Dict
    is_active: bool
    last_tested: Optional[datetime]
    test_status: Optional[str]
    created_at: datetime
    updated_at: datetime


class ConnectorTemplateResponse(BaseModel):
    id: str
    name: str
    connector_type: str
    category: str
    description: str
    configuration_schema: Dict
    credential_schema: Dict
    properties: Dict
    is_active: bool
    created_at: datetime


class ConnectionTestRequest(BaseModel):
    connector_id: Optional[str] = None
    configuration: Optional[Dict] = None
    credentials: Optional[Dict] = None


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    details: Optional[Dict] = None


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


# Connector interface
class BaseConnector(ABC):
    """Base class for all connectors"""

    def __init__(self, config: Dict, credentials: Dict):
        self.config = config
        self.credentials = credentials

    @abstractmethod
    async def test_connection(self) -> bool:
        """Test if the connection is valid"""
        pass

    @abstractmethod
    async def get_schema(self) -> Dict:
        """Get the schema/structure of the data source"""
        pass

    @abstractmethod
    async def get_sample_data(self, limit: int = 10) -> List[Dict]:
        """Get sample data from the source"""
        pass


class DatabaseConnector(BaseConnector):
    """Database connector for SQL databases"""

    async def test_connection(self) -> bool:
        try:
            db_type = self.config.get("database_type", "postgresql")

            if db_type == "postgresql":
                conn = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    database=self.config["database"],
                    user=self.credentials["username"],
                    password=self.credentials["password"]
                )
            elif db_type == "mysql":
                conn = pymysql.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 3306),
                    database=self.config["database"],
                    user=self.credentials["username"],
                    password=self.credentials["password"]
                )
            elif db_type == "sqlserver":
                conn_str = f"DRIVER={{SQL Server}};SERVER={self.config['host']};DATABASE={self.config['database']};UID={self.credentials['username']};PWD={self.credentials['password']}"
                conn = pyodbc.connect(conn_str)
            else:
                return False

            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False

    async def get_schema(self) -> Dict:
        """Get database schema information"""
        try:
            db_type = self.config.get("database_type", "postgresql")

            if db_type == "postgresql":
                conn = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    database=self.config["database"],
                    user=self.credentials["username"],
                    password=self.credentials["password"]
                )
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                """)

                schema = {}
                for row in cursor.fetchall():
                    table_name, column_name, data_type, is_nullable = row
                    if table_name not in schema:
                        schema[table_name] = []
                    schema[table_name].append({
                        "name": column_name,
                        "type": data_type,
                        "nullable": is_nullable == "YES"
                    })

                cursor.close()
                conn.close()
                return schema

        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}")
            return {}

    async def get_sample_data(self, limit: int = 10) -> List[Dict]:
        """Get sample data from database tables"""
        try:
            schema = await self.get_schema()
            sample_data = {}

            db_type = self.config.get("database_type", "postgresql")

            if db_type == "postgresql":
                conn = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config.get("port", 5432),
                    database=self.config["database"],
                    user=self.credentials["username"],
                    password=self.credentials["password"]
                )

                for table_name in list(schema.keys())[:5]:  # Limit to 5 tables
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")

                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    sample_data[table_name] = [
                        dict(zip(columns, row)) for row in rows
                    ]
                    cursor.close()

                conn.close()
                return sample_data

        except Exception as e:
            logger.error(f"Failed to get sample data: {str(e)}")
            return {}


class S3Connector(BaseConnector):
    """Amazon S3 connector"""

    async def test_connection(self) -> bool:
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.credentials["access_key_id"],
                aws_secret_access_key=self.credentials["secret_access_key"],
                region_name=self.config.get("region", "us-east-1")
            )

            bucket_name = self.config["bucket_name"]
            prefix = self.config.get("prefix", "")

            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )

            files = {}
            for obj in response.get("Contents", []):
                key = obj["Key"]
                file_extension = key.split(".")[-1].lower() if "." in key else "unknown"

                if file_extension not in files:
                    files[file_extension] = []

                files[file_extension].append({
                    "key": key,
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat()
                })

            return {
                "bucket": bucket_name,
                "file_types": files,
                "total_objects": len(response.get("Contents", []))
            }

        except Exception as e:
            logger.error(f"Failed to get S3 schema: {str(e)}")
            return {}

    async def get_sample_data(self, limit: int = 10) -> List[Dict]:
        """Get sample files from S3"""
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.credentials["access_key_id"],
                aws_secret_access_key=self.credentials["secret_access_key"],
                region_name=self.config.get("region", "us-east-1")
            )

            bucket_name = self.config["bucket_name"]
            prefix = self.config.get("prefix", "")

            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=limit
            )

            sample_files = []
            for obj in response.get("Contents", []):
                sample_files.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "etag": obj["ETag"].strip('"')
                })

            return sample_files

        except Exception as e:
            logger.error(f"Failed to get S3 sample data: {str(e)}")
            return []


class APIConnector(BaseConnector):
    """REST API connector"""

    async def test_connection(self) -> bool:
        try:
            url = self.config["base_url"]
            headers = self.config.get("headers", {})

            # Add authentication
            auth_type = self.credentials.get("auth_type", "none")
            if auth_type == "bearer":
                headers["Authorization"] = f"Bearer {self.credentials['token']}"
            elif auth_type == "api_key":
                headers[self.credentials["key_header"]] = self.credentials["api_key"]

            response = requests.get(
                f"{url.rstrip('/')}/health",
                headers=headers,
                timeout=30
            )

            return response.status_code < 400

        except Exception as e:
            logger.error(f"API connection test failed: {str(e)}")
            return False

    async def get_schema(self) -> Dict:
        """Get API endpoints and schema"""
        try:
            url = self.config["base_url"]
            headers = self.config.get("headers", {})

            # Add authentication
            auth_type = self.credentials.get("auth_type", "none")
            if auth_type == "bearer":
                headers["Authorization"] = f"Bearer {self.credentials['token']}"
            elif auth_type == "api_key":
                headers[self.credentials["key_header"]] = self.credentials["api_key"]

            # Try to get OpenAPI/Swagger schema
            schema_urls = ["/swagger.json", "/openapi.json", "/api-docs"]

            for schema_url in schema_urls:
                try:
                    response = requests.get(
                        f"{url.rstrip('/')}{schema_url}",
                        headers=headers,
                        timeout=30
                    )
                    if response.status_code == 200:
                        return response.json()
                except:
                    continue

            # Fallback: return basic info
            return {
                "base_url": url,
                "endpoints": self.config.get("endpoints", []),
                "schema_available": False
            }

        except Exception as e:
            logger.error(f"Failed to get API schema: {str(e)}")
            return {}

    async def get_sample_data(self, limit: int = 10) -> List[Dict]:
        """Get sample data from API endpoints"""
        try:
            url = self.config["base_url"]
            headers = self.config.get("headers", {})
            endpoints = self.config.get("endpoints", [])

            # Add authentication
            auth_type = self.credentials.get("auth_type", "none")
            if auth_type == "bearer":
                headers["Authorization"] = f"Bearer {self.credentials['token']}"
            elif auth_type == "api_key":
                headers[self.credentials["key_header"]] = self.credentials["api_key"]

            sample_data = {}

            for endpoint in endpoints[:3]:  # Limit to 3 endpoints
                try:
                    response = requests.get(
                        f"{url.rstrip('/')}/{endpoint.lstrip('/')}",
                        headers=headers,
                        params={"limit": limit},
                        timeout=30
                    )

                    if response.status_code == 200:
                        sample_data[endpoint] = response.json()

                except Exception as e:
                    logger.error(f"Failed to get sample data from {endpoint}: {str(e)}")
                    sample_data[endpoint] = {"error": str(e)}

            return sample_data

        except Exception as e:
            logger.error(f"Failed to get API sample data: {str(e)}")
            return []


class ConnectorFactory:
    """Factory for creating connector instances"""

    @staticmethod
    def create_connector(connector_type: str, config: Dict, credentials: Dict) -> BaseConnector:
        if connector_type in ["postgresql", "mysql", "sqlserver"]:
            return DatabaseConnector(config, credentials)
        elif connector_type == "s3":
            return S3Connector(config, credentials)
        elif connector_type == "api":
            return APIConnector(config, credentials)
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")


class EncryptionManager:
    """Manages encryption of sensitive data"""

    def __init__(self):
        self.key = os.getenv("ENCRYPTION_KEY", "default-key").encode()

    def encrypt(self, data: str) -> str:
        """Encrypt sensitive data"""
        # In production, use proper encryption like Fernet
        import base64
        return base64.b64encode(data.encode()).decode()

    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        # In production, use proper decryption like Fernet
        import base64
        return base64.b64decode(encrypted_data.encode()).decode()


encryption_manager = EncryptionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    await create_default_templates()
    logger.info("Connector Service started")
    yield
    # Shutdown
    logger.info("Connector Service shutting down")


async def create_default_templates():
    """Create default connector templates"""
    db = SessionLocal()

    templates = [
        {
            "id": "postgresql-template",
            "name": "PostgreSQL Database",
            "connector_type": "postgresql",
            "category": "databases",
            "description": "Connect to PostgreSQL database",
            "configuration_schema": {
                "type": "object",
                "properties": {
                    "host": {"type": "string", "title": "Host"},
                    "port": {"type": "integer", "default": 5432, "title": "Port"},
                    "database": {"type": "string", "title": "Database Name"},
                    "ssl_mode": {"type": "string", "enum": ["disable", "require"], "default": "require"}
                },
                "required": ["host", "database"]
            },
            "credential_schema": {
                "type": "object",
                "properties": {
                    "username": {"type": "string", "title": "Username"},
                    "password": {"type": "string", "format": "password", "title": "Password"}
                },
                "required": ["username", "password"]
            }
        },
        {
            "id": "s3-template",
            "name": "Amazon S3",
            "connector_type": "s3",
            "category": "cloud_storage",
            "description": "Connect to Amazon S3 bucket",
            "configuration_schema": {
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string", "title": "Bucket Name"},
                    "region": {"type": "string", "default": "us-east-1", "title": "AWS Region"},
                    "prefix": {"type": "string", "title": "Prefix (optional)"}
                },
                "required": ["bucket_name"]
            },
            "credential_schema": {
                "type": "object",
                "properties": {
                    "access_key_id": {"type": "string", "title": "Access Key ID"},
                    "secret_access_key": {"type": "string", "format": "password", "title": "Secret Access Key"}
                },
                "required": ["access_key_id", "secret_access_key"]
            }
        },
        {
            "id": "api-template",
            "name": "REST API",
            "connector_type": "api",
            "category": "saas",
            "description": "Connect to REST API endpoint",
            "configuration_schema": {
                "type": "object",
                "properties": {
                    "base_url": {"type": "string", "format": "uri", "title": "Base URL"},
                    "endpoints": {"type": "array", "items": {"type": "string"}, "title": "Endpoints"},
                    "headers": {"type": "object", "title": "Additional Headers"}
                },
                "required": ["base_url"]
            },
            "credential_schema": {
                "type": "object",
                "properties": {
                    "auth_type": {"type": "string", "enum": ["none", "bearer", "api_key"], "default": "none"},
                    "token": {"type": "string", "title": "Bearer Token"},
                    "api_key": {"type": "string", "title": "API Key"},
                    "key_header": {"type": "string", "title": "API Key Header Name"}
                }
            }
        }
    ]

    for template_data in templates:
        existing = db.query(ConnectorTemplate).filter(ConnectorTemplate.id == template_data["id"]).first()
        if not existing:
            template = ConnectorTemplate(
                id=template_data["id"],
                name=template_data["name"],
                connector_type=template_data["connector_type"],
                category=template_data["category"],
                description=template_data["description"],
                configuration_schema=json.dumps(template_data["configuration_schema"]),
                credential_schema=json.dumps(template_data["credential_schema"]),
                properties=json.dumps({})
            )
            db.add(template)

    db.commit()
    db.close()


# FastAPI app
app = FastAPI(
    title="Multi-Tenant Connector Service",
    description="Data source connector management service",
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
    return {"status": "healthy", "service": "connector-service"}


@app.get("/api/v1/connector-templates", response_model=List[ConnectorTemplateResponse])
async def list_connector_templates(db: Session = Depends(get_db)):
    """List all available connector templates"""
    templates = db.query(ConnectorTemplate).filter(ConnectorTemplate.is_active == True).all()

    return [
        ConnectorTemplateResponse(
            id=t.id,
            name=t.name,
            connector_type=t.connector_type,
            category=t.category,
            description=t.description,
            configuration_schema=json.loads(t.configuration_schema),
            credential_schema=json.loads(t.credential_schema),
            properties=json.loads(t.properties),
            is_active=t.is_active,
            created_at=t.created_at
        )
        for t in templates
    ]


@app.get("/api/v1/connector-templates/{template_id}", response_model=ConnectorTemplateResponse)
async def get_connector_template(template_id: str, db: Session = Depends(get_db)):
    """Get a specific connector template"""
    template = db.query(ConnectorTemplate).filter(ConnectorTemplate.id == template_id).first()

    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    return ConnectorTemplateResponse(
        id=template.id,
        name=template.name,
        connector_type=template.connector_type,
        category=template.category,
        description=template.description,
        configuration_schema=json.loads(template.configuration_schema),
        credential_schema=json.loads(template.credential_schema),
        properties=json.loads(template.properties),
        is_active=template.is_active,
        created_at=template.created_at
    )


@app.post("/api/v1/connectors", response_model=ConnectorResponse)
async def create_connector(
        connector: ConnectorCreate,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Create a new connector"""
    import uuid

    connector_id = str(uuid.uuid4())

    # Encrypt sensitive data
    encrypted_config = encryption_manager.encrypt(json.dumps(connector.configuration))
    encrypted_credentials = encryption_manager.encrypt(json.dumps(connector.credentials))

    db_connector = Connector(
        id=connector_id,
        name=connector.name,
        connector_type=connector.connector_type,
        organization_id=organization_id,
        configuration=encrypted_config,
        credentials=encrypted_credentials,
        properties=json.dumps(connector.properties)
    )

    db.add(db_connector)
    db.commit()
    db.refresh(db_connector)

    return ConnectorResponse(
        id=db_connector.id,
        name=db_connector.name,
        connector_type=db_connector.connector_type,
        organization_id=db_connector.organization_id,
        configuration=connector.configuration,  # Return unencrypted for response
        properties=json.loads(db_connector.properties),
        is_active=db_connector.is_active,
        last_tested=db_connector.last_tested,
        test_status=db_connector.test_status,
        created_at=db_connector.created_at,
        updated_at=db_connector.updated_at
    )


@app.get("/api/v1/connectors", response_model=List[ConnectorResponse])
async def list_connectors(
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """List all connectors for the organization"""
    connectors = db.query(Connector).filter(Connector.organization_id == organization_id).all()

    response_list = []
    for conn in connectors:
        # Decrypt configuration for response (remove sensitive credentials)
        config = json.loads(encryption_manager.decrypt(conn.configuration))

        response_list.append(ConnectorResponse(
            id=conn.id,
            name=conn.name,
            connector_type=conn.connector_type,
            organization_id=conn.organization_id,
            configuration=config,
            properties=json.loads(conn.properties),
            is_active=conn.is_active,
            last_tested=conn.last_tested,
            test_status=conn.test_status,
            created_at=conn.created_at,
            updated_at=conn.updated_at
        ))

    return response_list


@app.post("/api/v1/connectors/test", response_model=ConnectionTestResponse)
async def test_connection(
        test_request: ConnectionTestRequest,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Test a connector connection"""
    try:
        if test_request.connector_id:
            # Test existing connector
            connector = db.query(Connector).filter(
                Connector.id == test_request.connector_id,
                Connector.organization_id == organization_id
            ).first()

            if not connector:
                raise HTTPException(status_code=404, detail="Connector not found")

            config = json.loads(encryption_manager.decrypt(connector.configuration))
            credentials = json.loads(encryption_manager.decrypt(connector.credentials))
            connector_type = connector.connector_type
        else:
            # Test new connector configuration
            if not test_request.configuration or not test_request.credentials:
                raise HTTPException(status_code=400,
                                    detail="Configuration and credentials required for new connector test")

            config = test_request.configuration
            credentials = test_request.credentials
            connector_type = config.get("connector_type")

        if not connector_type:
            raise HTTPException(status_code=400, detail="Connector type not specified")

        # Create connector instance and test
        conn_instance = ConnectorFactory.create_connector(connector_type, config, credentials)
        success = await conn_instance.test_connection()

        # Update test status if testing existing connector
        if test_request.connector_id:
            connector.last_tested = datetime.utcnow()
            connector.test_status = "success" if success else "failed"
            db.commit()

        return ConnectionTestResponse(
            success=success,
            message="Connection successful" if success else "Connection failed",
            details={"connector_type": connector_type}
        )

    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return ConnectionTestResponse(
            success=False,
            message=f"Connection test failed: {str(e)}",
            details={"error": str(e)}
        )


@app.get("/api/v1/connectors/{connector_id}/schema")
async def get_connector_schema(
        connector_id: str,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Get schema information from a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.organization_id == organization_id
    ).first()

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        config = json.loads(encryption_manager.decrypt(connector.configuration))
        credentials = json.loads(encryption_manager.decrypt(connector.credentials))

        conn_instance = ConnectorFactory.create_connector(connector.connector_type, config, credentials)
        schema = await conn_instance.get_schema()

        return {"schema": schema}

    except Exception as e:
        logger.error(f"Failed to get schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")


@app.get("/api/v1/connectors/{connector_id}/sample-data")
async def get_connector_sample_data(
        connector_id: str,
        limit: int = 10,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Get sample data from a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.organization_id == organization_id
    ).first()

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    try:
        config = json.loads(encryption_manager.decrypt(connector.configuration))
        credentials = json.loads(encryption_manager.decrypt(connector.credentials))

        conn_instance = ConnectorFactory.create_connector(connector.connector_type, config, credentials)
        sample_data = await conn_instance.get_sample_data(limit)

        return {"sample_data": sample_data}

    except Exception as e:
        logger.error(f"Failed to get sample data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get sample data: {str(e)}")


@app.put("/api/v1/connectors/{connector_id}", response_model=ConnectorResponse)
async def update_connector(
        connector_id: str,
        connector_update: ConnectorUpdate,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Update a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.organization_id == organization_id
    ).first()

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    # Update fields
    if connector_update.name is not None:
        connector.name = connector_update.name

    if connector_update.configuration is not None:
        connector.configuration = encryption_manager.encrypt(json.dumps(connector_update.configuration))

    if connector_update.credentials is not None:
        connector.credentials = encryption_manager.encrypt(json.dumps(connector_update.credentials))

    if connector_update.properties is not None:
        connector.properties = json.dumps(connector_update.properties)

    if connector_update.is_active is not None:
        connector.is_active = connector_update.is_active

    connector.updated_at = datetime.utcnow()

    db.commit()
    db.refresh(connector)

    # Return response with decrypted config
    config = json.loads(encryption_manager.decrypt(connector.configuration))

    return ConnectorResponse(
        id=connector.id,
        name=connector.name,
        connector_type=connector.connector_type,
        organization_id=connector.organization_id,
        configuration=config,
        properties=json.loads(connector.properties),
        is_active=connector.is_active,
        last_tested=connector.last_tested,
        test_status=connector.test_status,
        created_at=connector.created_at,
        updated_at=connector.updated_at
    )


@app.delete("/api/v1/connectors/{connector_id}")
async def delete_connector(
        connector_id: str,
        organization_id: str = Depends(get_current_organization),
        db: Session = Depends(get_db)
):
    """Delete a connector"""
    connector = db.query(Connector).filter(
        Connector.id == connector_id,
        Connector.organization_id == organization_id
    ).first()

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    db.delete(connector)
    db.commit()

    return {"message": "Connector deleted successfully"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info"
    )("region", "us-east-1")
    )

    bucket_name = self.config["bucket_name"]
    s3_client.head_bucket(Bucket=bucket_name)
    return True

except Exception as e:
logger.error(f"S3 connection test failed: {str(e)}")
return False


async def get_schema(self) -> Dict:
    """Get S3 bucket structure"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.credentials["access_key_id"],
            aws_secret_access_key=self.credentials["secret_access_key"],
            region_name=self.config.get