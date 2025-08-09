from sqlalchemy import Column, String, Text, JSON, Boolean, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from enum import Enum
from .base import TenantAwareModel


class ConnectionType(str, Enum):
    # RDBMS
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQL_SERVER = "sql_server"

    # Cloud Storage
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"

    # Streaming
    KAFKA = "kafka"
    KINESIS = "kinesis"
    PUBSUB = "pubsub"

    # NoSQL
    MONGODB = "mongodb"
    DYNAMODB = "dynamodb"
    COSMOSDB = "cosmosdb"

    # Data Warehouses
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"

    # File Systems
    SHAREPOINT = "sharepoint"
    SFTP = "sftp"
    FTP = "ftp"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    TESTING = "testing"
    FAILED = "failed"
    DISABLED = "disabled"


class DataConnection(TenantAwareModel):
    __tablename__ = "data_connections"

    name = Column(String(200), nullable=False)
    description = Column(Text)
    connection_type = Column(String(50), nullable=False)
    status = Column(String(20), default=ConnectionStatus.TESTING)

    # Connection configuration (encrypted)
    config = Column(JSON, nullable=False)
    credentials = Column(JSON, nullable=False)  # Encrypted credentials

    # Validation
    last_tested_at = Column(DateTime)
    test_result = Column(JSON)
    is_valid = Column(Boolean, default=False)

    # Usage tracking
    usage_count = Column(Integer, default=0)
    last_used_at = Column(DateTime)

    # Relationships
    tenant = relationship("Tenant", back_populates="connections")


class ConnectionConfig(TenantAwareModel):
    __tablename__ = "connection_configs"

    connection_type = Column(String(50), nullable=False)
    config_schema = Column(JSON, nullable=False)
    credential_schema = Column(JSON, nullable=False)
    validation_rules = Column(JSON, default=list)

    # Documentation
    description = Column(Text)
    example_config = Column(JSON)
    required_permissions = Column(JSON, default=list)
