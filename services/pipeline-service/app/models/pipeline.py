"""Pipeline model definitions."""

from enum import Enum
from typing import Dict, Any, Optional, List

from sqlalchemy import Column, String, Text, JSON, Enum as SQLEnum, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID
from pydantic import BaseModel, Field

from app.models.base import BaseModel as DBBaseModel


class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class PipelineType(str, Enum):
    """Pipeline type."""
    BATCH = "batch"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"


class Pipeline(DBBaseModel):
    """Pipeline database model."""
    
    __tablename__ = "pipelines"
    
    name = Column(String(255), nullable=False)
    description = Column(Text)
    pipeline_type = Column(SQLEnum(PipelineType), nullable=False, default=PipelineType.BATCH)
    status = Column(SQLEnum(PipelineStatus), nullable=False, default=PipelineStatus.DRAFT)
    
    # Configuration
    source_config = Column(JSON, nullable=False)
    target_config = Column(JSON, nullable=False)
    transformation_config = Column(JSON)
    schedule_config = Column(JSON)
    
    # Databricks specific
    databricks_workspace_id = Column(String(100))
    databricks_job_id = Column(String(100))
    databricks_cluster_id = Column(String(100))
    
    # Resource limits
    max_concurrent_runs = Column(Integer, default=1)
    timeout_seconds = Column(Integer, default=3600)
    
    # Monitoring
    enable_monitoring = Column(Boolean, default=True)
    alert_on_failure = Column(Boolean, default=True)
    
    # Cost tracking
    estimated_cost_per_run = Column(Integer, default=0)  # in cents
    
    def __repr__(self):
        return f"<Pipeline(id={self.id}, name={self.name}, org={self.organization_id})>"


# Pydantic models for API
class PipelineBase(BaseModel):
    """Base pipeline schema."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    pipeline_type: PipelineType = PipelineType.BATCH
    status: PipelineStatus = PipelineStatus.DRAFT


class PipelineSourceConfig(BaseModel):
    """Pipeline source configuration."""
    type: str = Field(..., description="Source connector type")
    connection_id: Optional[str] = Field(None, description="Connection ID for reusable connections")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Source-specific parameters")
    
    class Config:
        schema_extra = {
            "example": {
                "type": "mysql",
                "connection_id": "finance-mysql-prod",
                "parameters": {
                    "table": "transactions",
                    "incremental_column": "updated_at",
                    "batch_size": 10000
                }
            }
        }


class PipelineTargetConfig(BaseModel):
    """Pipeline target configuration."""
    type: str = Field(..., description="Target type (iceberg, delta, etc.)")
    catalog: str = Field(..., description="Unity Catalog name")
    schema: str = Field(..., description="Schema name")
    table: str = Field(..., description="Table name")
    mode: str = Field(default="overwrite", description="Write mode")
    partition_columns: Optional[List[str]] = Field(None, description="Partition columns")
    
    class Config:
        schema_extra = {
            "example": {
                "type": "iceberg",
                "catalog": "finance_catalog",
                "schema": "analytics",
                "table": "daily_transactions",
                "mode": "append",
                "partition_columns": ["date"]
            }
        }


class PipelineTransformationConfig(BaseModel):
    """Pipeline transformation configuration."""
    transformations: List[Dict[str, Any]] = Field(default_factory=list)
    custom_sql: Optional[str] = Field(None, description="Custom SQL transformation")
    
    class Config:
        schema_extra = {
            "example": {
                "transformations": [
                    {
                        "type": "filter",
                        "condition": "amount > 0"
                    },
                    {
                        "type": "aggregate",
                        "group_by": ["customer_id", "date"],
                        "aggregations": {
                            "total_amount": "sum(amount)",
                            "transaction_count": "count(*)"
                        }
                    }
                ]
            }
        }


class PipelineScheduleConfig(BaseModel):
    """Pipeline schedule configuration."""
    enabled: bool = False
    cron_expression: Optional[str] = None
    timezone: str = "UTC"
    max_concurrent_runs: int = 1
    
    class Config:
        schema_extra = {
            "example": {
                "enabled": True,
                "cron_expression": "0 2 * * *",  # Daily at 2 AM
                "timezone": "UTC",
                "max_concurrent_runs": 1
            }
        }


class PipelineCreate(PipelineBase):
    """Create pipeline schema."""
    source_config: PipelineSourceConfig
    target_config: PipelineTargetConfig
    transformation_config: Optional[PipelineTransformationConfig] = None
    schedule_config: Optional[PipelineScheduleConfig] = None


class PipelineUpdate(BaseModel):
    """Update pipeline schema."""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[PipelineStatus] = None
    source_config: Optional[PipelineSourceConfig] = None
    target_config: Optional[PipelineTargetConfig] = None
    transformation_config: Optional[PipelineTransformationConfig] = None
    schedule_config: Optional[PipelineScheduleConfig] = None


class PipelineResponse(PipelineBase):
    """Pipeline response schema."""
    id: UUID
    organization_id: str
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    transformation_config: Optional[Dict[str, Any]] = None
    schedule_config: Optional[Dict[str, Any]] = None
    databricks_workspace_id: Optional[str] = None
    databricks_job_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class PipelineRun(DBBaseModel):
    """Pipeline run database model."""
    
    __tablename__ = "pipeline_runs"
    
    pipeline_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    run_id = Column(String(100), nullable=False, unique=True)
    status = Column(String(50), nullable=False, default="running")
    
    # Databricks execution details
    databricks_run_id = Column(String(100))
    databricks_job_id = Column(String(100))
    
    # Execution metadata
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer)
    
    # Results
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_message = Column(Text)
    
    # Cost tracking
    dbu_consumed = Column(Integer, default=0)
    estimated_cost_cents = Column(Integer, default=0)
    
    def __repr__(self):
        return f"<PipelineRun(id={self.id}, pipeline_id={self.pipeline_id}, status={self.status})>"
