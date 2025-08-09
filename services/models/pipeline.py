from sqlalchemy import Column, String, Text, Integer, JSON, Boolean, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
from .base import TenantAwareModel


class PipelineStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class PipelineType(str, Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"


class ExecutionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class Pipeline(TenantAwareModel):
    __tablename__ = "pipelines"

    name = Column(String(200), nullable=False)
    description = Column(Text)
    pipeline_type = Column(String(20), default=PipelineType.BATCH)
    status = Column(String(20), default=PipelineStatus.DRAFT)

    # Configuration
    source_connection_id = Column(UUID(as_uuid=True), ForeignKey("data_connections.id"))
    target_connection_id = Column(UUID(as_uuid=True), ForeignKey("data_connections.id"))
    transformation_config = Column(JSON, default=dict)
    schedule_config = Column(JSON, default=dict)

    # Resources
    databricks_cluster_config = Column(JSON, default=dict)
    resource_requirements = Column(JSON, default=dict)

    # Relationships
    tenant = relationship("Tenant", back_populates="pipelines")
    versions = relationship("PipelineVersion", back_populates="pipeline")
    executions = relationship("PipelineExecution", back_populates="pipeline")
    schedules = relationship("PipelineSchedule", back_populates="pipeline")


class PipelineVersion(TenantAwareModel):
    __tablename__ = "pipeline_versions"

    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id"), nullable=False)
    version = Column(String(20), nullable=False)
    description = Column(Text)
    configuration = Column(JSON, nullable=False)
    notebook_content = Column(Text)
    is_current = Column(Boolean, default=False)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="versions")


class PipelineExecution(TenantAwareModel):
    __tablename__ = "pipeline_executions"

    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id"), nullable=False)
    version_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_versions.id"))
    status = Column(String(20), default=ExecutionStatus.PENDING)

    # Execution details
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    databricks_run_id = Column(String(100))
    databricks_job_id = Column(String(100))

    # Metrics
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    bytes_processed = Column(Integer, default=0)
    execution_time_seconds = Column(Integer, default=0)

    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    retry_count = Column(Integer, default=0)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="executions")


class PipelineSchedule(TenantAwareModel):
    __tablename__ = "pipeline_schedules"

    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id"), nullable=False)
    cron_expression = Column(String(100), nullable=False)
    timezone = Column(String(50), default="UTC")
    is_enabled = Column(Boolean, default=True)
    next_run_time = Column(DateTime)

    # Relationships
    pipeline = relationship("Pipeline", back_populates="schedules")
