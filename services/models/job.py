from sqlalchemy import Column, String, Text, Integer, JSON, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
from .base import TenantAwareModel


class JobType(str, Enum):
    PIPELINE_EXECUTION = "pipeline_execution"
    DATA_QUALITY_CHECK = "data_quality_check"
    CATALOG_SYNC = "catalog_sync"
    BACKUP = "backup"
    MAINTENANCE = "maintenance"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRYING = "retrying"


class JobPriority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class Job(TenantAwareModel):
    __tablename__ = "jobs"

    name = Column(String(200), nullable=False)
    job_type = Column(String(50), nullable=False)
    priority = Column(String(20), default=JobPriority.NORMAL)
    status = Column(String(20), default=JobStatus.PENDING)

    # Configuration
    config = Column(JSON, nullable=False)
    parameters = Column(JSON, default=dict)

    # Scheduling
    scheduled_at = Column(DateTime)
    max_retry_count = Column(Integer, default=3)
    timeout_seconds = Column(Integer, default=3600)

    # Dependencies
    depends_on = Column(JSON, default=list)  # Job IDs this job depends on

    # Relationships
    executions = relationship("JobExecution", back_populates="job")


class JobExecution(TenantAwareModel):
    __tablename__ = "job_executions"

    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    status = Column(String(20), default=JobStatus.PENDING)

    # Execution tracking
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    retry_count = Column(Integer, default=0)

    # External system tracking
    external_job_id = Column(String(200))  # Databricks job ID, etc.
    external_run_id = Column(String(200))  # Databricks run ID, etc.

    # Results
    result = Column(JSON)
    error_message = Column(Text)
    error_details = Column(JSON)

    # Relationships
    job = relationship("Job", back_populates="executions")
    logs = relationship("JobLog", back_populates="execution")
    metrics = relationship("JobMetrics", back_populates="execution")


class JobLog(TenantAwareModel):
    __tablename__ = "job_logs"

    execution_id = Column(UUID(as_uuid=True), ForeignKey("job_executions.id"), nullable=False)
    level = Column(String(20), nullable=False)  # DEBUG, INFO, WARNING, ERROR
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    source = Column(String(100))  # Component that generated the log

    # Relationships
    execution = relationship("JobExecution", back_populates="logs")


class JobMetrics(TenantAwareModel):
    __tablename__ = "job_metrics"

    execution_id = Column(UUID(as_uuid=True), ForeignKey("job_executions.id"), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(String(500), nullable=False)
    metric_type = Column(String(50))  # counter, gauge, histogram, etc.
    timestamp = Column(DateTime, default=datetime.utcnow)

    # Additional context
    tags = Column(JSON, default=dict)

    # Relationships
    execution = relationship("JobExecution", back_populates="metrics")
