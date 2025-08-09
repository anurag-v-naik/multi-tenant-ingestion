from sqlalchemy import Column, String, Text, JSON, Boolean, Float, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
from .base import TenantAwareModel


class RuleType(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    CUSTOM = "custom"


class RuleSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class QualityStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    WARNING = "warning"
    ERROR = "error"


class DataQualityRule(TenantAwareModel):
    __tablename__ = "data_quality_rules"

    name = Column(String(200), nullable=False)
    description = Column(Text)
    rule_type = Column(String(50), nullable=False)
    severity = Column(String(20), default=RuleSeverity.MEDIUM)

    # Rule definition
    rule_config = Column(JSON, nullable=False)
    sql_expression = Column(Text)
    python_code = Column(Text)

    # Thresholds
    threshold_config = Column(JSON, default=dict)
    warning_threshold = Column(Float)
    error_threshold = Column(Float)

    # Scope
    table_pattern = Column(String(500))
    column_pattern = Column(String(500))
    data_source_types = Column(JSON, default=list)

    # Execution
    is_enabled = Column(Boolean, default=True)
    execution_frequency = Column(String(100))  # cron expression

    # Relationships
    results = relationship("DataQualityResult", back_populates="rule")


class DataQualityResult(TenantAwareModel):
    __tablename__ = "data_quality_results"

    rule_id = Column(UUID(as_uuid=True), ForeignKey("data_quality_rules.id"), nullable=False)
    pipeline_execution_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_executions.id"))

    # Execution context
    table_name = Column(String(500))
    column_name = Column(String(200))
    data_source = Column(String(500))

    # Results
    status = Column(String(20), nullable=False)
    score = Column(Float)  # 0.0 to 1.0
    total_records = Column(Integer)
    failed_records = Column(Integer)
    passed_records = Column(Integer)

    # Details
    execution_time_ms = Column(Integer)
    error_message = Column(Text)
    sample_failures = Column(JSON, default=list)

    # Thresholds applied
    warning_threshold = Column(Float)
    error_threshold = Column(Float)

    # Relationships
    rule = relationship("DataQualityRule", back_populates="results")


class DataQualityReport(TenantAwareModel):
    __tablename__ = "data_quality_reports"

    name = Column(String(200), nullable=False)
    report_type = Column(String(50))  # daily, weekly, monthly, adhoc

    # Scope
    pipeline_ids = Column(JSON, default=list)
    table_patterns = Column(JSON, default=list)
    date_range_start = Column(DateTime)
    date_range_end = Column(DateTime)

    # Results summary
    total_rules_executed = Column(Integer, default=0)
    rules_passed = Column(Integer, default=0)
    rules_failed = Column(Integer, default=0)
    rules_warning = Column(Integer, default=0)

    overall_score = Column(Float)  # 0.0 to 1.0

    # Report data
    report_data = Column(JSON, default=dict)
    generated_at = Column(DateTime, default=datetime.utcnow)

    # Export
    export_formats = Column(JSON, default=list)  # pdf, excel, csv
    export_paths = Column(JSON, default=dict)