return QualityReport(
    organization_id=organization_id,
    dataset_name=request.dataset_name,
    batch_id=request.batch_id,
    overall_score=overall_score,
    total_rules=total_rules,
    passed_rules=passed_count,
    failed_rules=failed_count,
    warning_rules=warning_count,
    check_results=check_results
)


async def _load_dataset(self, organization_id: str, dataset_name: str,
                        data_sample: Optional[Dict] = None) -> pd.DataFrame:
    """Load dataset from various sources."""

    if data_sample:
        # For real-time validation, use provided sample
        return pd.DataFrame([data_sample])

    try:
        # Try to load from S3 (assuming CSV format)
        bucket_name = f"multi-tenant-ingestion-{organization_id}-data"
        key = f"datasets/{dataset_name}.csv"

        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_csv(response['Body'])

    except Exception as e:
        logger.warning("Failed to load dataset from S3",
                       dataset_name=dataset_name,
                       error=str(e))

        # Could try other sources here (Databricks, local files, etc.)
        return None


async def _execute_rule(self, rule: DataQualityRule, dataset: pd.DataFrame,
                        batch_id: Optional[str]) -> Dict[str, Any]:
    """Execute a single data quality rule."""

    start_time = datetime.utcnow()

    try:
        validator = self.validators.get(rule.rule_type)
        if not validator:
            raise ValueError(f"No validator found for rule type: {rule.rule_type}")

        result = await validator(rule, dataset)

        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        return {
            "rule_id": str(rule.id),
            "rule_name": rule.name,
            "rule_type": rule.rule_type,
            "status": result["status"],
            "pass_rate": result.get("pass_rate", 0.0),
            "failed_records": result.get("failed_records", 0),
            "total_records": len(dataset),
            "details": result.get("details", {}),
            "execution_time_ms": execution_time,
            "severity": rule.severity
        }

    except Exception as e:
        logger.error("Rule execution error",
                     rule_id=str(rule.id),
                     error=str(e))
        raise


# Validator implementations
async def _validate_completeness(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data completeness (non-null values)."""

    rule_def = rule.rule_definition
    column = rule.target_column
    threshold = rule_def.get("threshold", 0.95)  # Default 95% completeness

    if column and column in dataset.columns:
        total_records = len(dataset)
        non_null_records = dataset[column].notna().sum()
        completeness_rate = non_null_records / total_records if total_records > 0 else 0
        failed_records = total_records - non_null_records

        status = QualityCheckStatus.PASSED if completeness_rate >= threshold else QualityCheckStatus.FAILED

        return {
            "status": status,
            "pass_rate": completeness_rate,
            "failed_records": failed_records,
            "details": {
                "column": column,
                "completeness_rate": completeness_rate,
                "threshold": threshold,
                "non_null_records": non_null_records,
                "total_records": total_records
            }
        }
    else:
        # Check overall dataset completeness
        total_cells = dataset.size
        non_null_cells = dataset.notna().sum().sum()
        completeness_rate = non_null_cells / total_cells if total_cells > 0 else 0

        status = QualityCheckStatus.PASSED if completeness_rate >= threshold else QualityCheckStatus.FAILED

        return {
            "status": status,
            "pass_rate": completeness_rate,
            "failed_records": total_cells - non_null_cells,
            "details": {
                "overall_completeness": completeness_rate,
                "threshold": threshold,
                "total_cells": total_cells,
                "non_null_cells": non_null_cells
            }
        }


async def _validate_uniqueness(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data uniqueness."""

    rule_def = rule.rule_definition
    columns = rule_def.get("columns", [rule.target_column] if rule.target_column else [])

    if not columns:
        raise ValueError("Uniqueness rule requires column specification")

    # Filter to existing columns
    existing_columns = [col for col in columns if col in dataset.columns]

    if not existing_columns:
        raise ValueError(f"None of specified columns {columns} exist in dataset")

    total_records = len(dataset)

    if len(existing_columns) == 1:
        unique_records = dataset[existing_columns[0]].nunique()
        duplicates = total_records - unique_records
    else:
        unique_records = len(dataset.drop_duplicates(subset=existing_columns))
        duplicates = total_records - unique_records

    uniqueness_rate = unique_records / total_records if total_records > 0 else 1.0
    threshold = rule_def.get("threshold", 1.0)  # Default 100% uniqueness

    status = QualityCheckStatus.PASSED if uniqueness_rate >= threshold else QualityCheckStatus.FAILED

    return {
        "status": status,
        "pass_rate": uniqueness_rate,
        "failed_records": duplicates,
        "details": {
            "columns": existing_columns,
            "uniqueness_rate": uniqueness_rate,
            "threshold": threshold,
            "unique_records": unique_records,
            "duplicate_records": duplicates
        }
    }


async def _validate_validity(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data against format/pattern rules."""

    rule_def = rule.rule_definition
    column = rule.target_column
    pattern = rule_def.get("pattern")
    data_type = rule_def.get("data_type")
    value_range = rule_def.get("range")
    allowed_values = rule_def.get("allowed_values")

    if not column or column not in dataset.columns:
        raise ValueError(f"Column '{column}' not found in dataset")

    total_records = len(dataset)
    valid_records = 0
    validation_errors = []

    for idx, value in dataset[column].items():
        is_valid = True
        error_reasons = []

        # Skip null values (handled by completeness check)
        if pd.isna(value):
            valid_records += 1
            continue

        # Pattern validation
        if pattern and isinstance(value, str):
            if not re.match(pattern, value):
                is_valid = False
                error_reasons.append(f"Pattern mismatch: {pattern}")

        # Data type validation
        if data_type:
            try:
                if data_type == "int":
                    int(value)
                elif data_type == "float":
                    float(value)
                elif data_type == "date":
                    pd.to_datetime(value)
                elif data_type == "email":
                    if not re.match(r'^[^@]+@[^@]+\.[^@]+# services/data-quality-service/app/main.py
                                    """
                                    Comprehensive data quality service with real-time validation,
                                    rule engine, lineage tracking, and automated remediation.
                                    """

                    import asyncio
                    import json
                    import logging
                    from datetime import datetime, timedelta
                    from typing import Dict, List, Optional, Any, Union, Callable
                    from enum import Enum
                    import uuid
                    from dataclasses import dataclass
                    import re
                    import pandas as pd
                    import numpy as np
                    from collections import defaultdict

                    from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
                    from fastapi.responses import JSONResponse
                    from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Boolean, JSON, Text, ForeignKey
                    from sqlalchemy.ext.declarative import declarative_base
                    from sqlalchemy.orm import sessionmaker, Session, relationship
                    from sqlalchemy.dialects.postgresql import UUID
                    from pydantic import BaseModel, Field, validator
                    import redis
                    import structlog
                    import boto3
                    from great_expectations import DataContext
                    from great_expectations.core.batch import BatchRequest
                    from great_expectations.checkpoint import SimpleCheckpoint

                    logger = structlog.get_logger()

                    # Database setup
                    Base = declarative_base()

                    # Enums


class DataQualityRuleType(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    CONFORMITY = "conformity"
    INTEGRITY = "integrity"


class QualityCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"
    ERROR = "error"


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RemediationAction(str, Enum):
    ALERT_ONLY = "alert_only"
    QUARANTINE = "quarantine"
    AUTO_FIX = "auto_fix"
    REJECT = "reject"
    MANUAL_REVIEW = "manual_review"


# Database Models
class DataQualityRule(Base):
    __tablename__ = "data_quality_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    rule_type = Column(String(50), nullable=False)
    target_table = Column(String(200), nullable=True)
    target_column = Column(String(200), nullable=True)
    rule_definition = Column(JSON, nullable=False)
    severity = Column(String(20), default=SeverityLevel.MEDIUM)
    remediation_action = Column(String(50), default=RemediationAction.ALERT_ONLY)
    is_active = Column(Boolean, default=True)
    created_by = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    check_results = relationship("QualityCheckResult", back_populates="rule")


class QualityCheckResult(Base):
    __tablename__ = "quality_check_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("data_quality_rules.id"), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    batch_id = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False)
    pass_rate = Column(Float, nullable=True)
    failed_records = Column(Integer, default=0)
    total_records = Column(Integer, default=0)
    details = Column(JSON, default=dict)
    execution_time_ms = Column(Float, nullable=True)
    executed_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    rule = relationship("DataQualityRule", back_populates="check_results")


class DataLineage(Base):
    __tablename__ = "data_lineage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    source_dataset = Column(String(200), nullable=False)
    target_dataset = Column(String(200), nullable=False)
    transformation_type = Column(String(100), nullable=False)
    pipeline_id = Column(String(100), nullable=True)
    metadata = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


class QualityProfile(Base):
    __tablename__ = "quality_profiles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    column_name = Column(String(200), nullable=False)
    profile_data = Column(JSON, nullable=False)
    profiled_at = Column(DateTime, default=datetime.utcnow)


class DataQualityIncident(Base):
    __tablename__ = "quality_incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    incident_type = Column(String(100), nullable=False)
    severity = Column(String(20), nullable=False)
    dataset_name = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    affected_records = Column(Integer, default=0)
    remediation_status = Column(String(50), default="open")
    remediation_details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)


# Pydantic Models
class RuleDefinition(BaseModel):
    condition: str  # SQL-like condition or Python expression
    parameters: Dict[str, Any] = Field(default_factory=dict)
    threshold: Optional[float] = None
    custom_sql: Optional[str] = None


class DataQualityRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    rule_type: DataQualityRuleType
    target_table: Optional[str] = None
    target_column: Optional[str] = None
    rule_definition: RuleDefinition
    severity: SeverityLevel = SeverityLevel.MEDIUM
    remediation_action: RemediationAction = RemediationAction.ALERT_ONLY


class QualityCheckRequest(BaseModel):
    dataset_name: str
    batch_id: Optional[str] = None
    rule_ids: Optional[List[str]] = None  # If None, run all active rules
    data_sample: Optional[Dict[str, Any]] = None  # For real-time checks


class QualityReport(BaseModel):
    organization_id: str
    dataset_name: str
    batch_id: Optional[str]
    overall_score: float
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    check_results: List[Dict[str, Any]]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# Data Quality Engine
class DataQualityEngine:
    """Core engine for data quality validation and monitoring."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client
        self.s3_client = boto3.client('s3')

        # Initialize rule validators
        self.validators = {
            DataQualityRuleType.COMPLETENESS: self._validate_completeness,
            DataQualityRuleType.UNIQUENESS: self._validate_uniqueness,
            DataQualityRuleType.VALIDITY: self._validate_validity,
            DataQualityRuleType.CONSISTENCY: self._validate_consistency,
            DataQualityRuleType.ACCURACY: self._validate_accuracy,
            DataQualityRuleType.TIMELINESS: self._validate_timeliness,
            DataQualityRuleType.CONFORMITY: self._validate_conformity,
            DataQualityRuleType.INTEGRITY: self._validate_integrity
        }

    async def execute_quality_checks(self, organization_id: str,
                                     request: QualityCheckRequest) -> QualityReport:
        """Execute data quality checks for a dataset."""

        start_time = datetime.utcnow()

        # Get applicable rules
        rules_query = self.db.query(DataQualityRule).filter(
            DataQualityRule.organization_id == organization_id,
            DataQualityRule.is_active == True
        )

        if request.target_table:
            rules_query = rules_query.filter(
                (DataQualityRule.target_table == request.dataset_name) |
                (DataQualityRule.target_table.is_(None))
            )

        if request.rule_ids:
            rules_query = rules_query.filter(
                DataQualityRule.id.in_(request.rule_ids)
            )

        rules = rules_query.all()

        if not rules:
            logger.warning("No active rules found",
                           organization_id=organization_id,
                           dataset_name=request.dataset_name)
            return QualityReport(
                organization_id=organization_id,
                dataset_name=request.dataset_name,
                batch_id=request.batch_id,
                overall_score=1.0,
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                warning_rules=0,
                check_results=[]
            )

        # Load dataset
        dataset = await self._load_dataset(organization_id, request.dataset_name, request.data_sample)

        if dataset is None or dataset.empty:
            raise ValueError(f"Dataset '{request.dataset_name}' not found or empty")

        # Execute rules
        check_results = []
        passed_count = 0
        failed_count = 0
        warning_count = 0

        for rule in rules:
            try:
                result = await self._execute_rule(rule, dataset, request.batch_id)
                check_results.append(result)

                # Store result in database
                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=result["status"],
                    pass_rate=result.get("pass_rate"),
                    failed_records=result.get("failed_records", 0),
                    total_records=result.get("total_records", len(dataset)),
                    details=result.get("details", {}),
                    execution_time_ms=result.get("execution_time_ms")
                )

                self.db.add(db_result)

                # Count results
                if result["status"] == QualityCheckStatus.PASSED:
                    passed_count += 1
                elif result["status"] == QualityCheckStatus.FAILED:
                    failed_count += 1
                    # Handle remediation
                    await self._handle_remediation(rule, result, dataset, organization_id)
                elif result["status"] == QualityCheckStatus.WARNING:
                    warning_count += 1

            except Exception as e:
                logger.error("Rule execution failed",
                             rule_id=str(rule.id),
                             rule_name=rule.name,
                             error=str(e))

                # Record error result
                error_result = {
                    "rule_id": str(rule.id),
                    "rule_name": rule.name,
                    "status": QualityCheckStatus.ERROR,
                    "error_message": str(e)
                }
                check_results.append(error_result)

                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=QualityCheckStatus.ERROR,
                    details={"error": str(e)}
                )
                self.db.add(db_result)

        self.db.commit()

        # Calculate overall score
        total_rules = len(rules)
        if total_rules > 0:
            overall_score = passed_count / total_rules
        else:
            overall_score = 1.0

        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        logger.info("Quality checks completed",
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    total_rules=total_rules,
                    passed=passed_count,
                    failed=failed_count,
                    warnings=warning_count,
                    overall_score=overall_score,
                    execution_time_ms=execution_time)

        return QualityReport(
            organization_id=organization_id,
            dataset_name=request.dataset_name,
            batch_id=request.batch_id,
            overall_score=overall_score,
            total_rules=total_rules,
            passed_rules=passed_count,, str(value)):
        raise ValueError("Invalid email format")

    except (ValueError, TypeError):
    is_valid = False
    error_reasons.append(f"Invalid {data_type}")

    # Range validation


if value_range and isinstance(value, (int, float)):
    min_val = value_range.get("min")
    max_val = value_range.get("max")
    if min_val is not None and value < min_val:
        is_valid = False
        error_reasons.append(f"Below minimum: {min_val}")
    if max_val is not None and value > max_val:
        is_valid = False
        error_reasons.append(f"Above maximum: {max_val}")

    # Allowed values validation
if allowed_values and value not in allowed_values:
    is_valid = False
    error_reasons.append(f"Not in allowed values: {allowed_values}")

if is_valid:
    valid_records += 1
else:
    validation_errors.append({
        "row": idx,
        "value": value,
        "errors": error_reasons
    })

validity_rate = valid_records / total_records if total_records > 0 else 1.0
threshold = rule_def.get("threshold", 1.0)
failed_records = total_records - valid_records

status = QualityCheckStatus.PASSED if validity_rate >= threshold else QualityCheckStatus.FAILED

return {
    "status": status,
    "pass_rate": validity_rate,
    "failed_records": failed_records,
    "details": {
        "column": column,
        "validity_rate": validity_rate,
        "threshold": threshold,
        "validation_errors": validation_errors[:10],  # Limit to first 10 errors
        "total_errors": len(validation_errors)
    }
}


async def _validate_consistency(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data consistency across columns or rows."""

    rule_def = rule.rule_definition
    consistency_type = rule_def.get("type", "cross_column")

    if consistency_type == "cross_column":
        return await self._validate_cross_column_consistency(rule, dataset)
    elif consistency_type == "temporal":
        return await self._validate_temporal_consistency(rule, dataset)
    else:
        raise ValueError(f"Unknown consistency type: {consistency_type}")


async def _validate_cross_column_consistency(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate consistency between columns."""

    rule_def = rule.rule_definition
    condition = rule_def.get("condition")

    if not condition:
        raise ValueError("Cross-column consistency rule requires condition")

    total_records = len(dataset)
    consistent_records = 0
    inconsistencies = []

    for idx, row in dataset.iterrows():
        try:
            # Evaluate condition with row data
            is_consistent = eval(condition, {"__builtins__": {}}, dict(row))
            if is_consistent:
                consistent_records += 1
            else:
                inconsistencies.append({
                    "row": idx,
                    "condition": condition,
                    "values": dict(row)
                })
        except Exception as e:
            inconsistencies.append({
                "row": idx,
                "error": str(e),
                "condition": condition
            })

    consistency_rate = consistent_records / total_records if total_records > 0 else 1.0
    threshold = rule_def.get("threshold", 1.0)
    failed_records = total_records - consistent_records

    status = QualityCheckStatus.PASSED if consistency_rate >= threshold else QualityCheckStatus.FAILED

    return {
        "status": status,
        "pass_rate": consistency_rate,
        "failed_records": failed_records,
        "details": {
            "consistency_rate": consistency_rate,
            "threshold": threshold,
            "condition": condition,
            "inconsistencies": inconsistencies[:10],
            "total_inconsistencies": len(inconsistencies)
        }
    }


async def _validate_temporal_consistency(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate temporal consistency (e.g., start_date <= end_date)."""

    rule_def = rule.rule_definition
    date_columns = rule_def.get("date_columns", [])

    if len(date_columns) < 2:
        raise ValueError("Temporal consistency requires at least 2 date columns")

    # Convert to datetime
    for col in date_columns:
        if col in dataset.columns:
            dataset[col] = pd.to_datetime(dataset[col], errors='coerce')

    total_records = len(dataset)
    consistent_records = 0
    inconsistencies = []

    for idx, row in dataset.iterrows():
        is_consistent = True

        # Check if dates are in chronological order
        dates = [row[col] for col in date_columns if col in dataset.columns and pd.notna(row[col])]

        if len(dates) >= 2:
            for i in range(len(dates) - 1):
                if dates[i] > dates[i + 1]:
                    is_consistent = False
                    break

        if is_consistent:
            consistent_records += 1
        else:
            inconsistencies.append({
                "row": idx,
                "dates": {col: str(row[col]) for col in date_columns if col in dataset.columns}
            })

    consistency_rate = consistent_records / total_records if total_records > 0 else 1.0
    threshold = rule_def.get("threshold", 1.0)
    failed_records = total_records - consistent_records

    status = QualityCheckStatus.PASSED if consistency_rate >= threshold else QualityCheckStatus.FAILED

    return {
        "status": status,
        "pass_rate": consistency_rate,
        "failed_records": failed_records,
        "details": {
            "consistency_rate": consistency_rate,
            "threshold": threshold,
            "date_columns": date_columns,
            "inconsistencies": inconsistencies[:10]
        }
    }


async def _validate_accuracy(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data accuracy against reference data."""

    rule_def = rule.rule_definition
    reference_source = rule_def.get("reference_source")
    reference_column = rule_def.get("reference_column")
    target_column = rule.target_column

    # This is a simplified implementation
    # In practice, you'd load reference data from external sources

    if not reference_source or not target_column:
        raise ValueError("Accuracy validation requires reference source and target column")

    # Mock reference data loading
    # reference_data = await self._load_reference_data(reference_source)

    # For demonstration, assume 95% accuracy
    total_records = len(dataset)
    accurate_records = int(total_records * 0.95)

    accuracy_rate = accurate_records / total_records if total_records > 0 else 1.0
    threshold = rule_def.get("threshold", 0.9)
    failed_records = total_records - accurate_records

    status = QualityCheckStatus.PASSED if accuracy_rate >= threshold else QualityCheckStatus.FAILED

    return {
        "status": status,
        "pass_rate": accuracy_rate,
        "failed_records": failed_records,
        "details": {
            "accuracy_rate": accuracy_rate,
            "threshold": threshold,
            "reference_source": reference_source,
            "accurate_records": accurate_records
        }
    }


async def _validate_timeliness(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data timeliness."""

    rule_def = rule.rule_definition
    date_column = rule.target_column or rule_def.get("date_column")
    max_age_hours = rule_def.get("max_age_hours", 24)

    if not date_column or date_column not in dataset.columns:
        raise ValueError(f"Date column '{date_column}' not found")

    # Convert to datetime
    dataset[date_column] = pd.to_datetime(dataset[date_column], errors='coerce')

    now = pd.Timestamp.now()
    cutoff_time = now - pd.Timedelta(hours=max_age_hours)

    total_records = len(dataset)
    timely_records = (dataset[date_column] >= cutoff_time).sum()

    timeliness_rate = timely_records / total_records if total_records > 0 else 1.0
    threshold = rule_def.get("threshold", 0.9)
    failed_records = total_records - timely_records

    status = QualityCheckStatus.PASSED if timeliness_rate >= threshold else QualityCheckStatus.FAILED

    return {
        "status": status,
        "pass_rate": timeliness_rate,
        "failed_records": failed_records,
        "details": {
            "timeliness_rate": timeliness_rate,
            "threshold": threshold,
            "max_age_hours": max_age_hours,
            "cutoff_time": str(cutoff_time),
            "timely_records": timely_records
        }
    }


async def _validate_conformity(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
    """Validate data conformity to standards."""

    rule_def = rule.rule_definition
    standard = rule_def.get("standard")
    column = rule.target_column

    if not column or column not in dataset.columns:
        raise ValueError(f"Column '{column}' not found")

    total_records = len(dataset)
    conforming_records = 0

    if standard == "ISO_8601_DATE":
        # Validate ISO 8601 date format
        for value in dataset[column]:
            if pd.isna(value):
                conforming_records += 1
                continue
            try:
                pd.to_datetime(value, format='%Y-%m-%d')
                conforming_records += 1
            except:
                pass
    elif standard == "EMAIL":
        # Validate email format
        email_pattern = r'^[^@]+@[^@]+\.[^@]+# services/data-quality-service/app/main.py


"""
Comprehensive data quality service with real-time validation,
rule engine, lineage tracking, and automated remediation.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from enum import Enum
import uuid
from dataclasses import dataclass
import re
import pandas as pd
import numpy as np
from collections import defaultdict

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Boolean, JSON, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.dialects.postgresql import UUID
from pydantic import BaseModel, Field, validator
import redis
import structlog
import boto3
from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

logger = structlog.get_logger()

# Database setup
Base = declarative_base()


# Enums
class DataQualityRuleType(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    CONFORMITY = "conformity"
    INTEGRITY = "integrity"


class QualityCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"
    ERROR = "error"


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RemediationAction(str, Enum):
    ALERT_ONLY = "alert_only"
    QUARANTINE = "quarantine"
    AUTO_FIX = "auto_fix"
    REJECT = "reject"
    MANUAL_REVIEW = "manual_review"


# Database Models
class DataQualityRule(Base):
    __tablename__ = "data_quality_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    rule_type = Column(String(50), nullable=False)
    target_table = Column(String(200), nullable=True)
    target_column = Column(String(200), nullable=True)
    rule_definition = Column(JSON, nullable=False)
    severity = Column(String(20), default=SeverityLevel.MEDIUM)
    remediation_action = Column(String(50), default=RemediationAction.ALERT_ONLY)
    is_active = Column(Boolean, default=True)
    created_by = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    check_results = relationship("QualityCheckResult", back_populates="rule")


class QualityCheckResult(Base):
    __tablename__ = "quality_check_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("data_quality_rules.id"), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    batch_id = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False)
    pass_rate = Column(Float, nullable=True)
    failed_records = Column(Integer, default=0)
    total_records = Column(Integer, default=0)
    details = Column(JSON, default=dict)
    execution_time_ms = Column(Float, nullable=True)
    executed_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    rule = relationship("DataQualityRule", back_populates="check_results")


class DataLineage(Base):
    __tablename__ = "data_lineage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    source_dataset = Column(String(200), nullable=False)
    target_dataset = Column(String(200), nullable=False)
    transformation_type = Column(String(100), nullable=False)
    pipeline_id = Column(String(100), nullable=True)
    metadata = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


class QualityProfile(Base):
    __tablename__ = "quality_profiles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    column_name = Column(String(200), nullable=False)
    profile_data = Column(JSON, nullable=False)
    profiled_at = Column(DateTime, default=datetime.utcnow)


class DataQualityIncident(Base):
    __tablename__ = "quality_incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    incident_type = Column(String(100), nullable=False)
    severity = Column(String(20), nullable=False)
    dataset_name = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    affected_records = Column(Integer, default=0)
    remediation_status = Column(String(50), default="open")
    remediation_details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)


# Pydantic Models
class RuleDefinition(BaseModel):
    condition: str  # SQL-like condition or Python expression
    parameters: Dict[str, Any] = Field(default_factory=dict)
    threshold: Optional[float] = None
    custom_sql: Optional[str] = None


class DataQualityRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    rule_type: DataQualityRuleType
    target_table: Optional[str] = None
    target_column: Optional[str] = None
    rule_definition: RuleDefinition
    severity: SeverityLevel = SeverityLevel.MEDIUM
    remediation_action: RemediationAction = RemediationAction.ALERT_ONLY


class QualityCheckRequest(BaseModel):
    dataset_name: str
    batch_id: Optional[str] = None
    rule_ids: Optional[List[str]] = None  # If None, run all active rules
    data_sample: Optional[Dict[str, Any]] = None  # For real-time checks


class QualityReport(BaseModel):
    organization_id: str
    dataset_name: str
    batch_id: Optional[str]
    overall_score: float
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    check_results: List[Dict[str, Any]]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# Data Quality Engine
class DataQualityEngine:
    """Core engine for data quality validation and monitoring."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client
        self.s3_client = boto3.client('s3')

        # Initialize rule validators
        self.validators = {
            DataQualityRuleType.COMPLETENESS: self._validate_completeness,
            DataQualityRuleType.UNIQUENESS: self._validate_uniqueness,
            DataQualityRuleType.VALIDITY: self._validate_validity,
            DataQualityRuleType.CONSISTENCY: self._validate_consistency,
            DataQualityRuleType.ACCURACY: self._validate_accuracy,
            DataQualityRuleType.TIMELINESS: self._validate_timeliness,
            DataQualityRuleType.CONFORMITY: self._validate_conformity,
            DataQualityRuleType.INTEGRITY: self._validate_integrity
        }

    async def execute_quality_checks(self, organization_id: str,
                                     request: QualityCheckRequest) -> QualityReport:
        """Execute data quality checks for a dataset."""

        start_time = datetime.utcnow()

        # Get applicable rules
        rules_query = self.db.query(DataQualityRule).filter(
            DataQualityRule.organization_id == organization_id,
            DataQualityRule.is_active == True
        )

        if request.target_table:
            rules_query = rules_query.filter(
                (DataQualityRule.target_table == request.dataset_name) |
                (DataQualityRule.target_table.is_(None))
            )

        if request.rule_ids:
            rules_query = rules_query.filter(
                DataQualityRule.id.in_(request.rule_ids)
            )

        rules = rules_query.all()

        if not rules:
            logger.warning("No active rules found",
                           organization_id=organization_id,
                           dataset_name=request.dataset_name)
            return QualityReport(
                organization_id=organization_id,
                dataset_name=request.dataset_name,
                batch_id=request.batch_id,
                overall_score=1.0,
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                warning_rules=0,
                check_results=[]
            )

        # Load dataset
        dataset = await self._load_dataset(organization_id, request.dataset_name, request.data_sample)

        if dataset is None or dataset.empty:
            raise ValueError(f"Dataset '{request.dataset_name}' not found or empty")

        # Execute rules
        check_results = []
        passed_count = 0
        failed_count = 0
        warning_count = 0

        for rule in rules:
            try:
                result = await self._execute_rule(rule, dataset, request.batch_id)
                check_results.append(result)

                # Store result in database
                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=result["status"],
                    pass_rate=result.get("pass_rate"),
                    failed_records=result.get("failed_records", 0),
                    total_records=result.get("total_records", len(dataset)),
                    details=result.get("details", {}),
                    execution_time_ms=result.get("execution_time_ms")
                )

                self.db.add(db_result)

                # Count results
                if result["status"] == QualityCheckStatus.PASSED:
                    passed_count += 1
                elif result["status"] == QualityCheckStatus.FAILED:
                    failed_count += 1
                    # Handle remediation
                    await self._handle_remediation(rule, result, dataset, organization_id)
                elif result["status"] == QualityCheckStatus.WARNING:
                    warning_count += 1

            except Exception as e:
                logger.error("Rule execution failed",
                             rule_id=str(rule.id),
                             rule_name=rule.name,
                             error=str(e))

                # Record error result
                error_result = {
                    "rule_id": str(rule.id),
                    "rule_name": rule.name,
                    "status": QualityCheckStatus.ERROR,
                    "error_message": str(e)
                }
                check_results.append(error_result)

                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=QualityCheckStatus.ERROR,
                    details={"error": str(e)}
                )
                self.db.add(db_result)

        self.db.commit()

        # Calculate overall score
        total_rules = len(rules)
        if total_rules > 0:
            overall_score = passed_count / total_rules
        else:
            overall_score = 1.0

        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        logger.info("Quality checks completed",
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    total_rules=total_rules,
                    passed=passed_count,
                    failed=failed_count,
                    warnings=warning_count,
                    overall_score=overall_score,
                    execution_time_ms=execution_time)

        return QualityReport(
            organization_id=organization_id,
            dataset_name=request.dataset_name,
            batch_id=request.batch_id,
            overall_score=overall_score,
            total_rules=total_rules,
            passed_rules=passed_count,
        for value in dataset[column]:
            if
        pd.isna(value) or re.match(email_pattern, str(value)):
        conforming_records += 1
        else:
        # Custom conformity check
        conforming_records = total_records  # Placeholder

        conformity_rate = conforming_records / total_records if total_records > 0 else 1.0
        threshold = rule_def.get("threshold", 1.0)
        failed_records = total_records - conforming_records

        status = QualityCheckStatus.PASSED if conformity_rate >= threshold else QualityCheckStatus.FAILED

        return {
            "status": status,
            "pass_rate": conformity_rate,
            "failed_records": failed_records,
            "details": {
                "conformity_rate": conformity_rate,
                "threshold": threshold,
                "standard": standard,
                "conforming_records": conforming_records
            }
        }

    async def _validate_integrity(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
        """Validate referential integrity."""

        rule_def = rule.rule_definition
        foreign_key_column = rule.target_column
        reference_table = rule_def.get("reference_table")
        reference_column = rule_def.get("reference_column", "id")

        if not foreign_key_column or not reference_table:
            raise ValueError("Integrity validation requires foreign key column and reference table")

        # In practice, you'd query the reference table
        # For demonstration, assume 98% integrity
        total_records = len(dataset)
        valid_references = int(total_records * 0.98)

        integrity_rate = valid_references / total_records if total_records > 0 else 1.0
        threshold = rule_def.get("threshold", 1.0)
        failed_records = total_records - valid_references

        status = QualityCheckStatus.PASSED if integrity_rate >= threshold else QualityCheckStatus.FAILED

        return {
            "status": status,
            "pass_rate": integrity_rate,
            "failed_records": failed_records,
            "details": {
                "integrity_rate": integrity_rate,
                "threshold": threshold,
                "reference_table": reference_table,
                "reference_column": reference_column,
                "valid_references": valid_references
            }
        }

    async def _handle_remediation(self, rule: DataQualityRule, result: Dict[str, Any],
                                  dataset: pd.DataFrame, organization_id: str):
        """Handle remediation actions for failed quality checks."""

        if rule.remediation_action == RemediationAction.ALERT_ONLY:
            # Just log the failure
            logger.warning("Data quality check failed",
                           rule_name=rule.name,
                           organization_id=organization_id,
                           failed_records=result.get("failed_records", 0))

        elif rule.remediation_action == RemediationAction.QUARANTINE:
            # Move failed records to quarantine
            await self._quarantine_failed_records(rule, result, dataset, organization_id)

        elif rule.remediation_action == RemediationAction.AUTO_FIX:
            # Attempt automatic fixes
            await self._auto_fix_records(rule, result, dataset, organization_id)

        elif rule.remediation_action == RemediationAction.REJECT:
            # Reject the entire batch
            await self._reject_batch(rule, result, organization_id)

        elif rule.remediation_action == RemediationAction.MANUAL_REVIEW:
            # Create incident for manual review
            await self._create_quality_incident(rule, result, organization_id)

    async def _quarantine_failed_records(self, rule: DataQualityRule, result: Dict[str, Any],
                                         dataset: pd.DataFrame, organization_id: str):
        """Move failed records to quarantine storage."""

        # Implementation would depend on your quarantine strategy
        logger.info("Quarantining failed records",
                    rule_name=rule.name,
                    failed_records=result.get("failed_records", 0))

    async def _auto_fix_records(self, rule: DataQualityRule, result: Dict[str, Any],
                                dataset: pd.DataFrame, organization_id: str):
        """Attempt automatic fixes for failed records."""

        # Implementation would depend on the rule type and fix strategy
        logger.info("Attempting auto-fix for failed records",
                    rule_name=rule.name,
                    failed_records=result.get("failed_records", 0))

    async def _reject_batch(self, rule: DataQualityRule, result: Dict[str, Any], organization_id: str):
        """Reject the entire batch due to quality failures."""

        logger.error("Rejecting batch due to quality failures",
                     rule_name=rule.name,
                     organization_id=organization_id)

    async def _create_quality_incident(self, rule: DataQualityRule, result: Dict[str, Any], organization_id: str):
        """Create a quality incident for manual review."""

        incident = DataQualityIncident(
            organization_id=organization_id,
            incident_type=f"Quality Rule Failure: {rule.rule_type}",
            severity=rule.severity,
            dataset_name=result.get("dataset_name", "unknown"),
            description=f"Rule '{rule.name}' failed with {result.get('failed_records', 0)} failed records",
            affected_records=result.get("failed_records", 0),
            remediation_details=result.get("details", {})
        )

        self.db.add(incident)
        self.db.commit()

        logger.info("Quality incident created",
                    incident_id=str(incident.id),
                    rule_name=rule.name)


# Data Profiler
class DataProfiler:
    """Generate comprehensive data profiles for datasets."""

    def __init__(self, db_session: Session):
        self.db = db_session

    async def profile_dataset(self, organization_id: str, dataset_name: str,
                              dataset: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive profile for a dataset."""

        profile = {
            "dataset_name": dataset_name,
            "organization_id": organization_id,
            "profiled_at": datetime.utcnow().isoformat(),
            "row_count": len(dataset),
            "column_count": len(dataset.columns),
            "columns": {}
        }

        for column in dataset.columns:
            column_profile = await self._profile_column(dataset[column])
            profile["columns"][column] = column_profile

            # Store column profile in database
            quality_profile = QualityProfile(
                organization_id=organization_id,
                dataset_name=dataset_name,
                column_name=column,
                profile_data=column_profile
            )
            self.db.add(quality_profile)

        self.db.commit()
        return profile

    async def _profile_column(self, series: pd.Series) -> Dict[str, Any]:
        """Generate profile for a single column."""

        profile = {
            "data_type": str(series.dtype),
            "null_count": series.isnull().sum(),
            "null_percentage": (series.isnull().sum() / len(series)) * 100,
            "unique_count": series.nunique(),
            "unique_percentage": (series.nunique() / len(series)) * 100 if len(series) > 0 else 0
        }

        # Numeric statistics
        if pd.api.types.is_numeric_dtype(series):
            profile.update({
                "mean": series.mean(),
                "median": series.median(),
                "std": series.std(),
                "min": series.min(),
                "max": series.max(),
                "quantiles": {
                    "25%": series.quantile(0.25),
                    "75%": series.quantile(0.75)
                }
            })

        # String statistics
        elif pd.api.types.is_string_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                profile.update({
                    "min_length": non_null_series.str.len().min(),
                    "max_length": non_null_series.str.len().max(),
                    "avg_length": non_null_series.str.len().mean(),
                    "most_common": non_null_series.value_counts().head(5).to_dict()
                })

        # Datetime statistics
        elif pd.api.types.is_datetime64_any_dtype(series):
            non_null_series = series.dropna()
            if len(non_null_series) > 0:
                profile.update({
                    "min_date": non_null_series.min().isoformat(),
                    "max_date": non_null_series.max().isoformat(),
                    "date_range_days": (non_null_series.max() - non_null_series.min()).days
                })

        return profile


# FastAPI Application
app = FastAPI(title="Data Quality Service", version="1.0.0")


# Dependency injection
def get_db():
    engine = create_engine("postgresql://user:pass@localhost/db")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_redis():
    return redis.Redis(host='redis', port=6379, decode_responses=True)


def get_quality_engine(db: Session = Depends(get_db), redis_client: redis.Redis = Depends(get_redis)):
    return DataQualityEngine(db, redis_client)


def get_profiler(db: Session = Depends(get_db)):
    return DataProfiler(db)


# API Endpoints
@app.post("/organizations/{organization_id}/rules")
async def create_quality_rule(
        organization_id: str,
        rule_data: DataQualityRuleCreate,
        current_user: str = "system",  # Would come from auth
        db: Session = Depends(get_db)
):
    """Create a new data quality rule."""

    rule = DataQualityRule(
        organization_id=organization_id,
        name=rule_data.name,
        description=rule_data.description,
        rule_type=rule_data.rule_type,
        target_table=rule_data.target_table,
        target_column=rule_data.target_column,
        rule_definition=rule_data.rule_definition.dict(),
        severity=rule_data.severity,
        remediation_action=rule_data.remediation_action,
        created_by=current_user
    )

    db.add(rule)
    db.commit()
    db.refresh(rule)

    return rule


@app.get("/organizations/{organization_id}/rules")
async def get_quality_rules(
        organization_id: str,
        rule_type: Optional[DataQualityRuleType] = None,
        is_active: bool = True,
        db: Session = Depends(get_db)
):
    """Get data quality rules for an organization."""

    query = db.query(DataQualityRule).filter(
        DataQualityRule.organization_id == organization_id,
        DataQualityRule.is_active == is_active
    )

    if rule_type:
        query = query.filter(DataQualityRule.rule_type == rule_type)

    rules = query.order_by(DataQualityRule.created_at.desc()).all()
    return rules


@app.post("/organizations/{organization_id}/checks")
async def execute_quality_checks(
        organization_id: str,
        request: QualityCheckRequest,
        quality_engine: DataQualityEngine = Depends(get_quality_engine)
) -> QualityReport:
    """Execute data quality checks for a dataset."""

    return await quality_engine.execute_quality_checks(organization_id, request)


@app.get("/organizations/{organization_id}/reports")
async def get_quality_reports(
        organization_id: str,
        dataset_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """Get quality check results and reports."""

    query = db.query(QualityCheckResult).filter(
        QualityCheck  # services/data-quality-service/app/main.py
    """
    Comprehensive data quality service with real-time validation,
    rule engine, lineage tracking, and automated remediation.
    """

    import asyncio
    import json
    import logging
    from datetime import datetime, timedelta
    from typing import Dict, List, Optional, Any, Union, Callable
    from enum import Enum
    import uuid
    from dataclasses import dataclass
    import re
    import pandas as pd
    import numpy as np
    from collections import defaultdict

    from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
    from fastapi.responses import JSONResponse
    from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Boolean, JSON, Text, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session, relationship
    from sqlalchemy.dialects.postgresql import UUID
    from pydantic import BaseModel, Field, validator
    import redis
    import structlog
    import boto3
    from great_expectations import DataContext
    from great_expectations.core.batch import BatchRequest
    from great_expectations.checkpoint import SimpleCheckpoint

    logger = structlog.get_logger()

    # Database setup
    Base = declarative_base()

    # Enums


class DataQualityRuleType(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    CONFORMITY = "conformity"
    INTEGRITY = "integrity"


class QualityCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"
    ERROR = "error"


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RemediationAction(str, Enum):
    ALERT_ONLY = "alert_only"
    QUARANTINE = "quarantine"
    AUTO_FIX = "auto_fix"
    REJECT = "reject"
    MANUAL_REVIEW = "manual_review"


# Database Models
class DataQualityRule(Base):
    __tablename__ = "data_quality_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    rule_type = Column(String(50), nullable=False)
    target_table = Column(String(200), nullable=True)
    target_column = Column(String(200), nullable=True)
    rule_definition = Column(JSON, nullable=False)
    severity = Column(String(20), default=SeverityLevel.MEDIUM)
    remediation_action = Column(String(50), default=RemediationAction.ALERT_ONLY)
    is_active = Column(Boolean, default=True)
    created_by = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    check_results = relationship("QualityCheckResult", back_populates="rule")


class QualityCheckResult(Base):
    __tablename__ = "quality_check_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("data_quality_rules.id"), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    batch_id = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False)
    pass_rate = Column(Float, nullable=True)
    failed_records = Column(Integer, default=0)
    total_records = Column(Integer, default=0)
    details = Column(JSON, default=dict)
    execution_time_ms = Column(Float, nullable=True)
    executed_at = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    rule = relationship("DataQualityRule", back_populates="check_results")


class DataLineage(Base):
    __tablename__ = "data_lineage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    source_dataset = Column(String(200), nullable=False)
    target_dataset = Column(String(200), nullable=False)
    transformation_type = Column(String(100), nullable=False)
    pipeline_id = Column(String(100), nullable=True)
    metadata = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


class QualityProfile(Base):
    __tablename__ = "quality_profiles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_name = Column(String(200), nullable=False)
    column_name = Column(String(200), nullable=False)
    profile_data = Column(JSON, nullable=False)
    profiled_at = Column(DateTime, default=datetime.utcnow)


class DataQualityIncident(Base):
    __tablename__ = "quality_incidents"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    incident_type = Column(String(100), nullable=False)
    severity = Column(String(20), nullable=False)
    dataset_name = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    affected_records = Column(Integer, default=0)
    remediation_status = Column(String(50), default="open")
    remediation_details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)


# Pydantic Models
class RuleDefinition(BaseModel):
    condition: str  # SQL-like condition or Python expression
    parameters: Dict[str, Any] = Field(default_factory=dict)
    threshold: Optional[float] = None
    custom_sql: Optional[str] = None


class DataQualityRuleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    rule_type: DataQualityRuleType
    target_table: Optional[str] = None
    target_column: Optional[str] = None
    rule_definition: RuleDefinition
    severity: SeverityLevel = SeverityLevel.MEDIUM
    remediation_action: RemediationAction = RemediationAction.ALERT_ONLY


class QualityCheckRequest(BaseModel):
    dataset_name: str
    batch_id: Optional[str] = None
    rule_ids: Optional[List[str]] = None  # If None, run all active rules
    data_sample: Optional[Dict[str, Any]] = None  # For real-time checks


class QualityReport(BaseModel):
    organization_id: str
    dataset_name: str
    batch_id: Optional[str]
    overall_score: float
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    check_results: List[Dict[str, Any]]
    generated_at: datetime = Field(default_factory=datetime.utcnow)


# Data Quality Engine
class DataQualityEngine:
    """Core engine for data quality validation and monitoring."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client
        self.s3_client = boto3.client('s3')

        # Initialize rule validators
        self.validators = {
            DataQualityRuleType.COMPLETENESS: self._validate_completeness,
            DataQualityRuleType.UNIQUENESS: self._validate_uniqueness,
            DataQualityRuleType.VALIDITY: self._validate_validity,
            DataQualityRuleType.CONSISTENCY: self._validate_consistency,
            DataQualityRuleType.ACCURACY: self._validate_accuracy,
            DataQualityRuleType.TIMELINESS: self._validate_timeliness,
            DataQualityRuleType.CONFORMITY: self._validate_conformity,
            DataQualityRuleType.INTEGRITY: self._validate_integrity
        }

    async def execute_quality_checks(self, organization_id: str,
                                     request: QualityCheckRequest) -> QualityReport:
        """Execute data quality checks for a dataset."""

        start_time = datetime.utcnow()

        # Get applicable rules
        rules_query = self.db.query(DataQualityRule).filter(
            DataQualityRule.organization_id == organization_id,
            DataQualityRule.is_active == True
        )

        if request.target_table:
            rules_query = rules_query.filter(
                (DataQualityRule.target_table == request.dataset_name) |
                (DataQualityRule.target_table.is_(None))
            )

        if request.rule_ids:
            rules_query = rules_query.filter(
                DataQualityRule.id.in_(request.rule_ids)
            )

        rules = rules_query.all()

        if not rules:
            logger.warning("No active rules found",
                           organization_id=organization_id,
                           dataset_name=request.dataset_name)
            return QualityReport(
                organization_id=organization_id,
                dataset_name=request.dataset_name,
                batch_id=request.batch_id,
                overall_score=1.0,
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
                warning_rules=0,
                check_results=[]
            )

        # Load dataset
        dataset = await self._load_dataset(organization_id, request.dataset_name, request.data_sample)

        if dataset is None or dataset.empty:
            raise ValueError(f"Dataset '{request.dataset_name}' not found or empty")

        # Execute rules
        check_results = []
        passed_count = 0
        failed_count = 0
        warning_count = 0

        for rule in rules:
            try:
                result = await self._execute_rule(rule, dataset, request.batch_id)
                check_results.append(result)

                # Store result in database
                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=result["status"],
                    pass_rate=result.get("pass_rate"),
                    failed_records=result.get("failed_records", 0),
                    total_records=result.get("total_records", len(dataset)),
                    details=result.get("details", {}),
                    execution_time_ms=result.get("execution_time_ms")
                )

                self.db.add(db_result)

                # Count results
                if result["status"] == QualityCheckStatus.PASSED:
                    passed_count += 1
                elif result["status"] == QualityCheckStatus.FAILED:
                    failed_count += 1
                    # Handle remediation
                    await self._handle_remediation(rule, result, dataset, organization_id)
                elif result["status"] == QualityCheckStatus.WARNING:
                    warning_count += 1

            except Exception as e:
                logger.error("Rule execution failed",
                             rule_id=str(rule.id),
                             rule_name=rule.name,
                             error=str(e))

                # Record error result
                error_result = {
                    "rule_id": str(rule.id),
                    "rule_name": rule.name,
                    "status": QualityCheckStatus.ERROR,
                    "error_message": str(e)
                }
                check_results.append(error_result)

                db_result = QualityCheckResult(
                    rule_id=rule.id,
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    batch_id=request.batch_id,
                    status=QualityCheckStatus.ERROR,
                    details={"error": str(e)}
                )
                self.db.add(db_result)

        self.db.commit()

        # Calculate overall score
        total_rules = len(rules)
        if total_rules > 0:
            overall_score = passed_count / total_rules
        else:
            overall_score = 1.0

        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        logger.info("Quality checks completed",
                    organization_id=organization_id,
                    dataset_name=request.dataset_name,
                    total_rules=total_rules,
                    passed=passed_count,
                    failed=failed_count,
                    warnings=warning_count,
                    overall_score=overall_score,
                    execution_time_ms=execution_time)

        return QualityReport(
            organization_id=organization_id,
            dataset_name=request.dataset_name,
            batch_id=request.batch_id,
            overall_score=overall_score,
            total_rules=total_rules,
            passed_rules=passed_count,
            failed_rules=failed_count,
            warning_rules=warning_count,
            check_results=check_results
        )

    async def _load_dataset(self, organization_id: str, dataset_name: str,
                            data_sample: Optional[Dict] = None) -> pd.DataFrame:
        """Load dataset from various sources."""

        if data_sample:
            # For real-time validation, use provided sample
            return pd.DataFrame([data_sample])

        try:
            # Try to load from S3 (assuming CSV format)
            bucket_name = f"multi-tenant-ingestion-{organization_id}-data"
            key = f"datasets/{dataset_name}.csv"

            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_csv(response['Body'])

        except Exception as e:
            logger.warning("Failed to load dataset from S3",
                           dataset_name=dataset_name,
                           error=str(e))

            # Could try other sources here (Databricks, local files, etc.)
            return None

    async def _execute_rule(self, rule: DataQualityRule, dataset: pd.DataFrame,
                            batch_id: Optional[str]) -> Dict[str, Any]:
        """Execute a single data quality rule."""

        start_time = datetime.utcnow()

        try:
            validator = self.validators.get(rule.rule_type)
            if not validator:
                raise ValueError(f"No validator found for rule type: {rule.rule_type}")

            result = await validator(rule, dataset)

            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

            return {
                "rule_id": str(rule.id),
                "rule_name": rule.name,
                "rule_type": rule.rule_type,
                "status": result["status"],
                "pass_rate": result.get("pass_rate", 0.0),
                "failed_records": result.get("failed_records", 0),
                "total_records": len(dataset),
                "details": result.get("details", {}),
                "execution_time_ms": execution_time,
                "severity": rule.severity
            }

        except Exception as e:
            logger.error("Rule execution error",
                         rule_id=str(rule.id),
                         error=str(e))
            raise

    # Validator implementations
    async def _validate_completeness(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
        """Validate data completeness (non-null values)."""

        rule_def = rule.rule_definition
        column = rule.target_column
        threshold = rule_def.get("threshold", 0.95)  # Default 95% completeness

        if column and column in dataset.columns:
            total_records = len(dataset)
            non_null_records = dataset[column].notna().sum()
            completeness_rate = non_null_records / total_records if total_records > 0 else 0
            failed_records = total_records - non_null_records

            status = QualityCheckStatus.PASSED if completeness_rate >= threshold else QualityCheckStatus.FAILED

            return {
                "status": status,
                "pass_rate": completeness_rate,
                "failed_records": failed_records,
                "details": {
                    "column": column,
                    "completeness_rate": completeness_rate,
                    "threshold": threshold,
                    "non_null_records": non_null_records,
                    "total_records": total_records
                }
            }
        else:
            # Check overall dataset completeness
            total_cells = dataset.size
            non_null_cells = dataset.notna().sum().sum()
            completeness_rate = non_null_cells / total_cells if total_cells > 0 else 0

            status = QualityCheckStatus.PASSED if completeness_rate >= threshold else QualityCheckStatus.FAILED

            return {
                "status": status,
                "pass_rate": completeness_rate,
                "failed_records": total_cells - non_null_cells,
                "details": {
                    "overall_completeness": completeness_rate,
                    "threshold": threshold,
                    "total_cells": total_cells,
                    "non_null_cells": non_null_cells
                }
            }

    async def _validate_uniqueness(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
        """Validate data uniqueness."""

        rule_def = rule.rule_definition
        columns = rule_def.get("columns", [rule.target_column] if rule.target_column else [])

        if not columns:
            raise ValueError("Uniqueness rule requires column specification")

        # Filter to existing columns
        existing_columns = [col for col in columns if col in dataset.columns]

        if not existing_columns:
            raise ValueError(f"None of specified columns {columns} exist in dataset")

        total_records = len(dataset)

        if len(existing_columns) == 1:
            unique_records = dataset[existing_columns[0]].nunique()
            duplicates = total_records - unique_records
        else:
            unique_records = len(dataset.drop_duplicates(subset=existing_columns))
            duplicates = total_records - unique_records

        uniqueness_rate = unique_records / total_records if total_records > 0 else 1.0
        threshold = rule_def.get("threshold", 1.0)  # Default 100% uniqueness

        status = QualityCheckStatus.PASSED if uniqueness_rate >= threshold else QualityCheckStatus.FAILED

        return {
            "status": status,
            "pass_rate": uniqueness_rate,
            "failed_records": duplicates,
            "details": {
                "columns": existing_columns,
                "uniqueness_rate": uniqueness_rate,
                "threshold": threshold,
                "unique_records": unique_records,
                "duplicate_records": duplicates
            }
        }

    async def _validate_validity(self, rule: DataQualityRule, dataset: pd.DataFrame) -> Dict[str, Any]:
        """Validate data against format/pattern rules."""

        rule_def = rule.rule_definition
        column = rule.target_column
        pattern = rule_def.get("pattern")
        data_type = rule_def.get("data_type")
        value_range = rule_def.get("range")
        allowed_values = rule_def.get("allowed_values")

        if not column or column not in dataset.columns:
            raise ValueError(f"Column '{column}' not found in dataset")

        total_records = len(dataset)
        valid_records = 0
        validation_errors = []

        for idx, value in dataset[column].items():
            is_valid = True
            error_reasons = []

            # Skip null values (handled by completeness check)
            if pd.isna(value):
                valid_records += 1
                continue

            # Pattern validation
            if pattern and isinstance(value, str):
                if not re.match(pattern, value):
                    is_valid = False
                    error_reasons.append(f"Pattern mismatch: {pattern}")

            # Data type validation
            if data_type:
                try:
                    if data_type == "int":
                        int(value)
                    elif data_type == "float":
                        float(value)
                    elif data_type == "date":
                        pd.to_datetime(value)
                    elif data_type == "email":
                        if not re.match(r'^[^@]+@[^@]+\.[^@]+# services/data-quality-service/app/main.py