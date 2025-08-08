# services/data-quality-service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, validator
import httpx
import uvicorn
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
import numpy as np
from enum import Enum
import re

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/dq_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class RuleType(str, Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    CUSTOM = "custom"

# Models
class DataQualityRule(Base):
    __tablename__ = "dq_rules"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    rule_type = Column(String, nullable=False)
    severity = Column(String, default="medium")
    target_table = Column(String, nullable=False)
    target_column = Column(String)
    rule_config = Column(Text)  # JSON
    threshold = Column(Float)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String)

class DataQualityCheck(Base):
    __tablename__ = "dq_checks"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    rule_id = Column(String, nullable=False, index=True)
    table_name = Column(String, nullable=False)
    check_timestamp = Column(DateTime, default=datetime.utcnow)
    total_records = Column(Integer)
    passed_records = Column(Integer)
    failed_records = Column(Integer)
    pass_rate = Column(Float)
    status = Column(String)  # passed, failed, error
    error_message = Column(Text)
    execution_time_ms = Column(Integer)
    sample_failures = Column(Text)  # JSON

class DataQualityReport(Base):
    __tablename__ = "dq_reports"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    report_name = Column(String, nullable=False)
    table_name = Column(String)
    report_period = Column(String)  # daily, weekly, monthly
    generated_at = Column(DateTime, default=datetime.utcnow)
    overall_score = Column(Float)
    total_rules = Column(Integer)
    passed_rules = Column(Integer)
    failed_rules = Column(Integer)
    report_data = Column(Text)  # JSON

# Pydantic models
class RuleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    rule_type: RuleType
    severity: SeverityLevel = SeverityLevel.MEDIUM
    target_table: str
    target_column: Optional[str] = None
    rule_config: Dict[str, Any]
    threshold: Optional[float] = None

class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[SeverityLevel] = None
    rule_config: Optional[Dict[str, Any]] = None
    threshold: Optional[float] = None
    is_active: Optional[bool] = None

class CheckRequest(BaseModel):
    table_name: str
    rule_ids: Optional[List[str]] = None
    sample_size: Optional[int] = 1000

class ReportRequest(BaseModel):
    table_name: Optional[str] = None
    period: str = "daily"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

# FastAPI app
app = FastAPI(
    title="Multi-Tenant Data Quality Service",
    description="Data quality validation and monitoring service",
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

# Data Quality Engine
class DataQualityEngine:
    def __init__(self):
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
    
    async def execute_sql_check(self, tenant_id: str, sql_query: str) -> Dict[str, Any]:
        """Execute SQL query for data quality check"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "statement": sql_query,
            "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID"),
            "catalog": f"{tenant_id}_catalog",
            "schema": "default"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.databricks_host}/api/2.0/sql/statements",
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                raise Exception(f"Databricks SQL API error: {response.text}")
            
            result = response.json()
            statement_id = result["statement_id"]
            
            # Poll for completion
            while True:
                status_response = await client.get(
                    f"{self.databricks_host}/api/2.0/sql/statements/{statement_id}",
                    headers=headers
                )
                
                if status_response.status_code != 200:
                    raise Exception(f"Failed to get statement status: {status_response.text}")
                
                status_data = status_response.json()
                state = status_data["status"]["state"]
                
                if state == "SUCCEEDED":
                    return status_data["result"]
                elif state in ["FAILED", "CANCELED"]:
                    error_msg = status_data.get("status", {}).get("error", {}).get("message", "Unknown error")
                    raise Exception(f"Query failed: {error_msg}")
                
                await asyncio.sleep(1)
    
    def generate_completeness_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for completeness check"""
        return f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT({column_name}) as non_null_records,
            COUNT(*) - COUNT({column_name}) as null_records,
            ROUND((COUNT({column_name}) * 100.0 / COUNT(*)), 2) as completeness_rate
        FROM {table_name}
        """
    
    def generate_validity_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for validity check"""
        pattern = config.get("pattern", "")
        min_value = config.get("min_value")
        max_value = config.get("max_value")
        allowed_values = config.get("allowed_values", [])
        
        if pattern:
            condition = f"REGEXP_LIKE({column_name}, '{pattern}')"
        elif min_value is not None and max_value is not None:
            condition = f"{column_name} BETWEEN {min_value} AND {max_value}"
        elif allowed_values:
            values_str = "', '".join(str(v) for v in allowed_values)
            condition = f"{column_name} IN ('{values_str}')"
        else:
            condition = f"{column_name} IS NOT NULL"
        
        return f"""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) as valid_records,
            SUM(CASE WHEN {condition} THEN 0 ELSE 1 END) as invalid_records,
            ROUND((SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as validity_rate
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
    
    def generate_uniqueness_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for uniqueness check"""
        return f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT {column_name}) as unique_records,
            COUNT(*) - COUNT(DISTINCT {column_name}) as duplicate_records,
            ROUND((COUNT(DISTINCT {column_name}) * 100.0 / COUNT(*)), 2) as uniqueness_rate
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
    
    def generate_consistency_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for consistency check"""
        reference_table = config.get("reference_table")
        reference_column = config.get("reference_column")
        
        if reference_table and reference_column:
            return f"""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN ref.{reference_column} IS NOT NULL THEN 1 ELSE 0 END) as consistent_records,
                SUM(CASE WHEN ref.{reference_column} IS NULL THEN 1 ELSE 0 END) as inconsistent_records,
                ROUND((SUM(CASE WHEN ref.{reference_column} IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as consistency_rate
            FROM {table_name} t
            LEFT JOIN {reference_table} ref ON t.{column_name} = ref.{reference_column}
            WHERE t.{column_name} IS NOT NULL
            """
        else:
            # Check for consistent format within the column
            return f"""
            WITH format_check AS (
                SELECT {column_name},
                       COUNT(*) as count,
                       LENGTH({column_name}) as len,
                       REGEXP_EXTRACT({column_name}, '[A-Za-z]+', 0) as alpha_part,
                       REGEXP_EXTRACT({column_name}, '[0-9]+', 0) as numeric_part
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                GROUP BY {column_name}, LENGTH({column_name}), 
                         REGEXP_EXTRACT({column_name}, '[A-Za-z]+', 0),
                         REGEXP_EXTRACT({column_name}, '[0-9]+', 0)
            ),
            format_stats AS (
                SELECT 
                    COUNT(DISTINCT len) as distinct_lengths,
                    COUNT(DISTINCT alpha_part) as distinct_alpha_patterns,
                    COUNT(DISTINCT numeric_part) as distinct_numeric_patterns,
                    COUNT(*) as total_distinct_values
                FROM format_check
            )
            SELECT 
                (SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NOT NULL) as total_records,
                CASE 
                    WHEN distinct_lengths <= 2 AND distinct_alpha_patterns <= 5 THEN total_distinct_values
                    ELSE 0
                END as consistent_records,
                CASE 
                    WHEN distinct_lengths <= 2 AND distinct_alpha_patterns <= 5 THEN 0
                    ELSE total_distinct_values
                END as inconsistent_records,
                CASE 
                    WHEN distinct_lengths <= 2 AND distinct_alpha_patterns <= 5 THEN 100.0
                    ELSE 0.0
                END as consistency_rate
            FROM format_stats
            """
    
    def generate_timeliness_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for timeliness check"""
        max_age_days = config.get("max_age_days", 30)
        
        return f"""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN DATEDIFF(day, {column_name}, CURRENT_DATE()) <= {max_age_days} THEN 1 ELSE 0 END) as timely_records,
            SUM(CASE WHEN DATEDIFF(day, {column_name}, CURRENT_DATE()) > {max_age_days} THEN 1 ELSE 0 END) as outdated_records,
            ROUND((SUM(CASE WHEN DATEDIFF(day, {column_name}, CURRENT_DATE()) <= {max_age_days} THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as timeliness_rate
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
    
    def generate_custom_check(self, table_name: str, column_name: str, config: Dict[str, Any]) -> str:
        """Generate SQL for custom check"""
        custom_sql = config.get("sql_template", "")
        if not custom_sql:
            raise ValueError("Custom rule requires sql_template in config")
        
        # Replace placeholders
        sql = custom_sql.replace("{table_name}", table_name)
        if column_name:
            sql = sql.replace("{column_name}", column_name)
        
        return sql
    
    async def execute_rule_check(self, tenant_id: str, rule: DataQualityRule, sample_size: int = 1000) -> Dict[str, Any]:
        """Execute a data quality rule check"""
        start_time = datetime.utcnow()
        
        try:
            # Generate appropriate SQL based on rule type
            if rule.rule_type == RuleType.COMPLETENESS:
                sql = self.generate_completeness_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            elif rule.rule_type == RuleType.VALIDITY:
                sql = self.generate_validity_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            elif rule.rule_type == RuleType.UNIQUENESS:
                sql = self.generate_uniqueness_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            elif rule.rule_type == RuleType.CONSISTENCY:
                sql = self.generate_consistency_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            elif rule.rule_type == RuleType.TIMELINESS:
                sql = self.generate_timeliness_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            elif rule.rule_type == RuleType.CUSTOM:
                sql = self.generate_custom_check(rule.target_table, rule.target_column, json.loads(rule.rule_config or "{}"))
            else:
                raise ValueError(f"Unsupported rule type: {rule.rule_type}")
            
            # Add sampling if specified
            if sample_size and sample_size > 0:
                sql = f"WITH sampled_data AS (SELECT * FROM {rule.target_table} TABLESAMPLE ({sample_size} ROWS)) " + sql.replace(rule.target_table, "sampled_data")
            
            # Execute SQL check
            result = await self.execute_sql_check(tenant_id, sql)
            
            # Parse results
            if result and "data_array" in result:
                data = result["data_array"][0] if result["data_array"] else []
                
                if len(data) >= 4:
                    total_records = data[0]
                    passed_records = data[1]
                    failed_records = data[2]
                    pass_rate = data[3]
                    
                    # Determine status based on threshold
                    threshold = rule.threshold or 90.0
                    status = "passed" if pass_rate >= threshold else "failed"
                    
                    execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
                    
                    return {
                        "total_records": total_records,
                        "passed_records": passed_records,
                        "failed_records": failed_records,
                        "pass_rate": pass_rate,
                        "status": status,
                        "execution_time_ms": execution_time,
                        "threshold": threshold
                    }
            
            raise Exception("Invalid result format from SQL execution")
            
        except Exception as e:
            execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            return {
                "total_records": 0,
                "passed_records": 0,
                "failed_records": 0,
                "pass_rate": 0.0,
                "status": "error",
                "error_message": str(e),
                "execution_time_ms": execution_time
            }

dq_engine = DataQualityEngine()

# API Routes
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "data-quality-service", "timestamp": datetime.utcnow()}

@app.post("/api/v1/rules")
async def create_rule(
    rule: RuleCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new data quality rule"""
    rule_id = f"rule_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    
    db_rule = DataQualityRule(
        id=rule_id,
        tenant_id=tenant_id,
        name=rule.name,
        description=rule.description,
        rule_type=rule.rule_type,
        severity=rule.severity,
        target_table=rule.target_table,
        target_column=rule.target_column,
        rule_config=json.dumps(rule.rule_config),
        threshold=rule.threshold
    )
    
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    
    return {"rule_id": rule_id, "status": "created"}

@app.get("/api/v1/rules")
async def list_rules(
    tenant_id: str = Depends(get_tenant_id),
    table_name: Optional[str] = None,
    rule_type: Optional[RuleType] = None,
    db: Session = Depends(get_db)
):
    """List data quality rules"""
    query = db.query(DataQualityRule).filter(DataQualityRule.tenant_id == tenant_id)
    
    if table_name:
        query = query.filter(DataQualityRule.target_table == table_name)
    if rule_type:
        query = query.filter(DataQualityRule.rule_type == rule_type)
    
    rules = query.all()
    
    result = []
    for rule in rules:
        result.append({
            "id": rule.id,
            "name": rule.name,
            "description": rule.description,
            "rule_type": rule.rule_type,
            "severity": rule.severity,
            "target_table": rule.target_table,
            "target_column": rule.target_column,
            "threshold": rule.threshold,
            "is_active": rule.is_active,
            "created_at": rule.created_at
        })
    
    return {"rules": result}

@app.get("/api/v1/rules/{rule_id}")
async def get_rule(
    rule_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get a specific rule"""
    rule = db.query(DataQualityRule).filter(
        DataQualityRule.id == rule_id,
        DataQualityRule.tenant_id == tenant_id
    ).first()
    
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    return {
        "id": rule.id,
        "name": rule.name,
        "description": rule.description,
        "rule_type": rule.rule_type,
        "severity": rule.severity,
        "target_table": rule.target_table,
        "target_column": rule.target_column,
        "rule_config": json.loads(rule.rule_config or "{}"),
        "threshold": rule.threshold,
        "is_active": rule.is_active,
        "created_at": rule.created_at
    }

@app.put("/api/v1/rules/{rule_id}")
async def update_rule(
    rule_id: str,
    rule_update: RuleUpdate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Update a data quality rule"""
    rule = db.query(DataQualityRule).filter(
        DataQualityRule.id == rule_id,
        DataQualityRule.tenant_id == tenant_id
    ).first()
    
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    update_data = rule_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        if field == "rule_config" and value:
            setattr(rule, field, json.dumps(value))
        else:
            setattr(rule, field, value)
    
    db.commit()
    return {"status": "updated"}

@app.delete("/api/v1/rules/{rule_id}")
async def delete_rule(
    rule_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Delete a data quality rule"""
    rule = db.query(DataQualityRule).filter(
        DataQualityRule.id == rule_id,
        DataQualityRule.tenant_id == tenant_id
    ).first()
    
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    
    db.delete(rule)
    db.commit()
    
    return {"status": "deleted"}

@app.post("/api/v1/checks")
async def run_data_quality_check(
    check_request: CheckRequest,
    background_tasks: BackgroundTasks,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Run data quality checks"""
    
    async def execute_checks():
        # Get rules to check
        query = db.query(DataQualityRule).filter(
            DataQualityRule.tenant_id == tenant_id,
            DataQualityRule.target_table == check_request.table_name,
            DataQualityRule.is_active == True
        )
        
        if check_request.rule_ids:
            query = query.filter(DataQualityRule.id.in_(check_request.rule_ids))
        
        rules = query.all()
        
        check_results = []
        
        for rule in rules:
            try:
                result = await dq_engine.execute_rule_check(
                    tenant_id, rule, check_request.sample_size
                )
                
                # Save check result
                check_id = f"check_{tenant_id}_{int(datetime.utcnow().timestamp())}_{rule.id}"
                
                db_check = DataQualityCheck(
                    id=check_id,
                    tenant_id=tenant_id,
                    rule_id=rule.id,
                    table_name=check_request.table_name,
                    total_records=result.get("total_records", 0),
                    passed_records=result.get("passed_records", 0),
                    failed_records=result.get("failed_records", 0),
                    pass_rate=result.get("pass_rate", 0.0),
                    status=result.get("status", "error"),
                    error_message=result.get("error_message"),
                    execution_time_ms=result.get("execution_time_ms", 0)
                )
                
                db.add(db_check)
                check_results.append({
                    "check_id": check_id,
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "result": result
                })
                
            except Exception as e:
                print(f"Error executing rule {rule.id}: {e}")
        
        db.commit()
        return check_results
    
    # Start background task
    background_tasks.add_task(execute_checks)
    
    return {"status": "checks_started", "table_name": check_request.table_name}

@app.get("/api/v1/checks")
async def list_checks(
    tenant_id: str = Depends(get_tenant_id),
    table_name: Optional[str] = None,
    rule_id: Optional[str] = None,
    limit: Optional[int] = 100,
    db: Session = Depends(get_db)
):
    """List data quality check results"""
    query = db.query(DataQualityCheck).filter(DataQualityCheck.tenant_id == tenant_id)
    
    if table_name:
        query = query.filter(DataQualityCheck.table_name == table_name)
    if rule_id:
        query = query.filter(DataQualityCheck.rule_id == rule_id)
    
    checks = query.order_by(DataQualityCheck.check_timestamp.desc()).limit(limit).all()
    
    result = []
    for check in checks:
        result.append({
            "id": check.id,
            "rule_id": check.rule_id,
            "table_name": check.table_name,
            "check_timestamp": check.check_timestamp,
            "total_records": check.total_records,
            "passed_records": check.passed_records,
            "failed_records": check.failed_records,
            "pass_rate": check.pass_rate,
            "status": check.status,
            "error_message": check.error_message,
            "execution_time_ms": check.execution_time_ms
        })
    
    return {"checks": result}

@app.get("/api/v1/checks/{check_id}")
async def get_check(
    check_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get detailed check result"""
    check = db.query(DataQualityCheck).filter(
        DataQualityCheck.id == check_id,
        DataQualityCheck.tenant_id == tenant_id
    ).first()
    
    if not check:
        raise HTTPException(status_code=404, detail="Check not found")
    
    # Get associated rule info
    rule = db.query(DataQualityRule).filter(DataQualityRule.id == check.rule_id).first()
    
    return {
        "id": check.id,
        "rule": {
            "id": rule.id,
            "name": rule.name,
            "rule_type": rule.rule_type,
            "severity": rule.severity,
            "threshold": rule.threshold
        } if rule else None,
        "table_name": check.table_name,
        "check_timestamp": check.check_timestamp,
        "total_records": check.total_records,
        "passed_records": check.passed_records,
        "failed_records": check.failed_records,
        "pass_rate": check.pass_rate,
        "status": check.status,
        "error_message": check.error_message,
        "execution_time_ms": check.execution_time_ms,
        "sample_failures": json.loads(check.sample_failures or "[]")
    }

@app.post("/api/v1/reports")
async def generate_report(
    report_request: ReportRequest,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Generate data quality report"""
    
    # Calculate date range
    end_date = report_request.end_date or datetime.utcnow()
    if report_request.period == "daily":
        start_date = report_request.start_date or (end_date - timedelta(days=1))
    elif report_request.period == "weekly":
        start_date = report_request.start_date or (end_date - timedelta(weeks=1))
    elif report_request.period == "monthly":
        start_date = report_request.start_date or (end_date - timedelta(days=30))
    else:
        start_date = report_request.start_date or (end_date - timedelta(days=7))
    
    # Query checks in date range
    query = db.query(DataQualityCheck).filter(
        DataQualityCheck.tenant_id == tenant_id,
        DataQualityCheck.check_timestamp >= start_date,
        DataQualityCheck.check_timestamp <= end_date
    )
    
    if report_request.table_name:
        query = query.filter(DataQualityCheck.table_name == report_request.table_name)
    
    checks = query.all()
    
    # Calculate metrics
    total_checks = len(checks)
    passed_checks = len([c for c in checks if c.status == "passed"])
    failed_checks = len([c for c in checks if c.status == "failed"])
    error_checks = len([c for c in checks if c.status == "error"])
    
    overall_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    
    # Group by table and rule type
    table_stats = {}
    rule_type_stats = {}
    
    for check in checks:
        # Table stats
        table = check.table_name
        if table not in table_stats:
            table_stats[table] = {"total": 0, "passed": 0, "failed": 0, "error": 0}
        
        table_stats[table]["total"] += 1
        table_stats[table][check.status] += 1
        
        # Rule type stats (get from rule)
        rule = db.query(DataQualityRule).filter(DataQualityRule.id == check.rule_id).first()
        if rule:
            rule_type = rule.rule_type
            if rule_type not in rule_type_stats:
                rule_type_stats[rule_type] = {"total": 0, "passed": 0, "failed": 0, "error": 0}
            
            rule_type_stats[rule_type]["total"] += 1
            rule_type_stats[rule_type][check.status] += 1
    
    # Create report
    report_id = f"report_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    report_name = f"DQ Report - {report_request.period} - {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    
    report_data = {
        "period": report_request.period,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "summary": {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "error_checks": error_checks,
            "overall_score": round(overall_score, 2)
        },
        "table_stats": table_stats,
        "rule_type_stats": rule_type_stats,
        "trend_data": []  # Could add trend analysis here
    }
    
    db_report = DataQualityReport(
        id=report_id,
        tenant_id=tenant_id,
        report_name=report_name,
        table_name=report_request.table_name,
        report_period=report_request.period,
        overall_score=overall_score,
        total_rules=total_checks,
        passed_rules=passed_checks,
        failed_rules=failed_checks,
        report_data=json.dumps(report_data)
    )
    
    db.add(db_report)
    db.commit()
    db.refresh(db_report)
    
    return {
        "report_id": report_id,
        "report_name": report_name,
        "report_data": report_data
    }

@app.get("/api/v1/reports")
async def list_reports(
    tenant_id: str = Depends(get_tenant_id),
    limit: Optional[int] = 50,
    db: Session = Depends(get_db)
):
    """List data quality reports"""
    reports = db.query(DataQualityReport).filter(
        DataQualityReport.tenant_id == tenant_id
    ).order_by(DataQualityReport.generated_at.desc()).limit(limit).all()
    
    result = []
    for report in reports:
        result.append({
            "id": report.id,
            "report_name": report.report_name,
            "table_name": report.table_name,
            "report_period": report.report_period,
            "generated_at": report.generated_at,
            "overall_score": report.overall_score,
            "total_rules": report.total_rules,
            "passed_rules": report.passed_rules,
            "failed_rules": report.failed_rules
        })
    
    return {"reports": result}

@app.get("/api/v1/reports/{report_id}")
async def get_report(
    report_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get detailed report"""
    report = db.query(DataQualityReport).filter(
        DataQualityReport.id == report_id,
        DataQualityReport.tenant_id == tenant_id
    ).first()
    
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    
    return {
        "id": report.id,
        "report_name": report.report_name,
        "table_name": report.table_name,
        "report_period": report.report_period,
        "generated_at": report.generated_at,
        "overall_score": report.overall_score,
        "total_rules": report.total_rules,
        "passed_rules": report.passed_rules,
        "failed_rules": report.failed_rules,
        "report_data": json.loads(report.report_data)
    }

@app.get("/api/v1/rule-templates")
async def get_rule_templates():
    """Get predefined rule templates"""
    templates = [
        {
            "name": "Null Check",
            "rule_type": "completeness",
            "description": "Check for null values in a column",
            "config_template": {},
            "threshold_default": 95.0
        },
        {
            "name": "Email Format",
            "rule_type": "validity",
            "description": "Validate email format",
            "config_template": {
                "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            },
            "threshold_default": 99.0
        },
        {
            "name": "Phone Number Format",
            "rule_type": "validity",
            "description": "Validate phone number format",
            "config_template": {
                "pattern": "^\\+?[1-9]\\d{1,14}$"
            },
            "threshold_default": 95.0
        },
        {
            "name": "Primary Key Uniqueness",
            "rule_type": "uniqueness",
            "description": "Ensure primary key uniqueness",
            "config_template": {},
            "threshold_default": 100.0
        },
        {
            "name": "Referential Integrity",
            "rule_type": "consistency",
            "description": "Check foreign key references",
            "config_template": {
                "reference_table": "table_name",
                "reference_column": "column_name"
            },
            "threshold_default": 99.0
        },
        {
            "name": "Recent Data",
            "rule_type": "timeliness",
            "description": "Check if data is recent",
            "config_template": {
