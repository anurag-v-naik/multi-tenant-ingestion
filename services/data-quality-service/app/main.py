# services/data-quality-service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import logging
import os
from datetime import datetime, timedelta
import asyncio

from .core.database import get_db, engine
from .core.config import settings
from .core.auth import get_current_user, verify_organization_access
from .models import schemas, models
from .rules.quality_engine import QualityRuleEngine
from .reports.generator import ReportGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Data Quality Service",
    description="Multi-tenant data quality validation and monitoring service",
    version="1.0.0",
    docs_url="/docs" if settings.ENVIRONMENT == "development" else None
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize quality engine and report generator
quality_engine = QualityRuleEngine()
report_generator = ReportGenerator()


@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    # Create database tables
    models.Base.metadata.create_all(bind=engine)
    logger.info("Data Quality Service started successfully")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "data-quality-service",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }


@app.get("/health/database")
async def database_health(db: Session = Depends(get_db)):
    """Database connectivity health check"""
    try:
        # Test database connection
        db.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


# Quality Rules Management
@app.post("/api/v1/quality-rules", response_model=schemas.QualityRuleResponse)
async def create_quality_rule(
        rule: schemas.QualityRuleCreate,
        db: Session = Depends(get_db),
        current_user: dict = Depends(get_current_user),
        x_organization_id: str = Header(..., alias="X-Organization-ID")
):
    """Create a new data quality rule"""
    # Verify organization access
    await verify_organization_access(current_user, x_organization_id)

    try:
        # Create rule in database
        db_rule = models.QualityRule(
            organization_id=x_organization_id,
            name=rule.name,
            description=rule.description,
            rule_type=rule.rule_type,
            table_name=rule.table_name,
            column_name=rule.column_name,
            rule_definition=rule.rule_definition,
            severity=rule.severity,
            is_active=rule.is_active,
            created_by=current_user.get("user_id"),
            created_at=datetime.utcnow()
        )

        db.add(db_rule)
        db.commit()
        db.refresh(db_rule)

        logger.info(f"Created quality rule {db_rule.id} for organization {x_organization_id}")
        return db_rule

    except Exception as e:
        logger.error(f"Failed to create quality rule: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Failed to create quality rule")


@app.get("/api/v1/quality-rules", response_model=List[schemas.QualityRuleResponse])
async def get_quality_rules(
        skip: int = 0,
        limit: int = 100,
        rule_type: Optional[str] = None,
        table_name: Optional[str] = None,
        db: Session = Depends(get_db),
        current_user: dict = Depends(get_current_user),
        x_organization_id: str = Header(..., alias="X-Organization-ID")
):
    """Get quality rules for an organization"""
    await verify_organization_access(current_user, x_organization_id)

    query = db.query(models.QualityRule).filter(
        models.QualityRule.organization_id == x_organization_id
    )

    if rule_type:
        query = query.filter(models.QualityRule.rule_type == rule_type)
    if table_name:
        query = query.filter(models.QualityRule.table_name == table_name)

    rules = query.offset(skip).limit(limit).all()
    return rules


@app.post("/api/v1/quality-checks", response_model=schemas.QualityCheckResponse)
async def run_quality_check(
        check_request: schemas.QualityCheckRequest,
        background_tasks: BackgroundTasks,
        db: Session = Depends(get_db),
        current_user: dict = Depends(get_current_user),
        x_organization_id: str = Header(..., alias="X-Organization-ID")
):
    """Run data quality check on specified dataset"""
    await verify_organization_access(current_user, x_organization_id)

    try:
        # Create quality check record
        db_check = models.QualityCheck(
            organization_id=x_organization_id,
            check_name=check_request.check_name,
            dataset_name=check_request.dataset_name,
            table_name=check_request.table_name,
            status="RUNNING",
            started_at=datetime.utcnow(),
            initiated_by=current_user.get("user_id")
        )

        db.add(db_check)
        db.commit()
        db.refresh(db_check)

        # Run quality check in background
        background_tasks.add_task(
            run_quality_check_async,
            db_check.id,
            x_organization_id,
            check_request.dataset_name,
            check_request.table_name,
            check_request.rule_ids
        )

        logger.info(f"Started quality check {db_check.id} for organization {x_organization_id}")
        return db_check

    except Exception as e:
        logger.error(f"Failed to start quality check: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Failed to start quality check")


@app.get("/api/v1/quality-checks/{check_id}", response_model=schemas.QualityCheckResponse)
async def get_quality_check(
        check_id: int,
        db: Session = Depends(get_db),
        current_user: dict = Depends(get_current_user),
        x_organization_id: str = Header(..., alias="X-Organization-ID")
):
    """Get quality check details"""
    await verify_organization_access(current_user, x_organization_id)

    check = db.query(models.QualityCheck).filter(
        models.QualityCheck.id == check_id,
        models.QualityCheck.organization_id == x_organization_id
    ).first()

    if not check:
        raise HTTPException(status_code=404, detail="Quality check not found")

    return check


@app.get("/api/v1/quality-reports", response_model=List[schemas.QualityReportResponse])
async def get_quality_reports(
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        table_name: Optional[str] = None,
        db: Session = Depends(get_db),
        current_user: dict = Depends(get_current_user),
        x_organization_id: str = Header(..., alias="X-Organization-ID")
):
    """Get quality reports for an organization"""
    await verify_organization_access(current_user, x_organization_id)

    # Default to last 30 days if no date range specified
    if not start_date:
        start_date = datetime.utcnow() - timedelta(days=30)
    if not end_date:
        end_date = datetime.utcnow()

    query = db.query(models.QualityCheck).filter(
        models.QualityCheck.organization_id == x_organization_id,
        models.QualityCheck.started_at >= start_date,
        models.QualityCheck.started_at <= end_date
    )

    if table_name:
        query = query.filter(models.QualityCheck.table_name == table_name)

    checks = query.all()

    # Generate report summary
    reports = report_generator.generate_summary_reports(checks)
    return reports


async def run_quality_check_async(
        check_id: int,
        organization_id: str,
        dataset_name: str,
        table_name: str,
        rule_ids: Optional[List[int]] = None
):
    """Run quality check asynchronously"""
    from .core.database import SessionLocal

    db = SessionLocal()
    try:
        # Get the quality check record
        check = db.query(models.QualityCheck).filter(
            models.QualityCheck.id == check_id
        ).first()

        if not check:
            logger.error(f"Quality check {check_id} not found")
            return

        # Get applicable rules
        query = db.query(models.QualityRule).filter(
            models.QualityRule.organization_id == organization_id,
            models.QualityRule.is_active == True
        )

        if rule_ids:
            query = query.filter(models.QualityRule.id.in_(rule_ids))
        else:
            query = query.filter(models.QualityRule.table_name == table_name)

        rules = query.all()

        # Execute quality checks
        results = await quality_engine.execute_quality_checks(
            organization_id, dataset_name, table_name, rules
        )

        # Update check record with results
        check.status = "COMPLETED" if results.get("success") else "FAILED"
        check.completed_at = datetime.utcnow()
        check.total_rules = len(rules)
        check.passed_rules = results.get("passed_count", 0)
        check.failed_rules = results.get("failed_count", 0)
        check.results = results

        db.commit()

        logger.info(f"Quality check {check_id} completed with status {check.status}")

    except Exception as e:
        logger.error(f"Quality check {check_id} failed: {e}")
        if check:
            check.status = "FAILED"
            check.completed_at = datetime.utcnow()
            check.error_message = str(e)
            db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8003)),
        reload=settings.ENVIRONMENT == "development"
    )