"""
Multi-Tenant Pipeline Service
Main FastAPI application for managing data ingestion pipelines
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
import requests
import json
import asyncio
import aioredis
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.clusters.api import ClustersApi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/multi_tenant_ingestion")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis setup
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Security
security = HTTPBearer()
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-jwt-secret-key")

# Models
class Pipeline(Base):
    __tablename__ = "pipelines"
    
    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)
    organization_id = Column(String, index=True)
    source_type = Column(String)
    destination_type = Column(String)
    configuration = Column(Text)  # JSON string
    schedule = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_run_at = Column(DateTime)
    status = Column(String, default="inactive")

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    id = Column(String, primary_key=True, index=True)
    pipeline_id = Column(String, index=True)
    organization_id = Column(String, index=True)
    status = Column(String, default="running")  # running, completed, failed
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    logs = Column(Text)
    metrics = Column(Text)  # JSON string
    databricks_run_id = Column(String)

# Pydantic models
class PipelineCreate(BaseModel):
    name: str
    source_type: str
    destination_type: str
    configuration: Dict
    schedule: Optional[str] = None

class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    configuration: Optional[Dict] = None
    schedule: Optional[str] = None
    is_active: Optional[bool] = None

class PipelineResponse(BaseModel):
    id: str
    name: str
    organization_id: str
    source_type: str
    destination_type: str
    configuration: Dict
    schedule: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_run_at: Optional[datetime]
    status: str

class PipelineRunResponse(BaseModel):
    id: str
    pipeline_id: str
    organization_id: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    logs: Optional[str]
    metrics: Optional[Dict]
    databricks_run_id: Optional[str]

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
    """Extract organization ID from JWT token or header"""
    if not x_organization_id:
        raise HTTPException(status_code=401, detail="Organization ID required")
    
    # In production, verify JWT token here
    # For now, just return the organization ID
    return x_organization_id

class DatabricksManager:
    """Manages Databricks workspace operations"""
    
    def __init__(self, organization_id: str):
        self.organization_id = organization_id
        self.workspace_url = self._get_workspace_url(organization_id)
        self.token = self._get_databricks_token(organization_id)
        self.api_client = ApiClient(host=self.workspace_url, token=self.token)
        self.jobs_api = JobsApi(self.api_client)
        self.clusters_api = ClustersApi(self.api_client)
    
    def _get_workspace_url(self, org_id: str) -> str:
        """Get organization-specific Databricks workspace URL"""
        return f"https://{org_id}.databricks.com"
    
    def _get_databricks_token(self, org_id: str) -> str:
        """Retrieve Databricks token from secrets manager"""
        # In production, retrieve from AWS Secrets Manager
        return os.getenv(f"DATABRICKS_TOKEN_{org_id.upper()}", "dapi-default-token")
    
    async def create_job(self, pipeline_config: Dict) -> str:
        """Create a Databricks job for the pipeline"""
        job_config = {
            "name": f"Pipeline-{pipeline_config['name']}-{self.organization_id}",
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            },
            "notebook_task": {
                "notebook_path": f"/Shared/{self.organization_id}/pipelines/{pipeline_config['name']}",
                "base_parameters": pipeline_config.get("parameters", {})
            },
            "timeout_seconds": 3600,
            "max_retries": 2
        }
        
        response = self.jobs_api.create_job(job_config)
        return str(response["job_id"])
    
    async def run_job(self, job_id: str, parameters: Dict = None) -> str:
        """Trigger a job run"""
        run_config = {"job_id": int(job_id)}
        if parameters:
            run_config["notebook_params"] = parameters
        
        response = self.jobs_api.run_now(**run_config)
        return str(response["run_id"])
    
    async def get_run_status(self, run_id: str) -> Dict:
        """Get status of a job run"""
        return self.jobs_api.get_run(int(run_id))

# Redis cache manager
class CacheManager:
    def __init__(self):
        self.redis = None
    
    async def connect(self):
        self.redis = await aioredis.from_url(REDIS_URL)
    
    async def set_pipeline_status(self, pipeline_id: str, status: str, ttl: int = 3600):
        if self.redis:
            await self.redis.setex(f"pipeline:{pipeline_id}:status", ttl, status)
    
    async def get_pipeline_status(self, pipeline_id: str) -> Optional[str]:
        if self.redis:
            result = await self.redis.get(f"pipeline:{pipeline_id}:status")
            return result.decode() if result else None
        return None

# Global cache manager
cache_manager = CacheManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    await cache_manager.connect()
    logger.info("Pipeline Service started")
    yield
    # Shutdown
    logger.info("Pipeline Service shutting down")

# FastAPI app
app = FastAPI(
    title="Multi-Tenant Pipeline Service",
    description="Enterprise-grade data ingestion pipeline management",
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
    return {"status": "healthy", "service": "pipeline-service"}

@app.get("/health/database")
async def database_health(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")

@app.get("/health/databricks")
async def databricks_health(organization_id: str = Depends(get_current_organization)):
    try:
        databricks = DatabricksManager(organization_id)
        # Test API connectivity
        clusters = databricks.clusters_api.list_clusters()
        return {"status": "healthy", "databricks": "connected", "clusters": len(clusters.get("clusters", []))}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Databricks unhealthy: {str(e)}")

@app.post("/api/v1/pipelines", response_model=PipelineResponse)
async def create_pipeline(
    pipeline: PipelineCreate,
    background_tasks: BackgroundTasks,
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """Create a new data ingestion pipeline"""
    import uuid
    
    pipeline_id = str(uuid.uuid4())
    
    db_pipeline = Pipeline(
        id=pipeline_id,
        name=pipeline.name,
        organization_id=organization_id,
        source_type=pipeline.source_type,
        destination_type=pipeline.destination_type,
        configuration=json.dumps(pipeline.configuration),
        schedule=pipeline.schedule
    )
    
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    
    # Create Databricks job in background
    background_tasks.add_task(setup_databricks_job, pipeline_id, organization_id, pipeline.dict())
    
    return PipelineResponse(
        id=db_pipeline.id,
        name=db_pipeline.name,
        organization_id=db_pipeline.organization_id,
        source_type=db_pipeline.source_type,
        destination_type=db_pipeline.destination_type,
        configuration=json.loads(db_pipeline.configuration),
        schedule=db_pipeline.schedule,
        is_active=db_pipeline.is_active,
        created_at=db_pipeline.created_at,
        updated_at=db_pipeline.updated_at,
        last_run_at=db_pipeline.last_run_at,
        status=db_pipeline.status
    )

@app.get("/api/v1/pipelines", response_model=List[PipelineResponse])
async def list_pipelines(
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """List all pipelines for the organization"""
    pipelines = db.query(Pipeline).filter(Pipeline.organization_id == organization_id).all()
    
    return [
        PipelineResponse(
            id=p.id,
            name=p.name,
            organization_id=p.organization_id,
            source_type=p.source_type,
            destination_type=p.destination_type,
            configuration=json.loads(p.configuration),
            schedule=p.schedule,
            is_active=p.is_active,
            created_at=p.created_at,
            updated_at=p.updated_at,
            last_run_at=p.last_run_at,
            status=p.status
        )
        for p in pipelines
    ]

@app.get("/api/v1/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """Get a specific pipeline"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.organization_id == organization_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        organization_id=pipeline.organization_id,
        source_type=pipeline.source_type,
        destination_type=pipeline.destination_type,
        configuration=json.loads(pipeline.configuration),
        schedule=pipeline.schedule,
        is_active=pipeline.is_active,
        created_at=pipeline.created_at,
        updated_at=pipeline.updated_at,
        last_run_at=pipeline.last_run_at,
        status=pipeline.status
    )

@app.put("/api/v1/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: str,
    pipeline_update: PipelineUpdate,
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """Update a pipeline"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.organization_id == organization_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    if pipeline_update.name is not None:
        pipeline.name = pipeline_update.name
    if pipeline_update.configuration is not None:
        pipeline.configuration = json.dumps(pipeline_update.configuration)
    if pipeline_update.schedule is not None:
        pipeline.schedule = pipeline_update.schedule
    if pipeline_update.is_active is not None:
        pipeline.is_active = pipeline_update.is_active
    
    pipeline.updated_at = datetime.utcnow()
    
    db.commit()
    db.refresh(pipeline)
    
    return PipelineResponse(
        id=pipeline.id,
        name=pipeline.name,
        organization_id=pipeline.organization_id,
        source_type=pipeline.source_type,
        destination_type=pipeline.destination_type,
        configuration=json.loads(pipeline.configuration),
        schedule=pipeline.schedule,
        is_active=pipeline.is_active,
        created_at=pipeline.created_at,
        updated_at=pipeline.updated_at,
        last_run_at=pipeline.last_run_at,
        status=pipeline.status
    )

@app.post("/api/v1/pipelines/{pipeline_id}/run", response_model=PipelineRunResponse)
async def run_pipeline(
    pipeline_id: str,
    background_tasks: BackgroundTasks,
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """Trigger a pipeline run"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.organization_id == organization_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    if not pipeline.is_active:
        raise HTTPException(status_code=400, detail="Pipeline is not active")
    
    import uuid
    run_id = str(uuid.uuid4())
    
    pipeline_run = PipelineRun(
        id=run_id,
        pipeline_id=pipeline_id,
        organization_id=organization_id,
        status="running"
    )
    
    db.add(pipeline_run)
    db.commit()
    db.refresh(pipeline_run)
    
    # Execute pipeline in background
    background_tasks.add_task(execute_pipeline, run_id, pipeline_id, organization_id)
    
    return PipelineRunResponse(
        id=pipeline_run.id,
        pipeline_id=pipeline_run.pipeline_id,
        organization_id=pipeline_run.organization_id,
        status=pipeline_run.status,
        started_at=pipeline_run.started_at,
        completed_at=pipeline_run.completed_at,
        logs=pipeline_run.logs,
        metrics=json.loads(pipeline_run.metrics) if pipeline_run.metrics else None,
        databricks_run_id=pipeline_run.databricks_run_id
    )

@app.get("/api/v1/pipelines/{pipeline_id}/runs", response_model=List[PipelineRunResponse])
async def list_pipeline_runs(
    pipeline_id: str,
    organization_id: str = Depends(get_current_organization),
    db: Session = Depends(get_db)
):
    """List runs for a specific pipeline"""
    runs = db.query(PipelineRun).filter(
        PipelineRun.pipeline_id == pipeline_id,
        PipelineRun.organization_id == organization_id
    ).order_by(PipelineRun.started_at.desc()).limit(50).all()
    
    return [
        PipelineRunResponse(
            id=run.id,
            pipeline_id=run.pipeline_id,
            organization_id=run.organization_id,
            status=run.status,
            started_at=run.started_at,
            completed_at=run.completed_at,
            logs=run.logs,
            metrics=json.loads(run.metrics) if run.metrics else None,
            databricks_run_id=run.databricks_run_id
        )
        for run in runs
    ]

async def setup_databricks_job(pipeline_id: str, organization_id: str, pipeline_config: Dict):
    """Background task to set up Databricks job"""
    try:
        databricks = DatabricksManager(organization_id)
        job_id = await databricks.create_job(pipeline_config)
        
        # Update pipeline with job ID
        db = SessionLocal()
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if pipeline:
            config = json.loads(pipeline.configuration)
            config["databricks_job_id"] = job_id
            pipeline.configuration = json.dumps(config)
            pipeline.status = "ready"
            db.commit()
        db.close()
        
        logger.info(f"Created Databricks job {job_id} for pipeline {pipeline_id}")
    except Exception as e:
        logger.error(f"Failed to create Databricks job for pipeline {pipeline_id}: {str(e)}")
        
        # Update pipeline status to error
        db = SessionLocal()
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if pipeline:
            pipeline.status = "error"
            db.commit()
        db.close()

async def execute_pipeline(run_id: str, pipeline_id: str, organization_id: str):
    """Background task to execute pipeline"""
    db = SessionLocal()
    
    try:
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        pipeline_run = db.query(PipelineRun).filter(PipelineRun.id == run_id).first()
        
        if not pipeline or not pipeline_run:
            return
        
        config = json.loads(pipeline.configuration)
        databricks_job_id = config.get("databricks_job_id")
        
        if not databricks_job_id:
            raise Exception("No Databricks job configured for this pipeline")
        
        databricks = DatabricksManager(organization_id)
        databricks_run_id = await databricks.run_job(databricks_job_id, config.get("parameters", {}))
        
        # Update run with Databricks run ID
        pipeline_run.databricks_run_id = databricks_run_id
        db.commit()
        
        # Cache status
        await cache_manager.set_pipeline_status(pipeline_id, "running")
        
        # Monitor job execution
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds
            
            run_status = await databricks.get_run_status(databricks_run_id)
            state = run_status["state"]["life_cycle_state"]
            
            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = run_status["state"].get("result_state", "FAILED")
                final_status = "completed" if result_state == "SUCCESS" else "failed"
                
                pipeline_run.status = final_status
                pipeline_run.completed_at = datetime.utcnow()
                pipeline_run.logs = json.dumps(run_status.get("cluster_instance", {}))
                
                # Update pipeline last run
                pipeline.last_run_at = datetime.utcnow()
                pipeline.status = final_status
                
                db.commit()
                
                # Update cache
                await cache_manager.set_pipeline_status(pipeline_id, final_status)
                
                logger.info(f"Pipeline run {run_id} completed with status {final_status}")
                break
                
    except Exception as e:
        logger.error(f"Pipeline run {run_id} failed: {str(e)}")
        
        if pipeline_run:
            pipeline_run.status = "failed"
            pipeline_run.completed_at = datetime.utcnow()
            pipeline_run.logs = str(e)
            db.commit()
        
        await cache_manager.set_pipeline_status(pipeline_id, "failed")
    
    finally:
        db.close()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )