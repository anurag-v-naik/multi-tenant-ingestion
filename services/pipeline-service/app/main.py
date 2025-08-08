# services/pipeline-service/app/main.py
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
import asyncio
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import httpx
import uvicorn
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import redis

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/pipeline_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis setup
redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

# Models
class Pipeline(Base):
    __tablename__ = "pipelines"
    
    id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    source_config = Column(Text)  # JSON
    destination_config = Column(Text)  # JSON
    transformation_config = Column(Text)  # JSON
    schedule = Column(String)
    status = Column(String, default="draft")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String)

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    
    id = Column(String, primary_key=True)
    pipeline_id = Column(String, nullable=False, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    status = Column(String, default="pending")
    databricks_run_id = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)
    metrics = Column(Text)  # JSON

# Pydantic models
class PipelineCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source_config: Dict[str, Any]
    destination_config: Dict[str, Any]
    transformation_config: Optional[Dict[str, Any]] = {}
    schedule: Optional[str] = None

class PipelineUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    source_config: Optional[Dict[str, Any]] = None
    destination_config: Optional[Dict[str, Any]] = None
    transformation_config: Optional[Dict[str, Any]] = None
    schedule: Optional[str] = None
    status: Optional[str] = None

class PipelineRunCreate(BaseModel):
    pipeline_id: str
    config_overrides: Optional[Dict[str, Any]] = {}

# FastAPI app
app = FastAPI(
    title="Multi-Tenant Pipeline Service",
    description="Enterprise data pipeline orchestration service",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

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

# Databricks integration
class DatabricksClient:
    def __init__(self):
        self.base_url = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        
    async def submit_notebook_run(self, tenant_id: str, pipeline_config: Dict[str, Any]) -> str:
        """Submit a notebook run to Databricks workspace"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Generate notebook parameters
        notebook_params = {
            "tenant_id": tenant_id,
            "source_config": json.dumps(pipeline_config["source_config"]),
            "destination_config": json.dumps(pipeline_config["destination_config"]),
            "transformation_config": json.dumps(pipeline_config.get("transformation_config", {}))
        }
        
        payload = {
            "run_name": f"Pipeline Run - {pipeline_config['name']} - {datetime.utcnow().isoformat()}",
            "notebook_task": {
                "notebook_path": f"/Workspaces/{tenant_id}/pipeline_execution_template",
                "base_parameters": notebook_params
            },
            "cluster_spec": {
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                    "spark_conf": {
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    "custom_tags": {
                        "tenant_id": tenant_id,
                        "service": "multi-tenant-ingestion"
                    }
                }
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/2.1/jobs/runs/submit",
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Databricks API error: {response.text}")
            
            return response.json()["run_id"]
    
    async def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a Databricks run"""
        headers = {"Authorization": f"Bearer {self.token}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/2.1/jobs/runs/get",
                headers=headers,
                params={"run_id": run_id}
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Databricks API error: {response.text}")
            
            return response.json()

databricks_client = DatabricksClient()

# API Routes
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "pipeline-service", "timestamp": datetime.utcnow()}

@app.get("/health/database")
async def health_database(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        return {"status": "healthy", "component": "database"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")

@app.get("/health/databricks")
async def health_databricks():
    try:
        headers = {"Authorization": f"Bearer {databricks_client.token}"}
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{databricks_client.base_url}/api/2.0/clusters/list", headers=headers)
            if response.status_code == 200:
                return {"status": "healthy", "component": "databricks"}
            else:
                raise HTTPException(status_code=503, detail="Databricks API error")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Databricks unhealthy: {str(e)}")

@app.post("/api/v1/pipelines")
async def create_pipeline(
    pipeline: PipelineCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Create a new data pipeline"""
    pipeline_id = f"pipeline_{tenant_id}_{int(datetime.utcnow().timestamp())}"
    
    db_pipeline = Pipeline(
        id=pipeline_id,
        tenant_id=tenant_id,
        name=pipeline.name,
        description=pipeline.description,
        source_config=json.dumps(pipeline.source_config),
        destination_config=json.dumps(pipeline.destination_config),
        transformation_config=json.dumps(pipeline.transformation_config),
        schedule=pipeline.schedule,
        status="draft"
    )
    
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    
    return {"pipeline_id": pipeline_id, "status": "created"}

@app.get("/api/v1/pipelines")
async def list_pipelines(
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all pipelines for a tenant"""
    pipelines = db.query(Pipeline).filter(Pipeline.tenant_id == tenant_id).all()
    
    result = []
    for pipeline in pipelines:
        result.append({
            "id": pipeline.id,
            "name": pipeline.name,
            "description": pipeline.description,
            "status": pipeline.status,
            "created_at": pipeline.created_at,
            "updated_at": pipeline.updated_at
        })
    
    return {"pipelines": result}

@app.get("/api/v1/pipelines/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get a specific pipeline"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "description": pipeline.description,
        "source_config": json.loads(pipeline.source_config),
        "destination_config": json.loads(pipeline.destination_config),
        "transformation_config": json.loads(pipeline.transformation_config or "{}"),
        "schedule": pipeline.schedule,
        "status": pipeline.status,
        "created_at": pipeline.created_at,
        "updated_at": pipeline.updated_at
    }

@app.put("/api/v1/pipelines/{pipeline_id}")
async def update_pipeline(
    pipeline_id: str,
    pipeline_update: PipelineUpdate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Update a pipeline"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    update_data = pipeline_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        if field in ["source_config", "destination_config", "transformation_config"] and value:
            setattr(pipeline, field, json.dumps(value))
        else:
            setattr(pipeline, field, value)
    
    pipeline.updated_at = datetime.utcnow()
    db.commit()
    
    return {"status": "updated"}

@app.post("/api/v1/pipelines/{pipeline_id}/run")
async def run_pipeline(
    pipeline_id: str,
    run_config: PipelineRunCreate,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Execute a pipeline"""
    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_id
    ).first()
    
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    # Create pipeline run record
    run_id = f"run_{pipeline_id}_{int(datetime.utcnow().timestamp())}"
    
    db_run = PipelineRun(
        id=run_id,
        pipeline_id=pipeline_id,
        tenant_id=tenant_id,
        status="submitted",
        started_at=datetime.utcnow()
    )
    
    db.add(db_run)
    db.commit()
    
    # Submit to Databricks
    try:
        pipeline_config = {
            "name": pipeline.name,
            "source_config": json.loads(pipeline.source_config),
            "destination_config": json.loads(pipeline.destination_config),
            "transformation_config": json.loads(pipeline.transformation_config or "{}")
        }
        
        # Apply config overrides
        if run_config.config_overrides:
            pipeline_config.update(run_config.config_overrides)
        
        databricks_run_id = await databricks_client.submit_notebook_run(tenant_id, pipeline_config)
        
        # Update run with Databricks run ID
        db_run.databricks_run_id = databricks_run_id
        db_run.status = "running"
        db.commit()
        
        return {"run_id": run_id, "databricks_run_id": databricks_run_id, "status": "submitted"}
        
    except Exception as e:
        db_run.status = "failed"
        db_run.error_message = str(e)
        db_run.completed_at = datetime.utcnow()
        db.commit()
        raise HTTPException(status_code=500, detail=f"Failed to submit pipeline: {str(e)}")

@app.get("/api/v1/pipelines/{pipeline_id}/runs")
async def list_pipeline_runs(
    pipeline_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """List all runs for a pipeline"""
    runs = db.query(PipelineRun).filter(
        PipelineRun.pipeline_id == pipeline_id,
        PipelineRun.tenant_id == tenant_id
    ).order_by(PipelineRun.started_at.desc()).all()
    
    result = []
    for run in runs:
        result.append({
            "id": run.id,
            "status": run.status,
            "databricks_run_id": run.databricks_run_id,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "error_message": run.error_message
        })
    
    return {"runs": result}

@app.get("/api/v1/runs/{run_id}/status")
async def get_run_status(
    run_id: str,
    tenant_id: str = Depends(get_tenant_id),
    db: Session = Depends(get_db)
):
    """Get status of a specific run"""
    run = db.query(PipelineRun).filter(
        PipelineRun.id == run_id,
        PipelineRun.tenant_id == tenant_id
    ).first()
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Get latest status from Databricks if run is active
    if run.databricks_run_id and run.status in ["submitted", "running"]:
        try:
            databricks_status = await databricks_client.get_run_status(run.databricks_run_id)
            
            # Update local status based on Databricks status
            state = databricks_status.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            result_state = state.get("result_state")
            
            if life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    run.status = "completed"
                else:
                    run.status = "failed"
                    run.error_message = state.get("state_message", "Unknown error")
                run.completed_at = datetime.utcnow()
                db.commit()
            
        except Exception as e:
            print(f"Error getting Databricks status: {e}")
    
    return {
        "id": run.id,
        "status": run.status,
        "databricks_run_id": run.databricks_run_id,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "error_message": run.error_message
    }

# Create tables
Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
