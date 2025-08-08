"""Databricks integration service."""

import json
import uuid
from typing import Dict, Any, Optional
from datetime import datetime

import httpx
from databricks import sql

from app.core.config import settings
from app.core.security import get_organization_config


class DatabricksService:
    """Service for managing Databricks jobs and clusters."""
    
    def __init__(self, organization_id: str):
        self.organization_id = organization_id
        self.config = self._get_organization_config()
        
    def _get_organization_config(self) -> Dict[str, Any]:
        """Get organization-specific Databricks configuration."""
        # In production, this would fetch from AWS Secrets Manager
        return {
            "host": settings.DATABRICKS_HOST,
            "token": settings.DATABRICKS_TOKEN,
            "workspace_id": settings.DATABRICKS_WORKSPACE_ID
        }
    
    async def health_check(self) -> bool:
        """Check Databricks connectivity."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.config['host']}/api/2.0/clusters/list",
                    headers={
                        "Authorization": f"Bearer {self.config['token']}"
                    }
                )
                return response.status_code == 200
        except Exception:
            return False
    
    async def create_job_for_pipeline(self, pipeline_id: uuid.UUID) -> str:
        """Create a Databricks job for a pipeline."""
        from app.core.database import get_database
        from app.models.pipeline import Pipeline
        from sqlalchemy import select
        
        # Get pipeline details
        async with get_database() as db:
            query = select(Pipeline).where(Pipeline.id == pipeline_id)
            result = await db.execute(query)
            pipeline = result.scalar_one()
        
        # Generate PySpark notebook
        notebook_content = self._generate_pyspark_notebook(pipeline)
        
        # Create job configuration
        job_config = {
            "name": f"{self.organization_id}-{pipeline.name}",
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
                "notebook_path": f"/pipelines/{pipeline.id}",
                "base_parameters": {
                    "organization_id": self.organization_id,
                    "pipeline_id": str(pipeline.id)
                }
            },
            "timeout_seconds": pipeline.timeout_seconds or 3600,
            "max_concurrent_runs": pipeline.max_concurrent_runs or 1,
            "tags": {
                "organization": self.organization_id,
                "pipeline_id": str(pipeline.id),
                "created_by": "multi-tenant-framework"
            }
        }
        
        # Create job via Databricks API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.config['host']}/api/2.1/jobs/create",
                headers={
                    "Authorization": f"Bearer {self.config['token']}",
                    "Content-Type": "application/json"
                },
                json=job_config
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to create Databricks job: {response.text}")
            
            job_id = response.json()["job_id"]
            
            # Update pipeline with job ID
            pipeline.databricks_job_id = str(job_id)
            await db.commit()
            
            return str(job_id)
    
    def _generate_pyspark_notebook(self, pipeline) -> str:
        """Generate PySpark notebook content for pipeline."""
        from app.templates.pyspark_template import PYSPARK_TEMPLATE
        
        # Replace template variables
        notebook_content = PYSPARK_TEMPLATE.format(
            organization_id=self.organization_id,
            pipeline_id=pipeline.id,
            pipeline_name=pipeline.name,
            source_config=json.dumps(pipeline.source_config, indent=2),
            target_config=json.dumps(pipeline.target_config, indent=2),
            transformation_config=json.dumps(pipeline.transformation_config or {}, indent=2)
        )
        
        return notebook_content
    
    async def trigger_pipeline_run(self, pipeline_id: uuid.UUID) -> str:
        """Trigger a pipeline run."""
        from app.core.database import get_database
        from app.models.pipeline import Pipeline, PipelineRun
        from sqlalchemy import select
        
        # Get pipeline
        async with get_database() as db:
            query = select(Pipeline).where(Pipeline.id == pipeline_id)
            result = await db.execute(query)
            pipeline = result.scalar_one()
            
            if not pipeline.databricks_job_id:
                raise Exception("Pipeline does not have associated Databricks job")
            
            # Create run record
            run = PipelineRun(
                organization_id=self.organization_id,
                pipeline_id=pipeline_id,
                run_id=str(uuid.uuid4()),
                status="running",
                databricks_job_id=pipeline.databricks_job_id,
                start_time=datetime.utcnow()
            )
            
            db.add(run)
            await db.commit()
            await db.refresh(run)
            
            # Trigger Databricks job
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.config['host']}/api/2.1/jobs/run-now",
                    headers={
                        "Authorization": f"Bearer {self.config['token']}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "job_id": int(pipeline.databricks_job_id),
                        "notebook_params": {
                            "run_id": run.run_id,
                            "organization_id": self.organization_id
                        }
                    }
                )
                
                if response.status_code != 200:
                    run.status = "failed"
                    run.error_message = f"Failed to trigger Databricks run: {response.text}"
                    await db.commit()
                    raise Exception(run.error_message)
                
                databricks_run_id = response.json()["run_id"]
                run.databricks_run_id = str(databricks_run_id)
                await db.commit()
                
                return run.run_id
