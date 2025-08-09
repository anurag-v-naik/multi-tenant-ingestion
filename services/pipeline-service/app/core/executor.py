import asyncio
import json
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from ..models.pipeline import PipelineExecution, ExecutionStatus
from ..models.connection import DataConnection
from ..core.databricks_client import DatabricksClient
from ..core.notebook_generator import NotebookGenerator
import logging

logger = logging.getLogger(__name__)


class PipelineExecutor:
    """Handles pipeline execution logic"""

    def __init__(self, db: Session):
        self.db = db
        self.databricks_client = DatabricksClient()
        self.notebook_generator = NotebookGenerator()

    async def execute_pipeline(self, execution_id: str, parameters: Dict[str, Any] = None):
        """Execute a pipeline"""

        execution = self.db.query(PipelineExecution).filter(
            PipelineExecution.id == execution_id
        ).first()

        if not execution:
            logger.error(f"Execution {execution_id} not found")
            return

        try:
            # Update status to running
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.utcnow()
            self.db.commit()

            # Get pipeline and connections
            pipeline = execution.pipeline
            version = execution.version

            source_connection = self.db.query(DataConnection).filter(
                DataConnection.id == pipeline.source_connection_id
            ).first()

            target_connection = self.db.query(DataConnection).filter(
                DataConnection.id == pipeline.target_connection_id
            ).first()

            # Generate notebook
            notebook_content = await self.notebook_generator.generate_notebook(
                pipeline=pipeline,
                version=version,
                source_connection=source_connection,
                target_connection=target_connection,
                parameters=parameters or {}
            )

            # Submit to Databricks
            job_result = await self.databricks_client.submit_notebook(
                tenant_id=execution.tenant_id,
                pipeline_id=pipeline.id,
                execution_id=execution.id,
                notebook_content=notebook_content,
                cluster_config=pipeline.databricks_cluster_config
            )

            # Update execution with Databricks job info
            execution.databricks_job_id = job_result.get("job_id")
            execution.databricks_run_id = job_result.get("run_id")
            self.db.commit()

            # Monitor execution
            await self._monitor_execution(execution)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.utcnow()
            self.db.commit()

    async def _monitor_execution(self, execution: PipelineExecution):
        """Monitor Databricks job execution"""

        max_poll_time = 3600  # 1 hour
        poll_interval = 30  # 30 seconds
        start_time = datetime.utcnow()

        while True:
            try:
                # Check if we've exceeded max poll time
                if (datetime.utcnow() - start_time).total_seconds() > max_poll_time:
                    execution.status = ExecutionStatus.TIMEOUT
                    execution.error_message = "Execution timed out"
                    execution.completed_at = datetime.utcnow()
                    self.db.commit()
                    break

                # Get job status from Databricks
                job_status = await self.databricks_client.get_job_status(
                    run_id=execution.databricks_run_id
                )

                if job_status["state"]["life_cycle_state"] == "TERMINATED":
                    if job_status["state"]["result_state"] == "SUCCESS":
                        execution.status = ExecutionStatus.SUCCESS

                        # Get execution metrics
                        metrics = await self.databricks_client.get_job_metrics(
                            run_id=execution.databricks_run_id
                        )

                        execution.records_processed = metrics.get("records_processed", 0)
                        execution.records_failed = metrics.get("records_failed", 0)
                        execution.bytes_processed = metrics.get("bytes_processed", 0)
                        execution.execution_time_seconds = metrics.get("execution_time_seconds", 0)

                    else:
                        execution.status = ExecutionStatus.FAILED
                        execution.error_message = job_status["state"].get("state_message", "Job failed")
                        execution.error_details = job_status

                    execution.completed_at = datetime.utcnow()
                    self.db.commit()
                    break

                elif job_status["state"]["life_cycle_state"] in ["INTERNAL_ERROR", "SKIPPED"]:
                    execution.status = ExecutionStatus.FAILED
                    execution.error_message = f"Job {job_status['state']['life_cycle_state']}"
                    execution.error_details = job_status
                    execution.completed_at = datetime.utcnow()
                    self.db.commit()
                    break

                # Wait before next poll
                await asyncio.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error monitoring execution {execution.id}: {str(e)}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = f"Monitoring error: {str(e)}"
                execution.completed_at = datetime.utcnow()
                self.db.commit()
                break
