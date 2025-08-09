from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from ..core.database import get_db
from ..core.executor import PipelineExecutor
from ..models.pipeline import Pipeline, PipelineVersion, PipelineExecution, PipelineStatus, PipelineType, \
    ExecutionStatus
from ..models.connection import DataConnection
from ..utils.auth import get_current_user, require_tenant_permission
from ..utils.tenant_context import get_tenant_context
from ..models.user import User

router = APIRouter()


class CreatePipelineRequest(BaseModel):
    name: str = Field(..., description="Pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")
    pipeline_type: PipelineType = Field(PipelineType.BATCH, description="Pipeline type")
    source_connection_id: str = Field(..., description="Source connection ID")
    target_connection_id: str = Field(..., description="Target connection ID")
    transformation_config: Dict[str, Any] = Field(default_factory=dict, description="Transformation configuration")
    schedule_config: Dict[str, Any] = Field(default_factory=dict, description="Schedule configuration")
    databricks_cluster_config: Dict[str, Any] = Field(default_factory=dict,
                                                      description="Databricks cluster configuration")


class UpdatePipelineRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[PipelineStatus] = None
    transformation_config: Optional[Dict[str, Any]] = None
    schedule_config: Optional[Dict[str, Any]] = None
    databricks_cluster_config: Optional[Dict[str, Any]] = None


class PipelineResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    pipeline_type: str
    status: str
    source_connection_id: str
    target_connection_id: str
    transformation_config: Dict[str, Any]
    schedule_config: Dict[str, Any]
    databricks_cluster_config: Dict[str, Any]
    created_at: str
    updated_at: str
    current_version: Optional[str] = None
    last_execution: Optional[Dict[str, Any]] = None


class ExecutePipelineRequest(BaseModel):
    version_id: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


@router.post("/", response_model=PipelineResponse)
async def create_pipeline(
        request: CreatePipelineRequest,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """Create a new pipeline"""

    require_tenant_permission(current_user, "pipeline:create", tenant_context.tenant_id)

    # Validate connections exist and belong to tenant
    source_conn = db.query(DataConnection).filter(
        DataConnection.id == request.source_connection_id,
        DataConnection.tenant_id == tenant_context.tenant_id,
        DataConnection.is_active == True
    ).first()

    if not source_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Source connection not found"
        )

    target_conn = db.query(DataConnection).filter(
        DataConnection.id == request.target_connection_id,
        DataConnection.tenant_id == tenant_context.tenant_id,
        DataConnection.is_active == True
    ).first()

    if not target_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Target connection not found"
        )

    # Create pipeline
    pipeline = Pipeline(
        tenant_id=tenant_context.tenant_id,
        name=request.name,
        description=request.description,
        pipeline_type=request.pipeline_type,
        source_connection_id=request.source_connection_id,
        target_connection_id=request.target_connection_id,
        transformation_config=request.transformation_config,
        schedule_config=request.schedule_config,
        databricks_cluster_config=request.databricks_cluster_config,
        created_by=current_user.id
    )

    db.add(pipeline)
    db.flush()

    # Create initial version
    initial_version = PipelineVersion(
        tenant_id=tenant_context.tenant_id,
        pipeline_id=pipeline.id,
        version="1.0.0",
        description="Initial version",
        configuration={
            "source_connection_id": request.source_connection_id,
            "target_connection_id": request.target_connection_id,
            "transformation_config": request.transformation_config,
            "databricks_cluster_config": request.databricks_cluster_config
        },
        is_current=True,
        created_by=current_user.id
    )

    db.add(initial_version)
    db.commit()
    db.refresh(pipeline)

    return PipelineResponse(
        id=str(pipeline.id),
        name=pipeline.name,
        description=pipeline.description,
        pipeline_type=pipeline.pipeline_type.value,
        status=pipeline.status.value,
        source_connection_id=str(pipeline.source_connection_id),
        target_connection_id=str(pipeline.target_connection_id),
        transformation_config=pipeline.transformation_config,
        schedule_config=pipeline.schedule_config,
        databricks_cluster_config=pipeline.databricks_cluster_config,
        created_at=pipeline.created_at.isoformat(),
        updated_at=pipeline.updated_at.isoformat(),
        current_version="1.0.0"
    )


@router.get("/", response_model=List[PipelineResponse])
async def list_pipelines(
        skip: int = 0,
        limit: int = 100,
        status: Optional[PipelineStatus] = None,
        pipeline_type: Optional[PipelineType] = None,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """List pipelines for the current tenant"""

    require_tenant_permission(current_user, "pipeline:read", tenant_context.tenant_id)

    query = db.query(Pipeline).filter(
        Pipeline.tenant_id == tenant_context.tenant_id,
        Pipeline.is_active == True
    )

    if status:
        query = query.filter(Pipeline.status == status)

    if pipeline_type:
        query = query.filter(Pipeline.pipeline_type == pipeline_type)

    pipelines = query.offset(skip).limit(limit).all()

    result = []
    for pipeline in pipelines:
        # Get current version
        current_version = db.query(PipelineVersion).filter(
            PipelineVersion.pipeline_id == pipeline.id,
            PipelineVersion.is_current == True
        ).first()

        # Get last execution
        last_execution = db.query(PipelineExecution).filter(
            PipelineExecution.pipeline_id == pipeline.id
        ).order_by(PipelineExecution.created_at.desc()).first()

        last_exec_data = None
        if last_execution:
            last_exec_data = {
                "id": str(last_execution.id),
                "status": last_execution.status.value,
                "started_at": last_execution.started_at.isoformat() if last_execution.started_at else None,
                "completed_at": last_execution.completed_at.isoformat() if last_execution.completed_at else None
            }

        result.append(PipelineResponse(
            id=str(pipeline.id),
            name=pipeline.name,
            description=pipeline.description,
            pipeline_type=pipeline.pipeline_type.value,
            status=pipeline.status.value,
            source_connection_id=str(pipeline.source_connection_id),
            target_connection_id=str(pipeline.target_connection_id),
            transformation_config=pipeline.transformation_config,
            schedule_config=pipeline.schedule_config,
            databricks_cluster_config=pipeline.databricks_cluster_config,
            created_at=pipeline.created_at.isoformat(),
            updated_at=pipeline.updated_at.isoformat(),
            current_version=current_version.version if current_version else None,
            last_execution=last_exec_data
        ))

    return result


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
        pipeline_id: str,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """Get pipeline by ID"""

    require_tenant_permission(current_user, "pipeline:read", tenant_context.tenant_id)

    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_context.tenant_id,
        Pipeline.is_active == True
    ).first()

    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )

    # Get current version
    current_version = db.query(PipelineVersion).filter(
        PipelineVersion.pipeline_id == pipeline.id,
        PipelineVersion.is_current == True
    ).first()

    # Get last execution
    last_execution = db.query(PipelineExecution).filter(
        PipelineExecution.pipeline_id == pipeline.id
    ).order_by(PipelineExecution.created_at.desc()).first()

    last_exec_data = None
    if last_execution:
        last_exec_data = {
            "id": str(last_execution.id),
            "status": last_execution.status.value,
            "started_at": last_execution.started_at.isoformat() if last_execution.started_at else None,
            "completed_at": last_execution.completed_at.isoformat() if last_execution.completed_at else None,
            "records_processed": last_execution.records_processed,
            "records_failed": last_execution.records_failed
        }

    return PipelineResponse(
        id=str(pipeline.id),
        name=pipeline.name,
        description=pipeline.description,
        pipeline_type=pipeline.pipeline_type.value,
        status=pipeline.status.value,
        source_connection_id=str(pipeline.source_connection_id),
        target_connection_id=str(pipeline.target_connection_id),
        transformation_config=pipeline.transformation_config,
        schedule_config=pipeline.schedule_config,
        databricks_cluster_config=pipeline.databricks_cluster_config,
        created_at=pipeline.created_at.isoformat(),
        updated_at=pipeline.updated_at.isoformat(),
        current_version=current_version.version if current_version else None,
        last_execution=last_exec_data
    )


@router.patch("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
        pipeline_id: str,
        request: UpdatePipelineRequest,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """Update pipeline"""

    require_tenant_permission(current_user, "pipeline:update", tenant_context.tenant_id)

    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_context.tenant_id,
        Pipeline.is_active == True
    ).first()

    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )

    # Update fields
    update_data = request.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(pipeline, field, value)

    pipeline.updated_by = current_user.id

    db.commit()
    db.refresh(pipeline)

    # Get current version for response
    current_version = db.query(PipelineVersion).filter(
        PipelineVersion.pipeline_id == pipeline.id,
        PipelineVersion.is_current == True
    ).first()

    return PipelineResponse(
        id=str(pipeline.id),
        name=pipeline.name,
        description=pipeline.description,
        pipeline_type=pipeline.pipeline_type.value,
        status=pipeline.status.value,
        source_connection_id=str(pipeline.source_connection_id),
        target_connection_id=str(pipeline.target_connection_id),
        transformation_config=pipeline.transformation_config,
        schedule_config=pipeline.schedule_config,
        databricks_cluster_config=pipeline.databricks_cluster_config,
        created_at=pipeline.created_at.isoformat(),
        updated_at=pipeline.updated_at.isoformat(),
        current_version=current_version.version if current_version else None
    )


@router.post("/{pipeline_id}/execute")
async def execute_pipeline(
        pipeline_id: str,
        request: ExecutePipelineRequest,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """Execute pipeline"""

    require_tenant_permission(current_user, "pipeline:execute", tenant_context.tenant_id)

    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_context.tenant_id,
        Pipeline.is_active == True
    ).first()

    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )

    if pipeline.status != PipelineStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline status is {pipeline.status.value}, must be active to execute"
        )

    # Get version to execute
    if request.version_id:
        version = db.query(PipelineVersion).filter(
            PipelineVersion.id == request.version_id,
            PipelineVersion.pipeline_id == pipeline.id
        ).first()
        if not version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Pipeline version not found"
            )
    else:
        version = db.query(PipelineVersion).filter(
            PipelineVersion.pipeline_id == pipeline.id,
            PipelineVersion.is_current == True
        ).first()
        if not version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No current version found for pipeline"
            )

    # Create execution record
    execution = PipelineExecution(
        tenant_id=tenant_context.tenant_id,
        pipeline_id=pipeline.id,
        version_id=version.id,
        status=ExecutionStatus.PENDING,
        created_by=current_user.id
    )

    db.add(execution)
    db.commit()
    db.refresh(execution)

    # Execute pipeline in background
    executor = PipelineExecutor(db)
    background_tasks.add_task(
        executor.execute_pipeline,
        execution.id,
        request.parameters
    )

    return {
        "execution_id": str(execution.id),
        "status": execution.status.value,
        "message": "Pipeline execution started"
    }


@router.delete("/{pipeline_id}")
async def delete_pipeline(
        pipeline_id: str,
        current_user: User = Depends(get_current_user),
        tenant_context=Depends(get_tenant_context),
        db: Session = Depends(get_db)
):
    """Soft delete pipeline"""

    require_tenant_permission(current_user, "pipeline:delete", tenant_context.tenant_id)

    pipeline = db.query(Pipeline).filter(
        Pipeline.id == pipeline_id,
        Pipeline.tenant_id == tenant_context.tenant_id,
        Pipeline.is_active == True
    ).first()

    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )

    # Check if pipeline has running executions
    running_executions = db.query(PipelineExecution).filter(
        PipelineExecution.pipeline_id == pipeline.id,
        PipelineExecution.status.in_([ExecutionStatus.PENDING, ExecutionStatus.RUNNING])
    ).count()

    if running_executions > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete pipeline with running executions"
        )

    # Soft delete
    pipeline.is_active = False
    pipeline.status = PipelineStatus.ARCHIVED
    pipeline.updated_by = current_user.id

    db.commit()

    return {"message": "Pipeline deleted successfully"}
