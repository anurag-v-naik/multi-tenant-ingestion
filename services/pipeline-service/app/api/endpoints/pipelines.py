"""Pipeline API endpoints."""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func

from app.core.database import get_database
from app.core.security import get_current_organization
from app.models.organization import Organization
from app.models.pipeline import Pipeline, PipelineCreate, PipelineUpdate, PipelineResponse
from app.services.databricks_service import DatabricksService
from app.services.unity_catalog_service import UnityCatalogService

router = APIRouter()


@router.get("/", response_model=List[PipelineResponse])
async def list_pipelines(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    pipeline_type: Optional[str] = Query(None),
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """List pipelines for the organization."""
    
    query = select(Pipeline).where(Pipeline.organization_id == organization.id)
    
    if status:
        query = query.where(Pipeline.status == status)
    if pipeline_type:
        query = query.where(Pipeline.pipeline_type == pipeline_type)
    
    query = query.offset(skip).limit(limit)
    
    result = await db.execute(query)
    pipelines = result.scalars().all()
    
    return pipelines


@router.post("/", response_model=PipelineResponse, status_code=status.HTTP_201_CREATED)
async def create_pipeline(
    pipeline_data: PipelineCreate,
    background_tasks: BackgroundTasks,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Create a new pipeline."""
    
    # Check if pipeline name already exists for organization
    existing_query = select(Pipeline).where(
        and_(
            Pipeline.organization_id == organization.id,
            Pipeline.name == pipeline_data.name
        )
    )
    existing_result = await db.execute(existing_query)
    existing_pipeline = existing_result.scalar_one_or_none()
    
    if existing_pipeline:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Pipeline with name '{pipeline_data.name}' already exists"
        )
    
    # Create pipeline instance
    pipeline = Pipeline(
        organization_id=organization.id,
        name=pipeline_data.name,
        description=pipeline_data.description,
        pipeline_type=pipeline_data.pipeline_type,
        status=pipeline_data.status,
        source_config=pipeline_data.source_config.dict(),
        target_config=pipeline_data.target_config.dict(),
        transformation_config=pipeline_data.transformation_config.dict() if pipeline_data.transformation_config else None,
        schedule_config=pipeline_data.schedule_config.dict() if pipeline_data.schedule_config else None
    )
    
    db.add(pipeline)
    await db.commit()
    await db.refresh(pipeline)
    
    # Create Databricks job in background
    background_tasks.add_task(
        create_databricks_job,
        pipeline.id,
        organization.id
    )
    
    return pipeline


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: UUID,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Get a specific pipeline."""
    
    query = select(Pipeline).where(
        and_(
            Pipeline.id == pipeline_id,
            Pipeline.organization_id == organization.id
        )
    )
    result = await db.execute(query)
    pipeline = result.scalar_one_or_none()
    
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )
    
    return pipeline


@router.put("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: UUID,
    pipeline_data: PipelineUpdate,
    background_tasks: BackgroundTasks,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Update a pipeline."""
    
    query = select(Pipeline).where(
        and_(
            Pipeline.id == pipeline_id,
            Pipeline.organization_id == organization.id
        )
    )
    result = await db.execute(query)
    pipeline = result.scalar_one_or_none()
    
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )
    
    # Update pipeline fields
    update_data = pipeline_data.dict(exclude_unset=True)
    for field, value in update_data.items():
        if hasattr(pipeline, field):
            if field in ['source_config', 'target_config', 'transformation_config', 'schedule_config']:
                if value is not None:
                    setattr(pipeline, field, value.dict() if hasattr(value, 'dict') else value)
            else:
                setattr(pipeline, field, value)
    
    await db.commit()
    await db.refresh(pipeline)
    
    # Update Databricks job in background
    background_tasks.add_task(
        update_databricks_job,
        pipeline.id,
        organization.id
    )
    
    return pipeline


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pipeline(
    pipeline_id: UUID,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Delete a pipeline."""
    
    query = select(Pipeline).where(
        and_(
            Pipeline.id == pipeline_id,
            Pipeline.organization_id == organization.id
        )
    )
    result = await db.execute(query)
    pipeline = result.scalar_one_or_none()
    
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )
    
    await db.delete(pipeline)
    await db.commit()


@router.post("/{pipeline_id}/run", status_code=status.HTTP_202_ACCEPTED)
async def trigger_pipeline_run(
    pipeline_id: UUID,
    background_tasks: BackgroundTasks,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Trigger a pipeline run."""
    
    query = select(Pipeline).where(
        and_(
            Pipeline.id == pipeline_id,
            Pipeline.organization_id == organization.id
        )
    )
    result = await db.execute(query)
    pipeline = result.scalar_one_or_none()
    
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline not found"
        )
    
    if pipeline.status != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Pipeline must be active to run"
        )
    
    # Trigger run in background
    background_tasks.add_task(
        trigger_databricks_run,
        pipeline.id,
        organization.id
    )
    
    return {"message": "Pipeline run triggered", "pipeline_id": pipeline_id}


async def create_databricks_job(pipeline_id: UUID, organization_id: str):
    """Create Databricks job for pipeline."""
    try:
        databricks_service = DatabricksService(organization_id)
        await databricks_service.create_job_for_pipeline(pipeline_id)
    except Exception as e:
        logger.error(f"Failed to create Databricks job for pipeline {pipeline_id}: {e}")


async def update_databricks_job(pipeline_id: UUID, organization_id: str):
    """Update Databricks job for pipeline."""
    try:
        databricks_service = DatabricksService(organization_id)
        await databricks_service.update_job_for_pipeline(pipeline_id)
    except Exception as e:
        logger.error(f"Failed to update Databricks job for pipeline {pipeline_id}: {e}")


async def trigger_databricks_run(pipeline_id: UUID, organization_id: str):
    """Trigger Databricks job run."""
    try:
        databricks_service = DatabricksService(organization_id)
        await databricks_service.trigger_pipeline_run(pipeline_id)
    except Exception as e:
        logger.error(f"Failed to trigger run for pipeline {pipeline_id}: {e}")
