"""Pipeline run API endpoints."""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc

from app.core.database import get_database
from app.core.security import get_current_organization
from app.models.organization import Organization
from app.models.pipeline import PipelineRun

router = APIRouter()


@router.get("/", response_model=List[dict])
async def list_pipeline_runs(
    pipeline_id: Optional[UUID] = Query(None),
    status: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """List pipeline runs for the organization."""
    
    query = select(PipelineRun).where(PipelineRun.organization_id == organization.id)
    
    if pipeline_id:
        query = query.where(PipelineRun.pipeline_id == pipeline_id)
    if status:
        query = query.where(PipelineRun.status == status)
    
    query = query.order_by(desc(PipelineRun.created_at)).offset(skip).limit(limit)
    
    result = await db.execute(query)
    runs = result.scalars().all()
    
    return [run.to_dict() for run in runs]


@router.get("/{run_id}", response_model=dict)
async def get_pipeline_run(
    run_id: UUID,
    organization: Organization = Depends(get_current_organization),
    db: AsyncSession = Depends(get_database)
):
    """Get a specific pipeline run."""
    
    query = select(PipelineRun).where(
        and_(
            PipelineRun.id == run_id,
            PipelineRun.organization_id == organization.id
        )
    )
    result = await db.execute(query)
    run = result.scalar_one_or_none()
    
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pipeline run not found"
        )
    
    return run.to_dict()
