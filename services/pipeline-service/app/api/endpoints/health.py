"""Health check endpoints."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_database
from app.core.config import settings

router = APIRouter()


@router.get("/")
async def health_check():
    """Basic health check."""
    return {
        "status": "healthy",
        "service": "pipeline-service",
        "version": "1.0.0"
    }


@router.get("/database")
async def database_health(db: AsyncSession = Depends(get_database)):
    """Database connectivity health check."""
    try:
        result = await db.execute(text("SELECT 1"))
        result.scalar()
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)}"
        )


@router.get("/databricks")
async def databricks_health():
    """Databricks connectivity health check."""
    try:
        from app.services.databricks_service import DatabricksService
        
        # Test with default organization
        databricks_service = DatabricksService("default")
        is_healthy = await databricks_service.health_check()
        
        if is_healthy:
            return {
                "status": "healthy",
                "databricks": "connected"
            }
        else:
            raise HTTPException(
                status_code=503,
                detail="Databricks connection failed"
            )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Databricks health check failed: {str(e)}"
        )
