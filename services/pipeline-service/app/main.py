from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from .core.config import get_settings
from .core.database import engine, SessionLocal
from .core.middleware import TenantContextMiddleware
from .api import pipelines, executions, schedules
from .models import Base
from .core.scheduler import scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Pipeline Service")
    Base.metadata.create_all(bind=engine)
    # Start scheduler
    scheduler.start()
    yield
    # Stop scheduler
    scheduler.shutdown()
    logger.info("Shutting down Pipeline Service")

app = FastAPI(
    title="Multi-Tenant Pipeline Service",
    description="Pipeline management and execution service for multi-tenant data ingestion platform",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(TenantContextMiddleware)

app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
app.include_router(executions.router, prefix="/api/v1/executions", tags=["executions"])
app.include_router(schedules.router, prefix="/api/v1/schedules", tags=["schedules"])

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "pipeline-service"}
