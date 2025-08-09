from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from .core.config import get_settings
from .core.database import engine, SessionLocal
from .core.middleware import TenantContextMiddleware
from .api import connectors, connections
from .models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Connector Service")
    Base.metadata.create_all(bind=engine)
    yield
    logger.info("Shutting down Connector Service")

app = FastAPI(
    title="Multi-Tenant Connector Service",
    description="Data connector management service for multi-tenant data ingestion platform",
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

app.include_router(connectors.router, prefix="/api/v1/connectors", tags=["connectors"])
app.include_router(connections.router, prefix="/api/v1/connections", tags=["connections"])

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "connector-service"}
