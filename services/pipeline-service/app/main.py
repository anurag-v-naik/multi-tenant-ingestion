"""
Multi-Tenant Pipeline Service

This service handles pipeline creation, execution, and monitoring
for multiple organizations with complete tenant isolation.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from prometheus_client import make_asgi_app
import structlog

from app.core.config import settings
from app.core.database import get_database
from app.core.security import get_current_organization
from app.api.endpoints import pipelines, health
from app.models.database import create_tables

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info("Starting Pipeline Service", version="1.0.0")
    
    # Create database tables
    await create_tables()
    
    # Initialize metrics
    logger.info("Pipeline Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Pipeline Service")


# Create FastAPI application
app = FastAPI(
    title="Multi-Tenant Pipeline Service",
    description="Enterprise-grade multi-tenant data pipeline service",
    version="1.0.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.ALLOWED_HOSTS
)

# Include routers
app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(
    pipelines.router, 
    prefix="/api/v1/pipelines", 
    tags=["pipelines"],
    dependencies=[Depends(get_current_organization)]
)

# Add Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.middleware("http")
async def log_requests(request, call_next):
    """Log all requests with tenant context."""
    start_time = time.time()
    
    # Extract organization context
    org_id = request.headers.get("X-Organization-ID", "unknown")
    
    # Create logger with context
    request_logger = logger.bind(
        organization_id=org_id,
        path=request.url.path,
        method=request.method,
        user_agent=request.headers.get("user-agent")
    )
    
    request_logger.info("Request started")
    
    try:
        response = await call_next(request)
        
        # Log response
        duration = time.time() - start_time
        request_logger.info(
            "Request completed",
            status_code=response.status_code,
            duration=duration
        )
        
        return response
    
    except Exception as e:
        duration = time.time() - start_time
        request_logger.error(
            "Request failed",
            error=str(e),
            duration=duration
        )
        raise


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
