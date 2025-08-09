from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from sqlalchemy.orm import Session
from typing import Optional
import logging
import time
import uuid
from .database import SessionLocal
from ..models.audit import AuditLog

logger = logging.getLogger(__name__)


class TenantContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        # Extract tenant ID from header
        tenant_id = request.headers.get("X-Tenant-ID")
        organization_id = request.headers.get("X-Organization-ID")

        # Set tenant context
        request.state.tenant_id = tenant_id or organization_id
        request.state.organization_id = organization_id or tenant_id

        response = await call_next(request)
        return response


class AuditLogMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.exclude_paths = ["/health", "/docs", "/openapi.json", "/favicon.ico"]

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Skip audit logging for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)

        # Extract request info
        user_id = getattr(request.state, "user_id", None)
        tenant_id = getattr(request.state, "tenant_id", None)
        ip_address = self._get_client_ip(request)
        user_agent = request.headers.get("user-agent", "")

        response = await call_next(request)

        # Log the request
        duration_ms = int((time.time() - start_time) * 1000)

        try:
            await self._log_request(
                user_id=user_id,
                tenant_id=tenant_id,
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=duration_ms,
                ip_address=ip_address,
                user_agent=user_agent
            )
        except Exception as e:
            logger.error(f"Failed to log audit entry: {e}")

        return response

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request"""
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        return request.client.host if request.client else "unknown"

    async def _log_request(self, **kwargs):
        """Log request to audit table"""
        db = SessionLocal()
        try:
            audit_log = AuditLog(
                user_id=kwargs.get("user_id"),
                tenant_id=kwargs.get("tenant_id"),
                action="api_request",
                resource_type="api_endpoint",
                description=f"{kwargs['method']} {kwargs['path']}",
                new_values={
                    "method": kwargs["method"],
                    "path": kwargs["path"],
                    "status_code": kwargs["status_code"],
                    "duration_ms": kwargs["duration_ms"]
                },
                ip_address=kwargs.get("ip_address"),
                user_agent=kwargs.get("user_agent"),
                severity="low"
            )
            db.add(audit_log)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to save audit log: {e}")
        finally:
            db.close()