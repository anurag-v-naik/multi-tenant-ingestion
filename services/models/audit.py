from sqlalchemy import Column, String, Text, JSON, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum
from .base import TenantAwareModel


class AuditAction(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    ACCESS = "access"
    EXECUTE = "execute"
    EXPORT = "export"
    IMPORT = "import"


class AuditLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AuditLog(TenantAwareModel):
    __tablename__ = "audit_logs"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    action = Column(String(50), nullable=False)
    resource_type = Column(String(100), nullable=False)
    resource_id = Column(UUID(as_uuid=True))

    # Details
    description = Column(Text)
    old_values = Column(JSON)
    new_values = Column(JSON)

    # Context
    ip_address = Column(String(45))
    user_agent = Column(Text)
    session_id = Column(UUID(as_uuid=True))

    # Classification
    severity = Column(String(20), default=AuditLevel.LOW)
    category = Column(String(100))

    # Compliance
    compliance_tags = Column(JSON, default=list)
    retention_until = Column(DateTime)


class ActivityLog(TenantAwareModel):
    __tablename__ = "activity_logs"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    activity_type = Column(String(100), nullable=False)
    activity_name = Column(String(200), nullable=False)

    # Details
    details = Column(JSON, default=dict)
    duration_ms = Column(Integer)

    # Status
    status = Column(String(50))
    error_message = Column(Text)