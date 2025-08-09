from sqlalchemy import Column, String, Text, Integer, JSON, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from enum import Enum
from .base import BaseModel, TenantAwareModel


class ComplianceLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TenantStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"
    PROVISIONING = "provisioning"


class Tenant(BaseModel):
    __tablename__ = "tenants"

    name = Column(String(100), unique=True, nullable=False)
    display_name = Column(String(200), nullable=False)
    description = Column(Text)
    cost_center = Column(String(50))
    compliance_level = Column(String(20), default=ComplianceLevel.MEDIUM)
    status = Column(String(20), default=TenantStatus.ACTIVE)

    # Resource quotas
    max_dbu_per_hour = Column(Integer, default=100)
    max_storage_gb = Column(Integer, default=10000)
    max_api_calls_per_minute = Column(Integer, default=1000)
    max_concurrent_jobs = Column(Integer, default=10)

    # Configuration
    databricks_workspace_url = Column(String(500))
    databricks_workspace_id = Column(String(100))
    unity_catalog_name = Column(String(100))

    # Relationships
    users = relationship("TenantUser", back_populates="tenant")
    connections = relationship("DataConnection", back_populates="tenant")
    pipelines = relationship("Pipeline", back_populates="tenant")


class TenantRole(str, Enum):
    ADMIN = "admin"
    DEVELOPER = "developer"
    ANALYST = "analyst"
    VIEWER = "viewer"


class TenantUser(TenantAwareModel):
    __tablename__ = "tenant_users"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    role = Column(String(20), default=TenantRole.VIEWER)
    permissions = Column(JSON, default=list)

    # Relationships
    tenant = relationship("Tenant", back_populates="users")
    user = relationship("User", back_populates="tenant_memberships")
