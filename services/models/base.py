from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, String, DateTime, Boolean, Text, JSON, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import Session
from uuid import uuid4

Base = declarative_base()


class BaseModel(Base):
    __abstract__ = True

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_by = Column(UUID(as_uuid=True), nullable=True)
    updated_by = Column(UUID(as_uuid=True), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    metadata = Column(JSON, default=dict)

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


class TenantAwareModel(BaseModel):
    __abstract__ = True

    tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    @declared_attr
    def __table_args__(cls):
        return (
            Index(f'idx_{cls.__name__.lower()}_tenant_id', 'tenant_id'),
            Index(f'idx_{cls.__name__.lower()}_tenant_active', 'tenant_id', 'is_active'),
        )

    @classmethod
    def for_tenant(cls, session: Session, tenant_id: str):
        return session.query(cls).filter(
            cls.tenant_id == tenant_id,
            cls.is_active == True
        )