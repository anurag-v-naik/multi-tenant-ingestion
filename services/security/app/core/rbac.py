# services/security/app/core/rbac.py
"""
Comprehensive Role-Based Access Control (RBAC) implementation
with tenant isolation, resource-level permissions, and audit logging.
"""

from typing import Dict, List, Optional, Set, Any, Union
from enum import Enum
import json
import asyncio
from datetime import datetime, timedelta
from functools import wraps
import jwt
from passlib.context import CryptContext
from sqlalchemy import Column, String, DateTime, Boolean, JSON, ForeignKey, Text, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import UUID
import uuid
from pydantic import BaseModel, Field
import redis
from cryptography.fernet import Fernet
import boto3
from botocore.exceptions import ClientError
import logging

# Configure logging
logger = logging.getLogger(__name__)

Base = declarative_base()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Enums for RBAC
class ResourceType(str, Enum):
    PIPELINE = "pipeline"
    CONNECTOR = "connector"
    CATALOG = "catalog"
    DATA_QUALITY = "data_quality"
    ORGANIZATION = "organization"
    USER = "user"
    ROLE = "role"
    DATABRICKS_WORKSPACE = "databricks_workspace"
    S3_BUCKET = "s3_bucket"
    UNITY_CATALOG = "unity_catalog"


class Action(str, Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXECUTE = "execute"
    MANAGE = "manage"
    ADMIN = "admin"


class UserRole(str, Enum):
    SUPER_ADMIN = "super_admin"
    ORG_ADMIN = "org_admin"
    DATA_ENGINEER = "data_engineer"
    DATA_ANALYST = "data_analyst"
    VIEWER = "viewer"
    API_CLIENT = "api_client"


# Database Models
class Organization(Base):
    __tablename__ = "organizations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False, index=True)
    display_name = Column(String(200), nullable=False)
    domain = Column(String(100), unique=True, nullable=True)
    settings = Column(JSON, default=dict)
    resource_quotas = Column(JSON, default=dict)
    compliance_level = Column(String(50), default="medium")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    users = relationship("User", back_populates="organization")
    roles = relationship("Role", back_populates="organization")
    audit_logs = relationship("AuditLog", back_populates="organization")


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(200), nullable=False)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    last_login = Column(DateTime, nullable=True)
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime, nullable=True)
    mfa_enabled = Column(Boolean, default=False)
    mfa_secret = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="users")
    user_roles = relationship("UserRole", back_populates="user")
    audit_logs = relationship("AuditLog", back_populates="user")


class Role(Base):
    __tablename__ = "roles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), nullable=False)
    display_name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True)
    is_system_role = Column(Boolean, default=False)
    permissions = Column(JSON, default=list)  # List of permission dictionaries
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="roles")
    user_roles = relationship("UserRole", back_populates="role")


class UserRole(Base):
    __tablename__ = "user_roles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    role_id = Column(UUID(as_uuid=True), ForeignKey("roles.id"), nullable=False)
    granted_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    granted_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)

    # Relationships
    user = relationship("User", back_populates="user_roles", foreign_keys=[user_id])
    role = relationship("Role", back_populates="user_roles")
    granter = relationship("User", foreign_keys=[granted_by])


class Permission(Base):
    __tablename__ = "permissions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resource_type = Column(String(50), nullable=False)
    resource_id = Column(String(255), nullable=True)  # Null means all resources of this type
    action = Column(String(50), nullable=False)
    conditions = Column(JSON, default=dict)  # Additional conditions for the permission
    created_at = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    action = Column(String(100), nullable=False)
    resource_type = Column(String(50), nullable=False)
    resource_id = Column(String(255), nullable=True)
    details = Column(JSON, default=dict)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    success = Column(Boolean, nullable=False)
    error_message = Column(Text, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    user = relationship("User", back_populates="audit_logs")
    organization = relationship("Organization", back_populates="audit_logs")


# Pydantic Models for API
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=100)
    email: str = Field(..., regex=r'^[^@]+@[^@]+\.[^@]+)
    password: str = Field(..., min_length=8)
    full_name: str = Field(..., min_length=1, max_length=200)
    organization_id: str
    roles: List[str] = Field(default_factory=list)


class UserUpdate(BaseModel):
    email: Optional[str] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = None
    roles: Optional[List[str]] = None


class RoleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    permissions: List[Dict[str, Any]] = Field(default_factory=list)


class PermissionCheck(BaseModel):
    resource_type: ResourceType
    resource_id: Optional[str] = None
    action: Action
    context: Dict[str, Any] = Field(default_factory=dict)


# RBAC Manager Class
class RBACManager:
    """
    Comprehensive RBAC manager with tenant isolation,
    caching, and audit logging.
    """

    def __init__(self, db_session, redis_client: redis.Redis, encryption_key: str):
        self.db = db_session
        self.redis = redis_client
        self.fernet = Fernet(encryption_key.encode())
        self.cache_ttl = 300  # 5 minutes

        # Initialize system roles
        self._ensure_system_roles()

    def _ensure_system_roles(self):
        """Create default system roles if they don't exist."""
        system_roles = {
            UserRole.SUPER_ADMIN: {
                "display_name": "Super Administrator",
                "description": "Full system access across all organizations",
                "permissions": [
                    {"resource_type": "*", "action": "*", "conditions": {}}
                ]
            },
            UserRole.ORG_ADMIN: {
                "display_name": "Organization Administrator",
                "description": "Full access within organization",
                "permissions": [
                    {"resource_type": ResourceType.ORGANIZATION, "action": Action.MANAGE,
                     "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.USER, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.ROLE, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.PIPELINE, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CONNECTOR, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CATALOG, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.DATA_QUALITY, "action": "*", "conditions": {"same_org": True}}
                ]
            },
            UserRole.DATA_ENGINEER: {
                "display_name": "Data Engineer",
                "description": "Create and manage data pipelines",
                "permissions": [
                    {"resource_type": ResourceType.PIPELINE, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CONNECTOR, "action": "*", "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CATALOG, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.DATA_QUALITY, "action": "*", "conditions": {"same_org": True}}
                ]
            },
            UserRole.DATA_ANALYST: {
                "display_name": "Data Analyst",
                "description": "Read access to data and basic pipeline operations",
                "permissions": [
                    {"resource_type": ResourceType.PIPELINE, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.PIPELINE, "action": Action.EXECUTE,
                     "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CONNECTOR, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CATALOG, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.DATA_QUALITY, "action": Action.READ,
                     "conditions": {"same_org": True}}
                ]
            },
            UserRole.VIEWER: {
                "display_name": "Viewer",
                "description": "Read-only access to organization resources",
                "permissions": [
                    {"resource_type": ResourceType.PIPELINE, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CONNECTOR, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CATALOG, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.DATA_QUALITY, "action": Action.READ,
                     "conditions": {"same_org": True}}
                ]
            },
            UserRole.API_CLIENT: {
                "display_name": "API Client",
                "description": "Programmatic access for external systems",
                "permissions": [
                    {"resource_type": ResourceType.PIPELINE, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.PIPELINE, "action": Action.EXECUTE,
                     "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.CONNECTOR, "action": Action.READ, "conditions": {"same_org": True}},
                    {"resource_type": ResourceType.DATA_QUALITY, "action": Action.READ,
                     "conditions": {"same_org": True}}
                ]
            }
        }

        for role_name, role_data in system_roles.items():
            existing_role = self.db.query(Role).filter(
                Role.name == role_name,
                Role.is_system_role == True
            ).first()

            if not existing_role:
                role = Role(
                    name=role_name,
                    display_name=role_data["display_name"],
                    description=role_data["description"],
                    is_system_role=True,
                    permissions=role_data["permissions"]
                )
                self.db.add(role)

        self.db.commit()

    async def create_user(self, user_data: UserCreate, created_by: Optional[str] = None) -> User:
        """Create a new user with proper validation and audit logging."""

        # Check if user exists
        existing_user = self.db.query(User).filter(
            (User.username == user_data.username) |
            (User.email == user_data.email)
        ).first()

        if existing_user:
            raise ValueError("User with this username or email already exists")

        # Validate organization exists
        org = self.db.query(Organization).filter(
            Organization.id == user_data.organization_id
        ).first()
        if not org:
            raise ValueError("Organization not found")

        # Hash password
        hashed_password = pwd_context.hash(user_data.password)

        # Create user
        user = User(
            username=user_data.username,
            email=user_data.email,
            hashed_password=hashed_password,
            full_name=user_data.full_name,
            organization_id=user_data.organization_id
        )

        self.db.add(user)
        self.db.flush()  # Get user ID

        # Assign roles
        for role_name in user_data.roles:
            role = self.db.query(Role).filter(
                Role.name == role_name,
                (Role.organization_id == user_data.organization_id) |
                (Role.is_system_role == True)
            ).first()

            if role:
                user_role = UserRole(
                    user_id=user.id,
                    role_id=role.id,
                    granted_by=created_by
                )
                self.db.add(user_role)

        self.db.commit()

        # Audit log
        await self._log_audit(
            user_id=created_by,
            organization_id=user_data.organization_id,
            action="create_user",
            resource_type="user",
            resource_id=str(user.id),
            details={"username": user_data.username, "roles": user_data.roles},
            success=True
        )

        return user

    async def authenticate_user(self, username: str, password: str, organization_name: str, ip_address: str = None) -> \
    Optional[Dict]:
        """Authenticate user with rate limiting and audit logging."""

        # Get organization
        org = self.db.query(Organization).filter(
            Organization.name == organization_name,
            Organization.is_active == True
        ).first()

        if not org:
            await self._log_audit(
                user_id=None,
                organization_id=None,
                action="login_attempt",
                resource_type="authentication",
                details={"username": username, "organization": organization_name, "error": "organization_not_found"},
                ip_address=ip_address,
                success=False
            )
            return None

        # Get user
        user = self.db.query(User).filter(
            User.username == username,
            User.organization_id == org.id,
            User.is_active == True
        ).first()

        if not user:
            await self._log_audit(
                user_id=None,
                organization_id=org.id,
                action="login_attempt",
                resource_type="authentication",
                details={"username": username, "error": "user_not_found"},
                ip_address=ip_address,
                success=False
            )
            return None

        # Check if account is locked
        if user.locked_until and user.locked_until > datetime.utcnow():
            await self._log_audit(
                user_id=user.id,
                organization_id=org.id,
                action="login_attempt",
                resource_type="authentication",
                details={"username": username, "error": "account_locked"},
                ip_address=ip_address,
                success=False
            )
            return None

        # Verify password
        if not pwd_context.verify(password, user.hashed_password):
            user.failed_login_attempts += 1

            # Lock account after 5 failed attempts
            if user.failed_login_attempts >= 5:
                user.locked_until = datetime.utcnow() + timedelta(minutes=30)

            self.db.commit()

            await self._log_audit(
                user_id=user.id,
                organization_id=org.id,
                action="login_attempt",
                resource_type="authentication",
                details={"username": username, "error": "invalid_password"},
                ip_address=ip_address,
                success=False
            )
            return None

        # Reset failed attempts on successful login
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        self.db.commit()

        # Get user roles and permissions
        user_roles = self.db.query(UserRole).filter(
            UserRole.user_id == user.id,
            UserRole.is_active == True,
            (UserRole.expires_at.is_(None)) | (UserRole.expires_at > datetime.utcnow())
        ).all()

        permissions = []
        roles = []

        for user_role in user_roles:
            roles.append({
                "name": user_role.role.name,
                "display_name": user_role.role.display_name
            })
            permissions.extend(user_role.role.permissions)

        # Generate JWT token
        token_data = {
            "user_id": str(user.id),
            "username": user.username,
            "organization_id": str(org.id),
            "organization_name": org.name,
            "roles": [role["name"] for role in roles],
            "exp": datetime.utcnow() + timedelta(hours=24),
            "iat": datetime.utcnow(),
            "iss": "multi-tenant-ingestion"
        }

        # Cache user session
        session_key = f"session:{user.id}"
        session_data = {
            "user_id": str(user.id),
            "username": user.username,
            "organization_id": str(org.id),
            "organization_name": org.name,
            "roles": roles,
            "permissions": permissions,
            "last_activity": datetime.utcnow().isoformat()
        }

        await self.redis.setex(
            session_key,
            3600 * 24,  # 24 hours
            json.dumps(session_data, default=str)
        )

        await self._log_audit(
            user_id=user.id,
            organization_id=org.id,
            action="login_success",
            resource_type="authentication",
            details={"username": username},
            ip_address=ip_address,
            success=True
        )

        return {
            "user": {
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name,
                "organization": {
                    "id": str(org.id),
                    "name": org.name,
                    "display_name": org.display_name
                }
            },
            "roles": roles,
            "permissions": permissions,
            "token": token_data
        }

    async def check_permission(self, user_id: str, permission: PermissionCheck, context: Dict = None) -> bool:
        """Check if user has specific permission with context evaluation."""

        # Get cached session
        session_key = f"session:{user_id}"
        cached_session = await self.redis.get(session_key)

        if not cached_session:
            return False

        session_data = json.loads(cached_session)
        permissions = session_data.get("permissions", [])
        user_org_id = session_data.get("organization_id")

        # Check each permission
        for perm in permissions:
            if self._matches_permission(perm, permission, user_org_id, context):
                return True

        return False

    def _matches_permission(self, perm: Dict, check: PermissionCheck, user_org_id: str, context: Dict = None) -> bool:
        """Check if a permission matches the requested check."""

        # Check resource type
        if perm["resource_type"] != "*" and perm["resource_type"] != check.resource_type:
            return False

        # Check action
        if perm["action"] != "*" and perm["action"] != check.action:
            return False

        # Check conditions
        conditions = perm.get("conditions", {})

        # Same organization check
        if conditions.get("same_org") and context:
            resource_org_id = context.get("organization_id")
            if resource_org_id and resource_org_id != user_org_id:
                return False

        # Resource owner check
        if conditions.get("owner_only") and context:
            resource_owner_id = context.get("owner_id")
            user_id = context.get("user_id")
            if resource_owner_id and user_id and resource_owner_id != user_id:
                return False

        return True

    async def _log_audit(self, action: str, resource_type: str, success: bool,
                         user_id: str = None, organization_id: str = None,
                         resource_id: str = None, details: Dict = None,
                         ip_address: str = None, user_agent: str = None,
                         error_message: str = None):
        """Log audit events."""

        audit_log = AuditLog(
            user_id=user_id,
            organization_id=organization_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            success=success,
            error_message=error_message
        )

        self.db.add(audit_log)
        self.db.commit()


# Security Decorators
def require_permission(resource_type: ResourceType, action: Action):
    """Decorator to enforce permissions on API endpoints."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract request and user info from FastAPI dependency injection
            request = kwargs.get('request')
            current_user = kwargs.get('current_user')

            if not current_user:
                raise HTTPException(status_code=401, detail="Authentication required")

            # Build permission check
            permission_check = PermissionCheck(
                resource_type=resource_type,
                action=action,
                resource_id=kwargs.get('resource_id')
            )

            # Get context from request
            context = {
                "user_id": current_user["user_id"],
                "organization_id": current_user["organization_id"]
            }

            # Add resource-specific context
            if 'organization_id' in kwargs:
                context["organization_id"] = kwargs['organization_id']

            # Check permission
            rbac_manager = request.app.state.rbac_manager
            has_permission = await rbac_manager.check_permission(
                current_user["user_id"],
                permission_check,
                context
            )

            if not has_permission:
                raise HTTPException(
                    status_code=403,
                    detail=f"Insufficient permissions for {action} on {resource_type}"
                )

            return await func(*args, **kwargs)

        return wrapper

    return decorator


def require_organization_access(func):
    """Decorator to ensure user can only access their organization's resources."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        current_user = kwargs.get('current_user')
        organization_id = kwargs.get('organization_id')

        if not current_user:
            raise HTTPException(status_code=401, detail="Authentication required")

        if organization_id and organization_id != current_user["organization_id"]:
            # Check if user has super admin role
            if "super_admin" not in current_user.get("roles", []):
                raise HTTPException(
                    status_code=403,
                    detail="Access denied to this organization"
                )

        return await func(*args, **kwargs)

    return wrapper


# Encryption utilities
class DataEncryption:
    """Handle encryption of sensitive data at rest."""

    def __init__(self, kms_client, kms_key_id: str):
        self.kms = kms_client
        self.kms_key_id = kms_key_id

    async def encrypt_data(self, data: Union[str, bytes]) -> str:
        """Encrypt data using AWS KMS."""
        if isinstance(data, str):
            data = data.encode('utf-8')

        try:
            response = self.kms.encrypt(
                KeyId=self.kms_key_id,
                Plaintext=data
            )
            return response['CiphertextBlob'].hex()
        except ClientError as e:
            logger.error(f"Encryption failed: {e}")
            raise

    async def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt data using AWS KMS."""
        try:
            ciphertext_blob = bytes.fromhex(encrypted_data)
            response = self.kms.decrypt(CiphertextBlob=ciphertext_blob)
            return response['Plaintext'].decode('utf-8')
        except ClientError as e:
            logger.error(f"Decryption failed: {e}")
            raise

    async def encrypt_field(self, table_name: str, field_name: str, value: str) -> str:
        """Encrypt a database field with additional context."""
        context = {
            'table': table_name,
            'field': field_name,
            'timestamp': str(datetime.utcnow())
        }

        try:
            response = self.kms.encrypt(
                KeyId=self.kms_key_id,
                Plaintext=value.encode('utf-8'),
                EncryptionContext=context
            )
            return response['CiphertextBlob'].hex()
        except ClientError as e:
            logger.error(f"Field encryption failed: {e}")
            raise