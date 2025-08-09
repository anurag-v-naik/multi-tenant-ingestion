from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from ..core.database import get_db
from ..core.security import security_manager
from ..models.user import User, UserRole
from ..models.tenant import TenantUser, TenantRole

security = HTTPBearer()

# Permission definitions
PERMISSIONS = {
    UserRole.SUPER_ADMIN: [
        "user:create", "user:read", "user:update", "user:delete",
        "tenant:create", "tenant:read", "tenant:update", "tenant:delete",
        "system:admin"
    ],
    UserRole.TENANT_ADMIN: [
        "user:read", "user:update",
        "tenant:read", "tenant:update"
    ],
    UserRole.USER: [
        "user:read_own"
    ]
}

TENANT_PERMISSIONS = {
    TenantRole.ADMIN: [
        "pipeline:create", "pipeline:read", "pipeline:update", "pipeline:delete",
        "connection:create", "connection:read", "connection:update", "connection:delete",
        "job:create", "job:read", "job:update", "job:delete",
        "user:invite", "user:read", "user:update_tenant"
    ],
    TenantRole.DEVELOPER: [
        "pipeline:create", "pipeline:read", "pipeline:update",
        "connection:create", "connection:read", "connection:update",
        "job:create", "job:read", "job:update"
    ],
    TenantRole.ANALYST: [
        "pipeline:read", "pipeline:execute",
        "connection:read", "connection:test",
        "job:read"
    ],
    TenantRole.VIEWER: [
        "pipeline:read",
        "connection:read",
        "job:read"
    ]
}


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
) -> User:
    """Get current authenticated user"""

    payload = security_manager.verify_token(credentials.credentials)
    user_id = payload.get("sub")

    user = db.query(User).filter(User.id == user_id, User.is_active == True).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user


def require_permission(user: User, permission: str, tenant_id: str = None):
    """Check if user has required permission"""

    # Check global permissions
    global_permissions = PERMISSIONS.get(user.role, [])
    if permission in global_permissions:
        return True

    # Check tenant-specific permissions
    if tenant_id and hasattr(user, 'tenant_memberships'):
        for membership in user.tenant_memberships:
            if str(membership.tenant_id) == tenant_id and membership.is_active:
                tenant_permissions = TENANT_PERMISSIONS.get(membership.role, [])
                if permission in tenant_permissions:
                    return True

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Insufficient permissions. Required: {permission}"
    )


def get_user_permissions(user: User, tenant_id: str = None) -> List[str]:
    """Get all permissions for a user"""

    permissions = set(PERMISSIONS.get(user.role, []))

    if tenant_id and hasattr(user, 'tenant_memberships'):
        for membership in user.tenant_memberships:
            if str(membership.tenant_id) == tenant_id and membership.is_active:
                tenant_permissions = TENANT_PERMISSIONS.get(membership.role, [])
                permissions.update(tenant_permissions)

    return list(permissions)