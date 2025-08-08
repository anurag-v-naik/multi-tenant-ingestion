"""Security utilities for multi-tenant authentication and authorization."""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_database
from app.models.organization import Organization

# Security configuration
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()


class SecurityService:
    """Security service for authentication and authorization."""
    
    def __init__(self):
        self.secret_key = settings.JWT_SECRET_KEY
        self.algorithm = settings.JWT_ALGORITHM
        self.expire_minutes = settings.JWT_EXPIRE_MINUTES
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """Generate password hash."""
        return pwd_context.hash(password)
    
    def create_access_token(self, data: Dict[str, Any]) -> str:
        """Create JWT access token."""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.expire_minutes)
        to_encode.update({"exp": expire})
        
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )


# Global security service instance
security_service = SecurityService()


async def get_current_organization(
    x_organization_id: Optional[str] = Header(None),
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_database)
) -> Organization:
    """Get current organization from request context."""
    
    if not settings.MULTI_TENANT_MODE:
        # Single tenant mode - return default organization
        return Organization(
            id=settings.DEFAULT_ORGANIZATION,
            name=settings.DEFAULT_ORGANIZATION,
            display_name="Default Organization"
        )
    
    # Verify JWT token
    payload = security_service.verify_token(credentials.credentials)
    
    # Extract organization from token or header
    org_id = x_organization_id or payload.get("organization_id")
    
    if not org_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Organization ID required"
        )
    
    # Fetch organization from database
    # In a real implementation, you would query the database
    # For now, we'll return a mock organization
    return Organization(
        id=org_id,
        name=org_id,
        display_name=f"{org_id.title()} Organization"
    )


async def verify_organization_access(
    organization: Organization = Depends(get_current_organization),
    resource_org_id: Optional[str] = None
) -> bool:
    """Verify that the current organization has access to a resource."""
    
    if not settings.MULTI_TENANT_MODE:
        return True
    
    if resource_org_id and resource_org_id != organization.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to organization resource"
        )
    
    return True
