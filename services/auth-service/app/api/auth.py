from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime, timedelta
from ..core.database import get_db
from ..core.security import security_manager
from ..models.user import User, UserSession, UserStatus
from ..models.tenant import TenantUser
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
security = HTTPBearer()


# Request/Response models
class LoginRequest(BaseModel):
    email: str
    password: str
    tenant_id: Optional[str] = None


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: dict
    tenant: Optional[dict] = None


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


@router.post("/login", response_model=LoginResponse)
async def login(
        request: LoginRequest,
        http_request: Request,
        db: Session = Depends(get_db)
):
    """Authenticate user and return access token"""

    # Find user
    user = db.query(User).filter(
        User.email == request.email.lower(),
        User.is_active == True
    ).first()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    # Check if user is locked out
    if security_manager.check_user_lockout(db, user):
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail="Account is temporarily locked due to too many failed login attempts"
        )

    # Verify password
    if not security_manager.verify_password(request.password, user.password_hash):
        security_manager.handle_failed_login(db, user)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    # Check user status
    if user.status != UserStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account is {user.status.value}"
        )

    # Handle tenant access
    tenant_membership = None
    if request.tenant_id:
        tenant_membership = db.query(TenantUser).filter(
            TenantUser.user_id == user.id,
            TenantUser.tenant_id == request.tenant_id,
            TenantUser.is_active == True
        ).first()

        if not tenant_membership:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have access to this tenant"
            )

    # Create tokens
    token_data = {
        "sub": str(user.id),
        "email": user.email,
        "role": user.role.value,
        "tenant_id": request.tenant_id,
        "tenant_role": tenant_membership.role.value if tenant_membership else None
    }

    access_token = security_manager.create_access_token(data=token_data)
    refresh_token = security_manager.create_refresh_token(data={"sub": str(user.id)})

    # Create session
    session = UserSession(
        user_id=user.id,
        token_hash=security_manager.get_password_hash(access_token),
        expires_at=datetime.utcnow() + timedelta(minutes=security_manager.settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
        ip_address=http_request.state.get("client_ip", "unknown"),
        user_agent=http_request.headers.get("user-agent", ""),
        tenant_id=request.tenant_id
    )
    db.add(session)

    # Update user login info
    security_manager.handle_successful_login(db, user)

    db.commit()

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=security_manager.settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user={
            "id": str(user.id),
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "role": user.role.value
        },
        tenant={
            "id": request.tenant_id,
            "role": tenant_membership.role.value if tenant_membership else None
        } if request.tenant_id else None
    )


@router.post("/refresh", response_model=LoginResponse)
async def refresh_token(
        request: RefreshTokenRequest,
        db: Session = Depends(get_db)
):
    """Refresh access token using refresh token"""

    try:
        payload = security_manager.verify_token(request.refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type"
            )

        user_id = payload.get("sub")
        user = db.query(User).filter(User.id == user_id, User.is_active == True).first()

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )

        # Create new access token
        token_data = {
            "sub": str(user.id),
            "email": user.email,
            "role": user.role.value
        }

        access_token = security_manager.create_access_token(data=token_data)

        return LoginResponse(
            access_token=access_token,
            refresh_token=request.refresh_token,  # Keep same refresh token
            expires_in=security_manager.settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            user={
                "id": str(user.id),
                "email": user.email,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "role": user.role.value
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not refresh token"
        )


@router.post("/logout")
async def logout(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
):
    """Logout user and invalidate session"""

    try:
        payload = security_manager.verify_token(credentials.credentials)
        user_id = payload.get("sub")

        # Find and deactivate session
        session = db.query(UserSession).filter(
            UserSession.user_id == user_id,
            UserSession.is_active == True
        ).first()

        if session:
            session.is_active = False
            db.commit()

        return {"message": "Successfully logged out"}

    except HTTPException:
        # Even if token is invalid, return success for logout
        return {"message": "Successfully logged out"}


@router.post("/change-password")
async def change_password(
        request: ChangePasswordRequest,
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
):
    """Change user password"""

    payload = security_manager.verify_token(credentials.credentials)
    user_id = payload.get("sub")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Verify current password
    if not security_manager.verify_password(request.current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )

    # Validate new password
    security_manager.validate_password_strength(request.new_password)

    # Update password
    user.password_hash = security_manager.get_password_hash(request.new_password)
    db.commit()

    return {"message": "Password changed successfully"}


@router.post("/forgot-password")
async def forgot_password(
        request: ForgotPasswordRequest,
        db: Session = Depends(get_db)
):
    """Send password reset email"""

    user = db.query(User).filter(
        User.email == request.email.lower(),
        User.is_active == True
    ).first()

    # Always return success to prevent email enumeration
    if not user:
        return {"message": "If the email exists, a password reset link has been sent"}

    # Generate reset token
    reset_token = security_manager.generate_reset_token()

    # Store reset token (in production, use Redis or similar)
    # For now, we'll store in user metadata
    user.metadata = user.metadata or {}
    user.metadata["reset_token"] = reset_token
    user.metadata["reset_token_expires"] = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    db.commit()

    # TODO: Send email with reset link
    logger.info(f"Password reset requested for user {user.email}")

    return {"message": "If the email exists, a password reset link has been sent"}


@router.post("/reset-password")
async def reset_password(
        request: ResetPasswordRequest,
        db: Session = Depends(get_db)
):
    """Reset password using reset token"""

    # Find user with valid reset token
    users = db.query(User).filter(User.is_active == True).all()
    user = None

    for u in users:
        if (u.metadata and
                u.metadata.get("reset_token") == request.token and
                u.metadata.get("reset_token_expires")):
            expires = datetime.fromisoformat(u.metadata["reset_token_expires"])
            if expires > datetime.utcnow():
                user = u
                break

    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )

    # Validate new password
    security_manager.validate_password_strength(request.new_password)

    # Update password and clear reset token
    user.password_hash = security_manager.get_password_hash(request.new_password)
    user.metadata.pop("reset_token", None)
    user.metadata.pop("reset_token_expires", None)
    user.failed_login_attempts = 0
    user.locked_until = None

    db.commit()

    return {"message": "Password reset successfully"}


@router.get("/me")
async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
):
    """Get current user information"""

    payload = security_manager.verify_token(credentials.credentials)
    user_id = payload.get("sub")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Get tenant memberships
    tenant_memberships = db.query(TenantUser).filter(
        TenantUser.user_id == user.id,
        TenantUser.is_active == True
    ).all()

    return {
        "id": str(user.id),
        "email": user.email,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "role": user.role.value,
        "status": user.status.value,
        "email_verified": user.email_verified,
        "last_login": user.last_login.isoformat() if user.last_login else None,
        "tenant_memberships": [
            {
                "tenant_id": str(tm.tenant_id),
                "role": tm.role.value,
                "permissions": tm.permissions
            }
            for tm in tenant_memberships
        ]
    }