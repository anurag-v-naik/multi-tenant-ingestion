from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from ..core.database import get_db
from ..core.security import security_manager
from ..models.user import User, UserRole, UserStatus
from ..models.tenant import TenantUser, TenantRole
from ..utils.permissions import require_permission, get_current_user

router = APIRouter()
security = HTTPBearer()


# Request/Response models
class CreateUserRequest(BaseModel):
    email: EmailStr
    username: str
    first_name: str
    last_name: str
    password: str
    role: UserRole = UserRole.USER
    tenant_id: Optional[str] = None
    tenant_role: TenantRole = TenantRole.VIEWER


class UpdateUserRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[UserRole] = None
    status: Optional[UserStatus] = None
    phone: Optional[str] = None
    timezone: Optional[str] = None


class UserResponse(BaseModel):
    id: str
    email: str
    username: str
    first_name: str
    last_name: str
    role: str
    status: str
    email_verified: bool
    last_login: Optional[str] = None
    created_at: str


@router.post("/", response_model=UserResponse)
async def create_user(
        request: CreateUserRequest,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Create a new user"""

    # Check permissions
    require_permission(current_user, "user:create")

    # Check if user already exists
    existing_user = db.query(User).filter(
        (User.email == request.email.lower()) | (User.username == request.username)
    ).first()

    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User with this email or username already exists"
        )

    # Validate password
    security_manager.validate_password_strength(request.password)

    # Create user
    user = User(
        email=request.email.lower(),
        username=request.username,
        first_name=request.first_name,
        last_name=request.last_name,
        password_hash=security_manager.get_password_hash(request.password),
        role=request.role,
        status=UserStatus.ACTIVE,
        created_by=current_user.id
    )

    db.add(user)
    db.flush()  # Get user ID

    # Add tenant membership if specified
    if request.tenant_id:
        tenant_user = TenantUser(
            user_id=user.id,
            tenant_id=request.tenant_id,
            role=request.tenant_role,
            created_by=current_user.id
        )
        db.add(tenant_user)

    db.commit()
    db.refresh(user)

    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role.value,
        status=user.status.value,
        email_verified=user.email_verified,
        last_login=user.last_login.isoformat() if user.last_login else None,
        created_at=user.created_at.isoformat()
    )


@router.get("/", response_model=List[UserResponse])
async def list_users(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
        search: Optional[str] = Query(None),
        role: Optional[UserRole] = Query(None),
        status: Optional[UserStatus] = Query(None),
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """List users with filtering and pagination"""

    require_permission(current_user, "user:read")

    query = db.query(User)

    # Apply filters
    if search:
        search_term = f"%{search.lower()}%"
        query = query.filter(
            (User.email.ilike(search_term)) |
            (User.first_name.ilike(search_term)) |
            (User.last_name.ilike(search_term)) |
            (User.username.ilike(search_term))
        )

    if role:
        query = query.filter(User.role == role)

    if status:
        query = query.filter(User.status == status)

    # Apply pagination
    users = query.offset(skip).limit(limit).all()

    return [
        UserResponse(
            id=str(user.id),
            email=user.email,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            role=user.role.value,
            status=user.status.value,
            email_verified=user.email_verified,
            last_login=user.last_login.isoformat() if user.last_login else None,
            created_at=user.created_at.isoformat()
        )
        for user in users
    ]


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
        user_id: str,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Get user by ID"""

    require_permission(current_user, "user:read")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role.value,
        status=user.status.value,
        email_verified=user.email_verified,
        last_login=user.last_login.isoformat() if user.last_login else None,
        created_at=user.created_at.isoformat()
    )


@router.patch("/{user_id}", response_model=UserResponse)
async def update_user(
        user_id: str,
        request: UpdateUserRequest,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Update user"""

    require_permission(current_user, "user:update")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Update fields
    update_data = request.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(user, field, value)

    user.updated_by = current_user.id

    db.commit()
    db.refresh(user)

    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role.value,
        status=user.status.value,
        email_verified=user.email_verified,
        last_login=user.last_login.isoformat() if user.last_login else None,
        created_at=user.created_at.isoformat()
    )


@router.delete("/{user_id}")
async def delete_user(
        user_id: str,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """Soft delete user"""

    require_permission(current_user, "user:delete")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Soft delete
    user.is_active = False
    user.status = UserStatus.INACTIVE
    user.updated_by = current_user.id

    db.commit()

    return {"message": "User deleted successfully"}
