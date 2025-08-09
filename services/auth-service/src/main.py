from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Optional
import jwt
import bcrypt
import os
from pydantic import BaseModel

from .database import get_db, engine
from .models import User, Tenant, Base

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Multi-Tenant Auth Service",
    description="Authentication and authorization for multi-tenant data ingestion",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# Pydantic models
class UserCredentials(BaseModel):
    username: str
    password: str
    tenant_id: str


class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    tenant_id: str
    role: str = "user"


class TenantCreate(BaseModel):
    name: str
    description: Optional[str] = None
    config: Optional[dict] = {}


class Token(BaseModel):
    access_token: str
    token_type: str
    tenant_id: str
    user_id: int


# Utility functions
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None


def authenticate_user(db: Session, username: str, password: str, tenant_id: str):
    user = db.query(User).filter(
        User.username == username,
        User.tenant_id == tenant_id,
        User.is_active == True
    ).first()

    if not user or not verify_password(password, user.password_hash):
        return False
    return user


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security),
                           db: Session = Depends(get_db)):
    token = credentials.credentials
    payload = verify_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(
        User.id == payload["user_id"],
        User.tenant_id == payload["tenant_id"]
    ).first()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user


# API Endpoints
@app.post("/auth/login", response_model=Token)
async def login(credentials: UserCredentials, db: Session = Depends(get_db)):
    user = authenticate_user(db, credentials.username, credentials.password, credentials.tenant_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username, password, or tenant",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"user_id": user.id, "tenant_id": user.tenant_id, "username": user.username},
        expires_delta=access_token_expires
    )

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "tenant_id": user.tenant_id,
        "user_id": user.id
    }


@app.get("/auth/validate")
async def validate_token(current_user: User = Depends(get_current_user)):
    return {
        "valid": True,
        "user_id": current_user.id,
        "username": current_user.username,
        "tenant_id": current_user.tenant_id,
        "role": current_user.role
    }


@app.post("/auth/users")
async def create_user(user_data: UserCreate, db: Session = Depends(get_db)):
    # Check if tenant exists
    tenant = db.query(Tenant).filter(Tenant.id == user_data.tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    # Check if user already exists
    existing_user = db.query(User).filter(
        User.username == user_data.username,
        User.tenant_id == user_data.tenant_id
    ).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User already exists")

    # Create new user
    hashed_password = hash_password(user_data.password)
    new_user = User(
        username=user_data.username,
        email=user_data.email,
        password_hash=hashed_password,
        tenant_id=user_data.tenant_id,
        role=user_data.role,
        is_active=True,
        created_at=datetime.utcnow()
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {"message": "User created successfully", "user_id": new_user.id}


@app.post("/auth/tenants")
async def create_tenant(tenant_data: TenantCreate, db: Session = Depends(get_db)):
    new_tenant = Tenant(
        name=tenant_data.name,
        description=tenant_data.description,
        config=tenant_data.config,
        is_active=True,
        created_at=datetime.utcnow()
    )

    db.add(new_tenant)
    db.commit()
    db.refresh(new_tenant)

    return {"message": "Tenant created successfully", "tenant_id": new_tenant.id}


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "auth-service"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
