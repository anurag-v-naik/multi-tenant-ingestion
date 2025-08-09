from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from ..models.user import User, UserSession
from .config import get_settings
import secrets
import re

settings = get_settings()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class SecurityManager:
    def __init__(self):
        self.settings = settings
        self.pwd_context = pwd_context

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return self.pwd_context.verify(plain_password, hashed_password)

    def get_password_hash(self, password: str) -> str:
        return self.pwd_context.hash(password)

    def validate_password_strength(self, password: str) -> bool:
        """Validate password meets security requirements"""
        if len(password) < self.settings.PASSWORD_MIN_LENGTH:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Password must be at least {self.settings.PASSWORD_MIN_LENGTH} characters long"
            )

        if self.settings.PASSWORD_REQUIRE_UPPERCASE and not re.search(r'[A-Z]', password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one uppercase letter"
            )

        if self.settings.PASSWORD_REQUIRE_LOWERCASE and not re.search(r'[a-z]', password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one lowercase letter"
            )

        if self.settings.PASSWORD_REQUIRE_DIGITS and not re.search(r'\d', password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one digit"
            )

        if self.settings.PASSWORD_REQUIRE_SPECIAL and not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must contain at least one special character"
            )

        return True

    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)

        to_encode.update({"exp": expire, "type": "access"})
        encoded_jwt = jwt.encode(to_encode, self.settings.JWT_SECRET_KEY, algorithm=self.settings.JWT_ALGORITHM)
        return encoded_jwt

    def create_refresh_token(self, data: Dict[str, Any]) -> str:
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=self.settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
        to_encode.update({"exp": expire, "type": "refresh"})
        encoded_jwt = jwt.encode(to_encode, self.settings.JWT_SECRET_KEY, algorithm=self.settings.JWT_ALGORITHM)
        return encoded_jwt

    def verify_token(self, token: str) -> Dict[str, Any]:
        try:
            payload = jwt.decode(token, self.settings.JWT_SECRET_KEY, algorithms=[self.settings.JWT_ALGORITHM])
            return payload
        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def check_user_lockout(self, db: Session, user: User) -> bool:
        """Check if user is locked out due to failed login attempts"""
        if user.locked_until and user.locked_until > datetime.utcnow():
            return True
        return False

    def handle_failed_login(self, db: Session, user: User):
        """Handle failed login attempt"""
        user.failed_login_attempts += 1
        if user.failed_login_attempts >= self.settings.MAX_LOGIN_ATTEMPTS:
            user.locked_until = datetime.utcnow() + timedelta(minutes=self.settings.LOCKOUT_DURATION_MINUTES)
        db.commit()

    def handle_successful_login(self, db: Session, user: User):
        """Handle successful login"""
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        db.commit()

    def generate_reset_token(self) -> str:
        """Generate secure reset token"""
        return secrets.token_urlsafe(32)


security_manager = SecurityManager()