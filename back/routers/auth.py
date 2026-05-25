"""
back/routers/auth.py
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from back.database import get_db
from back.models.db_models import PlatformUser
from back.schemas.schemas import LoginRequest, TokenResponse, UserMe
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["auth"])

SECRET_KEY         = os.environ.get("JWT_SECRET_KEY", "change-this-in-production")
ALGORITHM          = "HS256"
TOKEN_EXPIRE_HOURS = 12

pwd_context   = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_token(user_id: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": str(user_id), "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db:    Session = Depends(get_db),
) -> PlatformUser:
    err = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
    except (JWTError, TypeError, ValueError):
        raise err

    user = db.query(PlatformUser).filter_by(id=user_id, is_active=True).first()
    if not user:
        raise err
    return user


def require_admin(current_user: PlatformUser = Depends(get_current_user)) -> PlatformUser:
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


@router.post("/login", response_model=TokenResponse)
def login(request: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(PlatformUser).filter_by(
        email=request.email.lower().strip(), is_active=True
    ).first()

    if not user or not verify_password(request.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user.last_login = datetime.now(timezone.utc)
    db.commit()

    return TokenResponse(
        access_token=create_token(user.id),
        user=UserMe.model_validate(user),
    )


@router.get("/me", response_model=UserMe)
def get_me(current_user: PlatformUser = Depends(get_current_user)):
    return UserMe.model_validate(current_user)


class CreateUserRequest(BaseModel):
    email:     str
    password:  str
    full_name: Optional[str] = None
    role:      str = "manager"

from pydantic import BaseModel

@router.post("/users", response_model=UserMe)
def create_user(
    request: CreateUserRequest,
    db:      Session = Depends(get_db),
    _:       PlatformUser = Depends(require_admin),
):
    existing = db.query(PlatformUser).filter_by(email=request.email.lower().strip()).first()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    user = PlatformUser(
        email           = request.email.lower().strip(),
        full_name       = request.full_name,
        hashed_password = hash_password(request.password),
        role            = request.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return UserMe.model_validate(user)


@router.get("/users", response_model=list[UserMe])
def list_users(
    db: Session = Depends(get_db),
    _:  PlatformUser = Depends(require_admin),
):
    return [UserMe.model_validate(u) for u in db.query(PlatformUser).all()]