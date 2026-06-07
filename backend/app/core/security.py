import hashlib
import secrets
from datetime import datetime,timedelta, timezone

import bcrypt
import jwt

from app.core.config import settings

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), password_hash.encode())

def create_access_token(user_id: int) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "type": "access",
        "iat": now,
        "exp": now + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

def generate_refresh_token() -> str:
    return secrets.token_urlsafe(48)

def decode_access_token(token: str) -> int:
    """access 토근 검증"""
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM]
        )

    except jwt.PyJWTError:
        raise ValueError("유효하지 않은 토큰입니다.")

    if payload.get("type") != "access":
        raise ValueError("access 토큰이 아닙니다.")

    return int(payload["sub"])
