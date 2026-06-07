import asyncpg
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.database import get_pool
from app.core.security import decode_access_token

bearer = HTTPBearer(auto_error=True)

async def get_current_user(
    cred: HTTPAuthorizationCredentials = Depends(bearer),
    pool: asyncpg.Pool = Depends(get_pool),
) -> asyncpg.Record:
    """access token 검증 후 유저 레코드 반환"""
    try:
        user_id = decode_access_token(cred.credentials)

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await pool.fetchrow(
        """SELECT id, email, nickname, profile_image, is_active
           FROM users WHERE id = $1""",
        user_id,
    )

    if not user or not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="유효하지 않은 사용자입니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user