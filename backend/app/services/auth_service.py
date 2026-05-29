from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
from fastapi import HTTPException, status

from app.core.config import settings
from app.core.security import (
    create_access_token, hash_password, verify_password,
    generate_refresh_token, hash_token
)
from app.schema.user_schema import LoginRequest, SignupRequest, TokenResponse


async def _issue_tokens(executor, user_id: int) -> TokenResponse:
    access = create_access_token(user_id)
    refresh = generate_refresh_token()
    expires_at = datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )

    await executor.execute(
        """INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
           VALUES ($1, $2, $3)""",
        user_id, hash_token(refresh), expires_at,
    )

    return TokenResponse(access_token=access, refresh_token=refresh)

async def signup(pool: asyncpg.Pool, req: SignupRequest) -> TokenResponse:
    if await pool.fetchval("SELECT 1 FROM users WHERE email = $1", req.email):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="이미 가입된 이메일입니다.")

    user_id = await pool.fetchval(
        """ INSERT INTO users (email, password, nickname) VALUES ($1, $2, $3)
            VALUES ($1, $2, $3) RETURNING id;""",
        req.email, req.hash_password(req.password), req.nickname
    )

    return await _issue_tokens(pool, user_id)

async def login(pool: asyncpg.Pool, req: LoginRequest) -> TokenResponse:
    user = await pool.fetchrow(
        "SELECT id, password, is_active FROM users WHERE email = $1", req.email
    )

    # if not user or not user["password"]