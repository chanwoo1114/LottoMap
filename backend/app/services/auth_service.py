import secrets
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

async def send_email_code(pool: asyncpg.Pool, email: str) -> None:
    if await pool.fetchval("SELECT 1 FROM users WHERE email = $1", email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 가입된 이메일입니다.",
        )

    last_sent_at = await pool.fetchval(
        """SELECT created_at
           FROM email_verifications
           WHERE email = $1
           ORDER BY created_at DESC LIMIT 1""",
        email,
    )

    if last_sent_at:
        elapsed = (datetime.now(timezone.utc) - last_sent_at).total_seconds()
        if elapsed < settings.EMAIL_CODE_RESEND_COOLDOWN_SECONDS:
            wait = int(settings.EMAIL_CODE_RESEND_COOLDOWN_SECONDS - elapsed)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"{wait}초 후 다시 요청해주세요.",
            )

    code = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(
        minutes=settings.EMAIL_CODE_TTL_MINUTES
    )

    await pool.execute(
        """INSERT INTO email_verifications (email, code, expires_at)
           VALUES ($1, $2, $3)""",
        email, hash_token(code), expires_at,
    )


    await


async def verify_email_code(pool: asyncpg.Pool, email: str, code: str) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """SELECT id, code, expires_at, attempts, verified_at
                   FROM email_verifications
                   WHERE email = $1
                   ORDER BY created_at DESC LIMIT 1
                   FOR UPDATE""",
                email,
            )
            print(row)



async def login(pool: asyncpg.Pool, req: LoginRequest) -> TokenResponse:
    user = await pool.fetchrow(
        "SELECT id, password, is_active FROM users WHERE email = $1", req.email
    )

    # if not user or not user["password"]