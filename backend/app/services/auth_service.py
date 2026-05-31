import secrets
from datetime import datetime, timedelta, timezone

import asyncpg
from fastapi import HTTPException, status

from app.core.config import settings
from app.core.security import (
    create_access_token, hash_password, verify_password,
    generate_refresh_token, hash_token
)
from app.schema.user_schema import LoginRequest, SignupRequest, TokenResponse
from app.services.email_service import send_verification_code


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

async def _ensure_email_verified(pool: asyncpg.Pool, email: str) -> None:
    """최근 10분 내 이메일 인증이 완료됐는지 확인"""
    verified = await pool.fetchval(
        """SELECT 1 FROM email_verifications
                 WHERE email = $1
                     AND verified_at IS NOT NULL
                     AND verified_at > NOW() - INTERVAL '10 minutes'
                 ORDER BY verified_at DESC LIMIT 1""",
        email,
    )
    if not verified:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이메일 인증을 먼저 완료해주세요.",
        )

async def _ensure_email_available(pool: asyncpg.Pool, email: str) -> None:
    """이메일이 가입에 사용 가능한지 확인"""
    if await pool.fetchval("SELECT 1 FROM users WHERE email = $1", email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 가입된 이메일입니다.",
        )


async def signup(pool: asyncpg.Pool, req: SignupRequest) -> TokenResponse:
    # 회원가입
    await _ensure_email_verified(pool, req.email)
    await _ensure_email_available(pool, req.email)

    user_id = await pool.fetchval(
        """INSERT INTO users (email, password, nickname) VALUES ($1, $2, $3) RETURNING id""",
        req.email, hash_password(req.password), req.nickname
    )

    return await _issue_tokens(pool, user_id)

async def send_email_code(pool: asyncpg.Pool, email: str) -> None:
    await _ensure_email_available(pool, email)

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

    # 메일 발송 먼저 (실패하면 DB에 기록 안 남음)
    await send_verification_code(email, code)

    await pool.execute(
        """INSERT INTO email_verifications (email, code, expires_at)
           VALUES ($1, $2, $3)""",
        email, hash_token(code), expires_at,
    )


async def verify_email_code(pool: asyncpg.Pool, email: str, code: str) -> None:
    # 이메일 코드 검증
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

            if not row:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="인증 코드를 먼저 요청해주세요",
                )

            if row['verified_at'] is not None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="이미 인증된 이메일입니다."
                )

            if row["expires_at"] < datetime.now(timezone.utc):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="인증 코드가 만료되었습니다.",
                )

            if row["attempts"] >= 5:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="인증 시도 횟수를 초과했습니다.",
                )

            await conn.execute(
                """UPDATE email_verifications SET attempts = attempts + 1
                   WHERE id = $1""", row["id"],
                )

            if hash_token(code) != row["code"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="인증 코드가 올바르지 않습니다.",
                )

            await conn.execute(
                """UPDATE email_verifications SET verified_at = NOW() 
                   WHERE id = $1""", row["id"],
            )


async def login(pool: asyncpg.Pool, req: LoginRequest) -> TokenResponse:
    user = await pool.fetchrow(
        "SELECT id, password, is_active FROM users WHERE email = $1",
        req.email
    )

    if not user or not verify_password(req.password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="이메일 또는 비밀번호가 올바르지 않습니다.",
        )

    if not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="비활성화된 계정입니다.",
        )

    return await _issue_tokens(pool, user["id"])