import secrets
from datetime import datetime, timedelta, timezone

import asyncpg
from fastapi import HTTPException, status

from app.core.config import settings
from app.core.security import (
    create_access_token, hash_password, verify_password,
    generate_refresh_token, hash_token
)
from app.schema.user_schema import LoginRequest, SignupRequest, TokenResponse, UserResponse, AuthResponse
from app.services.email_service import send_verification_code
from app.services.social_providers import fetch_social_profile

async def _issue_auth(executor, user_id: int) -> AuthResponse:
    """토큰 발급 + 유저 정보를 함께 담아 반환 (로그인/회원가입/소셜 공용)"""
    tokens = await _issue_tokens(executor, user_id)
    row = await executor.fetchrow(
        """SELECT id, email, nickname, profile_image
           FROM users WHERE id = $1""",
        user_id,
    )
    return AuthResponse(**tokens.model_dump(), user=UserResponse(**dict(row)))

async def _issue_tokens(executor, user_id: int) -> TokenResponse:
    """access 토큰 발급 + refresh 토큰 생성·저장 후 토큰 쌍 반환."""
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

async def _consume_email_verification(conn, email: str) -> None:
    """최근 10분 내 인증된 행을 1건 찾아 '소비'(verified_at=NULL)해 재사용을 막는다.
    인증 내역이 없으면 400. 반드시 호출자 트랜잭션 안에서 사용."""
    row = await conn.fetchrow(
        """SELECT id FROM email_verifications
                 WHERE email = $1
                     AND verified_at IS NOT NULL
                     AND verified_at > NOW() - INTERVAL '10 minutes'
                 ORDER BY verified_at DESC LIMIT 1
                 FOR UPDATE""",
        email,
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="이메일 인증을 먼저 완료해주세요.",
        )
    await conn.execute(
        "UPDATE email_verifications SET verified_at = NULL WHERE id = $1",
        row["id"],
    )

async def _ensure_email_available(pool: asyncpg.Pool, email: str) -> None:
    """이메일이 가입에 사용 가능한지 확인"""
    if await pool.fetchval("SELECT 1 FROM users WHERE email = $1", email):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 가입된 이메일입니다.",
        )


async def _ensure_nickname_available(pool: asyncpg.Pool, nickname: str) -> None:
    """닉네임이 사용 가능여부 확인"""
    if await pool.fetchval("SELECT 1 FROM users WHERE nickname = $1", nickname):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="이미 사용 중인 닉네임입니다.",
        )


async def check_nickname_available(pool: asyncpg.Pool, nickname: str) -> bool:
    """닉네임 중복 검사"""
    exists = await pool.fetchval("SELECT 1 FROM users WHERE nickname = $1", nickname)
    return not exists


async def signup(pool: asyncpg.Pool, req: SignupRequest) -> AuthResponse:
    """회원가입"""
    await _ensure_email_available(pool, req.email)
    await _ensure_nickname_available(pool, req.nickname)

    # 인증 소비 → 유저 생성 → 토큰 발급을 한 트랜잭션으로
    # (인증 소비를 가용성 검증 뒤로 두어, 닉네임 충돌 등으로 실패해도 인증이 낭비되지 않게)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _consume_email_verification(conn, req.email)
            user_id = await conn.fetchval(
                """INSERT INTO users (email, password, nickname) VALUES ($1, $2, $3) RETURNING id""",
                req.email, hash_password(req.password), req.nickname,
            )
            return await _issue_auth(conn, user_id)

async def send_email_code(pool: asyncpg.Pool, email: str) -> None:
    """이메일 확인 코드 전송 (회원가입용: 아직 가입 안 된 이메일이어야 함)"""
    await _ensure_email_available(pool, email)
    await _create_and_send_code(pool, email)


async def verify_email_code(pool: asyncpg.Pool, email: str, code: str) -> None:
    """이메일 코드 검증"""
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

            if row["attempts"] >= settings.EMAIL_CODE_MAX_ATTEMPTS:
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


async def login(pool: asyncpg.Pool, req: LoginRequest) -> AuthResponse:
    """로그인"""
    user = await pool.fetchrow(
        "SELECT id, password, is_active FROM users WHERE email = $1",
        req.email,
    )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="이메일 또는 비밀번호가 올바르지 않습니다.",
        )

    if user["password"] is None:
        name = await _social_provider_name(pool, user["id"])
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"{name} 로그인으로 가입된 계정입니다. {name} 로그인을 이용해주세요.",
        )

    if not verify_password(req.password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="이메일 또는 비밀번호가 올바르지 않습니다.",
        )

    if not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="비활성화된 계정입니다.",
        )

    return await _issue_auth(pool, user["id"])


async def refresh(pool: asyncpg.Pool, refresh_token: str) -> TokenResponse:
    """리프레시 토큰으로 새 토큰 쌍 발급 (회전).

    기존 토큰은 무효화(revoked_at)하고 새 access/refresh 쌍을 내려준다.
    유효하지 않거나 만료/무효화된 토큰이면 401.
    """
    token_hash = hash_token(refresh_token)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """SELECT id, user_id, expires_at, revoked_at
                   FROM refresh_tokens
                   WHERE token_hash = $1
                   FOR UPDATE""",
                token_hash,
            )

            if row is None or row["revoked_at"] is not None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="유효하지 않은 토큰입니다. 다시 로그인해주세요.",
                )

            if row["expires_at"] < datetime.now(timezone.utc):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="세션이 만료되었습니다. 다시 로그인해주세요.",
                )

            is_active = await conn.fetchval(
                "SELECT is_active FROM users WHERE id = $1", row["user_id"]
            )
            if not is_active:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="비활성화된 계정입니다.",
                )

            await conn.execute(
                "UPDATE refresh_tokens SET revoked_at = NOW() WHERE id = $1",
                row["id"],
            )
            return await _issue_tokens(conn, row["user_id"])


async def _ensure_email_exists(pool: asyncpg.Pool, email: str) -> None:
    """이메일 가입 여부 확인"""
    if not await pool.fetchval("SELECT 1 FROM users WHERE email = $1", email):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="가입되지 않은 이메일입니다.",
        )

async def _create_and_send_code(pool: asyncpg.Pool, email: str) -> None:
    """24h 차단 → 쿨다운 → 코드 생성·발송·기록"""
    exhausted = await pool.fetchval(
        """SELECT COUNT(*) FROM email_verifications
           WHERE email = $1
             AND attempts >= $2
             AND created_at > NOW() - INTERVAL '24 hours'""",
        email, settings.EMAIL_CODE_MAX_ATTEMPTS,
    )
    if exhausted >= settings.EMAIL_CODE_MAX_EXHAUSTED_PER_DAY:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="인증 실패가 너무 많습니다. 24시간 후 다시 시도해주세요.",
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

    await send_verification_code(email, code)

    await pool.execute(
        """INSERT INTO email_verifications (email, code, expires_at)
           VALUES ($1, $2, $3)""",
        email, hash_token(code), expires_at,
    )


async def send_password_reset_code(pool: asyncpg.Pool, email: str) -> None:
    """비밀번호 재설정 코드 전송 (가입된 이메일이어야 함)."""
    await _ensure_email_exists(pool, email)
    await _create_and_send_code(pool, email)


async def reset_password(pool: asyncpg.Pool, email:str, new_password:str) -> None:
    """비밀번호 재설정"""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _consume_email_verification(conn, email)

            user_id = await conn.fetchval("SELECT id FROM users WHERE email = $1", email)
            if not user_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="가입되지 않은 이메일입니다.",
                )

            await conn.execute(
                "UPDATE users SET password = $1 WHERE id = $2",
                hash_password(new_password), user_id,
            )
            await conn.execute(
                "DELETE FROM refresh_tokens WHERE user_id = $1", user_id,
            )

_PROVIDER_NAMES = {"kakao": "카카오", "naver": "네이버", "google": "구글"}

async def _social_provider_name(executor, user_id: int) -> str:
    """user_id가 가입에 쓴 소셜 제공자 표시명('카카오' 등) 반환"""
    provider = await executor.fetchval(
        """SELECT provider FROM user_oauth
           WHERE user_id = $1 ORDER BY created_at LIMIT 1""",
           user_id,
    )
    return _PROVIDER_NAMES.get(provider, "소셜")


async def social_login(
        pool: asyncpg.Pool, provider: str, access_token: str
) -> AuthResponse:
    """소셜 로그인

    1. (provider, uid)로 기존 연동 조회 → 있으면 그 유저로 로그인
    2. 없는데 같은 이메일이 이미 가입돼 있으면 → 가입 방법 안내하며 거부
    3. 그래도 없으면 신규 가입 후 연동
    """
    profile = await fetch_social_profile(provider, access_token)

    async with pool.acquire() as conn:
        async with conn.transaction():
            user_id = await conn.fetchval(
                """SELECT user_id
                   FROM user_oauth
                   WHERE provider = $1
                     AND provider_uid = $2""",
                profile.provider, profile.uid,
            )

            if user_id is None:
                if profile.email:
                    existing = await conn.fetchrow(
                        "SELECT id, password FROM users WHERE email = $1",
                        profile.email,
                    )
                    if existing:
                        if existing["password"] is not None:
                            detail = "이메일로 가입된 계정입니다. 이메일 로그인을 이용해주세요."
                        else:
                            name = await _social_provider_name(conn, existing["id"])
                            detail = f"{name} 로그인으로 가입된 계정입니다. {name} 로그인을 이용해주세요."
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail=detail,
                        )

                user_id = await conn.fetchval(
                    """INSERT INTO users (email, nickname, profile_image)
                       VALUES ($1, $2, $3) RETURNING id""",
                    profile.email,
                    profile.nickname or f"{provider}_{profile.uid[:8]}",
                    profile.profile_image or "",
                )
                await conn.execute(
                    """INSERT INTO user_oauth (user_id, provider, provider_uid)
                       VALUES ($1, $2, $3)""",
                    user_id, profile.provider, profile.uid,
                )
                is_active = True
            else:
                is_active = await conn.fetchval(
                    "SELECT is_active FROM users WHERE id = $1", user_id
                )

            if not is_active:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="비활성화된 계정입니다.",
                )

            return await _issue_auth(conn, user_id)

async def get_me(pool: asyncpg.Pool, user_id: int) -> UserResponse:
    """현재 로그인한 사용자 정보"""
    row = await pool.fetchrow(
        """SELECT id, email, nickname, profile_image
           FROM users WHERE id = $1 AND is_active = TRUE""",
        user_id,
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )
    return UserResponse(**dict(row))