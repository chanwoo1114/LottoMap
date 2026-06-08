import asyncpg
from fastapi import APIRouter, Depends, Query, status

from app.core.database import get_pool
from app.schema.user_schema import (
    SocialLoginRequest, SocialProvider, LoginRequest, RefreshRequest,
    SignupRequest, TokenResponse, EmailSendRequest,
    EmailVerifyRequest, NicknameCheckResponse,
    PasswordResetRequest
)
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=['인증'])


@router.post(
    "/signup",
    response_model=TokenResponse,
    status_code = status.HTTP_201_CREATED,
    summary = "이메일 회원가입",
)
async def signup(req: SignupRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.signup(pool, req)

@router.get(
    "/nickname/check",
    response_model=NicknameCheckResponse,
    summary="닉네임 중복 검사",
)
async def check_nickname(
    nickname: str = Query(min_length=2, max_length=50, description="검사할 닉네임"),
    pool: asyncpg.Pool = Depends(get_pool),
):
    available = await auth_service.check_nickname_available(pool, nickname)
    return NicknameCheckResponse(available=available)

@router.post(
    "/email/send",
    status_code = status.HTTP_204_NO_CONTENT,
    summary="이메일 인증 코드 발송",
)
async def send_email(req: EmailSendRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.send_email_code(pool, req.email)

@router.post(
    "/email/verify",
    status_code = status.HTTP_204_NO_CONTENT,
    summary="이메일 인증 코드 검증",
)
async def verify_email(req: EmailVerifyRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.verify_email_code(pool, req.email, req.code)

@router.post(
    "/login",
    response_model = TokenResponse,
    summary="이메일 로그인",
)
async def login(req: LoginRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.login(pool, req)

@router.post(
    "/password/email/send",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="비밀번호 재설정 코드 발송"
)
async def send_password_reset(req: EmailSendRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.send_password_reset_code(pool, req.email)

@router.post(
    "/password/reset",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="비밀번호 재설정",
)
async def reset_password(req: PasswordResetRequest, pool: asyncpg.Pool =
Depends(get_pool)):
    return await auth_service.reset_password(pool, req.email, req.password)

@router.post(
  "/social/{provider}",
  response_model=TokenResponse,
  summary="소셜 로그인 (kakao/naver/google)",
)
async def social_login(
  provider: SocialProvider,
  req: SocialLoginRequest,
  pool: asyncpg.Pool = Depends(get_pool),
):
  return await auth_service.social_login(pool, provider.value, req.access_token)