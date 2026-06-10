import asyncpg
from fastapi import APIRouter, Depends, Query, status

from app.core.database import get_pool
from app.schema.user_schema import (
    SocialLoginRequest, SocialProvider, LoginRequest, RefreshRequest,
    SignupRequest, TokenResponse, EmailSendRequest,
    EmailVerifyRequest, NicknameCheckResponse,
    PasswordResetRequest, UserResponse, AuthResponse
)
from app.api.deps import get_current_user_id
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=['인증'])


@router.post(
    "/signup",
    response_model=AuthResponse,
    status_code = status.HTTP_201_CREATED,
    summary = "이메일 회원가입",
)
async def signup(req: SignupRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.signup(pool, req)

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
    "/login",
    response_model = AuthResponse,
    summary="이메일 로그인",
)
async def login(req: LoginRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.login(pool, req)

@router.get(
    "/me",
    response_model=UserResponse,
    summary="내 정보 조회",
)
async def me(
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.Pool = Depends(get_pool),
):
    return await auth_service.get_me(pool, user_id)

@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="토큰 갱신 (리프레시)",
)
async def refresh(req: RefreshRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.refresh(pool, req.refresh_token)

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
  response_model=AuthResponse,
  summary="소셜 로그인 (kakao/naver/google)",
)
async def social_login(
  provider: SocialProvider,
  req: SocialLoginRequest,
  pool: asyncpg.Pool = Depends(get_pool),
):
  return await auth_service.social_login(pool, provider.value, req.access_token)