import asyncpg
from fastapi import APIRouter, Depends, status

from app.core.database import get_pool
from app.schema.user_schema import (
    KakaoLoginRequest, LoginRequest, RefreshRequest,
    SignupRequest, TokenResponse, EmailSendRequest,
    EmailVerifyRequest
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

@router.post(
    "/email/send",
    status_code = status.HTTP_204_NO_CONTENT,
    summary="이메일 인증 코드 발송"
)
async def send_email(req: EmailSendRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.send_email_code(pool, req.email)




@router.post(
    "/login",
    response_model = TokenResponse,
    summary="이메일 로그인",
)
async def login(req: LoginRequest, pool: asyncpg.Pool = Depends(get_pool)):
    return await auth_service.login(pool, req)