from pydantic import BaseModel, EmailStr, Field
from enum import Enum


class SocialProvider(str, Enum):
    """소셜 로그인 제공자 종류 (카카오/네이버/구글)"""
    kakao = "kakao"
    naver = "naver"
    google = "google"

class UserResponse(BaseModel):
    """유저 정보 응답 (내 정보 조회 등)"""
    id: int
    email: EmailStr | None = None
    nickname: str
    profile_image: str | None = None

class AuthResponse(BaseModel):
    """토큰 + 유저 정보를 함께 담아 반환 (로그인/회원가입/소셜 공용)"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse

class SignupRequest(BaseModel):
    """이메일 회원가입 요청"""
    email: EmailStr = Field(description="이메일")
    password: str = Field(min_length=8, max_length=72, description="비밀번호")
    nickname: str = Field(min_length=2, max_length=50, description="닉네임")


class LoginRequest(BaseModel):
    """이메일 로그인 요청"""
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    """토큰 갱신(리프레시) 요청"""
    refresh_token: str = Field(description="리프레시 토큰")


class TokenResponse(BaseModel):
    """토큰만 반환 (리프레시 전용)"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class EmailSendRequest(BaseModel):
    """이메일 인증 코드 발송 요청"""
    email: EmailStr = Field(description="인증 코드 받을 이메일")


class EmailVerifyRequest(BaseModel):
    """이메일 인증 코드 검증 요청"""
    email: EmailStr
    code: str = Field(min_length=6, max_length=6,  pattern=r"^\d{6}$", description="6자리 숫자 코드")


class NicknameCheckResponse(BaseModel):
    """닉네임 중복 검사 응답"""
    available: bool = Field(description="사용 가능 여부")


class PasswordResetRequest(BaseModel):
    """비밀번호 재설정 요청"""
    email: EmailStr = Field(description="비밀번호를 재설정할 이메일")
    password: str = Field(min_length=8, max_length=72, description="새 비밀번호")


class SocialLoginRequest(BaseModel):
    """소셜 로그인 요청 (제공자 access_token 전달)"""
    access_token: str = Field(description="소셜 제공자 Access Token")
