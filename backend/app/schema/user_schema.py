from pydantic import BaseModel, EmailStr, Field

class SignupRequest(BaseModel):
    email: EmailStr = Field(description="이메일")
    password: str = Field(min_length=8, max_length=72, description="비밀번호")
    nickname: str = Field(min_length=2, max_length=50, description="닉네임")


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class KakaoLoginRequest(BaseModel):
    access_token: str = Field(description="카카오 JS SDK로 발급받은 access token")


class RefreshRequest(BaseModel):
    refresh_token: str = Field(description="리프레시 토큰")


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class EmailSendRequest(BaseModel):
    email: EmailStr = Field(description="인증 코드 받을 이메일")


class EmailVerifyRequest(BaseModel):
    email: EmailStr
    code: str = Field(min_length=6, max_length=6,  pattern=r"^\d{6}$", description="6자리 숫자 코드")


class NicknameCheckResponse(BaseModel):
    available: bool = Field(description="사용 가능 여부")
