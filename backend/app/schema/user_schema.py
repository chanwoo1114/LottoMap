from pydantic import BaseModel, EmailStr, Field

class SignupRequest(BaseModel):
    email: EmailStr = Field(description="이메일")
    password: str = Field(min_length=8, max_length=72, description="비밀번호")


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