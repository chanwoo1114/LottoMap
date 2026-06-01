import { api } from '@/lib/api';

export interface SignupRequest {
  email: string;
  password: string;
  nickname: string;
}

export interface LoginRequest {
  email: string;
  password: string;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

// 이메일 로그인
export const login = (body: LoginRequest) =>
  api.post<TokenResponse>('/auth/login', body).then((r) => r.data);

// 이메일 회원가입
export const signup = (body: SignupRequest) =>
  api.post<TokenResponse>('/auth/signup', body).then((r) => r.data);

// 인증 코드 발송
export const sendEmailCode = (email: string) =>
  api.post('/auth/email/send', { email }).then(() => undefined);

// 인증 코드 검증
export const verifyEmailCode = (email: string, code: string) =>
  api.post('/auth/email/verify', { email, code }).then(() => undefined);