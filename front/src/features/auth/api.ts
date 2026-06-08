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

export interface NicknameCheckResponse {
  available: boolean;
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

// 닉네임 중복 검사
export const checkNickname = (nickname: string) =>
  api.get<NicknameCheckResponse>('/auth/nickname/check', {params: {nickname}}).then((r) => r.data);

// 비밀번호 재설정 코드 발송
export const sendPasswordResetCode = (email: string) =>
  api.post('/auth/password/email/send', { email }).then(() => undefined);

// 비밀번호 재설정
export const resetPassword = (body: { email: string; password: string }) =>
  api.post('/auth/password/reset', body).then(() => undefined);

export type SocialProvider = 'kakao' | 'naver' | 'google';

// 소셜 로그인 (제공자 access_token → 우리 JWT)
export const socialLogin = (provider: SocialProvider, accessToken: string) =>
  api
    .post<TokenResponse>(`/auth/social/${provider}`, { access_token: accessToken })
    .then((r) => r.data);
