import axios from 'axios';
import { tokenStore } from '@/features/auth/storage';

const BackendURL = import.meta.env.VITE_BACKEND_URL;

// API 호출
export const api = axios.create({
  baseURL: BackendURL,
  timeout: 5000,
  headers: { 'Content-Type': 'application/json' },
});

// 요청마다 access 토큰 주입
api.interceptors.request.use((config) => {
    const token = tokenStore.getAccess();
    if (token) config.headers.Authorization = `Bearer ${token}`;
    return config;
});

// 에러 정규화 (백엔드 detail 메시지를 error.message 로)
api.interceptors.response.use(
    (res) => res,
    (error) => {
        const detail = error.response?.data?.detail ?? error.response?.data;
        if (detail) error.message = typeof detail === 'string' ? detail : JSON.stringify(detail);
        return Promise.reject(error);
    },
);

// 응답이 온 에러(401·403·409 등)는 위 인터셉터가 message를 한국어로 정규화해둠 → 그대로 사용.
// 응답 자체가 없으면(네트워크·타임아웃) fallback 문구.
export function getApiErrorMessage(err: unknown, fallback = '요청 처리에 실패했습니다.'): string {
    if (axios.isAxiosError(err) && err.response) return err.message;
    return fallback;
}