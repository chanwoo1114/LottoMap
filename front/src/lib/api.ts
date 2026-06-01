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