import axios from 'axios';

const BackendURL = import.meta.env.VITE_BACKEND_URL;

// API 호출
export const api = axios.create({
  baseURL: BackendURL,
  timeout: 5000,
  headers: { 'Content-Type': 'application/json' },
});

// 에러 정규화
api.interceptors.request.use(
  (res) => res,
  (error) => {
    const detail = error.response?.data;
    if (detail) error.message = typeof detail === 'string' ? detail : JSON.stringify(detail);
    return Promise.reject(error);
  },
);