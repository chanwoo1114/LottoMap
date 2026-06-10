import axios, { AxiosError, type InternalAxiosRequestConfig } from 'axios';
import { tokenStore } from '@/features/auth/storage';

const BackendURL = import.meta.env.VITE_BACKEND_URL;

// API 호출
export const api = axios.create({
  baseURL: BackendURL,
  timeout: 5000,
  headers: { 'Content-Type': 'application/json' },
});

api.interceptors.request.use((config) => {
    const token = tokenStore.getAccess();
    if (token) config.headers.Authorization = `Bearer ${token}`;
    return config;
});

let onSessionExpired: (() => void) | null = null;
export function setOnSessionExpired(fn: () => void) {
    onSessionExpired = fn;
}

let refreshing: Promise<string | null> | null = null;

function refreshAccessToken(): Promise<string | null> {
    const refreshToken = tokenStore.getRefresh();
    if (!refreshToken) return Promise.resolve(null);

    return axios
        .post<{ access_token: string; refresh_token: string }>(
            `${BackendURL}/auth/refresh`,
            { refresh_token: refreshToken },
            { timeout: 5000, headers: { 'Content-Type': 'application/json' } },
        )
        .then(({ data }) => {
            tokenStore.save({
                accessToken: data.access_token,
                refreshToken: data.refresh_token,
            });
            return data.access_token;
        })
        .catch(() => null);
}

api.interceptors.response.use(
    (res) => res,
    async (error: AxiosError) => {
        const data = error.response?.data as { detail?: unknown } | undefined;
        const detail = data?.detail ?? error.response?.data;
        if (detail) error.message = typeof detail === 'string' ? detail : JSON.stringify(detail);

        const original = error.config as
            | (InternalAxiosRequestConfig & { _retry?: boolean })
            | undefined;

        if (
            error.response?.status === 401 &&
            original &&
            !original._retry &&
            tokenStore.getRefresh()
        ) {
            original._retry = true;

            refreshing = refreshing ?? refreshAccessToken().finally(() => { refreshing = null; });
            const newAccess = await refreshing;

            if (newAccess) {
                original.headers.Authorization = `Bearer ${newAccess}`;
                return api(original);
            }

            tokenStore.clear();
            onSessionExpired?.();
        }

        return Promise.reject(error);
    },
);

export function getApiErrorMessage(err: unknown, fallback = '요청 처리에 실패했습니다.'): string {
    if (axios.isAxiosError(err) && err.response) return err.message;
    return fallback;
}
