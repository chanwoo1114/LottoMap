const ACCESS_KEY = 'ACCESS_KEY';
const REFRESH_KEY = 'REFRESH_KEY';

export interface Tokens {
    accessToken: string;
    refreshToken: string;
}

export const tokenStore = {
    getAccess: () => localStorage.getItem(ACCESS_KEY),
    getRefresh: () => localStorage.getItem(REFRESH_KEY),

    save: ({ accessToken, refreshToken }: Tokens) => {
        localStorage.setItem(ACCESS_KEY, accessToken);
        localStorage.setItem(REFRESH_KEY, refreshToken);
    },

    clear: () => {
        localStorage.removeItem(ACCESS_KEY);
        localStorage.removeItem(REFRESH_KEY);
    },
};