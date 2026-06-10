import type { SocialProvider } from './api';

/**
 * 제공자별로 access_token 을 받아온다. 백엔드(/auth/social/{provider})가
 * 3사 모두 access_token 을 받으므로, 프론트는 토큰을 구해 넘기기만 하면 된다.
 *
 * 필요한 환경변수:
 *   VITE_GOOGLE_CLIENT_ID, VITE_NAVER_CLIENT_ID, VITE_KAKAO_JS_KEY
 * 그리고 동일 출처 콜백 라우트 `/oauth/callback` (빈 페이지여도 됨) 가 있어야 한다.
 *
 * 취소(팝업 닫힘 등)는 Error('CANCELLED') 로 reject → 훅에서 조용히 무시.
 */

const env = import.meta.env as Record<string, string | undefined>;
const REDIRECT_URI = `${location.origin}/oauth/callback`;

/** 구글 GIS 스크립트 1회 로드 */
let gsiLoaded: Promise<void> | null = null;
function loadGsi(): Promise<void> {
  if (gsiLoaded) return gsiLoaded;
  gsiLoaded = new Promise<void>((resolve, reject) => {
    const el = document.createElement('script');
    el.src = 'https://accounts.google.com/gsi/client';
    el.async = true;
    el.onload = () => resolve();
    el.onerror = () => reject(new Error('구글 SDK를 불러오지 못했습니다.'));
    document.head.appendChild(el);
  });
  return gsiLoaded;
}

/**
 * OAuth 팝업을 띄우고, 동일 출처 콜백으로 돌아오면 query/hash 파라미터를 돌려준다.
 * (카카오=code in query, 네이버=token in hash)
 */
function openOAuthPopup(authUrl: string): Promise<URLSearchParams> {
  return new Promise((resolve, reject) => {
    const popup = window.open(authUrl, 'oauth', 'width=480,height=640');
    if (!popup) {
      reject(new Error('팝업이 차단되었습니다. 팝업을 허용해주세요.'));
      return;
    }
    const timer = window.setInterval(() => {
      try {
        if (popup.closed) {
          window.clearInterval(timer);
          reject(new Error('CANCELLED'));
          return;
        }
        // cross-origin 인 동안엔 popup.location.href 접근이 예외 → 콜백 도착 전까지 무시
        const href = popup.location.href;
        if (popup.location.origin === location.origin && (popup.location.search || popup.location.hash)) {
          window.clearInterval(timer);
          const raw = popup.location.search
            ? popup.location.search.slice(1)
            : popup.location.hash.slice(1);
          popup.close();
          resolve(new URLSearchParams(raw));
          void href;
        }
      } catch {
        /* cross-origin 접근 예외 → 폴링 계속 */
      }
    }, 400);
  });
}

// 구글: GIS 토큰 클라이언트(팝업) → access_token  ✅ SPA에 가장 깔끔
async function googleToken(): Promise<string> {
  const clientId = env.VITE_GOOGLE_CLIENT_ID;
  if (!clientId) throw new Error('VITE_GOOGLE_CLIENT_ID 가 설정되지 않았습니다.');
  await loadGsi();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const google = (window as any).google;
  return new Promise<string>((resolve, reject) => {
    const client = google.accounts.oauth2.initTokenClient({
      client_id: clientId,
      scope: 'openid email profile',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      callback: (resp: any) =>
        resp?.access_token ? resolve(resp.access_token) : reject(new Error('CANCELLED')),
      error_callback: () => reject(new Error('CANCELLED')),
    });
    client.requestAccessToken();
  });
}

// 네이버: OAuth implicit(response_type=token) → 콜백 hash 의 access_token
async function naverToken(): Promise<string> {
  const clientId = env.VITE_NAVER_CLIENT_ID;
  if (!clientId) throw new Error('VITE_NAVER_CLIENT_ID 가 설정되지 않았습니다.');
  const state = crypto.randomUUID();
  const url =
    `https://nid.naver.com/oauth2.0/authorize?response_type=token` +
    `&client_id=${clientId}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
    `&state=${state}`;
  const params = await openOAuthPopup(url);
  const token = params.get('access_token');
  if (!token) throw new Error('네이버 토큰을 받지 못했습니다.');
  return token;
}

// 카카오: OAuth code(response_type=code) → 토큰 교환
// ⚠️ 카카오 JS SDK v2 는 클라이언트 access_token 발급을 막아서 code 방식만 가능.
//    아래 토큰 교환(kauth)은 브라우저 CORS 로 막힐 수 있음 → 막히면 백엔드 교환으로 옮겨야 함.
async function kakaoToken(): Promise<string> {
  const jsKey = env.VITE_KAKAO_JS_KEY;
  if (!jsKey) throw new Error('VITE_KAKAO_JS_KEY 가 설정되지 않았습니다.');
  const authUrl =
    `https://kauth.kakao.com/oauth/authorize?response_type=code` +
    `&client_id=${jsKey}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`;
  const params = await openOAuthPopup(authUrl);
  const code = params.get('code');
  if (!code) throw new Error('카카오 인가코드를 받지 못했습니다.');

  const resp = await fetch('https://kauth.kakao.com/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8' },
    body: new URLSearchParams({
      grant_type: 'authorization_code',
      client_id: jsKey,
      redirect_uri: REDIRECT_URI,
      code,
    }),
  });
  const data = await resp.json();
  if (!data.access_token) throw new Error('카카오 토큰 교환에 실패했습니다.');
  return data.access_token as string;
}

const GETTERS: Record<SocialProvider, () => Promise<string>> = {
  google: googleToken,
  naver: naverToken,
  kakao: kakaoToken,
};

export function getSocialAccessToken(provider: SocialProvider): Promise<string> {
  return GETTERS[provider]();
}
