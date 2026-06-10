import { useState } from 'react';
import axios from 'axios';
import { useAuth } from '../AuthContext';
import { socialLogin, type SocialProvider } from '../api';
import { getSocialAccessToken } from '../socialSdk';
import { getApiErrorMessage } from '@/lib/api';

export function useSocialLogin(onClose: () => void) {
  const { setSession } = useAuth();
  const [loading, setLoading] = useState<SocialProvider | null>(null);
  const [error, setError] = useState<string | null>(null);

  const login = async (provider: SocialProvider) => {
    setError(null);
    setLoading(provider);
    try {
      const accessToken = await getSocialAccessToken(provider);
      const tokens = await socialLogin(provider, accessToken);
      setSession(tokens);
      onClose();
    } catch (err) {
      if (err instanceof Error && err.message === 'CANCELLED') return;
      if (axios.isAxiosError(err)) {

        setError(getApiErrorMessage(err, '네트워크 연결을 확인해주세요.'));
      } else {

        setError(err instanceof Error ? err.message : '소셜 로그인에 실패했습니다.');
      }
    } finally {
      setLoading(null);
    }
  };

  return { login, loading, error };
}
