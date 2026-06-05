import { useState, useEffect } from 'react';
import { checkNickname } from '../api';

export type NickStatus = 'idle' | 'checking' | 'available' | 'taken';

export function useNicknameCheck(nickname: string): NickStatus {
  const [status, setStatus] = useState<NickStatus>('idle');

  useEffect(() => {
    if (nickname.length < 2) {
      setStatus('idle');
      return;
    }
    setStatus('checking');
    let cancelled = false;
    const timer = setTimeout(async () => {
      try {
        const { available } = await checkNickname(nickname);
        if (!cancelled) setStatus(available ? 'available' : 'taken');
      } catch {
        if (!cancelled) setStatus('idle');
      }
    }, 400);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [nickname]);

  return status;
}
