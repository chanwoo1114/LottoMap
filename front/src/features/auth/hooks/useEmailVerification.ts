import { useState } from "react";
import { sendEmailCode, verifyEmailCode} from "@/features/auth/api.ts";

export type EmailVerifyStatus =
  | 'idle'       // 발송 전
  | 'sending'    // 코드 발송 중
  | 'sent'       // 발송됨 (코드 입력 대기)
  | 'verifying'  // 코드 확인 중
  | 'verified';  // 인증 완료

export function useEmailVerification(
  email: string,
  sendFn: (email: string) => Promise<void> = sendEmailCode,
) {
  const [status, setStatus] = useState<EmailVerifyStatus>('idle');
  const [error, setError] = useState<string | null>(null);
  const [prevEmail, setPrevEmail] = useState(email);

  if (email !== prevEmail) {
    setPrevEmail(email);
    setStatus('idle');
    setError(null);
  }

  // 인증코드 발송
  const sendCode = async () => {
    setError(null);
    setStatus('sending');
    try {
      await sendFn(email.trim());
      setStatus('sent');
    } catch (err) {
      setStatus('idle');
      setError(err instanceof Error ? err.message : '인증코드 발송에 실패했습니다.');
    }
  };

  // 인증코드 확인
  const verify = async (code: string) => {
    setError(null);
    setStatus('verifying');
    try {
      await verifyEmailCode(email.trim(), code.trim());
      setStatus('verified');
    } catch (err) {
      setStatus('sent'); // 다시 코드 입력 단계로
      setError(err instanceof Error ? err.message : '인증코드가 올바르지 않습니다.');
    }
  };

  return {
    status,
    error,
    sendCode,
    verify,
    verified: status === 'verified',
  };
}
