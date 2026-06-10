import { Button } from '@/components/ui/Button';
import { TextField } from '@/components/ui/TextField';
import { isValidEmail } from '../validation';
import type { useEmailVerification } from '../hooks/useEmailVerification';

export function EmailVerifyField({
email, setEmail,
code, setCode,
verification,
placeholder = "이메일",
  }: {
  email: string;
  setEmail: (v: string) => void;
  code: string;
  setCode: (v: string) => void;
  verification: ReturnType<typeof useEmailVerification>;
  placeholder?: string;
}) {
  const emailInvalid = email !== "" && !isValidEmail(email);
  const showCodeInput =
    !verification.verified &&
    (verification.status === 'sent' || verification.status === 'verifying');

  return (
    <>
      <TextField
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        autoComplete="email"
        placeholder={placeholder}
        disabled={verification.verified}
        invalid={emailInvalid}
        error={verification.error}
        right={
          <Button
            variant="outline"
            onClick={() => verification.sendCode()}
            disabled={email === "" || emailInvalid || verification.verified || verification.status === 'sending'}
          >
            {verification.verified
              ? '인증완료'
              : verification.status === 'sending'
                ? '발송 중…'
                : (verification.status === 'sent' || verification.status === 'verifying')
                  ? '재발송'
                  : '인증번호 발송'}
          </Button>
        }
      />

      {showCodeInput && (
        <TextField
          type="text"
          inputMode="numeric"
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder="인증코드 입력"
          right={
            <Button
              variant="outline"
              onClick={() => verification.verify(code)}
              disabled={code.length !== 6 || verification.status === 'verifying'}
            >
              {verification.status === 'verifying' ? '확인 중…' : '확인'}
            </Button>
          }
        />
      )}
    </>
  );
}
