import { useState, type FormEvent } from 'react';
import { resetPassword, sendPasswordResetCode } from '@/features/auth/api.ts';
import { getApiErrorMessage } from '@/lib/api';
import { useEmailVerification } from '../hooks/useEmailVerification';
import { Button } from '@/components/ui/Button'
import { EmailVerifyField } from "./EmailVerifyField";
import { PasswordFields } from "./PasswordFields";
import { isValidPassword } from '../validation';

export function PasswordResetForm({ onSwitchToLogin }: { onSwitchToLogin: () => void }) {
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [password1, setPassword1] = useState("");
  const [password2, setPassword2] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const verification = useEmailVerification(email, sendPasswordResetCode);

  const fieldsEnabled = verification.verified;

  const canSubmit =
    fieldsEnabled && isValidPassword(password1) && password1 === password2 && !loading;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;
    setError(null);
    setLoading(true);
    try {
      await resetPassword({ email, password: password1 });
      onSwitchToLogin();
    } catch (err) {
      setError(getApiErrorMessage(err, "네트워크 연결을 확인해주세요."));
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="flex flex-col gap-4" onSubmit={handleSubmit} noValidate>
      <h1 className="text-center text-lg font-bold">비밀번호 재설정</h1>

      <EmailVerifyField
        email={email} setEmail={setEmail}
        code={code} setCode={setCode}
        verification={verification}
        placeholder="가입한 이메일"
      />

      {fieldsEnabled && (
        <>
          <PasswordFields
            password1={password1} setPassword1={setPassword1}
            password2={password2} setPassword2={setPassword2}
            placeholder1="새 비밀번호"
            placeholder2="새 비밀번호 확인"
          />

          {error && <p className="text-sm text-red-500">{error}</p>}

          <Button type="submit" disabled={!canSubmit}>
            {loading ? "변경 중…" : "비밀번호 변경"}
          </Button>

        </>
      )}

      <p className="text-center text-sm text-gray-500">
        <Button variant="link" onClick={onSwitchToLogin}>
          로그인으로 돌아가기
        </Button>
      </p>
    </form>
  );
}
