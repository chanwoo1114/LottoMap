import { useState, type FormEvent } from 'react';
import { useAuth } from '../AuthContext';
import { signup } from '../api';
import { getApiErrorMessage } from '@/lib/api';
import { useNicknameCheck } from '../hooks/useNicknameCheck';
import { useEmailVerification } from '../hooks/useEmailVerification';
import { SocialButtons } from './SocialButtons';
import { Button } from '@/components/ui/Button'
import { TextField } from '@/components/ui/TextField'
import { EmailVerifyField } from "./EmailVerifyField";
import { PasswordFields } from "./PasswordFields";
import { isValidPassword } from '../validation';

export function SignupForm({
  onClose,
  onSwitchToLogin,
}: {
  onClose: () => void;
  onSwitchToLogin: () => void;
}) {
  const { setSession } = useAuth();
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const emailVerify = useEmailVerification(email);
  const [password1, setPassword1] = useState("");
  const [password2, setPassword2] = useState("");
  const [nickname, setNickname] = useState("");

  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const nickStatus = useNicknameCheck(nickname);
  const nicknameTooShort = nickname !== "" && nickname.length < 2;
  const nicknameInvalid = nicknameTooShort || nickStatus === 'taken';

  const fieldsEnabled = emailVerify.verified

  const canSubmit =
    fieldsEnabled &&
    isValidPassword(password1) &&
    password1 === password2 &&
    nickname.length >= 2 &&
    nickStatus === 'available' &&
    !loading;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;
    setError(null);
    setLoading(true);
    try {
      const tokens = await signup({ email, password: password1, nickname });
      setSession(tokens);
      onClose();
    } catch (err) {
      setError(getApiErrorMessage(err, "네트워크 연결을 확인해주세요."));
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="flex flex-col gap-4" onSubmit={handleSubmit} noValidate>
      <h1 className="text-center text-lg font-bold">회원가입</h1>

      <EmailVerifyField
        email={email} setEmail={setEmail}
        code={code} setCode={setCode}
        verification={emailVerify}
        placeholder="이메일 입력"
      />

      {fieldsEnabled  && (
        <>
          <PasswordFields
            password1={password1} setPassword1={setPassword1}
            password2={password2} setPassword2={setPassword2}
          />

          <TextField
            type="text"
            value={nickname}
            onChange={(e) => setNickname(e.target.value)}
            placeholder="닉네임 입력"
            invalid={nicknameInvalid}
            message={
              <>
                {nicknameTooShort && (
                  <span className="text-red-500">닉네임은 2자 이상이어야 합니다.</span>
                )}
                {!nicknameTooShort && nickStatus === 'checking' && (
                  <span className="text-gray-400">중복 확인 중…</span>
                )}
                {!nicknameTooShort && nickStatus === 'taken' && (
                  <span className="text-red-500">이미 사용 중인 닉네임입니다.</span>
                )}
                {!nicknameTooShort && nickStatus === 'available' && (
                  <span className="text-emerald-600">사용 가능한 닉네임입니다.</span>
                )}
              </>
            }
          />
          {error && <p className="text-sm text-red-500">{error}</p>}

          <Button type="submit" disabled={!canSubmit}>
            {loading ? "가입 중…" : "새 계정으로 계속"}
          </Button>

        </>
      )}

      <p className="text-center text-sm text-gray-500">
        이미 계정이 있으신가요?{' '}
        <Button variant="link" onClick={onSwitchToLogin}>
          로그인
        </Button>
      </p>

      <SocialButtons onClose={onClose} />
    </form>
  );
}
