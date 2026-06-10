import { useState, type FormEvent } from 'react';
import axios from 'axios';
import {useAuth} from "@/features/auth/AuthContext.tsx";
import { login } from '../api';
import { SocialButtons } from './SocialButtons';
import { Button } from '@/components/ui/Button'
import { TextField } from '@/components/ui/TextField'
import { isValidEmail } from '../validation'

export function LoginForm({
  onClose,
  onSwitchToSignup,
  onSwitchToReset,
} : {
  onClose: () => void;
  onSwitchToSignup: () => void;
  onSwitchToReset: () => void;
}) {
  const { setSession } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const emailInvalid = email !== "" && !isValidEmail(email);
  const canSubmit = email !== "" && password !== "" && !emailInvalid && !loading;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!canSubmit) return;

    setError(null);
    setLoading(true);

    try {
      const tokens = await login({email, password});
      setSession(tokens);
      onClose();
    } catch (err) {
      if (axios.isAxiosError(err) && err.response) {
        setError(err.message);
      } else {
        setError("네트워크 연결을 확인해주세요.");
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="flex flex-col gap-4" onSubmit={handleSubmit} noValidate>
      <TextField
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        autoComplete="email"
        placeholder="이메일"
        invalid={emailInvalid}
        error={emailInvalid ? "이메일 형식이 올바르지 않습니다." : undefined}
      />

      <TextField
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        autoComplete="current-password"
        placeholder="비밀번호"
      />

      {error && (
        <p className="text-sm text-red-500">{error}</p>
      )}

      <Button type="submit" disabled={!canSubmit}>
        {loading ? "로그인 중…" : "로그인"}
      </Button>

      <div className="flex items-center justify-center gap-3 text-sm text-gray-500">
        <Button variant="link" onClick={onSwitchToReset}>
          비밀번호 찾기
        </Button>
        <span className="text-gray-300">|</span>
        <Button variant="link" onClick={onSwitchToSignup}>
          회원가입
        </Button>
      </div>

      <SocialButtons onClose={onClose} />
    </form>
  )
}