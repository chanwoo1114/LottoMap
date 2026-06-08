import { useState, type FormEvent } from 'react';
import axios from 'axios';
import {useAuth} from "@/features/auth/AuthContext.tsx";
import { login } from '../api';
import { SocialButtons } from './SocialButtons';

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

  const inputClass = "rounded-lg border border-border px-3 py-2 outline-none focus:border-emerald-500";
  const linkClass = "cursor-pointer text-gray-500 hover:text-gray-700 hover:underline"

  const emailInvalid = email !== "" && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
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
      <label className="flex flex-col gap-1 text-sm">
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoComplete="email"
          placeholder="이메일"
          className={`${inputClass} ${emailInvalid ? "border-red-500 focus:border-red-500" : ""}`}
        />
        {emailInvalid && (
          <span className="px-1 text-red-500">이메일 형식이 올바르지 않습니다.</span>
        )}
      </label>

      <label className="flex flex-col gap-1 text-sm">
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
          placeholder="비밀번호"
          className={inputClass}
        />
      </label>

      {error && (
        <p className="text-sm text-red-500">{error}</p>
      )}

      <button
        type="submit"
        disabled={!canSubmit}
        className="cursor-pointer flex flex-col items-center rounded-lg bg-emerald-500 py-2.5 font-medium text-white transition hover:opacity-90 active:scale-[0.99] disabled:cursor-not-allowed disabled:opacity-50"
      >
        {loading ? "로그인 중…" : "로그인"}
      </button>

      <div className="flex items-center justify-center gap-3 text-sm text-gray-500">
        <button
          type="button"
          onClick={onSwitchToReset}
          className={linkClass}
        >
          비밀번호 찾기
        </button>
        <span className="text-gray-300">|</span>
        <button
          type="button"
          onClick={onSwitchToSignup}
          className={linkClass}
        >
          회원가입
        </button>
      </div>

      <SocialButtons onClose={onClose} />
    </form>
  )
}