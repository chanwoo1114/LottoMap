import { useState, type FormEvent } from 'react';
import {useAuth} from "@/features/auth/AuthContext.tsx";


export function LoginForm({
  onClose,
  onSwitchToSignup,
} : {
  onClose: () => void;
  onSwitchToSignup: () => void;
}) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const inputClass = "rounded-lg border border-border px-3 py-2 outline-none focus:border-emerald-500";
  const linkClass = "cursor-pointer text-gray-500 hover:text-gray-700 hover:underline"
  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();

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
          className={inputClass}
        />
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

      <button
        type="submit"
        className="cursor-pointer flex flex-col rounded-lg bg-emerald-500 py-2.5 font-medium text-white transition hover:opacity-90 active:scale-[0.99]"
      >
        로그인
      </button>

      <div className="flex items-center justify-center gap-3 text-sm text-gray-500">
        <button
          type="button"
          className={linkClass}
        >
          이메일 찾기
        </button>
        <span className="text-gray-300">|</span>
        <button
          type="button"
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

    </form>
  )
}