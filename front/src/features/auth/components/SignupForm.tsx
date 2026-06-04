import { useState, type FormEvent } from 'react';
import { useAuth } from '../AuthContext';

export function SignupForm({
  onClose,
  onSwitchToLogin,
}: {
  onClose: () => void;
  onSwitchToLogin: () => void;
}) {
  const [email, setEmail] = useState("");
  const [password1, setPassword1] = useState("");
  const [password2, setPassword2] = useState("");
  const [nickname, setNickname] = useState("");
  const inputClass = "rounded-lg border border-border px-3 py-2 outline-none text-base";

  const emailInvalid = email !== "" && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  const passwordInvalid = password1 !== "" && (password1.length < 8 || !/[A-Za-z]/.test(password1) || !/[0-9]/.test(password1));

  const borderClass = (invalid: boolean) =>
    `rounded-lg border px-3 py-2 outline-none text-base ${
      invalid
        ? "border-red-500 focus:border-red-500"
        : "border-border focus:border-emerald-500"
    }`;


  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
  };

  return (
    <form className="flex flex-col gap-4" onSubmit={handleSubmit} noValidate>
      <h1 className="text-center text-lg font-bold">회원가입</h1>

      <label className="flex flex-col gap-1 text-sm">
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoComplete="email"
          placeholder="이메일 입력"
          className={borderClass(emailInvalid)}
        />
        {emailInvalid && (
          <span className="px-1 text-red-500">이메일 형식이 올바르지 않습니다.</span>
        )}
      </label>


      {/* 비밀번호 */}
      <label className="flex flex-col gap-1 text-sm">
        <input
          type="password"
          value={password1}
          onChange={(e) => setPassword1(e.target.value)}
          autoComplete="new-password"
          placeholder="비밀번호 입력"
          className={borderClass(passwordInvalid)}
        />
        {passwordInvalid && (
          <span className="px-1 text-red-500">8자 이상, 영문과 숫자를 포함해야 합니다.</span>
        )}
      </label>

      <label className="flex flex-col gap-1 text-sm">
        <input
          type="password"
          value={password2}
          onChange={(e) => setPassword2(e.target.value)}
          autoComplete="new-password"
          placeholder="비밀번호 확인"
          className={inputClass}
        />
      </label>

      <label className="flex flex-col gap-1 text-sm">
        <input
          type="text"
          value={nickname}
          onChange={(e) => setNickname(e.target.value)}
          placeholder="닉네임 입력"
          className={inputClass}
        />
      </label>

      <button
        type="submit"
        className="cursor-pointer rounded-lg bg-emerald-500 py-2.5 text-center font-medium text-white transition hover:opacity-90 active:scale-[0.99]"
      >
        새 계정으로 계속
      </button>
    </form>
  );
}