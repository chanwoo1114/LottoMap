import { useState, type FormEvent } from 'react';
import { useAuth } from '../AuthContext';
import { signup } from '../api';
import { useNicknameCheck } from '../hooks/useNicknameCheck';
import { useEmailVerification } from '../hooks/useEmailVerification';
import { SocialButtons } from './SocialButtons';

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

  const emailInvalid = email !== "" && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  const passwordInvalid =
    password1 !== "" &&
    (password1.length < 8 || password1.length > 72 || !/[A-Za-z]/.test(password1) || !/[0-9]/.test(password1));
  const passwordMismatch = password2 !== "" && password1 !== password2;

  const nickStatus = useNicknameCheck(nickname);
  const nicknameTooShort = nickname !== "" && nickname.length < 2;
  const nicknameInvalid = nicknameTooShort || nickStatus === 'taken';
  
  const showCodeInput =
    !emailVerify.verified &&
    (emailVerify.status === 'sent' || emailVerify.status === 'verifying');

  const fieldsEnabled = emailVerify.verified

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

      {/* 이메일 */}
      <label className="flex flex-col gap-1 text-sm">
        <div className="flex gap-2">
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="email"
            placeholder="이메일 입력"
            disabled={emailVerify.verified}
            className={`flex-1 ${borderClass(emailInvalid)} disabled:bg-gray-50`}
          />
          <button
            type="button"
            onClick={() => emailVerify.sendCode()}
            disabled={email === "" || emailInvalid || emailVerify.verified || emailVerify.status === 'sending'}
            className="shrink-0 rounded-lg border border-emerald-500 px-3 py-2 text-sm font-medium text-emerald-600 transition hover:bg-emerald-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {emailVerify.verified
              ? '인증완료'
              : emailVerify.status === 'sending'
                ? '발송 중…'
                : emailVerify.status === 'sent' || emailVerify.status === 'verifying'
                  ? '재발송'
                  : '인증번호 발송'}
          </button>
        </div>
      </label>

      {/*이메일 인증코드*/}
      {showCodeInput && (
        <label className="flex flex-col gap-1 text-sm">
          <input
            type="text"
            inputMode="numeric"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            placeholder="인증코드 입력"
            className={`flex-1 ${borderClass(false)}`}
          >
          </input>
        </label>
      )}

      {/*이메일 인증 후 활성화*/}
      {fieldsEnabled  && (
        <>
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

          {/* 비밀번호 확인 */}
          <label className="flex flex-col gap-1 text-sm">
            <input
              type="password"
              value={password2}
              onChange={(e) => setPassword2(e.target.value)}
              autoComplete="new-password"
              placeholder="비밀번호 확인"
              className={borderClass(passwordMismatch)}
            />
            {passwordMismatch && (
              <span className="text-red-500">비밀번호가 일치하지 않습니다.</span>
            )}
          </label>

          {/* 닉네임 */}
          <label className="flex flex-col gap-1 text-sm">
            <input
              type="text"
              value={nickname}
              onChange={(e) => setNickname(e.target.value)}
              placeholder="닉네임 입력"
              className={borderClass(nicknameInvalid)}
            />
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
          </label>
          <button
            type="submit"
            className="cursor-pointer rounded-lg bg-emerald-500 py-2.5 text-center font-medium text-white transition hover:opacity-90 active:scale-[0.99]"
          >
            새 계정으로 계속
          </button>

        </>
      )}

      <p className="text-center text-sm text-gray-500">
        이미 계정이 있으신가요?{' '}
        <button
          type="button"
          onClick={onSwitchToLogin}
          className="font-medium hover:underline"
        >
          로그인
        </button>
      </p>

      <SocialButtons onClose={onClose} />
    </form>
  );
}
