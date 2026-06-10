import { TextField } from '@/components/ui/TextField';
import { isValidPassword } from '../validation';

export function PasswordFields({
  password1, setPassword1,
  password2, setPassword2,
  placeholder1 = "비밀번호 입력",
  placeholder2 = "비밀번호 확인",
}: {
  password1: string;
  setPassword1: (v: string) => void;
  password2: string;
  setPassword2: (v: string) => void;
  placeholder1?: string;
  placeholder2?: string;
}) {
  const passwordInvalid = password1 !== "" && !isValidPassword(password1);
  const passwordMismatch = password2 !== "" && password1 !== password2;

  return (
    <>
      <TextField
        type="password"
        value={password1}
        onChange={(e) => setPassword1(e.target.value)}
        autoComplete="new-password"
        placeholder={placeholder1}
        invalid={passwordInvalid}
        error={passwordInvalid ? "8자 이상, 영문과 숫자를 포함해야 합니다." : undefined}
      />
      <TextField
        type="password"
        value={password2}
        onChange={(e) => setPassword2(e.target.value)}
        autoComplete="new-password"
        placeholder={placeholder2}
        invalid={passwordMismatch}
        error={passwordMismatch ? "비밀번호가 일치하지 않습니다." : undefined}
      />
    </>
  );
}
