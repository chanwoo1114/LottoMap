import { useState, type FormEvent } from 'react';


export function LoginForm({
  onSuccess,
} : {
onSuccess: () => void;
}) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  return (
    <form className="flex flex-col gap-4">
      <label>
        <span>이메일</span>
        <input

        />
      </label>

    </form>
  )
}