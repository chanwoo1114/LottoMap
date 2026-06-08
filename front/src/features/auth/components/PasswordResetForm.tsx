import { useState, type FormEvent } from "react";
import { resetPassword, sendPasswordResetCode} from "@/features/auth/api.ts";


export function PasswordResetForm({
  onSwtichToLogin,
}: {
  onSwtichToLogin: () => void;
}) {
  const [email, setEmail] = useState("");

  return (
   <form className="flex flex-col gap-4">
     <h1>비밀번호 재설정</h1>

   </form>
  )
}