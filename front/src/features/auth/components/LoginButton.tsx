import { useState } from 'react';
import { useAuth } from '../AuthContext';
import { AuthModal } from './AuthModal';


export function LoginButton() {
  const { isAuthenticated, logout } = useAuth();
  const [open, setOpen] = useState(false);

  if (isAuthenticated) {
    return (
      <button
        type="button"
        onClick={logout}
        className="rounded-lg border border-border px-3 py-1.5 text-sm font-medium hover:bg-gray-50"
      >
        로그아웃
      </button>
    )
  }

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="rounded-lg border border-transparent bg-[#2E5BD6] px-3.5 py-1.5 text-sm font-medium text-white transition hover:bg-[#264FB8]"
      >
        로그인
      </button>

      <AuthModal open={open} onClose={() => setOpen(false)} />
    </>
  );
}
