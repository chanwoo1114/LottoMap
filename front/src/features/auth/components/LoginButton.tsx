import { useState } from 'react';
import { useAuth } from '../AuthContext';
import { AuthModal } from './AuthModal';
import { UserMenu } from './UserMenu';

export function LoginButton(
) {
  const { isAuthenticated } = useAuth();
  const [open, setOpen] = useState(false);

  if (isAuthenticated) return <UserMenu />;

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
