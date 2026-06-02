import { useState } from 'react';
import { AuthModal } from './AuthModal';

export function LoginButton() {
  const [open, setOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="rounded-lg bg-gray-100 px-3 py-1.5 text-sm font-medium text-text transition hover: opacity-90"
      >
        로그인
      </button>

      <AuthModal open={open} onClose={() => setOpen(false)} />
    </>
  );
}
