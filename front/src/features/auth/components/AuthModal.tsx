import { useState } from "react";
import { Modal } from '@/components/ui/Modal'
import {LoginForm} from "@/features/auth/components/LoginForm.tsx";
import {SignupForm} from "@/features/auth/components/SignupForm.tsx";

export function AuthModal({ open, onClose}: {open: boolean; onClose: () => void}) {
  const [mode, setMode] = useState<'login' | 'signup'>('login');

  return (
    <Modal open={open} onClose={onClose}>
      {mode === 'login' ? (
        <>
          <LoginForm onClose={onClose} onSwitchToSignup={() => setMode('signup')} />
        </>
      ) : (
        <>
          <SignupForm onClose={onClose} onSwitchToLogin={() => setMode('login')} />
        </>
      )}
    </Modal>
  )
}
