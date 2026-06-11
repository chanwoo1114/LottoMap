import { useState } from "react";
import { Modal } from '@/components/ui/Modal'
import {LoginForm} from "@/features/auth/components/LoginForm.tsx";
import {SignupForm} from "@/features/auth/components/SignupForm.tsx";
import { PasswordResetForm } from "@/features/auth/components/PasswordResetForm.tsx";

interface AuthModalProps {
  open: boolean;
  onClose: () => void;
}

export function AuthModal({ open, onClose }: AuthModalProps) {
  const [mode, setMode] = useState<'login' | 'signup' | 'reset'>('login');

  return (
    <Modal open={open} onClose={onClose}>
      {mode === 'login' && (
        <LoginForm
          onClose={onClose}
          onSwitchToSignup={() => setMode('signup')}
          onSwitchToReset={() => setMode('reset')}
        />
      )}
      {mode === 'signup' && (
        <SignupForm onClose={onClose} onSwitchToLogin={() => setMode('login')}
        />
      )}
      {mode === 'reset' && (
        <PasswordResetForm onSwitchToLogin={() => setMode('login')} />
      )}
    </Modal>
  );
}
