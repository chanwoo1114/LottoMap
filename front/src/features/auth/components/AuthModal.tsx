import { useState } from "react";
import { Modal } from '@/components/ui/Modal'
import {LoginForm} from "@/features/auth/components/LoginForm.tsx";

export function AuthModal({ open, onClose}: {open: boolean; onClose: () => void}) {
  const [mode, setMode] = useState<'login' | 'signup'>('login');

  return (
    <Modal open={open} onClose={onClose}>
      <LoginForm />
    </Modal>
  )
}