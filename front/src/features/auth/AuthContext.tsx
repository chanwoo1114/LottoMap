import { createContext, useContext, useState, type ReactNode } from 'react';
import { tokenStore } from './storage';
import type { TokenResponse } from './api';

interface AuthState {
  isAuthenticated: boolean;
  setSession: (tokens: TokenResponse) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(() => !!tokenStore.getAccess());
  // const [isAuthenticated, setIsAuthenticated] = useState(true)

  const setSession = (tokens: TokenResponse) => {
    tokenStore.save({
      accessToken: tokens.access_token,
      refreshToken: tokens.refresh_token,
    });
    setIsAuthenticated(true);
  };

  const logout = () => {
    tokenStore.clear();
    setIsAuthenticated(false);
  };


  return (
    <AuthContext.Provider value={{ isAuthenticated, setSession, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider');
  return ctx;
}
