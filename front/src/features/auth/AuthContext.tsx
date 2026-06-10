import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';
import { tokenStore } from './storage';
import { setOnSessionExpired } from '@/lib/api';
import { getMe, type AuthResponse, type UserResponse } from './api';

interface AuthState {
  isAuthenticated: boolean;
  user: UserResponse | null;
  setSession: (auth: AuthResponse) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(() => !!tokenStore.getAccess());
  const [user, setUser] = useState<UserResponse | null>(null);

  const setSession = (auth: AuthResponse) => {
    tokenStore.save({
      accessToken: auth.access_token,
      refreshToken: auth.refresh_token,
    });
    setIsAuthenticated(true);
    setUser(auth.user);
  };

  const logout = () => {
    tokenStore.clear();
    setIsAuthenticated(false);
    setUser(null);
  };

  useEffect(() => {
    if (!tokenStore.getAccess()) return;
    getMe()
      .then(setUser)
      .catch(() => setUser(null));
  }, []);

  useEffect(() => {
    setOnSessionExpired(() => {
      setIsAuthenticated(false);
      setUser(null);
    });
  }, []);

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, setSession, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within an AuthProvider');
  return ctx;
}
