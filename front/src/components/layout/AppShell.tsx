import { NavLink, Outlet } from 'react-router-dom';
import { Brand } from '@/components/ui/Brand';
import { LoginButton } from '@/features/auth/components/LoginButton';
import { cn } from '@/components/ui/cn';

const tabClass = ({ isActive }: { isActive: boolean }) =>
  cn(
    'flex items-center border-b-2 px-3.5 text-sm font-medium transition-colors',
    isActive ? 'border-accent font-bold text-accent' : 'border-transparent text-gray-500 hover:text-gray-900',
  );

export function AppShell() {
  return (
    <div className="flex flex-col bg-bg" style={{ height: '100dvh' }}>
      <nav className="flex h-14 shrink-0 items-center gap-6 border-b border-gray-200 bg-white px-5">
        <Brand />
        <div className="flex h-full items-stretch">
          <NavLink to="/" end className={tabClass}>지도</NavLink>
          <NavLink to="/results" className={tabClass}>당첨결과</NavLink>
          <NavLink to="/generate" className={tabClass}>번호생성</NavLink>
        </div>
        <div className="ml-auto">
          <LoginButton />
        </div>
      </nav>
      <main className="min-h-0 flex-1 overflow-hidden">
        <Outlet />
      </main>
    </div>
  );
}