import { Outlet } from 'react-router-dom';

export function AppShell() {
  return (
    <div className="flex bg-bg" style={{ height: '100dvh' }}>
      <main className="flex-1 overflow-hidden">
        <Outlet />
      </main>
    </div>
  );
}
