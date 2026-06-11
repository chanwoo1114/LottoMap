import { useEffect, useRef, useState, type ReactNode } from 'react';
import { useAuth } from '../AuthContext';
import { cn } from '@/components/ui/cn';

export function UserMenu() {
  const { user, logout } = useAuth();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [open]);

  const nickname = user?.nickname ?? '회원';

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="menu"
        aria-expanded={open}
        className="flex items-center gap-2 rounded-full py-1 pl-1 pr-2 hover:bg-gray-100"
      >
        {user?.profile_image ? (
          <img src={user.profile_image} alt="" className="h-8 w-8 rounded-full object-cover" />
        ) : (
          <span className="grid h-8 w-8 place-items-center rounded-full bg-accent/15 text-sm font-bold text-accent">
            {nickname.charAt(0)}
          </span>
        )}
        <span className="max-w-[7rem] truncate text-sm font-medium text-gray-800">{nickname}</span>
        <svg
          width="14" height="14" viewBox="0 0 24 24" fill="none"
          stroke="currentColor" strokeWidth="2"
          className={cn('text-gray-400 transition-transform', open && 'rotate-180')}
        >
          <path d="M6 9l6 6 6-6" />
        </svg>
      </button>

      {open && (
        <div
          role="menu"
          className="absolute right-0 top-11 z-30 w-52 overflow-hidden rounded-xl border border-gray-100 bg-white py-1 shadow-lg"
        >
          <div className="border-b border-gray-100 px-4 py-2.5">
            <p className="truncate text-sm font-semibold text-gray-900">{nickname}님</p>
            {user?.email && <p className="truncate text-xs text-gray-400">{user.email}</p>}
          </div>
          <MenuItem onClick={() => setOpen(false)}>⭐ 즐겨찾는 판매점</MenuItem>
          <button
            type="button"
            onClick={() => {setOpen(false); logout()}}
            className="flex w-full items-center gap-2 border-t border-gray-100 px-4 py-2 text-left text-sm text-red-600 hover:bg-red-50"
          >
            로그아웃
          </button>
        </div>
      )}

    </div>
  )
}

interface MenuItemProps {
  children: ReactNode;
  onClick: () => void;
}

function MenuItem({ children, onClick }: MenuItemProps) {
  return (
    <button
      type="button"
      role="menuitem"
      onClick={onClick}
      className="flex w-full items-center gap-2 px-4 py-2 text-left text-sm text-gray-700 hover:bg-gray-50"
    >
      {children}
    </button>
  )
}