import type { MouseEvent } from 'react'
import { useAuth } from '@/features/auth/AuthContext'
import { useFavorites } from '../FavoritesContext'
import { cn } from '@/components/ui/cn'

export function FavoriteButton({ storeId, onRequireLogin, className }: {
  storeId: number
  onRequireLogin: () => void
  className?: string
}) {
  const { isAuthenticated } = useAuth()
  const { isFavorite, toggle } = useFavorites()
  const active = isAuthenticated && isFavorite(storeId)

  const handle = (e: MouseEvent) => {
    e.stopPropagation()
    if (!isAuthenticated) return onRequireLogin()
    toggle(storeId)
  }

  return (
    <button type="button" onClick={handle} aria-label="즐겨찾기" className={cn('p-1', className)}>
      <svg width="20" height="20" viewBox="0 0 24 24"
           fill={active ? '#f59e0b' : 'none'} stroke={active ? '#f59e0b' : '#cbd5e1'} strokeWidth="2">
        <path d="M12 2l3 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.9 21l1.2-6.8-5-4.9 6.9-1z" />
      </svg>
    </button>
  )
}