import { useEffect, useState } from 'react'
import { getFavorites, type Store } from '../api'
import { useFavorites } from '../FavoritesContext'
import { StoreListItem } from './StoreListItem'

interface FavoritesPanelProps {
  onClose: () => void;
  onRequireLogin: () => void;
}

export function FavoritesPanel({ onClose, onRequireLogin }: FavoritesPanelProps) {
  const { isFavorite } = useFavorites()
  const [stores, setStores] = useState<Store[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    getFavorites()
      .then(setStores)
      .catch(() => setStores([]))
      .finally(() => setLoading(false))
  }, [])

  const visible = stores.filter((s) => isFavorite(s.id))

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <header className="flex items-center gap-2 border-b border-gray-100 px-4 py-3">
        <button onClick={onClose} className="rounded-md p-1 text-gray-500 hover:bg-gray-100" aria-label="뒤로">‹ 뒤로</button>
        <span className="font-semibold text-gray-900">즐겨찾는 판매점</span>
      </header>

      <div className="flex-1 overflow-y-auto">
        {loading ? (
          <p className="px-4 py-8 text-center text-sm text-gray-400">불러오는 중…</p>
        ) : visible.length === 0 ? (
          <div className="flex flex-col items-center px-6 py-16 text-center">
            <p className="text-sm font-medium text-gray-600">아직 즐겨찾은 판매점이 없어요</p>
            <p className="mt-1 text-xs text-gray-400">가게 상세에서 ★를 눌러 저장하세요</p>
          </div>
        ) : (
          <>
            <p className="px-4 pt-3 pb-1 text-xs text-gray-400">{visible.length}곳 저장됨</p>
            <ul>
              {visible.map((s) => (
                <li key={s.id}>
                  <StoreListItem store={s} onRequireLogin={onRequireLogin} />
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
    </div>
  )
}