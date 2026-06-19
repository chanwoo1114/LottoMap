import { StoreList } from '@/features/store/components/StoreList'
import { FavoritesPanel } from '@/features/store/components/FavoritesPanel'
import type { Store } from '@/features/store/api'

interface MapSidebarProps {
  stores: Store[];
  selectedId?: number | null;
  onSelect: (store: Store) => void;
  onRequireLogin: () => void;
  favOpen: boolean;
  onOpenFavorites: () => void;
  onCloseFavorites: () => void;
  onFavoriteSelect: (store: Store) => void;
}

export function MapSidebar({
                             stores, selectedId, onSelect, onRequireLogin,
                             favOpen, onOpenFavorites, onCloseFavorites, onFavoriteSelect,
                           }: MapSidebarProps) {
  return (
    <aside className="flex w-96 shrink-0 flex-col border-r border-gray-200 bg-white">
      {favOpen ? (
        <FavoritesPanel
          onClose={onCloseFavorites}
          onSelect={onFavoriteSelect}
          onRequireLogin={onRequireLogin}
        />
      ) : (
        <>
          <header className="flex items-center justify-between border-b border-gray-100 px-4 py-3">
            <span className="text-sm font-semibold text-gray-700">
              내 주변 <span className="font-bold text-accent">{stores.length}</span> 개 판매점
            </span>
            <button
              onClick={onOpenFavorites}
              className="flex items-center gap-1 text-sm font-medium text-gray-500 hover:text-accent"
            >
              ⭐ 즐겨찾기
            </button>
          </header>
          <div className="flex-1 overflow-y-auto">
            <StoreList
              stores={stores}
              selectedId={selectedId}
              onSelect={onSelect}
              onRequireLogin={onRequireLogin}
            />
          </div>
        </>
      )}
    </aside>
  )
}