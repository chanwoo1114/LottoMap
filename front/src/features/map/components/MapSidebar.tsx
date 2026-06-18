import { Brand } from '@/components/ui/Brand'
import { LoginButton } from '@/features/auth/components/LoginButton'
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
}

export function MapSidebar({
  stores,
  selectedId,
  onSelect,
  onRequireLogin,
  favOpen,
  onOpenFavorites,
  onCloseFavorites,
}: MapSidebarProps) {
  return (
    <aside className="flex w-96 shrink-0 flex-col border-r border-gray-200 bg-white">
      {favOpen ? (
        <FavoritesPanel onClose={onCloseFavorites} onRequireLogin={onRequireLogin} />
      ) : (
        <>
          <header className="flex items-center justify-between border-b border-border px-4 py-3">
            <Brand />
            <LoginButton onOpenFavorites={onOpenFavorites} />
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