import { useState } from 'react'
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
  stores,
  selectedId,
  onSelect,
  onRequireLogin,
  favOpen,
  onOpenFavorites,
  onCloseFavorites,
  onFavoriteSelect
}: MapSidebarProps) {
  // 모바일 바텀시트 확장 여부 (md 이상에서는 사용되지 않음)
  const [expanded, setExpanded] = useState(false)

  // 모바일: 선택하면 시트를 접어 지도/상세가 보이게 한다
  const selectFromSheet = (store: Store) => {
    setExpanded(false)
    onSelect(store)
  }
  const favoriteSelectFromSheet = (store: Store) => {
    setExpanded(false)
    onFavoriteSelect(store)
  }
  const openFavoritesFromSheet = () => {
    setExpanded(true)
    onOpenFavorites()
  }

  return (
    <>
      {/* 데스크톱: 좌측 사이드바 */}
      <aside className="hidden w-96 shrink-0 flex-col border-r border-gray-200 bg-white md:flex">
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

      {/* 모바일: 하단 바텀시트 (접힘 = 피크바만, 펼침 = 목록) */}
      <div className="absolute inset-x-0 bottom-0 z-10 flex flex-col rounded-t-2xl bg-white shadow-[0_-8px_24px_rgba(0,0,0,0.15)] md:hidden">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          aria-label={expanded ? '목록 접기' : '목록 펼치기'}
          className="flex justify-center pb-1 pt-2.5"
        >
          <span className="h-1 w-10 rounded-full bg-gray-300" />
        </button>

        {favOpen ? (
          <div className="flex h-[60dvh] flex-col">
            <FavoritesPanel
              onClose={onCloseFavorites}
              onSelect={favoriteSelectFromSheet}
              onRequireLogin={onRequireLogin}
            />
          </div>
        ) : (
          <>
            <div className="flex items-center justify-between px-4 pb-3">
              <button
                type="button"
                onClick={() => setExpanded((v) => !v)}
                className="text-sm font-semibold text-gray-700"
              >
                내 주변 <span className="font-bold text-accent">{stores.length}</span> 개 판매점
              </button>
              <button
                type="button"
                onClick={openFavoritesFromSheet}
                className="flex items-center gap-1 text-sm font-medium text-gray-500"
              >
                ⭐ 즐겨찾기
              </button>
            </div>
            {expanded && (
              <div className="h-[55dvh] overflow-y-auto border-t border-gray-100">
                <StoreList
                  stores={stores}
                  selectedId={selectedId}
                  onSelect={selectFromSheet}
                  onRequireLogin={onRequireLogin}
                />
              </div>
            )}
          </>
        )}
      </div>
    </>
  )
}
