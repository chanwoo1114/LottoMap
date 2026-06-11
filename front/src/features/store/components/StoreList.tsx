import type { Store } from '@/features/store/api'
import { StoreListItem } from "@/features/store/components/StoreListItem.tsx";

interface StoreListProps {
  stores: Store[]
  selectedId?: number | null
  onSelect?: (store: Store) => void
  onRequireLogin: () => void
}

export function StoreList({
  stores, onSelect, selectedId, onRequireLogin
}: StoreListProps) {

  return (
    <div>
      <h2 className="px-4 py-3 text-sm font-semibold text-gray-700">
        내 주변 <span className="font-bold text-accent">{stores.length}</span> 개 판매점
      </h2>
      {stores.length === 0 ? (
        <p className="px-4 py-8 text-center text-sm text-gray-400">지도를 움직여 판매점을 찾아보세요</p>
      ) : (
        <ul>
          {stores.map((s) => (
            <li key={s.id}>
              <StoreListItem
                store={s}
                selected={selectedId === s.id}
                onSelect={onSelect}
                onRequireLogin={onRequireLogin}
              />
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}