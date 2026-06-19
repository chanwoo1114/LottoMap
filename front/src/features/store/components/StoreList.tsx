import { StoreListItem } from './StoreListItem'
import type { Store } from '../api'

interface StoreListProps {
  stores: Store[];
  selectedId?: number | null;
  onSelect: (store: Store) => void;
  onRequireLogin: () => void;
}

export function StoreList({ stores, selectedId, onSelect, onRequireLogin }: StoreListProps) {
  if (stores.length === 0) {
    return <p className="px-4 py-8 text-center text-sm text-gray-400">지도를 움직여 판매점을 찾아보세요</p>
  }

  return (
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
  )
}