import { cn } from '@/components/ui/cn'
import type { Store } from '../api'
import { getProducts } from '../model'
import {ProductBadge} from "@/features/store/components/ProductBadge.tsx";


interface StoreListItemProps {
  store: Store
  selected?: boolean
  onSelect?: (store: Store) => void
  onRequireLogin: () => void
}

export function StoreListItem({ store, selected, onSelect, onRequireLogin }: StoreListItemProps) {

  return (
    <div className={cn(
      'flex items-start gap-1 border-l-2 pl-3 pr-2 py-2 transition-colors hover:bg-gray-50',
      selected ? 'border-accent bg-accent/5' : 'border-transparent',
    )}>
      <button
        type="button"
        onClick={() => onSelect?.(store)}
        className="flex-1 text-left focus:ountline:none"
      >
        <p className="font-medium text-gray-900">{store.name}</p>
        <p className="mt-0.5 text-sm text-gray-500">{store.address}</p>
        <div>
          {getProducts(store).map((p) =>
            <ProductBadge key={p.label} label={p.label} kind={p.kind} />)
          }
        </div>
      </button>

    </div>
  )
}