import type { Store } from '@/features/store/api'

interface StoreListProps {
  stores: Store[]
}

export function StoreList({ stores }: StoreListProps) {
  return (
    <div className="overflow-y-auto">
      <div>
        <h2 className="text-sm text-gray-700 font-semibold">
          내 주변 <span className="text-gray-400">{stores.length}</span> 개 판매점
        </h2>
      </div>
      {stores.length === 0 ? (
        <p className="px-4 py-8 text-center text-sm text-gray-400">
          지도를 움직여 판매점을 찾아보세요
        </p>
      ) : (
          <ul>
            {stores.map((store) => (
              <li key={store.id}>
                <button
                  type="button"
                  className="w-full px-4 py-3 text-left transition-colors hover:bg-gray-50
                  focous:bg-gray-50 focus:outline-none active:bg-gray-100
                  "
                >
                  <p className="font-medium text-gray-900">{store.name}</p>
                  <p className="mt-0.5 text-sm text-gray-500">{store.address}</p>
                </button>
              </li>
            ))}
          </ul>
      )}
    </div>
  )
}