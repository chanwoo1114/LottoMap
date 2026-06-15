import type { ReactNode } from 'react'
import type { Store } from '@/features/store/api'
import { getProducts } from '../model'
import { ProductBadge } from './ProductBadge'
import { FavoriteButton } from './FavoriteButton'

interface StoreDetailProps {
  store: Store
  onClose: () => void
  onShowOnMap?: (store: Store) => void
  onRequireLogin: () => void
}

export function StoreDetail({ store, onClose, onShowOnMap, onRequireLogin }: StoreDetailProps) {
  const products = getProducts(store)
  const region = [store.sido, store.sigungu, store.dong].filter(Boolean).join(' ')
  const kakaoLink =
    store.lat != null && store.lng != null
      ? `https://map.kakao.com/link/to/${encodeURIComponent(store.name)},${store.lat},${store.lng}`
      : null

  return (
    <div className="flex h-full flex-col bg-white">
      {/* 헤더: 뒤로가기 + 즐겨찾기 */}
      <div className="flex items-center justify-between border-b border-gray-200 px-3 py-3">
        <button
          type="button"
          onClick={onClose}
          aria-label="목록으로"
          className="flex items-center gap-1 rounded-lg px-2 py-1 text-sm text-gray-600 hover:bg-gray-100"
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M15 18l-6-6 6-6" />
          </svg>
          목록
        </button>
        <FavoriteButton storeId={store.id} onRequireLogin={onRequireLogin} />
      </div>

      {/* 본문 */}
      <div className="flex-1 overflow-y-auto px-4 py-4">
        <h2 className="text-lg font-bold text-gray-900">{store.name}</h2>
        {region && <p className="mt-0.5 text-sm text-gray-400">{region}</p>}

        {products.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-1">
            {products.map((p) => (
              <ProductBadge key={p.label} label={p.label} kind={p.kind} />
            ))}
          </div>
        )}

        <dl className="mt-5 space-y-3 text-sm">
          <InfoRow label="주소">
            <span className="text-gray-700">{store.address}</span>
          </InfoRow>
          <InfoRow label="전화">
            {store.phone ? (
              <a href={`tel:${store.phone}`} className="text-accent hover:underline">
                {store.phone}
              </a>
            ) : (
              <span className="text-gray-400">정보 없음</span>
            )}
          </InfoRow>
        </dl>
      </div>

      {/* 하단 액션 */}
      <div className="grid grid-cols-2 gap-2 border-t border-gray-200 p-3">
        <button
          type="button"
          onClick={() => onShowOnMap?.(store)}
          className="rounded-lg border border-gray-200 py-2.5 text-sm font-medium text-gray-700 hover:bg-gray-50"
        >
          지도에서 보기
        </button>
        {kakaoLink ? (
          <a
            href={kakaoLink}
            target="_blank"
            rel="noreferrer"
            className="rounded-lg bg-accent py-2.5 text-center text-sm font-medium text-white hover:opacity-90"
          >
            길찾기
          </a>
        ) : (
          <button type="button" disabled className="rounded-lg bg-gray-100 py-2.5 text-sm text-gray-400">
            길찾기
          </button>
        )}
      </div>
    </div>
  )
}

function InfoRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex gap-3">
      <dt className="w-12 shrink-0 text-gray-400">{label}</dt>
      <dd className="flex-1">{children}</dd>
    </div>
  )
}