# LottoMap 프론트 UI 구현 스펙

> 이 문서만 보고 그대로 파일을 생성/교체하면 지도·판매점 UI가 재현된다.
> 대상: `front/` (React 19 + Vite + TypeScript + Tailwind v4 + 카카오맵 JS SDK)

## 0. 디자인 결정 (요약)

- **테마**: 흰 베이스 + 포인트색 1개. 포인트색은 `accent` 토큰. 클로버 테마라 **초록(`#16a34a`)** 권장.
- **반응형**: `md:` 기준 — 데스크톱은 좌측 사이드바, 모바일은 지도 풀스크린 + 하단 바텀시트. 컴포넌트는 공유.
- **즐겨찾기(★)**: 현재 위치와 **무관한 내 북마크**(로그인 필요). 로그아웃 상태에서 ★ 누르면 **로그인 모달 유도(옵션 B)**.
- **지도 마커**: 카카오 `CustomOverlay` + SVG teardrop 핀. 일반=초록, 즐겨찾기=금색 별, 선택=확대+흰 링. 크기 28×35px.
- **재사용 분리**: 긴 Tailwind 클래스는 작은 컴포넌트/`cn`에 가둠. 2회 이상 재사용 또는 책임 분명한 것만 분리.

## 1. 의존성 / 전제

- 이미 존재(확인): `@/components/ui/cn`(clsx + tailwind-merge), `@/components/ui/Brand`, `CloverLogo`, `@/features/auth/AuthContext`(`useAuth`), `@/features/auth/components/{LoginButton,AuthModal}`, `@/features/store/api`(`Store`, `getStoresInBounds`), `@/features/store/model`(`getProducts`, `ProductKind`).
- 추가 설치 불필요(`cn`으로 충분). `tailwind-variants`는 쓰지 않음.

`Store` 타입 핵심 필드: `id:number, name, address, phone, lat:number|null, lng:number|null, sells_lotto, sells_pension, sells_speetto_2000, sells_speetto_1000, sells_speetto_500`.

`model.ts`의 `getProducts(store): { label:string; kind:'lotto'|'pension'|'speetto' }[]`.

## 2. index.css 추가

```css
/* @theme 안 — 포인트색을 클로버 초록으로 (기존 보라 #aa3bff 대체, 선택) */
--color-accent: #16a34a;

/* 파일 끝 — 지도 마커(CustomOverlay) 스타일 (필수) */
.lm-marker {
  cursor: pointer;
  filter: drop-shadow(0 2px 4px rgba(0, 0, 0, .3));
  transition: transform .15s ease;
  transform-origin: bottom center;
}
.lm-marker:hover { transform: scale(1.12); }
.lm-marker--sel  { transform: scale(1.25); }
```

## 3. 지도 마커 — `features/map/hooks/useStoresInBounds.ts` (전체 교체)

```tsx
import { useState, useRef, useEffect } from 'react'
import { getStoresInBounds, type Store } from '@/features/store/api'

const MIN_LEVEL_TO_FETCH = 6

// 깔끔한 teardrop 핀. favorite=금색별 / selected=확대+링
function buildPin(favorite: boolean, selected: boolean): HTMLDivElement {
  const green = '#16a34a', gold = '#f59e0b'
  const color = favorite ? gold : green
  const ring = selected ? '<circle cx="16" cy="15.5" r="9.5" fill="none" stroke="#fff" stroke-width="2"/>' : ''
  const inner = favorite
    ? `<path d="M16 11l1.4 2.9 3.2.4-2.3 2.2.6 3.1L16 18.1 13.1 19.6l.6-3.1-2.3-2.2 3.2-.4z" fill="${gold}"/>`
    : `<circle cx="16" cy="15.5" r="3.4" fill="${color}"/>`
  const el = document.createElement('div')
  el.className = 'lm-marker' + (selected ? ' lm-marker--sel' : '')
  el.innerHTML =
    `<svg width="28" height="35" viewBox="0 0 32 40" xmlns="http://www.w3.org/2000/svg">` +
    `<path d="M16 1C8 1 2 7 2 15c0 9 14 24 14 24s14-15 14-24C30 7 24 1 16 1z" fill="${color}"/>` +
    `<circle cx="16" cy="15.5" r="6.6" fill="#fff"/>${inner}${ring}</svg>`
  return el
}

export function useStoresInBounds(
  map: kakao.maps.Map | null,
  isFavorite: (storeId: number) => boolean,
) {
  const [stores, setStores] = useState<Store[]>([])
  const [selectedStore, setSelectedStore] = useState<Store | null>(null)
  const overlaysRef = useRef<kakao.maps.CustomOverlay[]>([])

  useEffect(() => {
    if (!map) return
    const handleIdle = async () => {
      if (map.getLevel() > MIN_LEVEL_TO_FETCH) { setStores([]); return }
      const b = map.getBounds(), sw = b.getSouthWest(), ne = b.getNorthEast()
      try {
        setStores(await getStoresInBounds({
          min_lat: sw.getLat(), min_lng: sw.getLng(),
          max_lat: ne.getLat(), max_lng: ne.getLng(), limit: 300,
        }))
      } catch (e) { console.error(e) }
    }
    kakao.maps.event.addListener(map, 'idle', handleIdle)
    return () => kakao.maps.event.removeListener(map, 'idle', handleIdle)
  }, [map])

  useEffect(() => {
    if (!map) return
    overlaysRef.current.forEach((o) => o.setMap(null))
    overlaysRef.current = []
    stores.forEach((store) => {
      if (store.lat == null || store.lng == null) return
      const selected = selectedStore?.id === store.id
      const el = buildPin(isFavorite(store.id), selected)
      el.addEventListener('click', () => setSelectedStore(store))
      const ov = new kakao.maps.CustomOverlay({
        position: new kakao.maps.LatLng(store.lat, store.lng),
        content: el, yAnchor: 1, zIndex: selected ? 10 : 3,
      })
      ov.setMap(map)
      overlaysRef.current.push(ov)
    })
  }, [map, stores, selectedStore, isFavorite])

  return { stores, selectedStore, setSelectedStore }
}
```

## 4. 즐겨찾기 상태 — `features/store/FavoritesContext.tsx` (신규)

```tsx
import { createContext, useCallback, useContext, useState, type ReactNode } from 'react'

interface FavoritesState {
  isFavorite: (storeId: number) => boolean
  toggle: (storeId: number) => void
}

const FavoritesContext = createContext<FavoritesState | null>(null)

export function FavoritesProvider({ children }: { children: ReactNode }) {
  const [ids, setIds] = useState<Set<number>>(new Set())

  // TODO(api): 로그인 시 GET /favorites 로 hydrate, toggle 시 POST/DELETE /favorites/{id}
  const isFavorite = useCallback((id: number) => ids.has(id), [ids])
  const toggle = useCallback((id: number) => {
    setIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  return <FavoritesContext.Provider value={{ isFavorite, toggle }}>{children}</FavoritesContext.Provider>
}

export function useFavorites() {
  const ctx = useContext(FavoritesContext)
  if (!ctx) throw new Error('useFavorites must be used within FavoritesProvider')
  return ctx
}
```

## 5. `features/store/components/ProductBadge.tsx`

```tsx
import { cn } from '@/components/ui/cn'
import type { ProductKind } from '../model'

const STYLE: Record<ProductKind, string> = {
  lotto:   'bg-green-50 text-green-700 ring-green-600/20',
  pension: 'bg-blue-50 text-blue-700 ring-blue-600/20',
  speetto: 'bg-orange-50 text-orange-700 ring-orange-600/20',
}

export function ProductBadge({ label, kind }: { label: string; kind: ProductKind }) {
  return (
    <span className={cn('inline-flex rounded px-1.5 py-0.5 text-[11px] font-medium ring-1 ring-inset', STYLE[kind])}>
      {label}
    </span>
  )
}
```

## 6. `features/store/components/FavoriteButton.tsx` (신규)

```tsx
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
    if (!isAuthenticated) return onRequireLogin()  // 옵션 B: 로그인 유도
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
```

## 7. `features/store/components/StoreListItem.tsx` (신규)

```tsx
import { cn } from '@/components/ui/cn'
import { getProducts } from '../model'
import { ProductBadge } from './ProductBadge'
import { FavoriteButton } from './FavoriteButton'
import type { Store } from '../api'

export function StoreListItem({ store, selected, onSelect, onRequireLogin }: {
  store: Store
  selected?: boolean
  onSelect?: (store: Store) => void
  onRequireLogin: () => void
}) {
  return (
    <div className={cn(
      'flex items-start gap-1 border-l-2 pl-3 pr-2 py-3 transition-colors hover:bg-gray-50',
      selected ? 'border-accent bg-accent/5' : 'border-transparent',
    )}>
      <button type="button" onClick={() => onSelect?.(store)} className="flex-1 text-left focus:outline-none">
        <p className="font-medium text-gray-900">{store.name}</p>
        <p className="mt-0.5 text-sm text-gray-500">{store.address}</p>
        <div className="mt-1.5 flex flex-wrap gap-1">
          {getProducts(store).map((p) => <ProductBadge key={p.label} label={p.label} kind={p.kind} />)}
        </div>
      </button>
      <FavoriteButton storeId={store.id} onRequireLogin={onRequireLogin} className="mt-0.5" />
    </div>
  )
}
```

## 8. `features/store/components/StoreList.tsx` (교체)

```tsx
import { StoreListItem } from './StoreListItem'
import type { Store } from '../api'

export function StoreList({ stores, selectedId, onSelect, onRequireLogin }: {
  stores: Store[]
  selectedId?: number | null
  onSelect?: (store: Store) => void
  onRequireLogin: () => void
}) {
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
              <StoreListItem store={s} selected={selectedId === s.id} onSelect={onSelect} onRequireLogin={onRequireLogin} />
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
```

## 9. `components/ui/SlideOverPanel.tsx` (신규)

```tsx
import type { ReactNode } from 'react'
import { cn } from './cn'

/** 데스크톱: 좌측 슬라이드 / 모바일: 하단 풀업 */
export function SlideOverPanel({ open, children, className }: {
  open: boolean
  children: ReactNode
  className?: string
}) {
  return (
    <div className={cn(
      'absolute z-30 flex flex-col bg-white shadow-xl transition-transform duration-300',
      'inset-x-0 bottom-0 h-[85%] rounded-t-3xl',
      'md:inset-y-0 md:left-0 md:bottom-auto md:h-full md:w-96 md:rounded-none md:border-r md:border-gray-200',
      open ? 'translate-y-0 md:translate-x-0' : 'translate-y-full md:translate-y-0 md:-translate-x-full',
      className,
    )}>
      {children}
    </div>
  )
}
```

## 10. `features/store/components/StoreDetail.tsx` (구현 — 기존 빈/깨진 파일 교체)

```tsx
import { SlideOverPanel } from '@/components/ui/SlideOverPanel'
import { getProducts } from '../model'
import { ProductBadge } from './ProductBadge'
import { FavoriteButton } from './FavoriteButton'
import type { Store } from '../api'

export function StoreDetail({ store, onClose, onRequireLogin }: {
  store: Store | null
  onClose: () => void
  onRequireLogin: () => void
}) {
  return (
    <SlideOverPanel open={!!store}>
      <header className="flex items-center gap-2 border-b border-gray-100 px-4 py-3">
        <button onClick={onClose} className="rounded-md p-1 text-gray-500 hover:bg-gray-100" aria-label="뒤로">‹ 뒤로</button>
        <span className="font-semibold text-gray-900">판매점 정보</span>
        {store && <FavoriteButton storeId={store.id} onRequireLogin={onRequireLogin} className="ml-auto" />}
      </header>

      {store && (
        <div className="flex-1 overflow-y-auto p-4">
          <h3 className="text-lg font-bold text-gray-900">{store.name}</h3>
          <p className="mt-1 text-sm text-gray-500">{store.address}</p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {getProducts(store).map((p) => <ProductBadge key={p.label} label={p.label} kind={p.kind} />)}
          </div>
          {store.phone && (
            <div className="mt-5 flex items-center gap-2 text-sm text-gray-700">
              <span className="text-gray-400">전화</span>
              <a href={`tel:${store.phone}`} className="font-medium text-gray-900">{store.phone}</a>
            </div>
          )}
          <div className="mt-6 grid grid-cols-2 gap-2">
            {store.lat != null && store.lng != null ? (
              <a
                href={`https://map.kakao.com/link/to/${encodeURIComponent(store.name)},${store.lat},${store.lng}`}
                target="_blank" rel="noreferrer"
                className="rounded-lg bg-accent px-3 py-2.5 text-center text-sm font-semibold text-white hover:opacity-90"
              >길찾기</a>
            ) : <span />}
            <a
              href={store.phone ? `tel:${store.phone}` : undefined}
              className="rounded-lg border border-gray-200 px-3 py-2.5 text-center text-sm font-semibold text-gray-700 hover:bg-gray-50"
            >전화걸기</a>
          </div>
        </div>
      )}
    </SlideOverPanel>
  )
}
```

## 11. `components/ui/IconButton.tsx` · `components/ui/BottomSheet.tsx` (신규)

```tsx
// IconButton.tsx
import type { ComponentProps } from 'react'
import { cn } from './cn'

export function IconButton({ className, ...props }: ComponentProps<'button'>) {
  return (
    <button className={cn('rounded-full bg-white p-3 shadow-md transition hover:bg-gray-100 active:scale-95', className)} {...props} />
  )
}
```

```tsx
// BottomSheet.tsx
import { useState, type ReactNode } from 'react'
import { cn } from './cn'

export function BottomSheet({ children, className }: { children: ReactNode; className?: string }) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div className={cn(
      'absolute inset-x-0 bottom-0 z-20 flex flex-col rounded-t-3xl bg-white',
      'shadow-[0_-8px_30px_rgba(0,0,0,0.12)] transition-[height] duration-300',
      expanded ? 'h-[75%]' : 'h-56', className,
    )}>
      <button onClick={() => setExpanded((v) => !v)} className="flex shrink-0 justify-center py-3" aria-label="목록 펼치기">
        <span className="h-1.5 w-10 rounded-full bg-gray-300" />
      </button>
      <div className="flex-1 overflow-y-auto">{children}</div>
    </div>
  )
}
```

## 12. `features/map/MapScreen.tsx` (조립 — 전체 교체)

```tsx
import { useEffect, useRef, useState } from 'react'
import { BiCurrentLocation } from 'react-icons/bi'
import { useGeolocation } from './hooks/useGeolocation'
import { useStoresInBounds } from './hooks/useStoresInBounds'
import { useKakaoMap } from './hooks/useKakaoMap'
import { useFavorites } from '@/features/store/FavoritesContext'
import { StoreList } from '@/features/store/components/StoreList'
import { StoreDetail } from '@/features/store/components/StoreDetail'
import { BottomSheet } from '@/components/ui/BottomSheet'
import { IconButton } from '@/components/ui/IconButton'
import { Brand } from '@/components/ui/Brand'
import { LoginButton } from '@/features/auth/components/LoginButton'
import { AuthModal } from '@/features/auth/components/AuthModal'

export function MapScreen() {
  const followRef = useRef(true)
  const location = useGeolocation()
  const myMarkerRef = useRef<kakao.maps.CustomOverlay | null>(null)
  const { containerRef, map } = useKakaoMap()
  const { isFavorite } = useFavorites()
  const { stores, selectedStore, setSelectedStore } = useStoresInBounds(map, isFavorite)
  const [authOpen, setAuthOpen] = useState(false)

  useEffect(() => {
    if (!map) return
    const pos = new kakao.maps.LatLng(location.lat, location.lng)
    if (myMarkerRef.current) {
      myMarkerRef.current.setPosition(pos)
    } else {
      const dot = document.createElement('div')
      dot.className = 'h-4 w-4 rounded-full bg-red-500 border-[3px] border-white shadow-md'
      myMarkerRef.current = new kakao.maps.CustomOverlay({ position: pos, content: dot, map })
    }
    if (followRef.current) map.panTo(pos)
  }, [map, location])

  useEffect(() => {
    if (!map) return
    const onDragStart = () => { followRef.current = false }
    kakao.maps.event.addListener(map, 'dragstart', onDragStart)
    return () => kakao.maps.event.removeListener(map, 'dragstart', onDragStart)
  }, [map])

  const goToMyLocation = () => {
    if (!map) return
    followRef.current = true
    map.panTo(new kakao.maps.LatLng(location.lat, location.lng))
  }

  const requireLogin = () => setAuthOpen(true)

  return (
    <div className="relative flex h-full overflow-hidden">
      <aside className="hidden w-96 shrink-0 flex-col border-r border-gray-200 bg-white md:flex">
        <header className="flex items-center justify-between border-b border-gray-100 px-4 py-3">
          <Brand />
          <LoginButton />
        </header>
        <div className="flex-1 overflow-y-auto">
          <StoreList stores={stores} selectedId={selectedStore?.id} onSelect={setSelectedStore} onRequireLogin={requireLogin} />
        </div>
      </aside>

      <div className="relative flex-1 overflow-hidden">
        <div ref={containerRef} className="h-full w-full" />

        <header className="absolute inset-x-3 top-3 z-20 flex items-center justify-between rounded-2xl bg-white/95 px-3.5 py-2.5 shadow-lg backdrop-blur md:hidden">
          <Brand />
          <LoginButton />
        </header>

        <IconButton onClick={goToMyLocation} aria-label="내 위치로 이동" className="absolute right-4 bottom-[15rem] z-20 md:bottom-6">
          <BiCurrentLocation className="h-5 w-5" />
        </IconButton>

        <BottomSheet className="md:hidden">
          <StoreList stores={stores} selectedId={selectedStore?.id} onSelect={setSelectedStore} onRequireLogin={requireLogin} />
        </BottomSheet>
      </div>

      <StoreDetail store={selectedStore} onClose={() => setSelectedStore(null)} onRequireLogin={requireLogin} />
      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} />
    </div>
  )
}
```

## 13. `App.tsx` (FavoritesProvider 추가)

```tsx
import { RouterProvider } from 'react-router-dom';
import { router } from './router';
import { AuthProvider } from '@/features/auth/AuthContext';
import { FavoritesProvider } from '@/features/store/FavoritesContext';

export function App() {
  return (
    <AuthProvider>
      <FavoritesProvider>
        <RouterProvider router={router} />
      </FavoritesProvider>
    </AuthProvider>
  );
}
```

## 14. 적용 체크리스트

1. `index.css`에 `accent` 토큰 + `.lm-marker` CSS 추가 (마커 CSS 누락 시 hover/선택 효과 안 됨).
2. `useStoresInBounds(map, isFavorite)` — 인자 추가됨. MapScreen에서 `useFavorites()`로 전달.
3. `App.tsx`를 `FavoritesProvider`로 감싸기 (안 하면 `useFavorites` 에러).
4. 사이드바 폭 `w-96`(384px). 기존 `w-128`은 너무 넓음.
5. 빌드 시 `noUnusedLocals`(에디터 빨간 줄) 주의 — 위 코드는 미사용 변수 없음. (참고: TS6.0.3/PyCharm 에디터 빨간 줄은 오탐일 수 있음, CLI 기준)
6. 기존 `/clover.svg` MarkerImage는 더 이상 안 씀(파일 남겨도 무방).

## 15. 미구현 / 다음 단계 (TODO)

- **favorites 백엔드 API**: `favorites(user_id, store_id)` 테이블 + `GET/POST/DELETE /favorites` (`get_current_user_id` 의존성 재사용). 생기면 `FavoritesContext`의 `TODO(api)` 자리에 연결.
- **즐겨찾기 패널**: 드롭다운 "즐겨찾는 판매점" → 저장 가게 목록(위치 무관) → "지도에서 보기"로 `map.panTo`. (`MapScreen`에 `focusStore(store)` 추가해 연결)
- **화면 밖 즐겨찾기 마커**: bounds 조회는 화면 범위만 → 먼 즐겨찾기를 상시 표시하려면 `GET /favorites` 좌표로 별도 오버레이.
