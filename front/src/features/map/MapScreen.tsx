import { useEffect, useRef, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { useGeolocation } from './hooks/useGeolocation'
import { useStoresInBounds } from './hooks/useStoresInBounds'
import { useKakaoMap } from './hooks/useKakaoMap'
import { useFavorites } from '@/features/store/FavoritesContext'
import { MapSidebar } from './components/MapSidebar'
import { MapArea } from './components/MapArea'
import { MobileSheet } from './components/MobileSheet'
import { StoreDetail } from '@/features/store/components/StoreDetail'
import { AuthModal } from '@/features/auth/components/AuthModal'
import type { Store } from '@/features/store/api'

export function MapScreen() {
  const followRef = useRef(true)
  const location = useGeolocation()
  const myMarkerRef = useRef<kakao.maps.CustomOverlay | null>(null)
  const { containerRef, map } = useKakaoMap()
  const { isFavorite } = useFavorites()
  const [favOpen, setFavOpen] = useState(false)
  const { stores, selectedStore, setSelectedStore, tooFar } = useStoresInBounds(map, isFavorite, favOpen)
  const [authOpen, setAuthOpen] = useState(false)

  const routerLocation = useLocation()
  const focusedRef = useRef(false)

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

  useEffect(() => {
    const focus = (routerLocation.state as { focus?: { lat: number; lng: number } } | null)?.focus
    if (!map || !focus || focusedRef.current) return
    focusedRef.current = true
    followRef.current = false
    map.setLevel(4)
    map.panTo(new kakao.maps.LatLng(focus.lat, focus.lng))
  }, [map, routerLocation.state])

  const goToMyLocation = () => {
    if (!map) return
    followRef.current = true
    map.setLevel(4)
    map.panTo(new kakao.maps.LatLng(location.lat, location.lng))
  }

  const panToStore = (store: Store) => {
    if (!map || store.lat == null || store.lng == null) return
    followRef.current = false
    map.panTo(new kakao.maps.LatLng(store.lat, store.lng))
  }

  const focusFavorite = (store: Store) => {
    if (!map || store.lat == null || store.lng == null) return
    map.setLevel(4)
    panToStore(store)
    setSelectedStore(store)
  }

  const requireLogin = () => setAuthOpen(true)

  return (
    <div className="relative flex h-full flex-row">
      <MapSidebar
        stores={stores}
        selectedId={selectedStore?.id}
        onSelect={setSelectedStore}
        onRequireLogin={requireLogin}
        favOpen={favOpen}
        onOpenFavorites={() => setFavOpen(true)}
        onCloseFavorites={() => setFavOpen(false)}
        onFavoriteSelect={focusFavorite}
      />

      {/* 데스크톱: 상세 사이드 패널 */}
      {selectedStore && (
        <aside className="hidden w-96 shrink-0 border-r border-gray-200 bg-white md:block">
          <StoreDetail
            store={selectedStore}
            onClose={() => setSelectedStore(null)}
            onShowOnMap={panToStore}
            onRequireLogin={requireLogin}
          />
        </aside>
      )}

      <MapArea containerRef={containerRef} tooFar={tooFar} onMyLocation={goToMyLocation} />

      {/* 모바일: 드래그 바텀시트 (peek로 등장, 핸들 드래그로 확장/닫기) */}
      {selectedStore && (
        <MobileSheet key={selectedStore.id} onClose={() => setSelectedStore(null)}>
          <StoreDetail
            store={selectedStore}
            onClose={() => setSelectedStore(null)}
            onShowOnMap={(s) => { panToStore(s); setSelectedStore(null) }}
            onRequireLogin={requireLogin}
          />
        </MobileSheet>
      )}

      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} />

    </div>
  )
}