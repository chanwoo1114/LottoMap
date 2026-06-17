import { useState, useEffect, useRef } from 'react';
import { useGeolocation } from './hooks/useGeolocation';
import { BiCurrentLocation } from "react-icons/bi";
import { useStoresInBounds } from './hooks/useStoresInBounds';
import { useKakaoMap } from './hooks/useKakaoMap';
import { StoreList } from '@/features/store/components/StoreList';
import { StoreDetail } from '@/features/store/components/StoreDetail';
import { useFavorites } from '@/features/store/FavoritesContext';
import { LoginButton } from "@/features/auth/components/LoginButton.tsx";
import { Brand } from '@/components/ui/Brand'
import type { Store } from '@/features/store/api';
import { IconButton } from '@/components/ui/IconButton'
import { AuthModal } from '@/features/auth/components/AuthModal';

export function MapScreen() {
  const followRef = useRef(true)
  const location = useGeolocation();
  const myMarkerRef = useRef<kakao.maps.CustomOverlay | null>(null);
  const { containerRef, map } = useKakaoMap();
  const { isFavorite } = useFavorites();
  const { stores, selectedStore, setSelectedStore, tooFar } = useStoresInBounds(map, isFavorite);
  const [authOpen, setAuthOpen] = useState(false)

  // 현재 위치로 이동
  useEffect(() => {
    if (!map) return;
    const pos = new kakao.maps.LatLng(location.lat, location.lng);
    if (myMarkerRef.current) {
      myMarkerRef.current.setPosition(pos);
    } else {
      const dot = document.createElement('div');
      dot.className = "h-4 w-4 rounded-full bg-red-500 border-[3px] border-white shadow-md"

      myMarkerRef.current = new kakao.maps.CustomOverlay({
        position: pos,
        content: dot,
        map,
      });
    }
    if (followRef.current) {
      map.panTo(pos);
    }
  }, [map, location])

  useEffect(() => {
    if (!map) return
    const handleDragStart = () => { followRef.current = false; };

    kakao.maps.event.addListener(map, 'dragstart', handleDragStart);
    return () => kakao.maps.event.removeListener(map, 'dragstart', handleDragStart);
  }, [map]);

  // 내 현재 위치로 이동
  const goToMyLocation = () => {
    if (!map) return;
    followRef.current = true;
    map.panTo(new kakao.maps.LatLng(location.lat, location.lng));
  }

  // 선택한 판매점 위치로 이동
  const showOnMap = (store: Store) => {
    if (!map || store.lat == null || store.lng == null) return;
    followRef.current = false;
    map.panTo(new kakao.maps.LatLng(store.lat, store.lng));
  }

  const requireLogin = () => setAuthOpen(true)

  return (
    <div className="flex h-full flex-col md:flex-row">
      <aside className="flex w-full md:w-96 shrink-0 flex-col border-r border-gray-200 bg-white">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <Brand />
          <LoginButton />
        </div>

        <div className="flex-1 overflow-y-auto">
          <StoreList
            stores={stores}
            selectedId={selectedStore?.id}
            onSelect={setSelectedStore}
            onRequireLogin={requireLogin}
          />
        </div>
      </aside>

      {/* 상세: 사이드바 오른쪽 옆 별도 컬럼 */}
      {selectedStore && (
        <aside className="absolute inset-0 z-30 bg-white md:static md:inset-auto md:w-96 md:shrink-0 md:border-r md:border-gray-200">          <StoreDetail
            store={selectedStore}
            onClose={() => setSelectedStore(null)}
            onShowOnMap={showOnMap}
            onRequireLogin={requireLogin}
          />
        </aside>
      )}

      <div className="relative flex-1 overflow-hidden">
        <div ref={containerRef} className="h-full w-full" />

        {/* 너무 축소됐을 때 안내 */}
        {tooFar && (
          <div className="pointer-events-none absolute inset-x-0 top-4 z-10 flex justify-center">
            <div className="rounded-full bg-gray-900/80 px-4 py-2 text-sm font-medium text-white shadow-md">
              지도를 확대하면 판매점이 표시됩니다
            </div>
          </div>
        )}

        <IconButton onClick={goToMyLocation} aria-label="내 위치로 이동" className="absolute bottom-6 right-4 z-10">
          <BiCurrentLocation className="h-5 w-5" />
        </IconButton>
      </div>

      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} />
    </div>
  );
}