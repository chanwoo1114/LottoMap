import { useState, useEffect, useRef } from 'react';
import { useGeolocation } from './hooks/useGeolocation';
import { BiCurrentLocation } from "react-icons/bi";
import { useStoresInBounds } from './hooks/useStoresInBounds';
import { useKakaoMap } from './hooks/useKakaoMap';
import { StoreList } from '@/features/store/components/StoreList';
import { LoginButton } from "@/features/auth/components/LoginButton.tsx";
import { Brand } from '@/components/ui/Brand'

export function MapScreen() {
  const followRef = useRef(true)
  const location = useGeolocation();
  const myMarkerRef = useRef<kakao.maps.CustomOverlay | null>(null);
  const { containerRef, map } = useKakaoMap();
  const { stores, selectedStore, setSelectedStore } = useStoresInBounds(map);
  const [authOpen, setAuthOpen] = useState(false)

  // 현재 위치로 이동
  useEffect(() => {
    if (!map) return;
    const pos = new kakao.maps.LatLng(location.lat, location.lng);
    if (myMarkerRef.current) {
      myMarkerRef.current.setPosition(pos);
    } else {
      const dot = document.createElement('div');
      dot.className="h-4 w-4 rounded-full bg-red-500 border-[3px] border-white shadow-md"

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

  const requireLogin = () => setAuthOpen(true)

  return (
    <div className="flex h-full flex-col md:flex-row">
      <aside className="flex w-128 shrink-0 flex-col border-r border-gray-200 bg-white">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <Brand />
          <LoginButton
          />
        </div>

        <StoreList
          stores={stores}
          selectedId={selectedStore?.id}
          onSelect={setSelectedStore}
          onRequireLogin={requireLogin}
        />
      </aside>
      <div className="relative flex-1 overflow-hidden">
        <div ref={containerRef} className="h-full w-full" />

        <button
          onClick={goToMyLocation}
          className="absolute bottom-6 right-4 z-10 rounded-full bg-white p-3 shadow-md cursor-pointer hover:bg-gray-100 active:scale-95 transition"
          aria-label="내 위치로 이동"
        >
          <BiCurrentLocation className="h-5 w-5"/>
        </button>
      </div>
    </div>
  );
}