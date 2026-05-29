import { useState, useRef, useEffect } from "react";
import { getStoresInBounds, type Store } from '@/features/store/api';

const MIN_LEVEL_TO_FETCH = 6;

export function useStoresInBounds(map: kakao.maps.Map | null) {
  const [stores, setStores] = useState<Store[]>([]);
  const [selectedStore, setSelectedStore] = useState<Store | null>(null);
  const markersRef = useRef<kakao.maps.Marker[]>([]);

  useEffect(() => {
    if (!map) return;

    const handleIdle = async () => {
      if (map.getLevel() > MIN_LEVEL_TO_FETCH) {
        setStores([]);
        return
      }

      const bounds = map.getBounds();
      const sw = bounds.getSouthWest();
      const ne = bounds.getNorthEast();

      try {
        const data = await getStoresInBounds({
          min_lat: sw.getLat(),
          min_lng: sw.getLng(),
          max_lat: ne.getLat(),
          max_lng: ne.getLng(),
          limit: 300,
        });
        setStores(data);
      } catch (e) {
        console.error(e)
      }
    };

    kakao.maps.event.addListener(map, 'idle', handleIdle);
    return () => kakao.maps.event.removeListener(map, 'idle', handleIdle);

  }, [map])

  useEffect(() => {
    if (!map) return;

    const storeImage = new kakao.maps.MarkerImage(
      '/clover.svg',
      new kakao.maps.Size(28, 44),
      { offset: new kakao.maps.Point(18, 44) },
    );

    markersRef.current.forEach((marker) => marker.setMap(null));
    markersRef.current = [];

    stores.forEach((store) => {
      if (store.lat == null || store.lng == null) return;
      const marker = new kakao.maps.Marker({
        position: new kakao.maps.LatLng(store.lat, store.lng),
        image: storeImage,
        zIndex: 3,
        map,
      });
      kakao.maps.event.addListener(marker, 'click', () => setSelectedStore(store))
      markersRef.current.push(marker);
    });
  }, [map, stores]);

  return { stores, selectedStore, setSelectedStore };
}