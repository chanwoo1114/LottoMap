import { useEffect, useRef, useState } from 'react';
import mapboxgl from 'mapbox-gl'
import MapboxLanguage from '@mapbox/mapbox-gl-language';
import 'mapbox-gl/dist/mapbox-gl.css';


export function MapScreen() {
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const [location, setLocation] = useState({ lng: 126.977139, lat: 37.565694 });

  // 지도 초기화
  useEffect(() => {
    if (!mapContainerRef.current) return;

    mapboxgl.accessToken = import.meta.env.VITE_MAPBOX_TOKEN;

    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/streets-v12',
      center: [location.lng, location.lat],
      zoom: 14,
    });

    mapRef.current.addControl(new MapboxLanguage({ defaultLanguage: 'ko' }));

    return () => {
      mapRef.current?.remove();
    };
  }, []);

  // 현재 위치 호출
  useEffect(() => {
    if (!navigator.geolocation) return;

    let cancelled = false;
    navigator.geolocation.getCurrentPosition(
      (position) => {
        if (cancelled) return;
        setLocation({
          lng: position.coords.longitude,
          lat: position.coords.latitude,
        });
      },
      (error) => {
        console.error(error);
      },
      { timeout: 5000, maximumAge: 60000 },
    );

    return () => {
      cancelled = true;
    };
  }, []);

  // 위치 변경되면 지도 이동
  useEffect(() => {
    mapRef.current?.flyTo({ center: [location.lng, location.lat], zoom: 14 });
  }, [location]);



  return (
    <div className="flex h-full flex-col md:flex-row">
      {/* ═══════════════════════ 사이드바 (데스크톱) / 헤더 (모바일) ═══════════════════════ */}
      <aside className="">

      </aside>
      <div className="relative flex-1 overflow-hidden">
        <div ref={mapContainerRef} className="h-full w-full" />
      </div>
    </div>
  );
}