import { useEffect, useRef, useState } from 'react';
import { loadKakaoMap } from "@/lib/kakaoMap";

export function useKakaoMap() {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [map, setMap] = useState<kakao.maps.Map | null>(null);

  useEffect(() => {
    loadKakaoMap().then(() => {
      if (!containerRef.current) return;
      const m = new kakao.maps.Map(containerRef.current, {
        center: new kakao.maps.LatLng(37.565694, 126.977139),
        level: 3,
      });
      setMap(m);
    });
  }, []);

  return { containerRef, map };
}