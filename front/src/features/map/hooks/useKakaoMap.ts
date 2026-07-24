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

  // 창 크기 변경(반응형 레이아웃 전환 포함) 시 지도 타일 재배치
  useEffect(() => {
    if (!map) return;
    const onResize = () => map.relayout();
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [map]);

  return { containerRef, map };
}