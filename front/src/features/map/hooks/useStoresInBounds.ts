import { useState, useRef, useEffect } from 'react'
import { getStoresInBounds, type Store } from '@/features/store/api'

const MIN_LEVEL_TO_FETCH = 6

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
  // isFavorite: (storeId: number) => boolean,
) {
  const [stores, setStores] = useState<Store[]>([])
  const [selectedStore, setSelectedStore] = useState<Store | null>(null)
  const overlaysRef = useRef<kakao.maps.CustomOverlay[]>([])

  // 화면 이동 시 범위 내 판매점 조회
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

  // // 마커 렌더
  // useEffect(() => {
  //   if (!map) return
  //   overlaysRef.current.forEach((o) => o.setMap(null))
  //   overlaysRef.current = []
  //
  //   stores.forEach((store) => {
  //     if (store.lat == null || store.lng == null) return
  //     const selected = selectedStore?.id === store.id
  //     const el = buildPin(isFavorite(store.id), selected)
  //     el.addEventListener('click', () => setSelectedStore(store))
  //     const ov = new kakao.maps.CustomOverlay({
  //       position: new kakao.maps.LatLng(store.lat, store.lng),
  //       content: el, yAnchor: 1, zIndex: selected ? 10 : 3,
  //     })
  //     ov.setMap(map)
  //     overlaysRef.current.push(ov)
  //   })
  // }, [map, stores, selectedStore, isFavorite])

  return { stores, selectedStore, setSelectedStore }
}