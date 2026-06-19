import { useState, useRef, useEffect } from 'react'
import { getStoresInBounds, getFavorites, type Store } from '@/features/store/api'

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
  isFavorite: (storeId: number) => boolean,
  favOpen: boolean,
) {
  const [stores, setStores] = useState<Store[]>([])
  const [favStores, setFavStores] = useState<Store[]>([])
  const [selectedStore, setSelectedStore] = useState<Store | null>(null)
  const [tooFar, setTooFar] = useState(false)
  const overlaysRef = useRef<kakao.maps.CustomOverlay[]>([])

  useEffect(() => {
    if (!map || favOpen) return
    const fetchBbox = async () => {
      if (map.getLevel() > MIN_LEVEL_TO_FETCH) {
        setTooFar(true)
        setStores([])
        return
      }
      setTooFar(false)
      const b = map.getBounds(), sw = b.getSouthWest(), ne = b.getNorthEast()
      try {
        setStores(await getStoresInBounds({
          min_lat: sw.getLat(), min_lng: sw.getLng(),
          max_lat: ne.getLat(), max_lng: ne.getLng(), limit: 300,
        }))
      } catch (e) { console.error(e) }
    }
    fetchBbox()                                  // 진입 / 즐겨찾기 닫힘 시 즉시 1회
    kakao.maps.event.addListener(map, 'idle', fetchBbox)
    return () => kakao.maps.event.removeListener(map, 'idle', fetchBbox)
  }, [map, favOpen])

  useEffect(() => {
    if (!map || !favOpen) return
    let cancelled = false
    setTooFar(false)
    getFavorites()
      .then((favs) => {
        if (cancelled) return
        setFavStores(favs)
        const pts = favs.filter((s) => s.lat != null && s.lng != null)
        if (pts.length) {
          const bounds = new kakao.maps.LatLngBounds()
          pts.forEach((s) => bounds.extend(new kakao.maps.LatLng(s.lat!, s.lng!)))
          map.setBounds(bounds)
        }
      })
      .catch(() => { if (!cancelled) setFavStores([]) })
    return () => { cancelled = true }
  }, [map, favOpen])

  useEffect(() => {
    if (!map) return
    overlaysRef.current.forEach((o) => o.setMap(null))
    overlaysRef.current = []

    const source = favOpen ? favStores : stores
    source.forEach((store) => {
      if (store.lat == null || store.lng == null) return
      const selected = selectedStore?.id === store.id
      const fav = favOpen ? true : isFavorite(store.id)   // 즐겨찾기 모드면 전부 금색
      const el = buildPin(fav, selected)
      el.addEventListener('click', () => setSelectedStore(store))
      const ov = new kakao.maps.CustomOverlay({
        position: new kakao.maps.LatLng(store.lat, store.lng),
        content: el, yAnchor: 1, zIndex: selected ? 10 : 3,
      })
      ov.setMap(map)
      overlaysRef.current.push(ov)
    })
  }, [map, favOpen, stores, favStores, selectedStore, isFavorite])

  return { stores, selectedStore, setSelectedStore, tooFar }
}