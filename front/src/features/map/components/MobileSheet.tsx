import { useEffect, useRef, useState, type PointerEvent as ReactPointerEvent, type ReactNode } from 'react'

const PEEK = 190              // peek 스냅에서 보이는 높이(px)
const CLOSE_THRESHOLD = 80    // peek에서 이만큼 더 내리면 닫힘(px)
const FLICK_VELOCITY = 0.5    // px/ms — 이보다 빠르게 튕기면 방향대로 스냅

type Snap = 'peek' | 'full'

interface MobileSheetProps {
  onClose: () => void
  children: ReactNode
}

const clamp = (v: number, min: number, max: number) => Math.min(Math.max(v, min), max)

/** 모바일 전용 드래그 바텀시트. peek로 등장 → 위로 드래그해 확장, 아래로 드래그해 닫기 */
export function MobileSheet({ onClose, children }: MobileSheetProps) {
  const [snap, setSnap] = useState<Snap>('peek')
  const [dragY, setDragY] = useState<number | null>(null)   // 드래그/닫힘 중 translateY(px)
  const [entered, setEntered] = useState(false)             // 등장 슬라이드업용
  const [closing, setClosing] = useState(false)
  const sheetRef = useRef<HTMLDivElement | null>(null)
  const handleRef = useRef<HTMLDivElement | null>(null)
  const gesture = useRef({
    active: false, fromHandle: false, moved: false,
    startY: 0, baseY: 0, prevY: 0, prevT: 0, lastY: 0, lastT: 0,
  })

  useEffect(() => {
    const id = requestAnimationFrame(() => setEntered(true))
    return () => cancelAnimationFrame(id)
  }, [])

  const fullH = () => sheetRef.current?.offsetHeight ?? 0
  const snapY = (s: Snap) => (s === 'peek' ? Math.max(fullH() - PEEK, 0) : 0)

  const onPointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (closing) return
    const fromHandle = handleRef.current?.contains(e.target as Node) ?? false
    // 전체 확장 상태에선 내용 스크롤과 충돌하지 않게 핸들에서만 드래그 시작
    if (snap === 'full' && !fromHandle) return
    const g = gesture.current
    g.active = true
    g.fromHandle = fromHandle
    g.moved = false
    g.startY = e.clientY
    g.baseY = dragY ?? snapY(snap)
    g.prevY = g.lastY = e.clientY
    g.prevT = g.lastT = e.timeStamp
    e.currentTarget.setPointerCapture(e.pointerId)
  }

  const onPointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    const g = gesture.current
    if (!g.active) return
    const dy = e.clientY - g.startY
    if (Math.abs(dy) > 5) g.moved = true
    g.prevY = g.lastY
    g.prevT = g.lastT
    g.lastY = e.clientY
    g.lastT = e.timeStamp
    setDragY(clamp(g.baseY + dy, 0, fullH()))
  }

  const settle = (next: Snap | 'close') => {
    if (next === 'close') {
      setClosing(true)
      setDragY(fullH())
      setTimeout(onClose, 250)
    } else {
      setSnap(next)
      setDragY(null)
    }
  }

  const onPointerUp = () => {
    const g = gesture.current
    if (!g.active) return
    g.active = false
    if (!g.moved) {
      // 핸들 탭 → peek/전체 토글
      if (g.fromHandle) setSnap((s) => (s === 'peek' ? 'full' : 'peek'))
      setDragY(null)
      return
    }
    const y = dragY ?? snapY(snap)
    const peekY = snapY('peek')
    const v = (g.lastY - g.prevY) / Math.max(g.lastT - g.prevT, 1)
    if (v <= -FLICK_VELOCITY) return settle('full')
    if (v >= FLICK_VELOCITY) return settle(y < peekY - 10 ? 'peek' : 'close')
    if (y > peekY + CLOSE_THRESHOLD) return settle('close')
    settle(y < peekY / 2 ? 'full' : 'peek')
  }

  const onPointerCancel = () => {
    gesture.current.active = false
    setDragY(null)
  }

  const transform = !entered
    ? 'translateY(100%)'
    : dragY != null
      ? `translateY(${dragY}px)`
      : snap === 'peek'
        ? `translateY(calc(100% - ${PEEK}px))`
        : 'translateY(0)'

  return (
    <div
      ref={sheetRef}
      className={
        'absolute inset-x-0 bottom-0 z-20 flex h-[75dvh] flex-col overflow-hidden rounded-t-2xl bg-white shadow-[0_-8px_24px_rgba(0,0,0,0.15)] md:hidden' +
        (dragY != null && !closing ? '' : ' transition-transform duration-300 ease-out')
      }
      style={{ transform, touchAction: snap === 'peek' ? 'none' : 'auto' }}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerCancel={onPointerCancel}
    >
      <div ref={handleRef} className="flex shrink-0 cursor-grab justify-center py-2" style={{ touchAction: 'none' }}>
        <div className="h-1 w-10 rounded-full bg-gray-300" />
      </div>
      <div className="min-h-0 flex-1">{children}</div>
    </div>
  )
}
