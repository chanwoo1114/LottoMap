import { useEffect, useRef, useState } from 'react'
import { WinningStoreSection } from './WinningStoreSection'
import type { SpeettoGame, WinningStore } from '../api'

// minRound = 백엔드 winning_stores 크롤러의 수집 시작 회차 (_ST_MIN_ROUND)
const TYPES = [
  { key: 'st2000', name: '스피또2000', lotteryType: 'speetto_2000', minRound: 14 },
  { key: 'st1000', name: '스피또1000', lotteryType: 'speetto_1000', minRound: 16 },
  { key: 'st500', name: '스피또500', lotteryType: 'speetto_500', minRound: 18 },
] as const

type TypeKey = (typeof TYPES)[number]['key']

interface SpeettoWinningSectionProps {
  games: SpeettoGame[];
  focusGame: SpeettoGame | null;   // 게임 카드 클릭 시 해당 게임으로 점프
  onShowOnMap: (store: WinningStore) => void;
}

export function SpeettoWinningSection({ games, focusGame, onShowOnMap }: SpeettoWinningSectionProps) {
  const [typeKey, setTypeKey] = useState<TypeKey>('st2000')
  const [round, setRound] = useState(0)   // 0이면 판매 중 최신 회차로 대체
  const sectionRef = useRef<HTMLDivElement>(null)

  const cur = TYPES.find((t) => t.key === typeKey)!
  const latest = Math.max(0, ...games.filter((g) => g.game_type === typeKey).map((g) => g.round_no))
  const curRound = round || latest

  useEffect(() => {
    if (!focusGame) return
    setTypeKey(focusGame.game_type)
    setRound(focusGame.round_no)
    sectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [focusGame])

  const step = (d: number) =>
    setRound(Math.min(latest, Math.max(cur.minRound, curRound + d)))

  return (
    <div ref={sectionRef} className='space-y-3'>
      <div className='flex flex-wrap items-center justify-between gap-2'>
        <div className='flex gap-1.5'>
          {TYPES.map((t) => (
            <button
              key={t.key}
              onClick={() => { setTypeKey(t.key); setRound(0) }}
              className={`rounded-full px-3 py-1 text-[13px] font-bold ${typeKey === t.key ? 'bg-orange-100 text-orange-700' : 'bg-gray-100 text-gray-500'}`}
            >
              {t.name}
            </button>
          ))}
        </div>
        <div className='flex items-center gap-1.5'>
          <button
            onClick={() => step(-1)}
            className='grid h-8 w-8 place-items-center rounded-lg border border-gray-200 bg-white text-gray-500 hover:bg-gray-50'
          >
            ‹
          </button>
          <span className='min-w-[86px] rounded-lg border border-gray-200 bg-white px-3 py-1 text-center text-sm font-bold text-gray-900'>
            제 {curRound}회
          </span>
          <button
            onClick={() => step(1)}
            className='grid h-8 w-8 place-items-center rounded-lg border border-gray-200 bg-white text-gray-500 hover:bg-gray-50'
          >
            ›
          </button>
        </div>
      </div>

      {curRound > 0 && (
        <WinningStoreSection lotteryType={cur.lotteryType} roundNo={curRound} onShowOnMap={onShowOnMap} />
      )}
    </div>
  )
}
