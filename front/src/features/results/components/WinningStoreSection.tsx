import { useEffect, useState } from 'react'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { getWinningStores, type WinningStore } from '../api'

const METHOD: Record<string, { label: string; cls: string }> = {
  auto:      { label: '자동',  cls: 'bg-blue-50 text-blue-600' },
  manual:    { label: '수동',  cls: 'bg-purple-50 text-purple-600' },
  semi_auto: { label: '반자동', cls: 'bg-gray-100 text-gray-600' },
}

const RANKS: Record<string, { label: string; rank: number }[]> = {
  lotto:   [{ label: '1등', rank: 1 }, { label: '2등', rank: 2 }],
  pension: [{ label: '1등', rank: 1 }, { label: '2등', rank: 2 }, { label: '보너스', rank: 3 }],
}

interface WinningStoreSectionProps {
  lotteryType: string;
  roundNo: number;
  onShowOnMap: (store: WinningStore) => void;
}

export function WinningStoreSection({ lotteryType, roundNo, onShowOnMap }: WinningStoreSectionProps) {
  const ranks = RANKS[lotteryType] ?? RANKS.lotto
  const [rank, setRank] = useState(1)
  const [stores, setStores] = useState<WinningStore[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => { setRank(1) }, [lotteryType])

  useEffect(() => {
    if (!roundNo) return
    let cancelled = false
    setLoading(true)
    getWinningStores(lotteryType, roundNo, rank)
      .then((d) => { if (!cancelled) setStores(d) })
      .catch(() => { if (!cancelled) setStores([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [lotteryType, roundNo, rank])

  const curLabel = ranks.find((o) => o.rank === rank)?.label ?? '1등'

  return (
    <Card>
      <div className='mb-3 flex items-center gap-2'>
        <span className='text-base font-bold text-gray-900'>🏆 {curLabel} 배출점</span>
        {!loading && <Badge color='amber' className='px-2 py-0.5'>{stores.length}곳</Badge>}

        <div className='ml-auto flex gap-1 rounded-lg bg-gray-100 p-0.5'>
          {ranks.map((o) => (
            <button
              key={o.rank}
              onClick={() => setRank(o.rank)}
              className={`rounded-md px-2.5 py-1 text-xs font-semibold ${rank === o.rank ? 'bg-white text-accent shadow-sm' : 'text-gray-500'}`}
            >
              {o.label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <p className='py-6 text-center text-sm text-gray-400'>불러오는 중...</p>
      ) : stores.length === 0 ? (
        <p className='py-6 text-center text-sm text-gray-400'>이 회차 {curLabel} 배출점 정보가 없어요</p>
      ) : (
        <ul className='divide-y divide-gray-100'>
          {stores.map((s) => {
            const m = METHOD[s.purchase_method]
            return (
              <li key={s.store_id} className='flex items-center gap-2 py-3'>
                {m && (
                  <span className={`shrink-0 rounded px-1.5 py-0.5 text-[11px] font-bold ${m.cls}`}>{m.label}</span>
                )}
                <div className='min-w-0 flex-1'>
                  <p className='truncate font-medium text-gray-900'>{s.name}</p>
                  <p className='truncate text-sm text-gray-500'>{s.address}</p>
                </div>
                {s.lat != null && s.lng != null && (
                  <button
                    onClick={() => onShowOnMap(s)}
                    className='shrink-0 rounded-lg border border-gray-200 px-2.5 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50'
                  >
                    지도에서 보기
                  </button>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </Card>
  )
}