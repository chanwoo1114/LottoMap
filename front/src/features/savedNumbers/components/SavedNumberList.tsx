import { LottoBall } from '@/features/results/components/LottoBall'
import type { SavedNumber } from '../api'
import { RANK_LABEL, statusOf } from '../status'

function ResultBadge({ s }: { s: SavedNumber }) {
  const st = statusOf(s)
  if (st === 'pending') {
    return <span className='rounded-md bg-blue-50 px-2 py-0.5 text-[11px] font-bold text-blue-500'>추첨 대기 중</span>
  }
  if (st === 'lose') {
    return <span className='rounded-md bg-gray-100 px-2 py-0.5 text-[11px] font-bold text-gray-400'>낙첨</span>
  }
  return (
    <span className='rounded-md bg-emerald-100 px-2 py-0.5 text-[11px] font-bold text-emerald-700'>
      🎉 {RANK_LABEL[s.matched_rank!]} 당첨
    </span>
  )
}

function SavedItem({ s, onRemove }: { s: SavedNumber; onRemove: (id: number) => void }) {
  const won = s.winning_numbers // 추첨 완료된 회차면 당첨번호 배열, 아니면 null
  const wonSet = won ? new Set(won) : null

  return (
    <li className='rounded-xl border border-gray-100 bg-white p-3 shadow-sm'>
      <div className='mb-2 flex items-center justify-between'>
        <div className='flex items-center gap-1.5 text-xs text-gray-400'>
          <span className='font-bold text-gray-600'>{s.round_no}회</span>
          {s.draw_date && <span>· {s.draw_date}</span>}
        </div>
        <div className='flex items-center gap-2'>
          <ResultBadge s={s} />
          <button
            onClick={() => onRemove(s.id)}
            aria-label='삭제'
            className='grid h-6 w-6 place-items-center rounded-lg text-gray-300 transition hover:bg-gray-50 hover:text-red-400'
          >
            ✕
          </button>
        </div>
      </div>

      <div className='flex flex-wrap items-center gap-1.5'>
        {s.numbers.map((n) => (
          <LottoBall key={n} n={n} size={34} dim={wonSet != null && !wonSet.has(n)} />
        ))}
        {s.matched_bonus && s.winning_bonus != null && (
          <span className='ml-1 flex items-center gap-1 text-[11px] font-bold text-amber-500'>
            + 보너스 {s.winning_bonus}
          </span>
        )}
        {wonSet != null && (
          <span className='ml-auto text-xs font-bold text-gray-500'>{s.matched_count}개 일치</span>
        )}
      </div>
    </li>
  )
}

export function SavedNumberList({ items, onRemove }: {
  items: SavedNumber[]
  onRemove: (id: number) => void
}) {
  return (
    <ul className='space-y-2'>
      {items.map((s) => <SavedItem key={s.id} s={s} onRemove={onRemove} />)}
    </ul>
  )
}
