import { useState, type ReactNode } from 'react'
import { LottoBall } from '@/features/results/components/LottoBall'
import type { LottoSet, PensionSet } from '../api'

function CopyButton({ text }: { text: string }) {
  const [done, setDone] = useState(false)
  const copy = () => {
    navigator.clipboard?.writeText(text)
    setDone(true)
    setTimeout(() => setDone(false), 1100)
  }
  return (
    <button
      onClick={copy}
      title='복사'
      className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg border p-0 transition ${
        done ? 'border-emerald-500 text-emerald-600' : 'border-gray-200 text-gray-400 hover:bg-gray-50'
      }`}
    >
      {done ? (
        <span className='text-xs font-bold'>✓</span>
      ) : (
        <svg width='15' height='15' viewBox='0 0 24 24' fill='none' stroke='currentColor' strokeWidth='2'>
          <rect x='9' y='9' width='11' height='11' rx='2' />
          <path d='M5 15V5a2 2 0 0 1 2-2h10' />
        </svg>
      )}
    </button>
  )
}

function SaveButton({ saved, onClick }: { saved: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      title={saved ? '저장됨 — 눌러서 해제' : '번호 저장'}
      className={`grid h-8 w-8 shrink-0 place-items-center rounded-lg border p-0 transition ${
        saved ? 'border-amber-400 text-amber-500' : 'border-gray-200 text-gray-400 hover:bg-gray-50'
      }`}
    >
      <svg width='16' height='16' viewBox='0 0 24 24' fill={saved ? 'currentColor' : 'none'} stroke='currentColor' strokeWidth='2'>
        <path d='M12 2l3 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.9 21l1.2-6.8-5-4.9 6.9-1z' />
      </svg>
    </button>
  )
}

function StatBadge({ children, green }: { children: ReactNode; green?: boolean }) {
  return (
    <span
      className={`rounded-md px-2 py-0.5 text-[11px] ${
        green ? 'bg-green-100 font-bold text-green-700' : 'bg-gray-100 font-semibold text-gray-500'
      }`}
    >
      {children}
    </span>
  )
}

export function LottoSetCard({ set, index, saved, onToggleSave }: {
  set: LottoSet
  index: number
  saved?: boolean
  onToggleSave?: () => void
}) {
  return (
    <li className='flex items-center gap-3 rounded-xl border border-gray-100 bg-white p-3 shadow-sm'>
      <span className='w-6 shrink-0 text-center text-sm font-bold text-gray-300'>{index + 1}</span>
      <div className='flex flex-1 flex-wrap items-center gap-1.5'>
        {set.numbers.map((n) => <LottoBall key={n} n={n} size={36} />)}
      </div>
      {set.confidence != null && (
        <div className='flex shrink-0 flex-col items-end gap-1'>
          <StatBadge green>신뢰도 {set.confidence}</StatBadge>
        </div>
      )}
      {onToggleSave && <SaveButton saved={!!saved} onClick={onToggleSave} />}
      <CopyButton text={set.numbers.join(', ')} />
    </li>
  )
}

export function PensionSetCard({ set, index }: { set: PensionSet; index: number }) {
  return (
    <li className='flex items-center gap-3 rounded-xl border border-gray-100 bg-white p-3 shadow-sm'>
      <span className='w-6 shrink-0 text-center text-sm font-bold text-gray-300'>{index + 1}</span>
      <span className='grid h-9 w-9 shrink-0 place-items-center rounded-md bg-blue-100 text-sm font-bold text-blue-700'>
        {set.group}조
      </span>
      <div className='flex flex-1 flex-wrap items-center gap-1'>
        {set.number.split('').map((d, i) => (
          <span
            key={i}
            className='grid h-9 w-8 place-items-center rounded-md border border-gray-200 text-base font-bold text-gray-800'
          >
            {d}
          </span>
        ))}
      </div>
      <CopyButton text={`${set.group}조 ${set.number}`} />
    </li>
  )
}
