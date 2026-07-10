import { RANK_LABEL, type Summary } from '../status'

function Tile({ label, value, tone }: { label: string; value: string; tone?: 'accent' | 'win' }) {
  return (
    <div className='flex-1 rounded-xl bg-white p-3 text-center shadow-sm'>
      <div
        className={`text-lg font-bold ${
          tone === 'win' ? 'text-emerald-600' : tone === 'accent' ? 'text-accent' : 'text-gray-900'
        }`}
      >
        {value}
      </div>
      <div className='mt-0.5 text-[11px] font-medium text-gray-400'>{label}</div>
    </div>
  )
}

export function SavedNumbersSummary({ summary }: { summary: Summary }) {
  const best = summary.bestRank == null ? '—' : RANK_LABEL[summary.bestRank]
  return (
    <div className='flex gap-2'>
      <Tile label='저장' value={String(summary.total)} />
      <Tile label='당첨' value={String(summary.wins)} tone={summary.wins > 0 ? 'win' : undefined} />
      <Tile label='최고 등수' value={best} tone={summary.bestRank != null ? 'win' : undefined} />
      <Tile label='추첨 대기' value={String(summary.pending)} tone={summary.pending > 0 ? 'accent' : undefined} />
    </div>
  )
}
