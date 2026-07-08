import type { SpeettoGame } from '../api'

const TONE: Record<SpeettoGame['game_type'], { badge: string; fill: string }> = {
  st2000: { badge: 'bg-orange-100 text-orange-700', fill: '#f97316' },
  st1000: { badge: 'bg-amber-100 text-amber-700', fill: '#f59e0b' },
  st500: { badge: 'bg-yellow-100 text-yellow-600', fill: '#eab308' },
}

const abbr = (n: number) =>
  n >= 10000 ? `${Math.round(n / 1000) / 10}만` : n.toLocaleString('ko-KR')

const shortDate = (d: string | null) => (d ? d.slice(2).replaceAll('-', '.') : '-')

function CountLabel({ left, total }: { left: number; total: number }) {
  if (left === 0) return <span className='text-sm font-bold text-red-500'>소진</span>
  return (
    <span className='text-base font-extrabold text-gray-900'>
      {abbr(left)}<span className='text-xs font-medium text-gray-400'> / {abbr(total)}매</span>
    </span>
  )
}

/** 1등: 매수가 적어 낱개 칸으로 표시 */
function Pips({ left, total, fill }: { left: number; total: number; fill: string }) {
  return (
    <div className='mt-1 flex gap-1'>
      {Array.from({ length: total }, (_, i) => (
        <span
          key={i}
          className='h-2.5 flex-1 rounded-sm'
          style={{ background: i < left ? fill : '#e5e7eb' }}
        />
      ))}
    </div>
  )
}

/** 2·3등: 수량이 많아 연속 진행바 */
function Bar({ left, total, fill }: { left: number; total: number; fill: string }) {
  const pct = total ? Math.round((left / total) * 100) : 0
  return (
    <div className='mt-1 h-2.5 w-full overflow-hidden rounded-full bg-gray-200'>
      <div className='h-full rounded-full' style={{ width: `${pct}%`, background: fill }} />
    </div>
  )
}

interface SpeettoGameCardProps {
  game: SpeettoGame;
  onClick?: () => void;
}

export function SpeettoGameCard({ game: g, onClick }: SpeettoGameCardProps) {
  const t = TONE[g.game_type]
  const dead = g.remaining_first_prizes === 0

  const prizeRow = (label: string, left: number, total: number, kind: 'pips' | 'bar') => (
    <div>
      <div className='flex items-baseline justify-between'>
        <span className='text-sm font-semibold text-gray-700'>{label}</span>
        <CountLabel left={left} total={total} />
      </div>
      {kind === 'pips'
        ? <Pips left={left} total={total} fill={dead ? '#fca5a5' : t.fill} />
        : <Bar left={left} total={total} fill={t.fill} />}
    </div>
  )

  return (
    <section
      onClick={onClick}
      className='cursor-pointer overflow-hidden rounded-2xl bg-white shadow-sm transition hover:-translate-y-0.5 hover:shadow-md'
    >
      <div className='flex flex-col sm:flex-row'>
        <div className='relative shrink-0 bg-gray-50 sm:w-[420px]'>
          {g.image_url && (
            <img
              src={g.image_url}
              alt={`${g.name} ${g.round_no}회`}
              className={`h-60 w-full object-cover sm:absolute sm:inset-0 sm:h-full ${dead ? 'opacity-40 grayscale' : ''}`}
            />
          )}
          {dead && (
            <div className='absolute inset-0 grid place-items-center'>
              <span className='-rotate-12 rounded-lg border-4 border-red-500 bg-white/80 px-4 py-1 text-xl font-black tracking-widest text-red-500'>
                1등 소진
              </span>
            </div>
          )}
        </div>

        <div className='flex min-w-0 flex-1 flex-col p-4 sm:p-6'>
          <div className='mb-4 flex items-center justify-between'>
            <div className='flex items-center gap-2'>
              <span className={`rounded-full px-3 py-1 text-sm font-bold ${t.badge}`}>{g.name}</span>
              <span className='text-xl font-bold text-gray-900'>제 {g.round_no}회</span>
            </div>
            <span className='rounded-lg bg-gray-100 px-2.5 py-1 text-sm font-bold text-gray-600'>
              {g.price.toLocaleString('ko-KR')}원
            </span>
          </div>

          <div className='space-y-3.5'>
            {prizeRow('1등 잔여', g.remaining_first_prizes, g.total_first_prizes, 'pips')}
            {prizeRow('2등 잔여', g.remaining_second_prizes, g.total_second_prizes, 'bar')}
            {prizeRow('3등 잔여', g.remaining_third_prizes, g.total_third_prizes, 'bar')}
          </div>

          <div className='mt-auto flex items-center justify-between border-t border-gray-100 pt-3 text-sm text-gray-400 sm:mt-4'>
            <span>판매기한 {shortDate(g.sale_end_date)}</span>
            <span>
              입고율 <b className={g.intake_rate >= 90 ? 'text-red-400' : 'text-gray-500'}>{g.intake_rate}%</b>
            </span>
          </div>
        </div>
      </div>
    </section>
  )
}
