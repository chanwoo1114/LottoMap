import { Card } from '@/components/ui/Card'

const ROWS = [
  { rank: '1등', match: 6, bonus: false, prize: '4·5등 제외 금액의 75%',   odds: '1/8,145,060' },
  { rank: '2등', match: 5, bonus: true,  prize: '4·5등 제외 금액의 12.5%', odds: '1/1,357,510' },
  { rank: '3등', match: 5, bonus: false, prize: '4·5등 제외 금액의 12.5%', odds: '1/35,724' },
  { rank: '4등', match: 4, bonus: false, prize: '50,000원',               odds: '1/733' },
  { rank: '5등', match: 3, bonus: false, prize: '5,000원',                odds: '1/45' },
]

function Dot({ on }: { on: boolean }) {
  return <span className={`inline-block h-3 w-3 rounded-full ${on ? 'bg-rose-400' : 'border border-gray-300 bg-white'}`} />
}

function MatchDots({ match, bonus }: { match: number; bonus: boolean }) {
  const base = bonus ? 5 : 6
  return (
    <div className='mt-1 flex items-center gap-1'>
      {Array.from({ length: base }).map((_, i) => <Dot key={i} on={i < match} />)}
      {bonus && <><span className='px-0.5 text-xs text-gray-400'>+</span><Dot on /></>}
    </div>
  )
}

export function LottoPrizeStructure() {
  return (
    <Card className='p-0'>
      <details className='group'>
        <summary className='flex cursor-pointer list-none items-center justify-between p-4 text-sm font-bold text-gray-900'>
          당첨구조
          <svg width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='#888' strokeWidth='2'
               className='transition-transform group-open:rotate-180'>
            <path d='M6 9l6 6 6-6' />
          </svg>
        </summary>

        <div className='overflow-x-auto px-4 pb-4'>
          <table className='w-full border-separate border-spacing-0 text-sm'>
            <thead>
            <tr className='bg-rose-500 text-xs text-white'>
              <th className='rounded-l-lg px-3 py-2 text-left font-medium'>등위</th>
              <th className='px-3 py-2 text-left font-medium'>당첨조건</th>
              <th className='px-3 py-2 text-right font-medium'>당첨금</th>
              <th className='rounded-r-lg px-3 py-2 text-right font-medium'>확률</th>
            </tr>
            </thead>
            <tbody>
            {ROWS.map((r) => (
              <tr key={r.rank} className='border-b border-gray-100'>
                <td className='border-b border-gray-100 px-3 py-3 font-bold text-rose-500'>{r.rank}</td>
                <td className='border-b border-gray-100 px-3 py-3'>
                    <span className='font-medium text-gray-700'>
                      {r.match}개 번호 일치{r.bonus && ' + 보너스'}
                    </span>
                  <MatchDots match={r.match} bonus={r.bonus} />
                </td>
                <td className='border-b border-gray-100 px-3 py-3 text-right text-gray-900'>{r.prize}</td>
                <td className='border-b border-gray-100 px-3 py-3 text-right text-gray-500'>{r.odds}</td>
              </tr>
            ))}
            </tbody>
          </table>
        </div>
      </details>
    </Card>
  )
}