import { Card } from '@/components/ui/Card'

const ROWS = [
  { rank: '1등', cond: '6개 번호 일치',        prize: '4·5등 제외 금액의 75%',   odds: '1/8,145,060' },
  { rank: '2등', cond: '5개 + 보너스 일치',     prize: '4·5등 제외 금액의 12.5%', odds: '1/1,357,510' },
  { rank: '3등', cond: '5개 번호 일치',        prize: '4·5등 제외 금액의 12.5%', odds: '1/35,724' },
  { rank: '4등', cond: '4개 번호 일치',        prize: '50,000원',               odds: '1/733' },
  { rank: '5등', cond: '3개 번호 일치',        prize: '5,000원',                odds: '1/45' },
]

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

        <div className='overflow-x-auto border-t border-gray-100 px-4 pb-4 pt-1'>
          <table className='w-full text-sm'>
            <thead>
            <tr className='text-xs text-gray-400'>
              <th className='py-2 text-left font-medium'>등위</th>
              <th className='text-left font-medium'>당첨조건</th>
              <th className='text-right font-medium'>당첨금</th>
              <th className='text-right font-medium'>확률</th>
            </tr>
            </thead>
            <tbody>
            {ROWS.map((r) => (
              <tr key={r.rank} className='border-t border-gray-50'>
                <td className='py-2 font-bold text-gray-700'>{r.rank}</td>
                <td className='text-gray-600'>{r.cond}</td>
                <td className='text-right text-gray-900'>{r.prize}</td>
                <td className='text-right text-gray-500'>{r.odds}</td>
              </tr>
            ))}
            </tbody>
          </table>
        </div>
      </details>
    </Card>
  )
}