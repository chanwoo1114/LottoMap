import { Card } from '@/components/ui/Card'

const ROWS = [
  { rank: '1등',   cond: '1등 추첨번호와 연속 7자리 모두 일치 시',              hl: 'full',       prize: '월 700만 원 × 20년', odds: '1/5,000,000' },
  { rank: '2등',   cond: '1등 추첨번호와 오른쪽 끝부터 연속 6자리 모두 일치 시',  hl: '연속 6자리', prize: '월 100만 원 × 10년', odds: '1/1,250,000' },
  { rank: '3등',   cond: '1등 추첨번호와 오른쪽 끝부터 연속 5자리 모두 일치 시',  hl: '연속 5자리', prize: '1백만 원',           odds: '1/111,111' },
  { rank: '4등',   cond: '1등 추첨번호와 오른쪽 끝부터 연속 4자리 모두 일치 시',  hl: '연속 4자리', prize: '1십만 원',           odds: '1/11,111' },
  { rank: '5등',   cond: '1등 추첨번호와 오른쪽 끝부터 연속 3자리 모두 일치 시',  hl: '연속 3자리', prize: '5만 원',             odds: '1/1,111' },
  { rank: '6등',   cond: '1등 추첨번호와 오른쪽 끝부터 연속 2자리 모두 일치 시',  hl: '연속 2자리', prize: '5천 원',             odds: '1/111' },
  { rank: '7등',   cond: '1등 추첨번호와 오른쪽 끝 1자리 일치 시',               hl: '1자리',      prize: '1천 원',             odds: '1/11' },
  { rank: '보너스', cond: '보너스 등위 추첨번호와 오른쪽 끝부터 연속 6자리 모두 일치 시', hl: 'full', prize: '월 100만 원 × 10년', odds: '1/1,000,000' },
]

function Cond({ text, hl }: { text: string; hl: string }) {
  if (hl === 'full') return <span className='text-amber-500'>{text}</span>
  const [before, after] = text.split(hl)
  return <>{before}<span className='text-amber-500'>{hl}</span>{after}</>
}

export function PensionPrizeStructure() {
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
            <tr className='bg-amber-500 text-xs whitespace-nowrap text-white'>
              <th className='rounded-l-lg px-3 py-2 text-left font-medium'>등위</th>
              <th className='px-3 py-2 text-left font-medium'>당첨조건</th>
              <th className='px-3 py-2 text-right font-medium'>당첨금</th>
              <th className='rounded-r-lg px-3 py-2 text-right font-medium'>당첨확률</th>
            </tr>
            </thead>
            <tbody>
            {ROWS.map((r) => (
              <tr key={r.rank}>
                <td className='whitespace-nowrap border-b border-gray-100 px-3 py-3 font-bold text-gray-700'>{r.rank}</td>
                <td className='border-b border-gray-100 px-3 py-3 font-medium text-gray-700'>
                  <Cond text={r.cond} hl={r.hl} />
                </td>
                <td className='whitespace-nowrap border-b border-gray-100 px-3 py-3 text-right text-gray-900'>{r.prize}</td>
                <td className='whitespace-nowrap border-b border-gray-100 px-3 py-3 text-right text-gray-500'>{r.odds}</td>
              </tr>
            ))}
            </tbody>
          </table>
        </div>
      </details>
    </Card>
  )
}