import { Card } from '@/components/ui/Card'

type Row = {
  rank: string
  cond: { text: string; hi?: string }
  full?: boolean
  prize: string
  odds: string
  store: string
  net: string
  total: string
}

const ROWS: Row[] = [
  { rank: '1등',   cond: { text: '1등 추첨번호와 연속 7자리 모두 일치 시' }, full: true,
    prize: '월 700만 원 X 20년', odds: '1/5,000,000', store: '1매', net: '1매', total: '2매' },
  { rank: '2등',   cond: { text: '1등 추첨번호와 오른쪽 끝부터 연속 6자리 모두 일치 시', hi: '연속 6자리' },
    prize: '월 100만 원 X 10년', odds: '1/1,250,000', store: '4매', net: '4매', total: '8매' },
  { rank: '3등',   cond: { text: '1등 추첨번호와 오른쪽 끝부터 연속 5자리 모두 일치 시', hi: '연속 5자리' },
    prize: '1백만 원', odds: '1/111,111', store: '45매', net: '45매', total: '90매' },
  { rank: '4등',   cond: { text: '1등 추첨번호와 오른쪽 끝부터 연속 4자리 모두 일치 시', hi: '연속 4자리' },
    prize: '1십만 원', odds: '1/11,111', store: '450매', net: '450매', total: '900매' },
  { rank: '5등',   cond: { text: '1등 추첨번호와 오른쪽 끝부터 연속 3자리 모두 일치 시', hi: '연속 3자리' },
    prize: '5만 원', odds: '1/1,111', store: '4,500매', net: '4,500매', total: '9,000매' },
  { rank: '6등',   cond: { text: '1등 추첨번호와 오른쪽 끝부터 연속 2자리 모두 일치 시', hi: '연속 2자리' },
    prize: '5천 원', odds: '1/111', store: '45,000매', net: '45,000매', total: '90,000매' },
  { rank: '7등',   cond: { text: '1등 추첨번호와 오른쪽 끝 1자리 일치 시', hi: '1자리' },
    prize: '1천 원', odds: '1/11', store: '450,000매', net: '450,000매', total: '900,000매' },
  { rank: '보너스', cond: { text: '보너스 등위 추첨번호와 오른쪽 끝부터 연속 6자리 일치 시' }, full: true,
    prize: '월 100만 원 X 10년', odds: '1/1,000,000', store: '5매', net: '5매', total: '10매' },
]

function Cond({ row }: { row: Row }) {
  if (row.full) return <span className='text-accent'>{row.cond.text}</span>
  if (!row.cond.hi) return <>{row.cond.text}</>
  const [before, after] = row.cond.text.split(row.cond.hi)
  return (
    <>
      {before}
      <span className='text-accent'>{row.cond.hi}</span>
      {after}
    </>
  )
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

        <div className='overflow-x-auto border-t border-gray-100 px-4 pb-4 pt-1'>
          <table className='w-full whitespace-nowrap text-sm'>
            <thead>
            <tr className='text-xs text-gray-400'>
              <th rowSpan={2} className='py-2 text-left font-medium'>등위</th>
              <th rowSpan={2} className='text-left font-medium'>당첨조건</th>
              <th rowSpan={2} className='text-right font-medium'>당첨금</th>
              <th rowSpan={2} className='text-right font-medium'>당첨확률</th>
              <th colSpan={3} className='pb-1 text-center font-medium'>당첨매수</th>
            </tr>
            <tr className='text-xs text-gray-400'>
              <th className='text-right font-medium'>판매점</th>
              <th className='text-right font-medium'>인터넷</th>
              <th className='text-right font-medium'>합계</th>
            </tr>
            </thead>
            <tbody>
            {ROWS.map((r) => (
              <tr key={r.rank} className='border-t border-gray-50'>
                <td className='whitespace-nowrap py-2 pr-4 font-bold text-gray-700'>{r.rank}</td>
                <td className='pr-3 text-gray-600'><Cond row={r} /></td>
                <td className='whitespace-nowrap pr-3 text-right text-gray-900'>{r.prize}</td>
                <td className='pr-3 text-right text-gray-500'>{r.odds}</td>
                <td className='pr-3 text-right text-gray-500'>{r.store}</td>
                <td className='pr-3 text-right text-gray-500'>{r.net}</td>
                <td className='text-right font-medium text-gray-700'>{r.total}</td>
              </tr>
            ))}
            </tbody>
          </table>
        </div>
      </details>
    </Card>
  )
}