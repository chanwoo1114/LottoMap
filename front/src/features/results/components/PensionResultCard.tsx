import type { PensionResult } from '../api'
import { Card } from '@/components/ui/Card'
import { ResultCardHeader } from './ResultCardHeader'

export function PensionResultCard({ result }: { result: PensionResult }) {
  return (
    <Card>
      <ResultCardHeader
        label='연금복권720+'
        color='blue'
        round={result.round_no}
        date={result.draw_date}
      />

      <div className='flex items-center gap-3'>
        <span className='w-12 text-sm font-semibold text-gray-500'>1등</span>
        <span className='grid h-10 w-10 place-items-center rounded-lg bg-blue-600 text-lg font-bold text-white'>
          {result.first_prize_group}조
        </span>
        <div className='flex gap-1.5'>
          {result.first_prize_number.split('').map((d, i) => <Digit key={i} d={d} />)}
        </div>
      </div>

      <div className='mt-3 flex items-center gap-3 border-t border-gray-100 pt-3'>
        <span className='w-12 text-sm font-semibold text-gray-400'>보너스</span>
        <span className='grid h-10 w-10 place-items-center rounded-lg bg-gray-400 text-sm font-bold text-white'>
          각조
        </span>
        <div className='flex gap-1.5'>
          {result.bonus_number.split('').map((d, i) => <Digit key={i} d={d} />)}
        </div>
      </div>
    </Card>
  )
}

function Digit({ d }: { d: string }) {
  return (
    <span className='grid h-9 w-8 place-items-center rounded-md border border-gray-200 bg-gray-50 text-lg font-bold text-gray-800'>
      {d}
    </span>
  )
}