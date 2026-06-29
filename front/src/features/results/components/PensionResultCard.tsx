import type { PensionResult } from '../api'
import { Card } from '@/components/ui/Card'
import { ResultCardHeader } from './ResultCardHeader'

const DIGIT_COLORS = ['#ef4444', '#f97316', '#f5b800', '#3b9fe8', '#a855f7', '#9ca3af']

export function PensionResultCard({ result }: { result: PensionResult }) {
  return (
    <Card>
      <ResultCardHeader label='연금복권720+' color='blue' round={result.round_no} date={result.draw_date} />

      <div className='flex items-center gap-3'>
        <span className='w-10 text-sm font-semibold text-gray-500'>1등</span>
        <span className='grid h-10 w-10 place-items-center rounded-lg bg-blue-600 text-lg font-bold text-white'>
          {result.first_prize_group}조
        </span>
        <div className='flex gap-1.5'>
          {result.first_prize_number.split('').map((d, i) => <Digit key={i} d={d} i={i} />)}
        </div>
      </div>

      <div className='mt-3 flex items-center gap-3 border-t border-gray-100 pt-3'>
        <span className='w-10 text-sm font-semibold text-gray-400'>보너스</span>
        <span className='grid h-10 w-10 place-items-center rounded-lg bg-gray-400 text-sm font-bold text-white'>각조</span>
        <div className='flex gap-1.5'>
          {result.bonus_number.split('').map((d, i) => <Digit key={i} d={d} i={i} />)}
        </div>
      </div>
    </Card>
  )
}

function Digit({ d, i }: { d: string; i: number }) {
  const color = DIGIT_COLORS[i % DIGIT_COLORS.length]
  return (
    <span
      className='grid h-10 w-10 place-items-center rounded-full border-2 bg-white text-lg font-bold'
      style={{ borderColor: color, color }}
    >
      {d}
    </span>
  )
}