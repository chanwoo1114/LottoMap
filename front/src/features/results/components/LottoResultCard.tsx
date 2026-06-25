import type { LottoResult } from '../api'
import { Card } from '@/components/ui/Card'
import { ResultCardHeader } from "@/features/results/components/ResultCardHeader.tsx";
import { LottoBall } from "@/features/results/components/LottoBall.tsx";

function won(n: number): string {
  if (n >= 1e8) return `${(n / 1e8).toFixed(1)}억`
  if (n >= 1e4) return `${Math.round(n / 1e4).toLocaleString()}만`
  return n.toLocaleString()
}

export function LottoResultCard({ result }: {result: LottoResult}) {
  const stats = [
    { label: '1등 당첨금', value: won(result.first_prize_amount) },
    { label: '1등 당첨자', value: `${result.first_prize_winners}명` },
    { label: '총 판매액',  value: won(result.total_sales) },
  ]

  return (
    <Card>
      <ResultCardHeader
        label='로또 6/45'
        color='green'
        round={result.round_no}
        date={result.draw_date}
      />

      <div className='flex flex-wrap items-center justify-center gap-2'>
        {result.numbers.map((n) => <LottoBall key={n} n={n} />)}
        <span>+</span>
        <LottoBall n={result.bonus} />
      </div>

      <div className="mt-4 grid grid-cols-3 gap-3 border-t border-gray-100 pt-3 text-center">
        {stats.map((s) => (
          <div key={s.label}>
            <p className="text-xs text-gray-400">{s.label}</p>
            <p className="font-bold text-gray-900">{s.value}</p>
          </div>
        ))}
      </div>

    </Card>
  )
}