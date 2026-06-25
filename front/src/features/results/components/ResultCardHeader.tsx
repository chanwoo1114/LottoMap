import { Badge } from '@/components/ui/Badge'

interface ResultCardHeaderProps {
  label: string;
  color: 'green' | 'blue';
  round: number;
  date: string;
}

export function ResultCardHeader({
  label,
  color,
  round,
  date,
}: ResultCardHeaderProps) {
  return (
    <div className='mb-3 flex justify-between items-end'>
      <div>
          <Badge color={color}>{label}</Badge>
        <span className="ml-3 text-lg font-bold text-gray-900">
          제 {round}회
        </span>
      </div>
      <span className='text-sm text-gray-400'>
        {date?.replace(/-/g, '.')} 추첨
      </span>
    </div>
  )
}