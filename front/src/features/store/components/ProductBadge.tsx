import type { ProductKind } from "@/features/store/model.ts";
import { cn } from '@/components/ui/cn'

const STYLE: Record<ProductKind, string> = {
  lotto: 'bg-green-50 text-green-700 ring-green-600/20',
  pension: 'bg-blue-50 text-blue-700 ring-blue-600/20',
  speetto: 'bg-orange-50 text-orange-700 ring-orange-600/20',
}

interface ProductBadgeProps {
  label: string,
  kind: ProductKind,
}

export function ProductBadge({label, kind}: ProductBadgeProps) {
  return (
    <span className={cn('inline-flex rounded px-1.5 py-0.5 text-[11px] font-medium ring-1 ring-inset', STYLE[kind])}>
      {label}
    </span>
  )
}