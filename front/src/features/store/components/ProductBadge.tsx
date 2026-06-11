import type { ProductKind } from "@/features/store/model.ts";
import { cn } from '@/components/ui/cn'

const STYLE: Record<ProductKind, string> = {
  lotto: '',
  pension: '',
  spetton: '',
}

export interface ProductBadgeProps {
  label: string,
  kind: ProductKind,
}

export function ProductBadge({label, kind}: ProductBadgeProps) {
  return (
    <span className={cn(), STYLE[kind]}>
      {label}
    </span>
  )
}