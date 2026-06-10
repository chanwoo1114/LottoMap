import type { Store } from './api'

export type ProductKind = 'lotto' | 'pension' | 'speetto'
export interface Product { label: string; kind: ProductKind }

export function getProducts(s: Store): Product[] {
  const out: Product[] = []
  if (s.sells_lotto)         out.push({ label: '로또',        kind: 'lotto' })
  if (s.sells_pension)       out.push({ label: '연금',        kind: 'pension' })
  if (s.sells_speetto_2000)  out.push({ label: '스피또2000',  kind: 'speetto' })
  if (s.sells_speetto_1000)  out.push({ label: '스피또1000',  kind: 'speetto' })
  if (s.sells_speetto_500)   out.push({ label: '스피또500',   kind: 'speetto' })
  return out
}