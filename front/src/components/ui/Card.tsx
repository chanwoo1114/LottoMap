import type { ReactNode } from 'react'
import { cn } from './cn'

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return <section className={cn('rounded-2xl bg-white p-5 shadow-sm', className)}>{children}</section>
}