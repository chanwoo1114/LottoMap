import type { ReactNode } from 'react'
import { cn } from './cn'

const COLOR = {
  green:  'bg-green-100 text-green-700',
  blue:   'bg-blue-100 text-blue-700',
  orange: 'bg-orange-100 text-orange-700',
  amber:  'bg-amber-100 text-amber-700',
} as const

interface BadgeProps {
  color: keyof typeof COLOR
  children: ReactNode
  className?: string
}

export  function Badge({
  color,
  children,
  className
}: BadgeProps) {
  return (
    <span className={cn('inline-flex items-center rounded-full px-2.5 py-1 text-xs font-bold', COLOR[color], className)}>
      {children}
    </span>
  )
}