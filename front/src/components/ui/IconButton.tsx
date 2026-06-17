import type { ComponentProps } from "react";
import { cn } from "./cn"

export function IconButton({ className, ...props }: ComponentProps<'button'>) {
  return (
    <button
      className={cn('rounded-full bg-white p-3 shadow-md transition hover:bg-gray-100 active:scale-95', className)}
      {...props}
    />
  )
}