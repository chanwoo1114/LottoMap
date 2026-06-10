import type {ButtonHTMLAttributes, Ref} from "react";

type Variant = 'primary' | 'outline' | 'link';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  ref?: Ref<HTMLButtonElement>;
}

const BASE = "cursor-pointer transition disabled:cursor-not-allowed disabled:opacity-50";

const VARIANTS: Record<Variant, string> = {
  primary:
    "rounded-lg bg-emerald-500 py-2.5 text-center font-medium text-white hover:opacity-90 active:scale-[0.99]",
  outline:
    "shrink-0 rounded-lg border border-emerald-500 px-3 py-2 text-sm font-medium text-emerald-600 hover:bg-emerald-50",
  link:
    "text-gray-500 hover:text-gray-700 hover:underline",
};


export function Button({
  variant = 'primary',
  type = 'button',
  className = '',
  ...props
}: ButtonProps) {
  return(
    <button
      type={type}
      className={`${BASE} ${VARIANTS[variant]} ${className}`.trim()}
      {...props}
    />
  )
}