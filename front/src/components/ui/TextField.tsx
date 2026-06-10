import type { InputHTMLAttributes, ReactNode } from "react";

const inputBase = "rounded-lg border px-3 py-2 text-base outline-none disabled:bg-gray-50";

export function inputClass(invalid = false) {
  return `${inputBase} ${
    invalid
      ? "border-red-500 focus:border-red-500"
      : "border-border focus:border-emerald-500"
  }`;
}

interface TextFieldProps extends InputHTMLAttributes<HTMLInputElement> {
  invalid?: boolean;
  error?: string | null;
  message?: ReactNode;
  right?: ReactNode;
}

export function TextField({
  invalid,
  error,
  message,
  right,
  className = "",
  ...props
}: TextFieldProps) {
  const isInvalid = invalid ?? !!error;
  const input = (
    <input
      className={`${right ? "flex-1 " : ""}${inputClass(isInvalid)} ${className}`.trim()}
      {...props}
    />
  );

  return (
    <label className="flex flex-col gap-1 text-sm">
      {right ? <div className="flex gap-2">{input}{right}</div> : input}
      {error && <span className="px-1 text-red-500">{error}</span>}
      {message}
    </label>
  );
}
