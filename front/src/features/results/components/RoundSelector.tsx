import { useEffect, useRef, useState } from 'react'

interface RoundSelectorProps {
  latest: number;
  value: number;
  onChange: (round: number) => void;
}

export function RoundSelector({ latest, value, onChange }: RoundSelectorProps) {
  const btnBase = 'rounded-lg border border-gray-200 bg-white hover:bg-gray-50'
  const arrowBtn = `grid h-9 w-9 place-items-center text-gray-500 disabled:opacity-40 ${btnBase}`

  const [open, setOpen] = useState(false)
  const [input, setInput] = useState('')
  const [err, setErr] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const WINDOW = 15
  const top = Math.min(latest, value + 2)
  const nearby = Array.from({ length: WINDOW }, (_, i) => top - i).filter((r) => r >= 1)

  const toggle = () => {
    if (!open) { setInput(''); setErr(false) }
    setOpen((v) => !v)
  }

  useEffect(() => {
    if (!open) return
    const t = setTimeout(() => inputRef.current?.focus(), 0)
    const onDown = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false) }
    document.addEventListener('mousedown', onDown)
    return () => { clearTimeout(t); document.removeEventListener('mousedown', onDown) }
  }, [open])

  const go = (r: number) => { onChange(r); setOpen(false) }

  const jump = () => {
    const v = parseInt(input, 10)
    if (!v || v < 1 || v > latest) { setErr(true); return }
    go(v)
  }

  return (
    <div className="flex items-center justify-center gap-2">
      <button
        className={arrowBtn}
        disabled={value >= latest}
        onClick={() => onChange(Math.min(latest, value + 1))}
      >
        ‹
      </button>

      <div ref={ref} className="relative">
        <button
          className={`flex min-w-[140px] items-center justify-center gap-2 px-4 py-2 font-bold text-gray-900 ${btnBase}`}
          onClick={toggle}
        >
          제 {value}회
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#888" strokeWidth="2"><path d="M6 9l6 6 6-6" /></svg>
        </button>

        {open && (
          <div className="absolute left-1/2 top-12 z-30 w-60 -translate-x-1/2 rounded-xl border border-gray-100 bg-white p-2 shadow-lg">
            <div className="flex gap-1.5 p-1">
              <input
                ref={inputRef}
                type="number" min={1} max={latest} value={input}
                onChange={(e) => { setInput(e.target.value); setErr(false) }}
                onKeyDown={(e) => { if (e.key === 'Enter') jump() }}
                placeholder="회차 번호"
                className="w-full rounded-lg border border-gray-200 px-3 py-2 text-sm focus:border-accent focus:outline-none"
              />
              <button
                onClick={jump}
                className="shrink-0 rounded-lg bg-accent px-3 text-sm font-semibold text-white hover:opacity-90"
              >
                이동
              </button>
            </div>
            {err && <p className="px-2 text-xs text-red-500">1 ~ {latest} 회차만 가능해요</p>}

            {value !== latest && (
              <button
                onClick={() => go(latest)}
                className="mt-1 block w-full rounded-lg px-3 py-2 text-left text-sm font-semibold text-accent hover:bg-gray-50">
                ↑ 최신 제 {latest}회로
              </button>
            )}

            <div className="max-h-52 overflow-y-auto">
              {nearby.map((r) => (
                <button
                  key={r}
                  onClick={() => go(r)}
                  className={`block w-full rounded-lg px-3 py-2 text-left text-sm hover:bg-gray-50 ${r === value ? 'font-bold text-accent' : 'text-gray-700'}`}
                >
                  제 {r}회{r === latest ? ' (최신)' : ''}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      <button
        className={arrowBtn} disabled={value <= 1}
        onClick={() => onChange(Math.max(1, value - 1))}>›
      </button>
    </div>
  )
}