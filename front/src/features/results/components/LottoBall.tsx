
function ballColor(n: number): string {
  if (n <= 10) return '#fbc400'
  if (n <= 20) return '#69c8f2'
  if (n <= 30) return '#ff7272'
  if (n <= 40) return '#9ca3af'
  return '#b0d840'
}

export function LottoBall({ n, size=40 }: { n: number; size?: number }) {
  return (
    <span
      className='grid shrink-0 place-items-center rounded-full font-bold text-white'
      style={{ width: size, height: size, background: ballColor(n) }}
    >
      {n}
    </span>
  )
}