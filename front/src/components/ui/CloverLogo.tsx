export function CloverLogo({ size = 26 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden>
      <g fill="#16a34a">
        <path d="M12 12c0-2.5-1-4.5-3-4.5S6 9 6 11c0 1.7 1.3 3 3 3 1 0 2-.5 3-2z" />
        <path d="M12 12c0-2.5 1-4.5 3-4.5S18 9 18 11c0 1.7-1.3 3-3 3-1 0-2-.5-3-2z" />
        <path d="M12 12c-2.5 0-4.5 1-4.5 3S9 18 11 18c1.7 0 3-1.3 3-3 0-1-.5-2-2-3z" />
        <path d="M12 12c2.5 0 4.5 1 4.5 3S15 18 13 18c-1.7 0-3-1.3-3-3 0-1 .5-2 2-3z" />
      </g>
      <path d="M12 13.5c-.5 2-1.3 4-3 6.5" stroke="#15803d" strokeWidth="1.3" fill="none" strokeLinecap="round" />
    </svg>
  )
}