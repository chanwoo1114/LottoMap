// Shared UI atoms — map rendering, number balls, pins, etc.

const BG_TONES = {
  peach: '#FFF5EC', mint: '#EEF8F3', lavender: '#F4F0FA',
};

// Lucky mascot — a simple friendly character (clover/cat hybrid)
const Mascot = ({ size = 40, mood = 'happy' }) => (
  <svg width={size} height={size} viewBox="0 0 60 60" style={{ display: 'block' }}>
    <defs>
      <radialGradient id={`mg-${size}`} cx="50%" cy="40%" r="55%">
        <stop offset="0%" stopColor="#F6F1E4" />
        <stop offset="100%" stopColor="#E0D6BE" />
      </radialGradient>
    </defs>
    {/* ears */}
    <path d="M14 18 L10 6 L22 12 Z" fill="#E0D6BE"/>
    <path d="M46 18 L50 6 L38 12 Z" fill="#E0D6BE"/>
    <path d="M14 18 L12 10 L19 13 Z" fill="#0E9488"/>
    <path d="M46 18 L48 10 L41 13 Z" fill="#0E9488"/>
    {/* face */}
    <circle cx="30" cy="33" r="20" fill={`url(#mg-${size})`} stroke="#0E9488" strokeWidth="1.5"/>
    {/* cheeks */}
    <circle cx="18" cy="36" r="3.5" fill="#F5B93D" opacity="0.55"/>
    <circle cx="42" cy="36" r="3.5" fill="#F5B93D" opacity="0.55"/>
    {/* eyes */}
    {mood === 'happy' ? (
      <>
        <path d="M22 30 Q25 27 28 30" fill="none" stroke="#3A2418" strokeWidth="2" strokeLinecap="round"/>
        <path d="M32 30 Q35 27 38 30" fill="none" stroke="#3A2418" strokeWidth="2" strokeLinecap="round"/>
      </>
    ) : (
      <>
        <circle cx="25" cy="30" r="2" fill="#3A2418"/>
        <circle cx="35" cy="30" r="2" fill="#3A2418"/>
      </>
    )}
    {/* mouth */}
    <path d="M27 39 Q30 42 33 39" fill="none" stroke="#3A2418" strokeWidth="1.8" strokeLinecap="round"/>
    {/* clover on head */}
    <g transform="translate(30 8)">
      <circle cx="-3" cy="0" r="2.5" fill="#9BD4B7"/>
      <circle cx="3" cy="0" r="2.5" fill="#9BD4B7"/>
      <circle cx="0" cy="-3" r="2.5" fill="#9BD4B7"/>
      <circle cx="0" cy="3" r="2.5" fill="#9BD4B7"/>
      <circle cx="0" cy="0" r="1.5" fill="#5DC49A"/>
    </g>
  </svg>
);

// Lotto number ball
const Ball = ({ n, size = 32, highlight = false }) => {
  const bg = window.numColor(n);
  return (
    <div style={{
      width: size, height: size, borderRadius: size / 2,
      background: `radial-gradient(circle at 35% 30%, rgba(255,255,255,.8), ${bg} 70%)`,
      color: '#fff', fontWeight: 800, fontSize: size * 0.44,
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      textShadow: '0 1px 2px rgba(0,0,0,.25)',
      boxShadow: highlight
        ? `0 0 0 3px #fff, 0 0 0 5px ${bg}, 0 4px 14px ${bg}66`
        : '0 2px 6px rgba(0,0,0,.12), inset 0 -3px 6px rgba(0,0,0,.1)',
      flexShrink: 0,
      fontVariantNumeric: 'tabular-nums',
    }}>{n}</div>
  );
};

// Faux Naver-map tile — hand-drawn road/park SVG background
const MapBackdrop = ({ width, height, mode = 'pins', T }) => {
  const heatColors = [T.mint, T.accent, T.primary, T.hot];
  return (
    <svg viewBox={`0 0 ${width} ${height}`} width={width} height={height}
      style={{ display: 'block', position: 'absolute', inset: 0 }}>
      {/* water / park base */}
      <rect width={width} height={height} fill="#EEF2E8" />
      {/* blocks */}
      <g fill="#F7F2EA" stroke="#E4DCCB" strokeWidth="0.5">
        <rect x="10" y="20" width="100" height="80" rx="4"/>
        <rect x="120" y="20" width="110" height="60" rx="4"/>
        <rect x="240" y="30" width="110" height="90" rx="4"/>
        <rect x="10" y="110" width="140" height="100" rx="4"/>
        <rect x="160" y="90" width="80" height="120" rx="4"/>
        <rect x="250" y="130" width="100" height="100" rx="4"/>
        <rect x="10" y="220" width="120" height="110" rx="4"/>
        <rect x="140" y="220" width="110" height="90" rx="4"/>
        <rect x="260" y="240" width="90" height="110" rx="4"/>
        <rect x="10" y="340" width="100" height="90" rx="4"/>
        <rect x="120" y="320" width="120" height="100" rx="4"/>
        <rect x="250" y="360" width="100" height="80" rx="4"/>
        <rect x="10" y="440" width="150" height="110" rx="4"/>
        <rect x="170" y="430" width="100" height="120" rx="4"/>
        <rect x="280" y="450" width="70" height="100" rx="4"/>
      </g>
      {/* park (green) */}
      <path d="M 30 400 Q 80 380 110 420 Q 95 450 50 440 Z" fill="#D8E8C4" />
      <circle cx="310" cy="80" r="18" fill="#D8E8C4"/>
      {/* river */}
      <path d="M -20 170 Q 100 140 200 180 Q 280 210 400 170 L 400 200 Q 280 240 200 205 Q 100 170 -20 200 Z" fill="#CFE4F0"/>
      {/* main roads */}
      <g stroke="#FFFFFF" strokeWidth="7" fill="none" strokeLinecap="round">
        <line x1="0" y1="105" x2={width} y2="85"/>
        <line x1="0" y1="325" x2={width} y2="315"/>
        <line x1="115" y1="0" x2="125" y2={height}/>
        <line x1="245" y1="0" x2="255" y2={height}/>
      </g>
      <g stroke="#E8E0D0" strokeWidth="1.5" fill="none">
        <line x1="0" y1="215" x2={width} y2="210"/>
        <line x1="60" y1="0" x2="60" y2={height}/>
        <line x1="185" y1="0" x2="190" y2={height}/>
        <line x1="310" y1="0" x2="315" y2={height}/>
      </g>
      {/* heatmap overlay */}
      {mode === 'heat' && (
        <g>
          {window.STORES.map((s) => (
            <circle key={s.id} cx={s.x} cy={s.y} r={s.lucky * 4 + 18}
              fill={heatColors[Math.min(3, Math.floor(s.lucky / 4))]} opacity="0.35"
              style={{ mixBlendMode: 'multiply' }}/>
          ))}
        </g>
      )}
      {/* labels */}
      <text x="60" y="55" fontSize="9" fill="#A89C82" fontWeight="500">역삼동</text>
      <text x="200" y="160" fontSize="9" fill="#A89C82" fontWeight="500">삼성동</text>
      <text x="60" y="280" fontSize="9" fill="#A89C82" fontWeight="500">서초동</text>
      <text x="280" y="300" fontSize="9" fill="#A89C82" fontWeight="500">대치동</text>
    </svg>
  );
};

// Pin with lucky count
const StorePin = ({ store, onClick, T, selected = false, compact = false }) => {
  const color = store.hot ? T.hot : (store.lucky >= 8 ? T.primary : store.lucky >= 4 ? T.accent : T.cool);
  const size = compact ? 26 : 34;
  return (
    <div onClick={onClick} style={{
      position: 'absolute', left: store.x - size/2, top: store.y - size,
      cursor: 'pointer', zIndex: selected ? 20 : (store.hot ? 10 : 5),
      transform: selected ? 'scale(1.15)' : 'scale(1)',
      transition: 'transform .15s',
    }}>
      <div style={{
        width: size, height: size, borderRadius: '50% 50% 50% 0',
        background: color, transform: 'rotate(-45deg)',
        boxShadow: `0 3px 8px ${color}66`,
        border: '2px solid #fff',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        <div style={{
          transform: 'rotate(45deg)', color: '#fff',
          fontSize: compact ? 10 : 12, fontWeight: 800,
          fontVariantNumeric: 'tabular-nums',
        }}>{store.lucky}</div>
      </div>
      {store.hot && !compact && (
        <div style={{
          position: 'absolute', top: -6, right: -6,
          background: '#FFCE54', color: '#3A2418',
          fontSize: 8, fontWeight: 800, padding: '1px 4px', borderRadius: 6,
          border: '1.5px solid #fff', whiteSpace: 'nowrap',
        }}>HOT</div>
      )}
    </div>
  );
};

// Pretty rating stars
const Stars = ({ rating, size = 12 }) => (
  <span style={{ display: 'inline-flex', gap: 1, alignItems: 'center' }}>
    {[0,1,2,3,4].map(i => (
      <svg key={i} width={size} height={size} viewBox="0 0 12 12" style={{ display: 'block' }}>
        <path d="M6 1l1.5 3 3.3.3-2.5 2.2.8 3.2L6 8.2 2.9 9.7l.8-3.2L1.2 4.3 4.5 4z"
          fill={i < Math.floor(rating) ? '#FFCE54' : '#E4DCCB'}/>
      </svg>
    ))}
  </span>
);

// Chip
const Chip = ({ children, color = '#C2E5E0', text = '#0F2E33', size = 'sm' }) => (
  <span style={{
    display: 'inline-flex', alignItems: 'center', gap: 4,
    background: color, color: text,
    padding: size === 'sm' ? '3px 8px' : '5px 10px',
    borderRadius: 999, fontSize: size === 'sm' ? 11 : 13, fontWeight: 600,
    whiteSpace: 'nowrap',
  }}>{children}</span>
);

// Status bar (iOS)
const IOSStatus = ({ dark = false }) => {
  const c = dark ? '#fff' : '#3A2418';
  return (
    <div style={{
      height: 44, display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: '0 24px', fontWeight: 700, color: c, fontSize: 15, fontVariantNumeric: 'tabular-nums',
      flexShrink: 0,
    }}>
      <span>9:41</span>
      <div style={{ display: 'flex', gap: 5, alignItems: 'center' }}>
        <svg width="16" height="10" viewBox="0 0 16 10"><path d="M1 8h2v1H1zM4 6h2v3H4zM7 4h2v5H7zM10 2h2v7h-2zM13 0h2v9h-2z" fill={c}/></svg>
        <svg width="14" height="10" viewBox="0 0 14 10"><path d="M7 2a7 7 0 0 0-7 7M7 2a7 7 0 0 1 7 7M7 5a4 4 0 0 0-4 4M7 5a4 4 0 0 1 4 4" stroke={c} strokeWidth="1" fill="none"/><circle cx="7" cy="9" r="0.8" fill={c}/></svg>
        <svg width="24" height="11" viewBox="0 0 24 11"><rect x="0.5" y="0.5" width="20" height="10" rx="2.5" stroke={c} fill="none"/><rect x="2" y="2" width="17" height="7" rx="1.5" fill={c}/><rect x="21" y="3.5" width="2" height="4" rx="0.5" fill={c} opacity="0.5"/></svg>
      </div>
    </div>
  );
};

// Android status bar
const AndroidStatus = ({ dark = false }) => {
  const c = dark ? '#fff' : '#3A2418';
  return (
    <div style={{
      height: 32, display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: '0 18px', color: c, fontSize: 13, fontWeight: 600, fontVariantNumeric: 'tabular-nums',
      flexShrink: 0,
    }}>
      <span>9:41</span>
      <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
        <svg width="12" height="10" viewBox="0 0 12 10"><path d="M6 1 Q10 4 11 8 L1 8 Q2 4 6 1Z" fill={c}/></svg>
        <svg width="12" height="10" viewBox="0 0 12 10"><path d="M0 8h2v1H0zM3 6h2v3H3zM6 4h2v5H6zM9 1h2v8H9z" fill={c}/></svg>
        <span style={{ fontSize: 11 }}>89%</span>
        <svg width="20" height="10" viewBox="0 0 20 10"><rect x="0" y="1" width="17" height="8" rx="1.5" fill="none" stroke={c}/><rect x="1.5" y="2.5" width="13" height="5" rx="0.5" fill={c}/><rect x="17.5" y="3.5" width="1.5" height="3" rx="0.3" fill={c}/></svg>
      </div>
    </div>
  );
};

Object.assign(window, { Mascot, Ball, MapBackdrop, StorePin, Stars, Chip, IOSStatus, AndroidStatus, BG_TONES });
