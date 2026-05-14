// Web version frames + platform wrappers

// Device chrome: iPhone frame
const IPhoneFrame = ({ children, T }) => (
  <div style={{
    width: 360, padding: 12, background: '#1D1815', borderRadius: 48,
    boxShadow: '0 20px 60px rgba(58,36,24,.25), 0 4px 12px rgba(58,36,24,.1)',
    position: 'relative',
  }}>
    <div style={{
      width: 336, height: 680, borderRadius: 38, overflow: 'hidden',
      background: T.bg, position: 'relative',
    }}>
      {/* Dynamic island */}
      <div style={{
        position: 'absolute', top: 10, left: '50%', transform: 'translateX(-50%)',
        width: 105, height: 30, borderRadius: 18, background: '#000', zIndex: 100,
      }}/>
      {children}
    </div>
  </div>
);

// Android frame
const AndroidFrame = ({ children, T }) => (
  <div style={{
    width: 360, padding: 8, background: '#2B2B2F', borderRadius: 36,
    boxShadow: '0 20px 60px rgba(0,0,0,.2)',
    position: 'relative',
  }}>
    <div style={{
      width: 344, height: 688, borderRadius: 30, overflow: 'hidden',
      background: T.bg, position: 'relative',
    }}>
      {/* Camera punchhole */}
      <div style={{
        position: 'absolute', top: 10, left: '50%', transform: 'translateX(-50%)',
        width: 10, height: 10, borderRadius: 5, background: '#000', zIndex: 100,
      }}/>
      {children}
    </div>
  </div>
);

// Browser chrome
const BrowserFrame = ({ children, T, width = 1080, height = 700 }) => (
  <div style={{
    width, background: '#fff', borderRadius: 12, overflow: 'hidden',
    boxShadow: '0 10px 40px rgba(58,36,24,.12)',
    border: `1px solid ${T.primarySoft}77`,
  }}>
    <div style={{
      height: 40, background: '#F4F1EC', display: 'flex', alignItems: 'center',
      padding: '0 14px', gap: 10, borderBottom: `1px solid ${T.primarySoft}55`,
    }}>
      <div style={{ display: 'flex', gap: 6 }}>
        {['#FF5F57','#FEBC2E','#28C840'].map(c => (
          <div key={c} style={{ width: 11, height: 11, borderRadius: 6, background: c }}/>
        ))}
      </div>
      <div style={{
        flex: 1, maxWidth: 500, margin: '0 auto', height: 26, background: '#fff',
        borderRadius: 6, display: 'flex', alignItems: 'center', padding: '0 10px',
        fontSize: 11, color: T.inkSoft, gap: 6,
        border: `1px solid ${T.primarySoft}66`,
      }}>
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="1.2"><path d="M2 5 v3 h6 V5 M5 6 V1 M3 3 l2-2 l2 2" strokeLinecap="round"/></svg>
        <span style={{ fontWeight: 600, color: T.ink }}>lucky-map.kr</span>
        <span>/map</span>
      </div>
      <div style={{ width: 40 }}/>
    </div>
    <div style={{ height, overflow: 'hidden', background: T.bg }}>{children}</div>
  </div>
);

// ─── WEB MAP VIEW ───────────────────────────────────────────
const WebMapView = ({ T, mapMode = 'pins', screen = 'map' }) => {
  const [sel, setSel] = React.useState('s7');
  return (
    <div style={{ height: '100%', display: 'flex', fontFamily: 'Pretendard, -apple-system, sans-serif' }}>
      {/* Sidebar */}
      <div style={{
        width: 340, background: '#fff', borderRight: `1px solid ${T.primarySoft}55`,
        display: 'flex', flexDirection: 'column', overflow: 'hidden',
      }}>
        {/* Brand */}
        <div style={{ padding: '16px 18px 12px', display: 'flex', alignItems: 'center', gap: 10 }}>
          <Mascot size={36}/>
          <div>
            <div style={{ fontSize: 16, fontWeight: 900, color: T.ink, letterSpacing: -0.3 }}>복권지도</div>
            <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>Lucky Map · AI</div>
          </div>
          <div style={{ flex: 1 }}/>
          <div style={{
            width: 28, height: 28, borderRadius: 14, background: T.primarySoft,
            color: T.primary, fontSize: 12, fontWeight: 800,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>소</div>
        </div>

        {/* Search */}
        <div style={{ padding: '0 18px' }}>
          <div style={{
            background: T.bg, borderRadius: 10, padding: '10px 12px',
            display: 'flex', alignItems: 'center', gap: 8,
          }}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke={T.inkSoft} strokeWidth="1.8"><circle cx="6" cy="6" r="4.5"/><path d="M9.5 9.5L12 12" strokeLinecap="round"/></svg>
            <input readOnly value="역삼동 명당 찾기" style={{
              flex: 1, border: 'none', background: 'transparent', outline: 'none',
              fontSize: 12, color: T.ink, fontWeight: 500,
            }}/>
          </div>
        </div>

        {/* Mode toggle */}
        <div style={{ padding: '12px 18px' }}>
          <div style={{ display: 'flex', background: T.bg, borderRadius: 10, padding: 3 }}>
            {[{id:'pins',l:'📍 판매점'},{id:'heat',l:'🔥 당첨 히트맵'}].map(m => (
              <div key={m.id} style={{
                flex: 1, padding: '8px 0', textAlign: 'center', fontSize: 11, fontWeight: 700,
                background: mapMode === m.id ? '#fff' : 'transparent',
                color: mapMode === m.id ? T.primary : T.inkSoft,
                borderRadius: 8, cursor: 'pointer',
                boxShadow: mapMode === m.id ? '0 1px 3px rgba(0,0,0,.08)' : 'none',
              }}>{m.l}</div>
            ))}
          </div>
        </div>

        {/* Filter chips */}
        <div style={{ padding: '0 18px 12px', display: 'flex', gap: 5, flexWrap: 'wrap' }}>
          {['🎱 로또','💰 연금','🎫 스피또','🔥 HOT'].map(f => (
            <div key={f} style={{
              padding: '5px 10px', borderRadius: 999, fontSize: 10, fontWeight: 600,
              background: T.bg, color: T.inkSoft, border: `1px solid ${T.primarySoft}`,
            }}>{f}</div>
          ))}
        </div>

        {/* Store list */}
        <div style={{ flex: 1, overflow: 'auto', padding: '0 10px' }}>
          <div style={{ padding: '0 8px 6px', fontSize: 10, fontWeight: 700, color: T.inkSoft }}>
            내 주변 {window.STORES.length}개 판매점
          </div>
          {window.STORES.slice().sort((a,b) => b.lucky - a.lucky).map(s => (
            <div key={s.id} onClick={() => setSel(s.id)} style={{
              padding: 10, borderRadius: 10, marginBottom: 4, cursor: 'pointer',
              background: sel === s.id ? T.primarySoft + '55' : 'transparent',
              display: 'flex', gap: 10, alignItems: 'center',
            }}>
              <div style={{
                width: 36, height: 36, borderRadius: 10,
                background: s.hot ? T.hot : s.lucky >= 8 ? T.primary : s.lucky >= 4 ? T.accent : T.cool,
                color: '#fff', fontSize: 13, fontWeight: 900,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0,
              }}>{s.lucky}</div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: T.ink, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{s.name}</span>
                  {s.hot && <span style={{ fontSize: 9, color: T.hot, fontWeight: 800 }}>HOT</span>}
                </div>
                <div style={{ fontSize: 10, color: T.inkSoft, marginTop: 2 }}>
                  <Stars rating={s.rating} size={8}/> <span style={{ marginLeft: 2 }}>{s.rating} · {s.distance}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Map area */}
      <div style={{ flex: 1, position: 'relative', background: T.bg }}>
        <div style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}>
          <div style={{ transform: 'scale(1.6)', transformOrigin: 'center center',
            width: 360, height: 560, position: 'absolute',
            top: '50%', left: '50%', marginLeft: -180, marginTop: -280 }}>
            <MapBackdrop width={360} height={560} mode={mapMode} T={T}/>
            {mapMode === 'pins' && window.STORES.map(s => (
              <StorePin key={s.id} store={s} T={T} selected={sel === s.id} onClick={() => setSel(s.id)}/>
            ))}
          </div>
        </div>

        {/* Top-right AI chip */}
        <div style={{
          position: 'absolute', top: 16, right: 16, zIndex: 20,
          display: 'flex', gap: 10,
        }}>
          <div style={{
            background: '#fff', padding: '8px 14px', borderRadius: 999,
            display: 'flex', alignItems: 'center', gap: 8,
            boxShadow: '0 4px 14px rgba(0,0,0,.1)',
            fontSize: 12, fontWeight: 700, color: T.ink,
          }}>
            <span style={{ width: 8, height: 8, borderRadius: 4, background: T.mint }}/>
            내 주변 명당 <b style={{ color: T.primary }}>9곳</b>
          </div>
          <button style={{
            background: `linear-gradient(135deg, ${T.primary}, ${T.pink})`,
            color: '#fff', border: 'none', padding: '8px 16px', borderRadius: 999,
            fontSize: 12, fontWeight: 800, cursor: 'pointer',
            boxShadow: `0 4px 14px ${T.primary}44`,
            display: 'flex', alignItems: 'center', gap: 6,
          }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="#fff"><path d="M12 3 L14 9 L20 10 L15 14 L17 20 L12 17 L7 20 L9 14 L4 10 L10 9 Z"/></svg>
            AI로 번호 뽑기
          </button>
        </div>

        {/* Map control dock */}
        <div style={{
          position: 'absolute', right: 16, bottom: 20,
          display: 'flex', flexDirection: 'column', gap: 6,
        }}>
          {['＋','−','⌖'].map(k => (
            <button key={k} style={{
              width: 36, height: 36, borderRadius: 10, border: 'none',
              background: '#fff', color: T.ink, fontSize: 14, fontWeight: 700,
              boxShadow: '0 2px 8px rgba(0,0,0,.1)', cursor: 'pointer',
            }}>{k}</button>
          ))}
        </div>

        {/* Selected store panel */}
        {sel && mapMode === 'pins' && (() => {
          const s = window.STORES.find(x => x.id === sel);
          return (
            <div style={{
              position: 'absolute', left: 16, bottom: 20, width: 360,
              background: '#fff', borderRadius: 18, padding: 16,
              boxShadow: '0 12px 36px rgba(0,0,0,.14)',
              zIndex: 20,
            }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
                <div style={{
                  width: 54, height: 54, borderRadius: 14,
                  background: `linear-gradient(135deg, ${T.primarySoft}, ${T.accent}66)`,
                  display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 26,
                }}>🏪</div>
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                    <span style={{ fontSize: 16, fontWeight: 900, color: T.ink }}>{s.name}</span>
                    {s.hot && <Chip color={T.hot} text="#fff">🔥 HOT</Chip>}
                  </div>
                  <div style={{ fontSize: 11, color: T.inkSoft, marginTop: 3 }}>
                    <Stars rating={s.rating} size={10}/>
                    <span style={{ marginLeft: 4 }}>{s.rating} ({s.reviews})</span>
                    <span style={{ margin: '0 6px' }}>·</span>
                    {s.addr}
                  </div>
                </div>
                <button style={{
                  background: T.primary, color: '#fff', border: 'none',
                  padding: '10px 16px', borderRadius: 10, fontSize: 12, fontWeight: 700, cursor: 'pointer',
                }}>길찾기 →</button>
              </div>
              <div style={{
                marginTop: 12, display: 'flex', gap: 10,
              }}>
                {[
                  ['1등 배출', s.lucky + '회', T.primary],
                  ['최근 당첨', s.recent, T.ink],
                  ['거리', s.distance, T.ink],
                ].map(([l,v,c], i) => (
                  <div key={i} style={{ flex: 1, padding: 10, background: T.bg, borderRadius: 10 }}>
                    <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 600 }}>{l}</div>
                    <div style={{ fontSize: 14, fontWeight: 800, color: c }}>{v}</div>
                  </div>
                ))}
              </div>
            </div>
          );
        })()}

        {/* Legend */}
        {mapMode === 'heat' && (
          <div style={{
            position: 'absolute', left: 16, bottom: 20,
            background: '#fff', padding: 14, borderRadius: 12,
            boxShadow: '0 4px 14px rgba(0,0,0,.08)',
          }}>
            <div style={{ fontSize: 11, fontWeight: 800, color: T.ink, marginBottom: 8 }}>당첨 밀도</div>
            <div style={{
              width: 200, height: 10, borderRadius: 5,
              background: `linear-gradient(90deg, ${T.mint}88, ${T.accent}aa, ${T.primary}cc, ${T.hot})`,
            }}/>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4, fontSize: 9, color: T.inkSoft }}>
              <span>낮음</span><span>최고</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

Object.assign(window, { IPhoneFrame, AndroidFrame, BrowserFrame, WebMapView });
