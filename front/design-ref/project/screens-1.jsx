// Five screens. Each takes { T, device, mapMode, aiNumbers } props.
// Screens are layout-parametric on device (web = wider + sidebar, mobile = stacked).

const PhoneFrame = ({ children, T, platform = 'ios', showStatus = true, statusDark = false }) => {
  const w = platform === 'ios' ? 320 : 320;
  const h = 640;
  return (
    <div style={{
      width: w, height: h, background: T.bg,
      display: 'flex', flexDirection: 'column',
      overflow: 'hidden', position: 'relative',
      fontFamily: 'Pretendard, -apple-system, BlinkMacSystemFont, sans-serif',
    }}>
      {showStatus && (platform === 'ios' ? <IOSStatus dark={statusDark}/> : <AndroidStatus dark={statusDark}/>)}
      <div style={{ flex: 1, overflow: 'hidden', position: 'relative' }}>
        {children}
      </div>
    </div>
  );
};

// Bottom tab nav
const BottomTab = ({ T, active = 'map', platform = 'ios' }) => {
  const tabs = [
    { id: 'map', label: '지도', icon: 'M12 2 L4 8 v12 h16 V8 Z M9 20 v-6 h6 v6' },
    { id: 'winners', label: '당첨번호', icon: 'M12 2 a10 10 0 1 0 0 20 a10 10 0 0 0 0 -20 M8 12 l3 3 l5 -6' },
    { id: 'ai', label: 'AI 추첨', icon: 'M12 3 L14 9 L20 10 L15 14 L17 20 L12 17 L7 20 L9 14 L4 10 L10 9 Z', accent: true },
    { id: 'saved', label: '보관함', icon: 'M5 3 h14 v18 l-7-4 l-7 4 Z' },
    { id: 'stats', label: '랭킹', icon: 'M4 20 h4 v-6 h-4 Z M10 20 h4 v-12 h-4 Z M16 20 h4 v-9 h-4 Z' },
  ];
  const h = platform === 'ios' ? 78 : 64;
  return (
    <div style={{
      position: 'absolute', bottom: 0, left: 0, right: 0, height: h,
      background: '#FFFFFFF2', backdropFilter: 'blur(20px)',
      borderTop: `1px solid ${T.primarySoft}88`,
      display: 'flex', paddingBottom: platform === 'ios' ? 20 : 0,
      zIndex: 30,
    }}>
      {tabs.map(t => (
        <button key={t.id} style={{
          flex: 1, border: 'none', background: 'transparent', cursor: 'pointer',
          display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
          gap: 3, padding: 6, color: active === t.id ? T.primary : T.inkSoft,
          position: 'relative',
        }}>
          {t.accent ? (
            <div style={{
              width: 42, height: 42, borderRadius: 21,
              background: `linear-gradient(135deg, ${T.primary}, ${T.pink})`,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              boxShadow: `0 4px 12px ${T.primary}55`,
              marginTop: -14,
            }}>
              <svg width="22" height="22" viewBox="0 0 24 24" fill="#fff"><path d={t.icon}/></svg>
            </div>
          ) : (
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round"><path d={t.icon}/></svg>
          )}
          <span style={{ fontSize: 10, fontWeight: 600 }}>{t.label}</span>
        </button>
      ))}
    </div>
  );
};

// ─── 1. MAP SCREEN ────────────────────────────────────────────
const MapScreen = ({ T, device = 'mobile', mapMode = 'pins', onStorePick, selectedStore }) => {
  const [selected, setSelected] = React.useState(selectedStore || 's7');
  const curStore = window.STORES.find(s => s.id === selected);

  React.useEffect(() => { if (selectedStore) setSelected(selectedStore); }, [selectedStore]);

  const mapWidth = 360;
  const mapHeight = 560;

  return (
    <div style={{ height: '100%', width: '100%', position: 'relative', background: T.bg }}>
      {/* Search header */}
      <div style={{
        position: 'absolute', top: 12, left: 12, right: 12, zIndex: 25,
      }}>
        <div style={{
          background: '#fff', borderRadius: 14, padding: '11px 14px',
          display: 'flex', alignItems: 'center', gap: 10,
          boxShadow: '0 4px 14px rgba(0,0,0,.08)',
        }}>
          <Mascot size={26}/>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>안녕 소희야! 오늘의 행운 스팟 🍀</div>
            <div style={{ fontSize: 13, color: T.ink, fontWeight: 700 }}>서울 강남구 역삼동</div>
          </div>
          <button style={{
            width: 32, height: 32, borderRadius: 16, border: 'none',
            background: T.primarySoft, color: T.primary,
            display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer',
          }}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="7" cy="7" r="5"/><path d="M11 11l3 3" strokeLinecap="round"/></svg>
          </button>
        </div>
        {/* Mode toggle */}
        <div style={{
          marginTop: 10, display: 'flex', gap: 6, alignItems: 'center',
        }}>
          <div style={{
            display: 'flex', background: '#fff', borderRadius: 999, padding: 3,
            boxShadow: '0 2px 8px rgba(0,0,0,.06)',
          }}>
            {[{id:'pins',l:'판매점',i:'📍'},{id:'heat',l:'히트맵',i:'🔥'}].map(m => (
              <div key={m.id} style={{
                padding: '6px 14px', borderRadius: 999, fontSize: 12, fontWeight: 700,
                background: mapMode === m.id ? T.primary : 'transparent',
                color: mapMode === m.id ? '#fff' : T.inkSoft,
                transition: 'all .2s',
              }}>{m.i} {m.l}</div>
            ))}
          </div>
          <div style={{ flex: 1 }}/>
          <div style={{
            background: '#fff', padding: '6px 10px', borderRadius: 999,
            fontSize: 11, fontWeight: 600, color: T.inkSoft,
            boxShadow: '0 2px 8px rgba(0,0,0,.06)',
            display: 'flex', alignItems: 'center', gap: 4,
          }}>
            <span style={{ width: 6, height: 6, borderRadius: 3, background: T.mint }}/>
            반경 2km
          </div>
        </div>
      </div>

      {/* Map canvas */}
      <div style={{
        position: 'absolute', top: 120, left: 0, right: 0, bottom: 0,
        overflow: 'hidden',
      }}>
        <div style={{ position: 'relative', width: mapWidth, height: mapHeight, marginLeft: -20 }}>
          <MapBackdrop width={mapWidth} height={mapHeight} mode={mapMode} T={T}/>
          {mapMode === 'pins' && window.STORES.map(s => (
            <StorePin key={s.id} store={s} T={T}
              selected={selected === s.id}
              onClick={() => { setSelected(s.id); onStorePick?.(s.id); }}/>
          ))}
          {/* User location */}
          <div style={{
            position: 'absolute', left: 180 - 10, top: 260 - 10,
            width: 20, height: 20, borderRadius: 10,
            background: T.cool, border: '3px solid #fff',
            boxShadow: `0 0 0 8px ${T.cool}33`,
          }}/>
        </div>
      </div>

      {/* Heat legend */}
      {mapMode === 'heat' && (
        <div style={{
          position: 'absolute', top: 180, right: 12, zIndex: 15,
          background: '#fff', padding: 10, borderRadius: 10,
          boxShadow: '0 2px 8px rgba(0,0,0,.08)',
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: T.ink, marginBottom: 6 }}>당첨 밀도</div>
          {[['낮음',T.mint],['보통',T.accent],['높음',T.primary],['최고',T.hot]].map(([l,c]) => (
            <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, color: T.inkSoft, marginTop: 3 }}>
              <div style={{ width: 12, height: 12, borderRadius: 2, background: c, opacity: 0.5 }}/>
              {l}
            </div>
          ))}
        </div>
      )}

      {/* Store card (peek) */}
      {mapMode === 'pins' && curStore && (
        <div style={{
          position: 'absolute', left: 12, right: 12, bottom: 92,
          background: '#fff', borderRadius: 18, padding: 14,
          boxShadow: '0 8px 24px rgba(0,0,0,.12)',
          zIndex: 20,
        }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
            <div style={{
              width: 48, height: 48, borderRadius: 12,
              background: `linear-gradient(135deg, ${T.primarySoft}, ${T.accent}44)`,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 24, flexShrink: 0,
            }}>🏪</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 14, fontWeight: 800, color: T.ink }}>{curStore.name}</span>
                {curStore.hot && <Chip color={T.hot} text="#fff" size="sm">🔥 HOT</Chip>}
              </div>
              <div style={{ fontSize: 11, color: T.inkSoft, marginTop: 2 }}>
                <Stars rating={curStore.rating} size={10}/>
                <span style={{ marginLeft: 4 }}>{curStore.rating} ({curStore.reviews}) · {curStore.distance}</span>
              </div>
            </div>
          </div>
          <div style={{
            marginTop: 10, padding: 10, borderRadius: 10,
            background: `${T.primarySoft}66`,
            display: 'flex', gap: 12, alignItems: 'center',
          }}>
            <div>
              <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 600 }}>1등 배출</div>
              <div style={{ fontSize: 18, fontWeight: 900, color: T.primary, lineHeight: 1 }}>
                {curStore.lucky}<span style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>회</span>
              </div>
            </div>
            <div style={{ width: 1, height: 24, background: T.primarySoft }}/>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 600 }}>최근 당첨</div>
              <div style={{ fontSize: 12, fontWeight: 700, color: T.ink }}>{curStore.recent}</div>
            </div>
            <button style={{
              background: T.primary, color: '#fff', border: 'none',
              padding: '8px 14px', borderRadius: 12, fontSize: 12, fontWeight: 700,
              cursor: 'pointer', whiteSpace: 'nowrap',
            }}>길찾기 →</button>
          </div>
        </div>
      )}

      <BottomTab T={T} active="map" platform={device === 'android' ? 'android' : 'ios'}/>
    </div>
  );
};

// ─── 2. STORE DETAIL ──────────────────────────────────────────
const StoreDetail = ({ T, storeId = 's7' }) => {
  const s = window.STORES.find(x => x.id === storeId);
  const [tab, setTab] = React.useState('history');
  return (
    <div style={{ height: '100%', overflow: 'auto', background: T.bg }}>
      {/* Hero */}
      <div style={{
        background: `linear-gradient(160deg, ${T.primary} 0%, ${T.pink} 100%)`,
        padding: '14px 16px 58px', color: '#fff', position: 'relative',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button style={{
            width: 32, height: 32, borderRadius: 16, border: 'none',
            background: 'rgba(255,255,255,.25)', color: '#fff', cursor: 'pointer',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>←</button>
          <div style={{ flex: 1 }}/>
          <button style={{
            width: 32, height: 32, borderRadius: 16, border: 'none',
            background: 'rgba(255,255,255,.25)', color: '#fff', cursor: 'pointer',
          }}>♡</button>
          <button style={{
            width: 32, height: 32, borderRadius: 16, border: 'none',
            background: 'rgba(255,255,255,.25)', color: '#fff', cursor: 'pointer',
          }}>↗</button>
        </div>
        {/* Floating mascot */}
        <div style={{ position: 'absolute', top: 48, right: 16 }}>
          <Mascot size={56}/>
        </div>
        <div style={{ marginTop: 18 }}>
          {s.hot && <Chip color="rgba(255,255,255,.3)" text="#fff" size="sm">🔥 이번 달 HOT 명당</Chip>}
          <div style={{ fontSize: 22, fontWeight: 900, marginTop: 8, letterSpacing: -0.5 }}>{s.name}</div>
          <div style={{ fontSize: 12, opacity: 0.9, marginTop: 4 }}>{s.addr}</div>
        </div>
      </div>

      {/* Stats card */}
      <div style={{
        margin: '-44px 16px 0', background: '#fff', borderRadius: 18,
        padding: 16, boxShadow: '0 8px 24px rgba(0,0,0,.08)', position: 'relative', zIndex: 2,
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-around', textAlign: 'center' }}>
          {[
            { n: s.lucky, l: '1등 배출', c: T.primary },
            { n: Math.floor(s.lucky * 2.3), l: '2등 배출', c: T.accent },
            { n: s.reviews, l: '방문 리뷰', c: T.mint },
          ].map((x, i) => (
            <div key={i}>
              <div style={{ fontSize: 24, fontWeight: 900, color: x.c, fontVariantNumeric: 'tabular-nums' }}>{x.n}</div>
              <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600, marginTop: 2 }}>{x.l}</div>
            </div>
          ))}
        </div>
        <div style={{ marginTop: 14, display: 'flex', gap: 8 }}>
          <button style={{
            flex: 1, background: T.primary, color: '#fff', border: 'none',
            padding: '11px 0', borderRadius: 12, fontSize: 13, fontWeight: 700, cursor: 'pointer',
          }}>길찾기</button>
          <button style={{
            flex: 1, background: T.primarySoft, color: T.primary, border: 'none',
            padding: '11px 0', borderRadius: 12, fontSize: 13, fontWeight: 700, cursor: 'pointer',
          }}>전화하기</button>
        </div>
      </div>

      {/* Sold games */}
      <div style={{ padding: '18px 16px 0' }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: T.inkSoft, marginBottom: 8 }}>판매 복권</div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {s.games.map(g => {
            const game = window.GAMES.find(x => x.id === g);
            return <Chip key={g} color={T.primarySoft} text={T.ink}>{game.emoji} {game.name}</Chip>;
          })}
        </div>
      </div>

      {/* Tabs */}
      <div style={{
        margin: '18px 16px 0', display: 'flex', gap: 4,
        background: T.primarySoft + '55', padding: 4, borderRadius: 12,
      }}>
        {[['history','당첨 이력'],['reviews','리뷰'],['info','정보']].map(([id,l]) => (
          <button key={id} onClick={() => setTab(id)} style={{
            flex: 1, padding: '8px 0', border: 'none', borderRadius: 9,
            background: tab === id ? '#fff' : 'transparent',
            color: tab === id ? T.primary : T.inkSoft,
            fontSize: 12, fontWeight: 700, cursor: 'pointer',
            boxShadow: tab === id ? '0 1px 3px rgba(0,0,0,.08)' : 'none',
          }}>{l}</button>
        ))}
      </div>

      {/* Tab content */}
      <div style={{ padding: '14px 16px 100px' }}>
        {tab === 'history' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {[
              { round: 1210, date: '2026.03.21', rank: 1, prize: '22억 4,812만', game: '로또' },
              { round: 1204, date: '2026.02.07', rank: 1, prize: '18억 7,320만', game: '로또' },
              { round: 1198, date: '2025.12.27', rank: 2, prize: '5,820만', game: '로또' },
              { round: 1194, date: '2025.11.29', rank: 1, prize: '27억 900만', game: '로또' },
            ].map((h,i) => (
              <div key={i} style={{
                background: '#fff', padding: 12, borderRadius: 12,
                display: 'flex', alignItems: 'center', gap: 10,
                boxShadow: '0 1px 4px rgba(0,0,0,.04)',
              }}>
                <div style={{
                  width: 44, height: 44, borderRadius: 10,
                  background: h.rank === 1 ? `linear-gradient(135deg, ${T.accent}, ${T.primary})` : T.primarySoft,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 11, fontWeight: 900, color: h.rank === 1 ? '#fff' : T.primary,
                  flexShrink: 0,
                }}>{h.rank}등</div>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 12, color: T.inkSoft, fontWeight: 600 }}>{h.round}회 · {h.game}</div>
                  <div style={{ fontSize: 14, fontWeight: 800, color: T.ink }}>{h.prize}원</div>
                </div>
                <div style={{ fontSize: 10, color: T.inkSoft }}>{h.date}</div>
              </div>
            ))}
          </div>
        )}
        {tab === 'reviews' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {[
              { u: '김○○', r: 5, d: '2일 전', t: '여기서 5등 당첨됐어요! 사장님도 친절하심 🍀' },
              { u: '이○○', r: 5, d: '1주 전', t: '명당 소문나서 줄 섭니다. 그래도 가치 있어요' },
              { u: '박○○', r: 4, d: '2주 전', t: '주차가 조금 불편한데 자동번호 잘 뽑아주세요' },
            ].map((r,i) => (
              <div key={i} style={{ background: '#fff', padding: 12, borderRadius: 12 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 28, height: 28, borderRadius: 14, background: T.lavender,
                    display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 11, fontWeight: 700 }}>{r.u[0]}</div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 12, fontWeight: 700, color: T.ink }}>{r.u}</div>
                    <div style={{ fontSize: 10, color: T.inkSoft }}><Stars rating={r.r} size={9}/> · {r.d}</div>
                  </div>
                </div>
                <div style={{ fontSize: 12, color: T.ink, marginTop: 8, lineHeight: 1.5 }}>{r.t}</div>
              </div>
            ))}
          </div>
        )}
        {tab === 'info' && (
          <div style={{ background: '#fff', borderRadius: 12, padding: 14 }}>
            {[['운영시간','매일 07:00 - 23:00'],['주차','불가 (인근 공영주차장)'],['전화','02-123-4567'],['특이사항','자동 추첨 가능']].map(([k,v]) => (
              <div key={k} style={{ display: 'flex', gap: 12, padding: '8px 0', borderBottom: `1px solid ${T.primarySoft}44` }}>
                <div style={{ width: 70, fontSize: 11, color: T.inkSoft, fontWeight: 600 }}>{k}</div>
                <div style={{ fontSize: 12, color: T.ink, flex: 1 }}>{v}</div>
              </div>
            ))}
          </div>
        )}
      </div>
      <BottomTab T={T} active="map"/>
    </div>
  );
};

Object.assign(window, { PhoneFrame, BottomTab, MapScreen, StoreDetail });
