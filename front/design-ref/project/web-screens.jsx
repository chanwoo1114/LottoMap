// Richer web-only views for all 5 screens

// Shared web shell: top nav + optional sidebar
const WebShell = ({ T, active = 'map', children, showSidebar = true }) => {
  const navItems = [
    { id: 'map', label: '명당 지도', icon: '📍' },
    { id: 'winners', label: '당첨번호', icon: '🎱' },
    { id: 'ai', label: 'AI 번호 추첨', icon: '✨' },
    { id: 'saved', label: '내 번호', icon: '🎫' },
    { id: 'stats', label: '지역 랭킹', icon: '🏆' },
  ];
  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: T.bg, fontFamily: 'Pretendard, -apple-system, sans-serif' }}>
      {/* Top nav */}
      <div style={{
        height: 56, background: '#fff', borderBottom: `1px solid ${T.primarySoft}66`,
        display: 'flex', alignItems: 'center', padding: '0 28px', gap: 28, flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Mascot size={32}/>
          <div>
            <div style={{ fontSize: 15, fontWeight: 900, color: T.ink, letterSpacing: -0.3 }}>복권지도</div>
            <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 600, letterSpacing: 0.8 }}>LUCKY MAP · AI</div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 4 }}>
          {navItems.map(n => (
            <div key={n.id} style={{
              padding: '8px 14px', borderRadius: 8, fontSize: 13, fontWeight: 700,
              color: active === n.id ? T.primary : T.inkSoft,
              background: active === n.id ? T.primarySoft + '77' : 'transparent',
              cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6,
            }}>
              <span style={{ fontSize: 14 }}>{n.icon}</span> {n.label}
            </div>
          ))}
        </div>
        <div style={{ flex: 1 }}/>
        <div style={{
          background: T.bg, borderRadius: 8, padding: '7px 12px',
          display: 'flex', alignItems: 'center', gap: 8, width: 280,
        }}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke={T.inkSoft} strokeWidth="1.8"><circle cx="6" cy="6" r="4.5"/><path d="M9.5 9.5L12 12" strokeLinecap="round"/></svg>
          <span style={{ fontSize: 12, color: T.inkSoft }}>판매점·지역·회차 검색</span>
          <div style={{ flex: 1 }}/>
          <span style={{ fontSize: 10, color: T.inkSoft, background: '#fff', padding: '1px 5px', borderRadius: 3, border: `1px solid ${T.primarySoft}` }}>⌘ K</span>
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <div style={{ fontSize: 11, color: T.inkSoft }}>1213회 <b style={{ color: T.ink }}>2일 14시간</b></div>
          <div style={{
            width: 32, height: 32, borderRadius: 16, background: T.primary, color: '#fff',
            fontSize: 13, fontWeight: 800, display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>소</div>
        </div>
      </div>
      <div style={{ flex: 1, overflow: 'hidden' }}>{children}</div>
    </div>
  );
};

// ── Web: Store Detail ─────────────────────────────────────────
const WebStoreDetail = ({ T }) => {
  const s = window.STORES.find(x => x.id === 's7');
  return (
    <WebShell T={T} active="map">
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={{ maxWidth: 1120, margin: '0 auto', padding: '20px 28px 40px' }}>
          {/* Breadcrumb */}
          <div style={{ fontSize: 12, color: T.inkSoft, marginBottom: 14 }}>
            명당 지도 › 강남구 › <b style={{ color: T.ink }}>{s.name}</b>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 340px', gap: 20 }}>
            {/* Left col */}
            <div>
              {/* Hero */}
              <div style={{
                borderRadius: 18, padding: '28px 32px', color: '#fff',
                background: `linear-gradient(135deg, ${T.primary} 0%, ${T.accent} 100%)`,
                position: 'relative', overflow: 'hidden',
              }}>
                <div style={{ position: 'absolute', top: 16, right: 16 }}><Mascot size={52}/></div>
                <div style={{ display: 'flex', gap: 8, marginBottom: 12 }}>
                  {s.hot && <Chip color="rgba(255,255,255,.25)" text="#fff">🔥 이번 달 HOT 명당</Chip>}
                  <Chip color="rgba(255,255,255,.25)" text="#fff">✓ 인증 판매점</Chip>
                </div>
                <div style={{ fontSize: 32, fontWeight: 900, letterSpacing: -0.8 }}>{s.name}</div>
                <div style={{ fontSize: 13, opacity: 0.9, marginTop: 6 }}>📍 {s.addr} · 현위치에서 {s.distance}</div>
                <div style={{ display: 'flex', gap: 10, marginTop: 18 }}>
                  <button style={{ background: '#fff', color: T.primary, border: 'none', padding: '10px 20px', borderRadius: 10, fontSize: 13, fontWeight: 800, cursor: 'pointer' }}>🧭 길찾기</button>
                  <button style={{ background: 'rgba(255,255,255,.22)', color: '#fff', border: 'none', padding: '10px 18px', borderRadius: 10, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>♡ 즐겨찾기</button>
                  <button style={{ background: 'rgba(255,255,255,.22)', color: '#fff', border: 'none', padding: '10px 18px', borderRadius: 10, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>↗ 공유</button>
                </div>
              </div>

              {/* Stats */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 10, marginTop: 16 }}>
                {[
                  { n: s.lucky, l: '1등 배출', c: T.primary, sub: '누적' },
                  { n: Math.floor(s.lucky * 2.3), l: '2등 배출', c: T.accent, sub: '누적' },
                  { n: s.rating, l: '평균 별점', c: T.mint, sub: `${s.reviews}건` },
                  { n: '93%', l: '재방문률', c: T.cool, sub: '단골 비율' },
                ].map((x, i) => (
                  <div key={i} style={{ background: '#fff', borderRadius: 12, padding: '14px 16px' }}>
                    <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 700, letterSpacing: 0.5 }}>{x.l.toUpperCase()}</div>
                    <div style={{ fontSize: 26, fontWeight: 900, color: x.c, marginTop: 4, fontVariantNumeric: 'tabular-nums' }}>{x.n}</div>
                    <div style={{ fontSize: 10, color: T.inkSoft }}>{x.sub}</div>
                  </div>
                ))}
              </div>

              {/* History table */}
              <div style={{ background: '#fff', borderRadius: 14, marginTop: 16, padding: 20 }}>
                <div style={{ display: 'flex', alignItems: 'center', marginBottom: 14 }}>
                  <div style={{ fontSize: 15, fontWeight: 800, color: T.ink }}>최근 당첨 이력</div>
                  <div style={{ flex: 1 }}/>
                  <div style={{ display: 'flex', gap: 4 }}>
                    {['전체','1등','2등'].map((l, i) => (
                      <div key={l} style={{
                        padding: '4px 10px', borderRadius: 6, fontSize: 11, fontWeight: 700,
                        background: i === 0 ? T.primary : T.bg, color: i === 0 ? '#fff' : T.inkSoft,
                      }}>{l}</div>
                    ))}
                  </div>
                </div>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr style={{ color: T.inkSoft, fontSize: 10, fontWeight: 700, textAlign: 'left' }}>
                      <th style={{ padding: '8px 10px' }}>회차</th>
                      <th style={{ padding: '8px 10px' }}>날짜</th>
                      <th style={{ padding: '8px 10px' }}>등수</th>
                      <th style={{ padding: '8px 10px' }}>당첨금</th>
                      <th style={{ padding: '8px 10px' }}>당첨번호</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      { r:1210, d:'2026.03.21', k:1, p:'22억 4,812만', nums:[3,14,22,26,33,42] },
                      { r:1204, d:'2026.02.07', k:1, p:'18억 7,320만', nums:[1,8,16,27,35,44] },
                      { r:1198, d:'2025.12.27', k:2, p:'5,820만',      nums:[5,12,19,26,33,41] },
                      { r:1194, d:'2025.11.29', k:1, p:'27억 900만',   nums:[4,11,23,28,37,45] },
                    ].map((h,i) => (
                      <tr key={i} style={{ borderTop: `1px solid ${T.primarySoft}55`, color: T.ink }}>
                        <td style={{ padding: '10px' }}><b>{h.r}</b>회</td>
                        <td style={{ padding: '10px', color: T.inkSoft }}>{h.d}</td>
                        <td style={{ padding: '10px' }}>
                          <Chip color={h.k === 1 ? T.primary : T.primarySoft} text={h.k === 1 ? '#fff' : T.primary}>{h.k}등</Chip>
                        </td>
                        <td style={{ padding: '10px', fontWeight: 800, fontVariantNumeric: 'tabular-nums' }}>{h.p}원</td>
                        <td style={{ padding: '10px' }}>
                          <div style={{ display: 'flex', gap: 3 }}>
                            {h.nums.map(n => <Ball key={n} n={n} size={22}/>)}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Reviews */}
              <div style={{ background: '#fff', borderRadius: 14, marginTop: 16, padding: 20 }}>
                <div style={{ fontSize: 15, fontWeight: 800, color: T.ink, marginBottom: 14 }}>방문 리뷰 <span style={{ color: T.inkSoft, fontWeight: 500 }}>· {s.reviews}건</span></div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                  {[
                    { u:'김○○', r:5, d:'2일 전', t:'여기서 5등 당첨됐어요! 사장님도 친절하심 🍀' },
                    { u:'이○○', r:5, d:'1주 전', t:'명당 소문나서 줄 섭니다. 그래도 가치 있어요' },
                    { u:'박○○', r:4, d:'2주 전', t:'주차가 조금 불편한데 자동번호 잘 뽑아주세요' },
                    { u:'정○○', r:5, d:'3주 전', t:'매주 가는 단골집. 4등 두번 맞았어요!' },
                  ].map((r,i) => (
                    <div key={i} style={{ padding: 12, background: T.bg, borderRadius: 10 }}>
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
              </div>
            </div>

            {/* Right col — sticky info */}
            <div>
              <div style={{ background: '#fff', borderRadius: 14, padding: 18, position: 'sticky', top: 20 }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: T.ink, marginBottom: 12 }}>판매 정보</div>
                {[
                  ['운영시간','매일 07:00 – 23:00'],
                  ['주차','불가 (공영주차장 인근)'],
                  ['전화','02-123-4567'],
                  ['자동번호','지원'],
                ].map(([k,v]) => (
                  <div key={k} style={{ display: 'flex', gap: 10, padding: '8px 0', borderBottom: `1px solid ${T.primarySoft}44`, fontSize: 12 }}>
                    <div style={{ width: 60, color: T.inkSoft, fontWeight: 600 }}>{k}</div>
                    <div style={{ color: T.ink, flex: 1 }}>{v}</div>
                  </div>
                ))}
                <div style={{ marginTop: 16, padding: 12, borderRadius: 10, background: `${T.accent}22` }}>
                  <div style={{ fontSize: 11, color: T.inkSoft, fontWeight: 700 }}>판매 복권</div>
                  <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 6 }}>
                    {s.games.map(g => {
                      const game = window.GAMES.find(x => x.id === g);
                      return <Chip key={g} color="#fff" text={T.ink}>{game.emoji} {game.name}</Chip>;
                    })}
                  </div>
                </div>
                <button style={{
                  width: '100%', marginTop: 14, padding: '12px 0',
                  background: `linear-gradient(135deg, ${T.primary}, ${T.accent})`,
                  color: '#fff', border: 'none', borderRadius: 10, fontSize: 13, fontWeight: 800, cursor: 'pointer',
                }}>✨ 이 명당 번호 AI로 뽑기</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </WebShell>
  );
};

// ── Web: AI Pick ──────────────────────────────────────────────
const WebAIPick = ({ T, aiSets }) => {
  const [sel, setSel] = React.useState('A');
  const cur = aiSets.find(s => s.id === sel) || aiSets[0];
  return (
    <WebShell T={T} active="ai">
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={{ maxWidth: 1160, margin: '0 auto', padding: '20px 28px 40px' }}>
          {/* Hero row */}
          <div style={{
            background: `linear-gradient(135deg, ${T.lavender}44, ${T.primarySoft}77)`,
            borderRadius: 18, padding: '26px 30px',
            display: 'grid', gridTemplateColumns: '1fr 320px', gap: 24, alignItems: 'center',
          }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ width: 6, height: 6, borderRadius: 3, background: T.mint, animation: 'pulseD 1.4s ease-in-out infinite' }}/>
                <div style={{ fontSize: 10, fontWeight: 800, color: T.mint, letterSpacing: 1 }}>AI ENGINE · LIVE</div>
              </div>
              <div style={{ fontSize: 30, fontWeight: 900, color: T.ink, letterSpacing: -0.8, marginTop: 6 }}>
                1213회 번호 추천 완료 ✨
              </div>
              <div style={{ fontSize: 13, color: T.inkSoft, marginTop: 6, lineHeight: 1.5 }}>
                최근 52회차 출현 빈도 · 짝홀 비율 · 끝수 합 · 이월 패턴을 분석하여<br/>
                신뢰도가 높은 5개 세트를 생성했습니다.
              </div>
              <div style={{ display: 'flex', gap: 14, marginTop: 18 }}>
                {[['52','분석 회차'],['1,240','패턴 DB'],['87%','평균 신뢰도']].map(([n,l]) => (
                  <div key={l}>
                    <div style={{ fontSize: 20, fontWeight: 900, color: T.ink, fontVariantNumeric: 'tabular-nums' }}>{n}</div>
                    <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>{l}</div>
                  </div>
                ))}
              </div>
            </div>
            <div style={{ background: '#fff', borderRadius: 14, padding: 18, textAlign: 'center' }}>
              <Mascot size={48}/>
              <div style={{ fontSize: 11, color: T.inkSoft, fontWeight: 700, marginTop: 8 }}>추천 세트 {cur.id} · {cur.confidence}% 신뢰도</div>
              <div style={{ display: 'flex', gap: 4, justifyContent: 'center', marginTop: 10 }}>
                {cur.numbers.map(n => <Ball key={n} n={n} size={32} highlight/>)}
              </div>
              <button style={{
                marginTop: 12, width: '100%', padding: '10px 0',
                background: T.primary, color: '#fff', border: 'none', borderRadius: 10,
                fontSize: 13, fontWeight: 800, cursor: 'pointer',
              }}>💾 보관함 저장</button>
            </div>
          </div>

          {/* 5 sets grid */}
          <div style={{ marginTop: 20, fontSize: 13, fontWeight: 800, color: T.ink }}>AI 추천 5세트</div>
          <div style={{ marginTop: 10, display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12 }}>
            {aiSets.map(s => (
              <div key={s.id} onClick={() => setSel(s.id)} style={{
                background: '#fff', borderRadius: 14, padding: 14, cursor: 'pointer',
                boxShadow: sel === s.id ? `0 0 0 2px ${T.primary}, 0 6px 18px ${T.primary}22` : '0 2px 6px rgba(0,0,0,.05)',
                transition: 'all .2s',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <div style={{
                    width: 24, height: 24, borderRadius: 12,
                    background: sel === s.id ? T.primary : T.primarySoft,
                    color: sel === s.id ? '#fff' : T.primary, fontSize: 11, fontWeight: 900,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                  }}>{s.id}</div>
                  <div style={{ flex: 1 }}/>
                  <div style={{ fontSize: 11, fontWeight: 800, color: T.primary }}>{s.confidence}%</div>
                </div>
                <div style={{ fontSize: 10, color: T.inkSoft, marginTop: 8, lineHeight: 1.4, minHeight: 28 }}>{s.basis}</div>
                <div style={{ marginTop: 10, display: 'flex', gap: 3, justifyContent: 'space-between' }}>
                  {s.numbers.map(n => <Ball key={n} n={n} size={26}/>)}
                </div>
              </div>
            ))}
          </div>

          {/* Frequency analysis */}
          <div style={{ marginTop: 24, background: '#fff', borderRadius: 14, padding: 20 }}>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <div style={{ fontSize: 15, fontWeight: 800, color: T.ink }}>📊 최근 52회차 출현 빈도</div>
              <div style={{ flex: 1 }}/>
              <div style={{ display: 'flex', gap: 14, fontSize: 11, color: T.inkSoft }}>
                <span><span style={{display:'inline-block',width:10,height:10,background:T.primary,borderRadius:2,marginRight:4,verticalAlign:'middle'}}/>이번 추천</span>
                <span><span style={{display:'inline-block',width:10,height:10,background:T.accent,borderRadius:2,marginRight:4,verticalAlign:'middle'}}/>핫넘버 (11+ 출현)</span>
                <span><span style={{display:'inline-block',width:10,height:10,background:T.primarySoft,borderRadius:2,marginRight:4,verticalAlign:'middle'}}/>일반</span>
              </div>
            </div>
            <div style={{ marginTop: 14, display: 'grid', gridTemplateColumns: 'repeat(45, 1fr)', gap: 3 }}>
              {window.FREQ.map(f => {
                const h = 10 + f.c * 3.2;
                const isHot = f.c >= 11;
                const isPicked = cur.numbers.includes(f.n);
                return (
                  <div key={f.n} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 3 }}>
                    <div style={{ height: 60, display: 'flex', alignItems: 'flex-end', width: '100%' }}>
                      <div style={{
                        width: '100%', height: h, borderRadius: 2,
                        background: isPicked ? T.primary : isHot ? T.accent : T.primarySoft,
                      }}/>
                    </div>
                    <div style={{ fontSize: 9, color: isPicked ? T.primary : T.inkSoft,
                      fontWeight: isPicked ? 800 : 500, fontVariantNumeric: 'tabular-nums' }}>{f.n}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>
      <style>{`@keyframes pulseD { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }`}</style>
    </WebShell>
  );
};

// ── Web: My Numbers ───────────────────────────────────────────
const WebMyNumbers = ({ T }) => {
  const winningNums = [3, 14, 22, 26, 33, 42];
  const statusLabel = { pending: '추첨 대기', lost: '미당첨', won5th: '5등 당첨', won4th: '4등 당첨' };
  const statusColor = { pending: T.cool, lost: T.inkSoft, won5th: T.mint, won4th: T.primary };
  return (
    <WebShell T={T} active="saved">
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={{ maxWidth: 1120, margin: '0 auto', padding: '20px 28px 40px' }}>
          <div style={{ display: 'flex', alignItems: 'flex-end', marginBottom: 20 }}>
            <div>
              <div style={{ fontSize: 26, fontWeight: 900, color: T.ink, letterSpacing: -0.6 }}>내 번호 보관함</div>
              <div style={{ fontSize: 12, color: T.inkSoft, marginTop: 4 }}>AI 추천 번호와 직접 입력한 번호를 한 곳에 모아 관리하세요</div>
            </div>
            <div style={{ flex: 1 }}/>
            <button style={{ background: T.primary, color: '#fff', border: 'none', padding: '10px 18px', borderRadius: 10, fontSize: 13, fontWeight: 800, cursor: 'pointer' }}>+ 번호 추가</button>
          </div>

          {/* Summary strip */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 10, marginBottom: 20 }}>
            {[
              { n: '12', l: '보관 중', c: T.ink },
              { n: '3', l: '추첨 대기', c: T.cool },
              { n: '1', l: '당첨', c: T.mint, sub: '5등 1회' },
              { n: '₩5,000', l: '누적 상금', c: T.primary },
            ].map((x, i) => (
              <div key={i} style={{ background: '#fff', borderRadius: 12, padding: '14px 16px' }}>
                <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 700, letterSpacing: 0.5 }}>{x.l.toUpperCase()}</div>
                <div style={{ fontSize: 26, fontWeight: 900, color: x.c, marginTop: 4, fontVariantNumeric: 'tabular-nums' }}>{x.n}</div>
                {x.sub && <div style={{ fontSize: 10, color: T.inkSoft }}>{x.sub}</div>}
              </div>
            ))}
          </div>

          {/* Countdown */}
          <div style={{
            background: `linear-gradient(90deg, ${T.primary}ee, ${T.accent}ee)`, color: '#fff',
            borderRadius: 14, padding: '16px 22px',
            display: 'flex', alignItems: 'center', gap: 14, marginBottom: 20,
          }}>
            <div style={{ fontSize: 32 }}>🎯</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 11, opacity: 0.9, fontWeight: 700 }}>다음 추첨까지</div>
              <div style={{ fontSize: 20, fontWeight: 900 }}>1213회 · 2일 14시간 22분</div>
            </div>
            <div style={{ display: 'flex', gap: 6 }}>
              {[['2','일'],['14','시간'],['22','분']].map(([n,l]) => (
                <div key={l} style={{ background: 'rgba(255,255,255,.25)', padding: '8px 14px', borderRadius: 8, textAlign: 'center', minWidth: 58 }}>
                  <div style={{ fontSize: 20, fontWeight: 900, fontVariantNumeric: 'tabular-nums', lineHeight: 1 }}>{n}</div>
                  <div style={{ fontSize: 9, fontWeight: 700, opacity: 0.9, marginTop: 3 }}>{l}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Filter + list */}
          <div style={{ display: 'flex', gap: 6, marginBottom: 12 }}>
            {['전체','추첨 대기','당첨','미당첨'].map((l,i) => (
              <div key={l} style={{
                padding: '7px 14px', borderRadius: 999, fontSize: 12, fontWeight: 700,
                background: i === 0 ? T.ink : '#fff', color: i === 0 ? '#fff' : T.inkSoft,
              }}>{l}</div>
            ))}
          </div>
          <div style={{ background: '#fff', borderRadius: 14, padding: 4 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ color: T.inkSoft, fontSize: 10, fontWeight: 700, textAlign: 'left' }}>
                  <th style={{ padding: '12px 16px' }}>상태</th>
                  <th style={{ padding: '12px 16px' }}>회차</th>
                  <th style={{ padding: '12px 16px' }}>번호</th>
                  <th style={{ padding: '12px 16px' }}>출처</th>
                  <th style={{ padding: '12px 16px' }}>저장일</th>
                  <th style={{ padding: '12px 16px', textAlign: 'right' }}>결과</th>
                </tr>
              </thead>
              <tbody>
                {window.MY_SAVED.map(item => (
                  <tr key={item.id} style={{ borderTop: `1px solid ${T.primarySoft}44`, color: T.ink }}>
                    <td style={{ padding: '14px 16px' }}>
                      <Chip color={statusColor[item.status] + '22'} text={statusColor[item.status]}>
                        {statusLabel[item.status]}{item.matched ? ` · ${item.matched}개` : ''}
                      </Chip>
                    </td>
                    <td style={{ padding: '14px 16px', fontWeight: 700 }}>{item.round}회</td>
                    <td style={{ padding: '14px 16px' }}>
                      <div style={{ display: 'flex', gap: 3 }}>
                        {item.numbers.map(n => {
                          const matched = item.status !== 'pending' && winningNums.includes(n);
                          return (
                            <div key={n} style={{ opacity: item.status === 'lost' && !matched ? 0.4 : 1 }}>
                              <Ball n={n} size={24} highlight={matched}/>
                            </div>
                          );
                        })}
                      </div>
                    </td>
                    <td style={{ padding: '14px 16px', color: T.inkSoft }}>{item.source}</td>
                    <td style={{ padding: '14px 16px', color: T.inkSoft }}>{item.date}</td>
                    <td style={{ padding: '14px 16px', textAlign: 'right', fontWeight: 800,
                      color: item.prize ? T.mint : T.inkSoft }}>
                      {item.prize ? `+₩${item.prize.toLocaleString()}` : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </WebShell>
  );
};

// ── Web: Regional Stats ───────────────────────────────────────
const WebStats = ({ T }) => {
  const maxWins = Math.max(...window.REGIONS.map(r => r.wins));
  const colorMap = { hot: T.hot, primary: T.primary, accent: T.accent, mint: T.mint };
  return (
    <WebShell T={T} active="stats">
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={{ maxWidth: 1120, margin: '0 auto', padding: '20px 28px 40px' }}>
          <div style={{ display: 'flex', alignItems: 'flex-end', marginBottom: 20 }}>
            <div>
              <div style={{ fontSize: 26, fontWeight: 900, color: T.ink, letterSpacing: -0.6 }}>지역별 당첨 랭킹</div>
              <div style={{ fontSize: 12, color: T.inkSoft, marginTop: 4 }}>2024년 이후 누적 1·2등 기준 · 서울 지역</div>
            </div>
            <div style={{ flex: 1 }}/>
            <div style={{ display: 'flex', gap: 6 }}>
              {['서울','경기','전국'].map((l,i) => (
                <div key={l} style={{ padding: '8px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700,
                  background: i === 0 ? T.primary : '#fff', color: i === 0 ? '#fff' : T.inkSoft }}>{l}</div>
              ))}
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: 20 }}>
            {/* Left: ranking */}
            <div style={{ background: '#fff', borderRadius: 14, padding: 20 }}>
              <div style={{ fontSize: 14, fontWeight: 800, color: T.ink, marginBottom: 14 }}>전체 순위</div>
              {window.REGIONS.map((r, i) => (
                <div key={r.name} style={{
                  display: 'flex', alignItems: 'center', gap: 12, padding: '12px 4px',
                  borderBottom: i < window.REGIONS.length - 1 ? `1px solid ${T.primarySoft}44` : 'none',
                }}>
                  <div style={{ width: 32, fontSize: 18, fontWeight: 900, color: r.rank <= 3 ? T.primary : T.inkSoft, fontVariantNumeric: 'tabular-nums', textAlign: 'center' }}>
                    {r.rank <= 3 ? ['🥇','🥈','🥉'][r.rank-1] : r.rank}
                  </div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 14, fontWeight: 800, color: T.ink }}>{r.name}</div>
                    <div style={{ marginTop: 5, height: 5, background: T.primarySoft + '55', borderRadius: 3, overflow: 'hidden' }}>
                      <div style={{ width: `${(r.wins / maxWins) * 100}%`, height: '100%', background: colorMap[r.color], borderRadius: 3 }}/>
                    </div>
                  </div>
                  <div style={{ textAlign: 'right', minWidth: 90 }}>
                    <div style={{ fontSize: 16, fontWeight: 900, color: T.ink, fontVariantNumeric: 'tabular-nums' }}>{r.wins}회</div>
                    <div style={{ fontSize: 10, color: T.inkSoft }}>{(r.wins / r.pop * 100000).toFixed(1)}/10만명</div>
                  </div>
                </div>
              ))}
            </div>

            {/* Right: podium + insight */}
            <div>
              <div style={{ background: '#fff', borderRadius: 14, padding: 20 }}>
                <div style={{ fontSize: 14, fontWeight: 800, color: T.ink, marginBottom: 16 }}>🏆 TOP 3</div>
                <div style={{ display: 'flex', alignItems: 'flex-end', gap: 10, justifyContent: 'center' }}>
                  {[window.REGIONS[1], window.REGIONS[0], window.REGIONS[2]].map((r, i) => {
                    const medals = ['🥈','🥇','🥉'];
                    const heights = [80, 110, 60];
                    const order = [1, 0, 2][i];
                    return (
                      <div key={r.name} style={{ flex: 1, textAlign: 'center' }}>
                        <div style={{ fontSize: 30, marginBottom: 4 }}>{medals[i]}</div>
                        <div style={{ fontSize: 13, fontWeight: 800, color: T.ink }}>{r.name}</div>
                        <div style={{ fontSize: 11, color: T.inkSoft, fontWeight: 600 }}>{r.wins}회</div>
                        <div style={{
                          marginTop: 8, height: heights[i],
                          background: order === 0 ? `linear-gradient(180deg, ${T.accent}, ${T.primary})` :
                                      order === 1 ? `linear-gradient(180deg, ${T.primarySoft}, ${T.primary}aa)` :
                                                    `linear-gradient(180deg, ${T.primarySoft}, ${T.primary}66)`,
                          borderRadius: '10px 10px 0 0',
                          display: 'flex', alignItems: 'center', justifyContent: 'center',
                          color: '#fff', fontSize: 18, fontWeight: 900,
                        }}>{r.rank}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
              <div style={{
                marginTop: 14, padding: 18, borderRadius: 14,
                background: `linear-gradient(135deg, ${T.lavender}44, ${T.mint}44)`,
                display: 'flex', gap: 12,
              }}>
                <Mascot size={44}/>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 12, fontWeight: 800, color: T.ink }}>💡 이번 주 인사이트</div>
                  <div style={{ fontSize: 12, color: T.inkSoft, marginTop: 6, lineHeight: 1.6 }}>
                    강남구는 인구 10만명당 당첨률이 가장 높아요. 소희님 위치 근처 명당 <b style={{ color: T.primary }}>로또명당 삼성점</b>을 확인해보세요!
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </WebShell>
  );
};

Object.assign(window, { WebShell, WebStoreDetail, WebAIPick, WebMyNumbers, WebStats });
