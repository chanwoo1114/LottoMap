// Winning Numbers screens — web + mobile

// ── Mobile WinningNumbersScreen ──────────────────────────────
const WinningNumbersScreen = ({ T }) => {
  const [selectedRound, setSelectedRound] = React.useState(0);
  const draws = window.WINNING_DRAWS;
  const cur = draws[selectedRound];
  const sum = cur.numbers.reduce((a, b) => a + b, 0);
  const evenCount = cur.numbers.filter(n => n % 2 === 0).length;

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: T.bg, overflow: 'hidden' }}>
      {/* Hero — latest draw */}
      <div style={{
        background: `linear-gradient(160deg, ${T.primary} 0%, ${T.cool} 100%)`,
        padding: '14px 16px 24px', color: '#fff', position: 'relative', overflow: 'hidden',
      }}>
        <div style={{ position: 'absolute', top: -10, right: -20, opacity: 0.15, fontSize: 140 }}>🎱</div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ fontSize: 11, fontWeight: 700, opacity: 0.9 }}>LATEST DRAW</div>
          <div style={{ width: 5, height: 5, borderRadius: 3, background: T.accent, animation: 'wnPulse 1.4s infinite' }}/>
          <div style={{ fontSize: 10, color: T.accent, fontWeight: 800 }}>LIVE</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 10, marginTop: 6 }}>
          <div style={{ fontSize: 32, fontWeight: 900, letterSpacing: -1, lineHeight: 1 }}>{cur.round}회</div>
          <div style={{ fontSize: 12, opacity: 0.9, marginBottom: 4 }}>{cur.date} 추첨</div>
        </div>

        {/* Number balls */}
        <div style={{ marginTop: 16, padding: '14px 12px', background: 'rgba(255,255,255,.16)',
          backdropFilter: 'blur(10px)', borderRadius: 14 }}>
          <div style={{ display: 'flex', gap: 5, alignItems: 'center', justifyContent: 'center' }}>
            {cur.numbers.map(n => <Ball key={n} n={n} size={36}/>)}
            <div style={{ color: '#fff', fontSize: 22, fontWeight: 300, margin: '0 4px' }}>+</div>
            <div style={{ position: 'relative' }}>
              <Ball n={cur.bonus} size={36}/>
              <div style={{ position: 'absolute', bottom: -14, left: 0, right: 0, textAlign: 'center',
                fontSize: 8, fontWeight: 800, opacity: 0.9 }}>보너스</div>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 14, marginTop: 22, justifyContent: 'space-around', fontSize: 11 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ opacity: 0.8, fontSize: 9, fontWeight: 700, letterSpacing: 0.5 }}>1등 당첨금</div>
              <div style={{ fontSize: 14, fontWeight: 900, marginTop: 2 }}>{cur.prize1}원</div>
            </div>
            <div style={{ width: 1, background: 'rgba(255,255,255,.3)' }}/>
            <div style={{ textAlign: 'center' }}>
              <div style={{ opacity: 0.8, fontSize: 9, fontWeight: 700, letterSpacing: 0.5 }}>1등 당첨자</div>
              <div style={{ fontSize: 14, fontWeight: 900, marginTop: 2 }}>{cur.winners1}명</div>
            </div>
            <div style={{ width: 1, background: 'rgba(255,255,255,.3)' }}/>
            <div style={{ textAlign: 'center' }}>
              <div style={{ opacity: 0.8, fontSize: 9, fontWeight: 700, letterSpacing: 0.5 }}>총 판매액</div>
              <div style={{ fontSize: 14, fontWeight: 900, marginTop: 2 }}>{cur.totalSales}</div>
            </div>
          </div>
        </div>
      </div>

      <div style={{ flex: 1, overflow: 'auto', padding: '14px 16px 80px' }}>
        {/* Pattern stats */}
        <div style={{ background: '#fff', borderRadius: 12, padding: 14, marginBottom: 14 }}>
          <div style={{ fontSize: 12, fontWeight: 800, color: T.ink, marginBottom: 10 }}>📐 이번 회차 패턴</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8 }}>
            {[
              { l: '번호 합', v: sum, hint: sum >= 100 && sum <= 175 ? '평균 구간' : '편차' },
              { l: '짝:홀', v: `${evenCount}:${6-evenCount}`, hint: '균형' },
              { l: '연속수', v: '없음', hint: '드뭄' },
            ].map(x => (
              <div key={x.l} style={{ background: T.bg, borderRadius: 8, padding: '8px 6px', textAlign: 'center' }}>
                <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 700 }}>{x.l}</div>
                <div style={{ fontSize: 16, fontWeight: 900, color: T.primary, marginTop: 2, fontVariantNumeric: 'tabular-nums' }}>{x.v}</div>
                <div style={{ fontSize: 8, color: T.inkSoft, marginTop: 1 }}>{x.hint}</div>
              </div>
            ))}
          </div>
        </div>

        {/* My numbers check */}
        <div style={{ background: `linear-gradient(135deg, ${T.accent}33, ${T.primarySoft})`, borderRadius: 12, padding: 14, marginBottom: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{ fontSize: 22 }}>🎯</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 12, fontWeight: 800, color: T.ink }}>내 번호 결과 확인</div>
              <div style={{ fontSize: 10, color: T.inkSoft, marginTop: 2 }}>보관함의 4개 번호 자동 매칭</div>
            </div>
            <div style={{ background: T.primary, color: '#fff', padding: '6px 12px', borderRadius: 8,
              fontSize: 11, fontWeight: 800 }}>확인 →</div>
          </div>
          <div style={{ marginTop: 10, padding: 8, background: 'rgba(255,255,255,.6)', borderRadius: 8,
            fontSize: 11, color: T.ink, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ color: T.mint, fontWeight: 900 }}>5등 1건</span>
            <span style={{ color: T.inkSoft }}>+₩5,000 · 미당첨 3건</span>
          </div>
        </div>

        {/* Past draws list */}
        <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
          <div style={{ fontSize: 12, fontWeight: 800, color: T.ink }}>지난 회차</div>
          <div style={{ flex: 1 }}/>
          <div style={{ fontSize: 11, color: T.primary, fontWeight: 700 }}>전체 보기 ›</div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {draws.slice(1, 6).map((d, i) => (
            <div key={d.round} onClick={() => setSelectedRound(i + 1)} style={{
              background: '#fff', borderRadius: 10, padding: 12,
              boxShadow: selectedRound === i + 1 ? `0 0 0 2px ${T.primary}` : 'none',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                <div style={{ fontSize: 12, fontWeight: 800, color: T.ink }}>{d.round}회</div>
                <div style={{ fontSize: 10, color: T.inkSoft, marginLeft: 6 }}>{d.date}</div>
                <div style={{ flex: 1 }}/>
                <div style={{ fontSize: 10, fontWeight: 700, color: T.primary }}>{d.winners1}명 당첨</div>
              </div>
              <div style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
                {d.numbers.map(n => <Ball key={n} n={n} size={22}/>)}
                <span style={{ color: T.inkSoft, fontSize: 14, margin: '0 2px' }}>+</span>
                <Ball n={d.bonus} size={22}/>
              </div>
            </div>
          ))}
        </div>
      </div>
      <BottomTab T={T} active="winners"/>
    </div>
  );
};

const WebWinningNumbers = ({ T }) => {
  const [selectedRound, setSelectedRound] = React.useState(0);
  const draws = window.WINNING_DRAWS;
  const cur = draws[selectedRound];
  const sum = cur.numbers.reduce((a, b) => a + b, 0);
  const evenCount = cur.numbers.filter(n => n % 2 === 0).length;
  const lowCount = cur.numbers.filter(n => n <= 22).length;

  return (
    <WebShell T={T} active="winners">
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={{ maxWidth: 1160, margin: '0 auto', padding: '20px 28px 40px' }}>
          {/* Hero */}
          <div style={{
            borderRadius: 18, padding: '28px 36px', color: '#fff',
            background: `linear-gradient(135deg, ${T.primary} 0%, ${T.cool} 60%, ${T.lavender} 100%)`,
            position: 'relative', overflow: 'hidden',
          }}>
            <div style={{ position: 'absolute', top: -30, right: -20, opacity: 0.12, fontSize: 200 }}>🎱</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 24, alignItems: 'center' }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{ fontSize: 11, fontWeight: 800, letterSpacing: 1 }}>LATEST DRAW</div>
                  <div style={{ width: 6, height: 6, borderRadius: 3, background: T.accent, animation: 'wnPulse 1.4s infinite' }}/>
                  <div style={{ fontSize: 10, color: T.accent, fontWeight: 800 }}>LIVE</div>
                </div>
                <div style={{ display: 'flex', alignItems: 'flex-end', gap: 12, marginTop: 4 }}>
                  <div style={{ fontSize: 48, fontWeight: 900, letterSpacing: -1.5, lineHeight: 1 }}>{cur.round}회</div>
                  <div style={{ fontSize: 14, opacity: 0.9, marginBottom: 8 }}>{cur.date} 추첨 · 매주 토요일 20:35</div>
                </div>
                <div style={{ display: 'flex', gap: 6, marginTop: 18 }}>
                  {cur.numbers.map(n => <Ball key={n} n={n} size={56}/>)}
                  <div style={{ color: '#fff', fontSize: 36, fontWeight: 200, margin: '0 6px', alignSelf: 'center' }}>+</div>
                  <div style={{ position: 'relative' }}>
                    <Ball n={cur.bonus} size={56}/>
                    <div style={{ position: 'absolute', bottom: -16, left: 0, right: 0, textAlign: 'center',
                      fontSize: 10, fontWeight: 800, letterSpacing: 0.5 }}>보너스</div>
                  </div>
                </div>
              </div>
              <div style={{ background: 'rgba(255,255,255,.16)', backdropFilter: 'blur(10px)', borderRadius: 14, padding: 18, minWidth: 240 }}>
                <div style={{ fontSize: 10, opacity: 0.9, fontWeight: 700, letterSpacing: 0.5 }}>1등 당첨금</div>
                <div style={{ fontSize: 28, fontWeight: 900, marginTop: 2, lineHeight: 1.1 }}>{cur.prize1}<span style={{ fontSize: 16 }}>원</span></div>
                <div style={{ display: 'flex', gap: 16, marginTop: 14, fontSize: 11 }}>
                  <div>
                    <div style={{ opacity: 0.8, fontWeight: 700 }}>당첨자</div>
                    <div style={{ fontSize: 16, fontWeight: 900, marginTop: 1 }}>{cur.winners1}명</div>
                  </div>
                  <div>
                    <div style={{ opacity: 0.8, fontWeight: 700 }}>총 판매액</div>
                    <div style={{ fontSize: 16, fontWeight: 900, marginTop: 1 }}>{cur.totalSales}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* My result + Pattern panels */}
          <div style={{ display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 16, marginTop: 18 }}>
            {/* My result */}
            <div style={{ background: '#fff', borderRadius: 14, padding: 20 }}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ fontSize: 14, fontWeight: 800, color: T.ink }}>🎯 내 번호 결과</div>
                <div style={{ flex: 1 }}/>
                <div style={{ fontSize: 11, color: T.primary, fontWeight: 700, cursor: 'pointer' }}>보관함 →</div>
              </div>
              <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
                {window.MY_SAVED.filter(x => x.round === 1212 || x.round === 1213).slice(0, 3).map(item => {
                  const isPending = item.status === 'pending';
                  return (
                    <div key={item.id} style={{
                      padding: 12, background: T.bg, borderRadius: 10,
                      display: 'flex', alignItems: 'center', gap: 12,
                    }}>
                      <div style={{
                        padding: '4px 10px', borderRadius: 999, fontSize: 10, fontWeight: 800,
                        background: isPending ? T.cool + '22' : item.status === 'won5th' ? T.mint + '22' : T.inkSoft + '22',
                        color: isPending ? T.cool : item.status === 'won5th' ? T.mint : T.inkSoft,
                        minWidth: 64, textAlign: 'center',
                      }}>
                        {isPending ? '대기' : item.status === 'won5th' ? '5등' : '미당첨'}
                      </div>
                      <div style={{ display: 'flex', gap: 3 }}>
                        {item.numbers.map(n => {
                          const matched = !isPending && cur.numbers.includes(n);
                          return (
                            <div key={n} style={{ opacity: !isPending && !matched ? 0.4 : 1 }}>
                              <Ball n={n} size={22} highlight={matched}/>
                            </div>
                          );
                        })}
                      </div>
                      <div style={{ flex: 1 }}/>
                      <div style={{ fontSize: 10, color: T.inkSoft }}>{item.source}</div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Pattern */}
            <div style={{ background: '#fff', borderRadius: 14, padding: 20 }}>
              <div style={{ fontSize: 14, fontWeight: 800, color: T.ink, marginBottom: 12 }}>📐 이번 회차 패턴 분석</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 8 }}>
                {[
                  { l: '번호 합', v: sum, hint: '평균 138' },
                  { l: '짝:홀', v: `${evenCount}:${6-evenCount}`, hint: '균형' },
                  { l: '저:고', v: `${lowCount}:${6-lowCount}`, hint: '1~22 / 23~45' },
                  { l: '끝수 합', v: cur.numbers.reduce((a, b) => a + b % 10, 0), hint: '평균 30' },
                ].map(x => (
                  <div key={x.l} style={{ background: T.bg, borderRadius: 10, padding: 10, textAlign: 'center' }}>
                    <div style={{ fontSize: 9, color: T.inkSoft, fontWeight: 700 }}>{x.l}</div>
                    <div style={{ fontSize: 20, fontWeight: 900, color: T.primary, marginTop: 2, fontVariantNumeric: 'tabular-nums' }}>{x.v}</div>
                    <div style={{ fontSize: 9, color: T.inkSoft, marginTop: 1 }}>{x.hint}</div>
                  </div>
                ))}
              </div>
              {/* 1-45 grid heat */}
              <div style={{ marginTop: 14 }}>
                <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 700, marginBottom: 6 }}>당첨 번호 분포</div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(15,1fr)', gap: 3 }}>
                  {Array.from({ length: 45 }, (_, i) => i + 1).map(n => {
                    const isWinner = cur.numbers.includes(n);
                    const isBonus = cur.bonus === n;
                    return (
                      <div key={n} style={{
                        aspectRatio: '1', borderRadius: 4, fontSize: 9, fontWeight: 700,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        background: isWinner ? T.primary : isBonus ? T.accent : T.bg,
                        color: isWinner ? '#fff' : isBonus ? T.ink : T.inkSoft + '99',
                        fontVariantNumeric: 'tabular-nums',
                      }}>{n}</div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>

          {/* Past rounds table */}
          <div style={{ background: '#fff', borderRadius: 14, padding: 20, marginTop: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', marginBottom: 14 }}>
              <div style={{ fontSize: 15, fontWeight: 800, color: T.ink }}>지난 회차 당첨 결과</div>
              <div style={{ flex: 1 }}/>
              <div style={{ display: 'flex', gap: 6, fontSize: 11 }}>
                <input placeholder="회차 검색" style={{
                  border: `1px solid ${T.primarySoft}`, borderRadius: 6, padding: '5px 10px',
                  fontSize: 11, background: T.bg, color: T.ink, outline: 'none', width: 100,
                  fontFamily: 'inherit',
                }}/>
                <div style={{ padding: '5px 10px', borderRadius: 6, background: T.primary, color: '#fff', fontWeight: 700, cursor: 'pointer' }}>회차별 조회</div>
              </div>
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead>
                <tr style={{ color: T.inkSoft, fontSize: 10, fontWeight: 700, textAlign: 'left' }}>
                  <th style={{ padding: '8px 10px' }}>회차</th>
                  <th style={{ padding: '8px 10px' }}>추첨일</th>
                  <th style={{ padding: '8px 10px' }}>당첨번호 + 보너스</th>
                  <th style={{ padding: '8px 10px', textAlign: 'right' }}>1등 당첨금</th>
                  <th style={{ padding: '8px 10px', textAlign: 'right' }}>당첨자</th>
                  <th style={{ padding: '8px 10px', textAlign: 'right' }}>총 판매액</th>
                </tr>
              </thead>
              <tbody>
                {draws.map((d, i) => (
                  <tr key={d.round} onClick={() => setSelectedRound(i)} style={{
                    borderTop: `1px solid ${T.primarySoft}44`, color: T.ink, cursor: 'pointer',
                    background: selectedRound === i ? T.primarySoft + '33' : 'transparent',
                  }}>
                    <td style={{ padding: '12px 10px', fontWeight: 800 }}>{d.round}회</td>
                    <td style={{ padding: '12px 10px', color: T.inkSoft }}>{d.date}</td>
                    <td style={{ padding: '12px 10px' }}>
                      <div style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
                        {d.numbers.map(n => <Ball key={n} n={n} size={22}/>)}
                        <span style={{ color: T.inkSoft, fontSize: 14, margin: '0 2px' }}>+</span>
                        <Ball n={d.bonus} size={22}/>
                      </div>
                    </td>
                    <td style={{ padding: '12px 10px', textAlign: 'right', fontWeight: 800, fontVariantNumeric: 'tabular-nums' }}>{d.prize1}원</td>
                    <td style={{ padding: '12px 10px', textAlign: 'right', color: T.inkSoft, fontVariantNumeric: 'tabular-nums' }}>{d.winners1}명</td>
                    <td style={{ padding: '12px 10px', textAlign: 'right', color: T.inkSoft, fontVariantNumeric: 'tabular-nums' }}>{d.totalSales}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      <style>{`@keyframes wnPulse { 0%,100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.3; transform: scale(1.4); } }`}</style>
    </WebShell>
  );
};

Object.assign(window, { WinningNumbersScreen, WebWinningNumbers });
