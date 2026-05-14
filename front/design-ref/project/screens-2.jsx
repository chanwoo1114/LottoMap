// AI Pick, My Numbers, Regional Stats

// ─── 3. AI PICK RESULT ────────────────────────────────────────
const AIPickScreen = ({ T, aiNumbers }) => {
  const sets = aiNumbers || window.AI_SETS;
  const [sel, setSel] = React.useState('A');
  const curSet = sets.find(s => s.id === sel) || sets[0];

  return (
    <div style={{ height: '100%', overflow: 'auto', background: T.bg, paddingBottom: 100 }}>
      {/* Header */}
      <div style={{
        background: `linear-gradient(180deg, ${T.lavender} 0%, ${T.bg} 100%)`,
        padding: '14px 16px 24px', position: 'relative',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button style={{
            width: 32, height: 32, borderRadius: 16, border: 'none',
            background: 'rgba(255,255,255,.5)', cursor: 'pointer', color: T.ink,
          }}>←</button>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 800, color: T.ink }}>AI 번호 추첨</div>
            <div style={{ fontSize: 10, color: T.inkSoft }}>최근 52회차 데이터 분석 완료</div>
          </div>
          <div style={{
            background: '#fff', padding: '4px 10px', borderRadius: 999,
            fontSize: 10, fontWeight: 700, color: T.primary,
            display: 'flex', alignItems: 'center', gap: 4,
          }}>
            <span style={{ width: 6, height: 6, borderRadius: 3, background: T.mint,
              animation: 'pulse 1.4s ease-in-out infinite' }}/>
            LIVE
          </div>
        </div>

        {/* Hero card */}
        <div style={{
          marginTop: 16, background: '#fff', borderRadius: 20,
          padding: 18, boxShadow: '0 8px 30px rgba(0,0,0,.08)',
          position: 'relative', overflow: 'hidden',
        }}>
          <div style={{
            position: 'absolute', top: -20, right: -20, width: 100, height: 100,
            borderRadius: 50, background: `${T.accent}33`, filter: 'blur(20px)',
          }}/>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, position: 'relative' }}>
            <Mascot size={36}/>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>1213회 · 추천 세트 {curSet.id}</div>
              <div style={{ fontSize: 13, fontWeight: 700, color: T.ink }}>{curSet.basis}</div>
            </div>
            <div style={{
              textAlign: 'center',
              background: `linear-gradient(135deg, ${T.primary}, ${T.pink})`,
              color: '#fff', padding: '6px 10px', borderRadius: 10,
            }}>
              <div style={{ fontSize: 9, opacity: 0.9 }}>신뢰도</div>
              <div style={{ fontSize: 16, fontWeight: 900, lineHeight: 1 }}>{curSet.confidence}<span style={{ fontSize: 9 }}>%</span></div>
            </div>
          </div>

          {/* Number balls */}
          <div style={{
            marginTop: 18, display: 'flex', gap: 6, justifyContent: 'center',
          }}>
            {curSet.numbers.map((n, i) => (
              <div key={n} style={{
                animation: `ballIn .5s ease-out ${i * 0.08}s both`,
              }}>
                <Ball n={n} size={36} highlight/>
              </div>
            ))}
          </div>

          <div style={{
            marginTop: 14, display: 'flex', gap: 8,
          }}>
            <button style={{
              flex: 1, background: T.primarySoft, color: T.primary, border: 'none',
              padding: '10px 0', borderRadius: 10, fontSize: 12, fontWeight: 700, cursor: 'pointer',
            }}>💾 보관함 저장</button>
            <button style={{
              flex: 1, background: T.primary, color: '#fff', border: 'none',
              padding: '10px 0', borderRadius: 10, fontSize: 12, fontWeight: 700, cursor: 'pointer',
            }}>🏪 명당 찾기 →</button>
          </div>
        </div>
      </div>

      {/* Set list */}
      <div style={{ padding: '10px 16px' }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: T.inkSoft, marginBottom: 8 }}>
          전체 5세트 · AI가 골라준 번호
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {sets.map(s => (
            <div key={s.id} onClick={() => setSel(s.id)} style={{
              background: '#fff', borderRadius: 14,
              padding: 12, cursor: 'pointer',
              boxShadow: sel === s.id ? `0 0 0 2px ${T.primary}, 0 4px 12px ${T.primary}22` : '0 1px 3px rgba(0,0,0,.05)',
              transition: 'all .2s',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{
                  width: 28, height: 28, borderRadius: 14,
                  background: sel === s.id ? T.primary : T.primarySoft,
                  color: sel === s.id ? '#fff' : T.primary,
                  fontSize: 12, fontWeight: 900,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                }}>{s.id}</div>
                <div style={{ flex: 1, fontSize: 11, color: T.inkSoft, fontWeight: 600 }}>{s.basis}</div>
                <div style={{ fontSize: 11, fontWeight: 800, color: T.primary }}>{s.confidence}%</div>
              </div>
              <div style={{ marginTop: 10, display: 'flex', gap: 4, justifyContent: 'space-between' }}>
                {s.numbers.map(n => <Ball key={n} n={n} size={28}/>)}
              </div>
            </div>
          ))}
        </div>

        {/* Pattern analysis */}
        <div style={{
          marginTop: 18, background: '#fff', borderRadius: 16, padding: 14,
        }}>
          <div style={{ fontSize: 12, fontWeight: 800, color: T.ink, marginBottom: 10 }}>
            📊 최근 52회차 출현 빈도
          </div>
          <div style={{
            display: 'grid', gridTemplateColumns: 'repeat(15, 1fr)', gap: 3,
          }}>
            {window.FREQ.map(f => {
              const h = 6 + f.c * 1.6;
              const isHot = f.c >= 11;
              const isPicked = curSet.numbers.includes(f.n);
              return (
                <div key={f.n} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
                  <div style={{ height: 26, display: 'flex', alignItems: 'flex-end' }}>
                    <div style={{
                      width: '100%', height: h,
                      background: isPicked ? T.primary : isHot ? T.accent : T.primarySoft,
                      borderRadius: 1.5,
                    }}/>
                  </div>
                  <div style={{
                    fontSize: 7, color: isPicked ? T.primary : T.inkSoft,
                    fontWeight: isPicked ? 800 : 500,
                    fontVariantNumeric: 'tabular-nums',
                  }}>{f.n}</div>
                </div>
              );
            })}
          </div>
          <div style={{ marginTop: 10, display: 'flex', gap: 10, fontSize: 10, color: T.inkSoft }}>
            <span><span style={{display:'inline-block',width:8,height:8,background:T.primary,borderRadius:2,marginRight:3,verticalAlign:'middle'}}/>이번 추천</span>
            <span><span style={{display:'inline-block',width:8,height:8,background:T.accent,borderRadius:2,marginRight:3,verticalAlign:'middle'}}/>핫넘버</span>
            <span><span style={{display:'inline-block',width:8,height:8,background:T.primarySoft,borderRadius:2,marginRight:3,verticalAlign:'middle'}}/>일반</span>
          </div>
        </div>
      </div>

      <BottomTab T={T} active="ai"/>
      <style>{`@keyframes ballIn { from { opacity: 0; transform: translateY(-10px) scale(.7); } to { opacity: 1; transform: none; } }
      @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }`}</style>
    </div>
  );
};

// ─── 4. MY NUMBERS / 보관함 ──────────────────────────────────
const MyNumbersScreen = ({ T }) => {
  const [filter, setFilter] = React.useState('all');
  const items = window.MY_SAVED;
  const statusLabel = { pending: '추첨 대기', lost: '미당첨', won5th: '5등 당첨', won4th: '4등 당첨' };
  const statusColor = { pending: T.cool, lost: T.inkSoft, won5th: T.mint, won4th: T.primary };
  const winningNums = [3, 14, 22, 26, 33, 42]; // for matching viz

  return (
    <div style={{ height: '100%', overflow: 'auto', background: T.bg, paddingBottom: 100 }}>
      {/* Header */}
      <div style={{
        background: `linear-gradient(160deg, ${T.primary}, ${T.accent})`,
        padding: '14px 16px 20px', color: '#fff', position: 'relative',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
          <div style={{ fontSize: 15, fontWeight: 800 }}>내 번호 보관함</div>
          <div style={{ flex: 1 }}/>
          <button style={{
            background: 'rgba(255,255,255,.25)', border: 'none', padding: '5px 10px',
            borderRadius: 8, color: '#fff', fontSize: 11, fontWeight: 700, cursor: 'pointer',
          }}>+ 추가</button>
        </div>

        {/* Stats */}
        <div style={{
          display: 'flex', justifyContent: 'space-around',
          background: 'rgba(255,255,255,.2)', borderRadius: 14, padding: '10px 0',
        }}>
          {[['12','보관 중'],['1','당첨'],['₩5,000','누적 상금']].map(([n,l],i) => (
            <div key={i} style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 16, fontWeight: 900, fontVariantNumeric: 'tabular-nums' }}>{n}</div>
              <div style={{ fontSize: 9, opacity: 0.9, fontWeight: 600 }}>{l}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Current round banner */}
      <div style={{ padding: '14px 16px 0' }}>
        <div style={{
          background: '#fff', borderRadius: 14, padding: 12,
          display: 'flex', alignItems: 'center', gap: 10,
          boxShadow: '0 2px 8px rgba(0,0,0,.04)',
        }}>
          <div style={{
            width: 36, height: 36, borderRadius: 10,
            background: `${T.accent}44`,
            display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18,
          }}>🎯</div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>다음 추첨까지</div>
            <div style={{ fontSize: 13, fontWeight: 800, color: T.ink }}>1213회 · 2일 14시간 남음</div>
          </div>
          <div style={{ fontSize: 11, color: T.primary, fontWeight: 700 }}>→</div>
        </div>
      </div>

      {/* Filter */}
      <div style={{ padding: '14px 16px 8px', display: 'flex', gap: 6, overflowX: 'auto' }}>
        {[['all','전체'],['pending','대기'],['won','당첨'],['lost','미당첨']].map(([id,l]) => (
          <div key={id} onClick={() => setFilter(id)} style={{
            padding: '6px 12px', borderRadius: 999, fontSize: 11, fontWeight: 700,
            background: filter === id ? T.ink : '#fff',
            color: filter === id ? '#fff' : T.inkSoft,
            cursor: 'pointer', whiteSpace: 'nowrap',
            boxShadow: '0 1px 3px rgba(0,0,0,.04)',
          }}>{l}</div>
        ))}
      </div>

      {/* List */}
      <div style={{ padding: '0 16px', display: 'flex', flexDirection: 'column', gap: 8 }}>
        {items.map(item => (
          <div key={item.id} style={{
            background: '#fff', borderRadius: 14, padding: 14,
            boxShadow: '0 1px 3px rgba(0,0,0,.04)',
            position: 'relative', overflow: 'hidden',
          }}>
            {item.status === 'won5th' && (
              <div style={{
                position: 'absolute', top: 10, right: -30, transform: 'rotate(30deg)',
                background: T.mint, color: '#fff', fontSize: 10, fontWeight: 800,
                padding: '2px 32px',
              }}>WON</div>
            )}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10 }}>
              <Chip color={statusColor[item.status] + '22'} text={statusColor[item.status]}>
                {statusLabel[item.status]}{item.matched ? ` · ${item.matched}개 일치` : ''}
              </Chip>
              <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>{item.round}회</div>
              <div style={{ flex: 1 }}/>
              <div style={{ fontSize: 10, color: T.inkSoft }}>{item.date}</div>
            </div>
            <div style={{ display: 'flex', gap: 4, justifyContent: 'space-between' }}>
              {item.numbers.map(n => {
                const matched = item.status !== 'pending' && winningNums.includes(n);
                return (
                  <div key={n} style={{ opacity: item.status === 'lost' && !matched ? 0.45 : 1 }}>
                    <Ball n={n} size={30} highlight={matched}/>
                  </div>
                );
              })}
            </div>
            <div style={{
              marginTop: 10, paddingTop: 10, borderTop: `1px dashed ${T.primarySoft}`,
              display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: T.inkSoft,
            }}>
              <span>📍 {item.source}</span>
              {item.prize && (
                <>
                  <span style={{ flex: 1 }}/>
                  <span style={{ color: T.mint, fontWeight: 800 }}>+₩{item.prize.toLocaleString()}</span>
                </>
              )}
              {!item.prize && <div style={{ flex: 1 }}/>}
            </div>
          </div>
        ))}
      </div>

      <BottomTab T={T} active="saved"/>
    </div>
  );
};

// ─── 5. REGIONAL STATS / 랭킹 ────────────────────────────────
const StatsScreen = ({ T }) => {
  const maxWins = Math.max(...window.REGIONS.map(r => r.wins));
  const colorMap = { hot: T.hot, primary: T.primary, accent: T.accent, mint: T.mint };

  return (
    <div style={{ height: '100%', overflow: 'auto', background: T.bg, paddingBottom: 100 }}>
      <div style={{
        padding: '14px 16px 16px', background: `linear-gradient(180deg, ${T.mint}66 0%, transparent 100%)`,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 800, color: T.ink }}>지역별 당첨 랭킹</div>
            <div style={{ fontSize: 10, color: T.inkSoft, marginTop: 2 }}>2024년 이후 누적 1·2등 기준</div>
          </div>
          <div style={{
            background: '#fff', padding: '6px 10px', borderRadius: 8,
            fontSize: 10, fontWeight: 700, color: T.inkSoft,
            display: 'flex', alignItems: 'center', gap: 4,
            boxShadow: '0 1px 3px rgba(0,0,0,.04)',
          }}>서울 ▾</div>
        </div>

        {/* Podium */}
        <div style={{
          marginTop: 14, background: '#fff', borderRadius: 18, padding: '18px 12px',
          boxShadow: '0 4px 14px rgba(0,0,0,.06)',
        }}>
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, justifyContent: 'center' }}>
            {[window.REGIONS[1], window.REGIONS[0], window.REGIONS[2]].map((r, i) => {
              const medals = ['🥈','🥇','🥉'];
              const heights = [56, 78, 44];
              const order = [1, 0, 2][i];
              return (
                <div key={r.name} style={{ flex: 1, textAlign: 'center' }}>
                  <div style={{ fontSize: 24, marginBottom: 4 }}>{medals[i]}</div>
                  <div style={{ fontSize: 12, fontWeight: 800, color: T.ink }}>{r.name}</div>
                  <div style={{ fontSize: 10, color: T.inkSoft, fontWeight: 600 }}>{r.wins}회 당첨</div>
                  <div style={{
                    marginTop: 6,
                    height: heights[i],
                    background: order === 0 ? `linear-gradient(180deg, ${T.accent}, ${T.primary})` :
                                order === 1 ? `linear-gradient(180deg, ${T.primarySoft}, ${T.primary}aa)` :
                                              `linear-gradient(180deg, ${T.primarySoft}, ${T.primary}66)`,
                    borderRadius: '10px 10px 0 0',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    color: '#fff', fontSize: 16, fontWeight: 900,
                  }}>{r.rank}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Full ranking */}
      <div style={{ padding: '6px 16px' }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: T.inkSoft, marginBottom: 8 }}>전체 순위</div>
        <div style={{
          background: '#fff', borderRadius: 14, padding: 4,
          boxShadow: '0 1px 3px rgba(0,0,0,.04)',
        }}>
          {window.REGIONS.map((r, i) => (
            <div key={r.name} style={{
              display: 'flex', alignItems: 'center', gap: 10,
              padding: '10px 12px',
              borderBottom: i < window.REGIONS.length - 1 ? `1px solid ${T.primarySoft}44` : 'none',
            }}>
              <div style={{
                width: 24, fontSize: 13, fontWeight: 900,
                color: r.rank <= 3 ? T.primary : T.inkSoft,
                fontVariantNumeric: 'tabular-nums',
              }}>{r.rank}</div>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: T.ink }}>{r.name}</div>
                <div style={{
                  marginTop: 4, height: 4, background: T.primarySoft + '55', borderRadius: 2,
                  overflow: 'hidden',
                }}>
                  <div style={{
                    width: `${(r.wins / maxWins) * 100}%`, height: '100%',
                    background: colorMap[r.color], borderRadius: 2,
                  }}/>
                </div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: T.ink, fontVariantNumeric: 'tabular-nums' }}>{r.wins}</div>
                <div style={{ fontSize: 9, color: T.inkSoft }}>
                  {(r.wins / r.pop * 100000).toFixed(1)}건/10만명
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Insight card */}
        <div style={{
          marginTop: 14, padding: 14, borderRadius: 14,
          background: `linear-gradient(135deg, ${T.lavender}44, ${T.mint}44)`,
          display: 'flex', gap: 10,
        }}>
          <Mascot size={36}/>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 11, fontWeight: 800, color: T.ink }}>💡 이번 주 인사이트</div>
            <div style={{ fontSize: 11, color: T.inkSoft, marginTop: 4, lineHeight: 1.5 }}>
              강남구는 인구 10만명당 당첨률이 가장 높아요. 
              소희님 위치 근처 명당 <b style={{ color: T.primary }}>로또명당 삼성점</b>을 확인해보세요!
            </div>
          </div>
        </div>
      </div>

      <BottomTab T={T} active="stats"/>
    </div>
  );
};

Object.assign(window, { AIPickScreen, MyNumbersScreen, StatsScreen });
