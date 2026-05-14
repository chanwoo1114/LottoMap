// Main app — design canvas with 5 screens × 3 platforms

const TWEAKS = /*EDITMODE-BEGIN*/{
  "theme": "cobalt",
  "mapMode": "pins",
  "aiSetA": "3,14,17,26,38,42",
  "aiSetB": "7,11,23,29,34,41",
  "aiSetC": "5,12,19,28,33,45",
  "activeScreen": "map"
}/*EDITMODE-END*/;

// Mini preview card for a theme — shows a condensed map screen
function ThemePreviewCard({ themeKey, selected, onSelect }) {
  const Tp = window.THEMES[themeKey];
  return (
    <div onClick={onSelect} style={{
      cursor: 'pointer', borderRadius: 20, overflow: 'hidden',
      background: Tp.bg, position: 'relative',
      boxShadow: selected
        ? `0 0 0 3px ${Tp.primary}, 0 12px 32px ${Tp.primary}33`
        : '0 4px 14px rgba(0,0,0,.08)',
      transition: 'all .2s',
    }}>
      {/* Header */}
      <div style={{
        padding: '14px 16px',
        background: `linear-gradient(135deg, ${Tp.primary}, ${Tp.accent})`,
        color: '#fff',
      }}>
        <div style={{ fontSize: 14, fontWeight: 900, letterSpacing: -0.3 }}>{Tp.name}</div>
        <div style={{ fontSize: 10, opacity: 0.9, marginTop: 2 }}>{Tp.tagline}</div>
      </div>
      {/* Mini map preview */}
      <div style={{ padding: 14 }}>
        <div style={{ position: 'relative', height: 110, borderRadius: 10, overflow: 'hidden', background: '#EEF2E8' }}>
          <MapBackdrop width={280} height={110} mode="pins" T={Tp}/>
          {window.STORES.slice(0, 4).map((s, i) => (
            <div key={s.id} style={{
              position: 'absolute',
              left: [40, 110, 170, 220][i],
              top: [40, 60, 30, 70][i],
              width: 18, height: 18, borderRadius: '50% 50% 50% 0',
              background: s.hot ? Tp.hot : i === 0 ? Tp.primary : i === 1 ? Tp.accent : Tp.cool,
              transform: 'rotate(-45deg)', border: '2px solid #fff',
              boxShadow: '0 2px 4px rgba(0,0,0,.2)',
            }}/>
          ))}
        </div>
        {/* Number balls preview */}
        <div style={{ display: 'flex', gap: 4, marginTop: 10, justifyContent: 'center' }}>
          {[7, 14, 23, 29, 34, 41].map(n => <Ball key={n} n={n} size={22}/>)}
        </div>
        {/* Chip + Button preview */}
        <div style={{ display: 'flex', gap: 6, marginTop: 10, alignItems: 'center' }}>
          <div style={{
            fontSize: 9, fontWeight: 700, padding: '3px 8px', borderRadius: 999,
            background: Tp.primarySoft, color: Tp.primary,
          }}>🔥 HOT 명당</div>
          <div style={{
            fontSize: 9, fontWeight: 700, padding: '3px 8px', borderRadius: 999,
            background: Tp.accent + '33', color: Tp.ink,
          }}>⭐ 4.9</div>
          <div style={{ flex: 1 }}/>
          <div style={{
            fontSize: 9, fontWeight: 800, padding: '4px 10px', borderRadius: 8,
            background: Tp.primary, color: '#fff',
          }}>길찾기</div>
        </div>
        {/* Swatches */}
        <div style={{ display: 'flex', gap: 4, marginTop: 12 }}>
          {[['primary','P'],['accent','A'],['mint','M'],['cool','C'],['hot','H']].map(([k,l]) => (
            <div key={k} style={{ flex: 1, textAlign: 'center' }}>
              <div style={{
                height: 20, borderRadius: 5, background: Tp[k],
                boxShadow: 'inset 0 -2px 4px rgba(0,0,0,.1)',
              }}/>
              <div style={{ fontSize: 8, color: Tp.inkSoft, marginTop: 2, fontWeight: 600 }}>{l}</div>
            </div>
          ))}
        </div>
      </div>
      {selected && (
        <div style={{
          position: 'absolute', top: 10, right: 10,
          background: '#fff', width: 22, height: 22, borderRadius: 11,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          boxShadow: '0 2px 6px rgba(0,0,0,.2)',
          color: Tp.primary, fontSize: 13, fontWeight: 900,
        }}>✓</div>
      )}
    </div>
  );
}

const ALL_SCREENS = ['map','winners','detail','ai','saved','stats'];
const SCREEN_LABEL = {
  map: '01 지도',
  winners: '02 당첨번호',
  detail: '03 판매점 상세',
  ai: '04 AI 번호 추첨',
  saved: '05 내 번호 보관함',
  stats: '06 지역별 랭킹',
};

function App() {
  const [tweaks, setTweaks] = React.useState(TWEAKS);
  const [edit, setEdit] = React.useState(false);
  const T = window.THEMES[tweaks.theme] || window.THEMES.cobalt;

  // Parse tweakable AI numbers
  const aiSets = React.useMemo(() => {
    const parse = (s) => s.split(',').map(x => parseInt(x.trim())).filter(n => !isNaN(n) && n >= 1 && n <= 45).slice(0, 6);
    const base = window.AI_SETS.map(s => ({ ...s }));
    base[0].numbers = parse(tweaks.aiSetA).length === 6 ? parse(tweaks.aiSetA) : base[0].numbers;
    base[1].numbers = parse(tweaks.aiSetB).length === 6 ? parse(tweaks.aiSetB) : base[1].numbers;
    base[2].numbers = parse(tweaks.aiSetC).length === 6 ? parse(tweaks.aiSetC) : base[2].numbers;
    return base;
  }, [tweaks.aiSetA, tweaks.aiSetB, tweaks.aiSetC]);

  // Tweaks protocol
  React.useEffect(() => {
    const onMsg = (e) => {
      if (!e.data || typeof e.data !== 'object') return;
      if (e.data.type === '__activate_edit_mode') setEdit(true);
      if (e.data.type === '__deactivate_edit_mode') setEdit(false);
    };
    window.addEventListener('message', onMsg);
    window.parent.postMessage({ type: '__edit_mode_available' }, '*');
    return () => window.removeEventListener('message', onMsg);
  }, []);

  const apply = (k, v) => {
    setTweaks(prev => {
      const next = { ...prev, [k]: v };
      window.parent.postMessage({ type: '__edit_mode_set_keys', edits: { [k]: v } }, '*');
      return next;
    });
  };

  // Only render the active screen group to keep canvas light
  const screensToShow = tweaks.activeScreen === 'all' ? ALL_SCREENS :
                        tweaks.activeScreen === 'compare' ? [] :
                        [tweaks.activeScreen];

  // Pick screen component for each platform
  const renderScreen = (screen, platform) => {
    const device = platform;
    const mapMode = tweaks.mapMode;
    if (screen === 'map')    return <MapScreen T={T} device={device} mapMode={mapMode}/>;
    if (screen === 'winners') return <WinningNumbersScreen T={T}/>;
    if (screen === 'detail') return <StoreDetail T={T} storeId="s7"/>;
    if (screen === 'ai')     return <AIPickScreen T={T} aiNumbers={aiSets}/>;
    if (screen === 'saved')  return <MyNumbersScreen T={T}/>;
    if (screen === 'stats')  return <StatsScreen T={T}/>;
    return null;
  };

  // For web view: full web-specific screens
  const renderWeb = (screen) => {
    if (screen === 'map')    return <WebMapView T={T} mapMode={tweaks.mapMode}/>;
    if (screen === 'winners') return <WebWinningNumbers T={T}/>;
    if (screen === 'detail') return <WebStoreDetail T={T}/>;
    if (screen === 'ai')     return <WebAIPick T={T} aiSets={aiSets}/>;
    if (screen === 'saved')  return <WebMyNumbers T={T}/>;
    if (screen === 'stats')  return <WebStats T={T}/>;
    return null;
  };

  return (
    <>
      <DesignCanvas>
        {/* Theme comparison section — always visible */}
        <DCSection id="themes" title="🎨 테마 비교" subtitle="6가지 톤을 나란히 보고 선택하세요 · 카드를 클릭하면 아래 캔버스가 해당 테마로 바뀝니다">
          {Object.keys(window.THEMES).map(k => (
            <DCArtboard key={k} id={`theme-${k}`} label={window.THEMES[k].name} width={320} height={420}>
              <ThemePreviewCard themeKey={k} selected={tweaks.theme === k} onSelect={() => apply('theme', k)}/>
            </DCArtboard>
          ))}
        </DCSection>

        {screensToShow.map(screen => (
          <DCSection key={screen} id={screen} title={SCREEN_LABEL[screen]}
            subtitle="Web · iOS · Android 동일 화면을 비교합니다">

            <DCArtboard id={`${screen}-web`} label="💻 Web · lucky-map.kr" width={1280} height={820}>
              <div data-screen-label={`${SCREEN_LABEL[screen]} — Web`} style={{ height: '100%', padding: 20, background: '#E8E2D7', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <BrowserFrame T={T} width={1240} height={760}>
                  {renderWeb(screen)}
                </BrowserFrame>
              </div>
            </DCArtboard>

            <DCArtboard id={`${screen}-ios`} label="📱 iOS · iPhone 15 Pro" width={400} height={760}>
              <div data-screen-label={`${SCREEN_LABEL[screen]} — iOS`} style={{ height: '100%', padding: 20, background: '#E8E2D7', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <IPhoneFrame T={T}>
                  <PhoneFrame T={T} platform="ios">
                    {renderScreen(screen, 'ios')}
                  </PhoneFrame>
                </IPhoneFrame>
              </div>
            </DCArtboard>

            <DCArtboard id={`${screen}-android`} label="🤖 Android · Pixel 8" width={400} height={760}>
              <div data-screen-label={`${SCREEN_LABEL[screen]} — Android`} style={{ height: '100%', padding: 20, background: '#E8E2D7', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <AndroidFrame T={T}>
                  <PhoneFrame T={T} platform="android">
                    {renderScreen(screen, 'android')}
                  </PhoneFrame>
                </AndroidFrame>
              </div>
            </DCArtboard>

            <DCPostIt top={-10} right={80} rotate={3} width={200}>
              {screen === 'map' && '지도 + 검색 + 판매점 리스트. 사이드바 vs. 풀스크린 지도로 플랫폼별 정보 밀도 차이'}
              {screen === 'winners' && '최신 회차 당첨번호 + 패턴 분석 + 내 번호 자동 매칭. LIVE 인디케이터로 회차감 강조'}
              {screen === 'detail' && '히어로 이미지 대신 그라디언트 + 마스코트. 탭으로 당첨이력/리뷰/정보 전환'}
              {screen === 'ai' && '5세트 추천 + 출현빈도 차트. 신뢰도 %로 친근하게 포장'}
              {screen === 'saved' && '당첨 여부 배지, 일치 번호 하이라이트. 카운트다운으로 긴장감'}
              {screen === 'stats' && '포디움 + 지역별 바차트. 마스코트 인사이트 카드'}
            </DCPostIt>
          </DCSection>
        ))}
      </DesignCanvas>

      {/* Tweaks panel */}
      {edit && <TweaksPanel tweaks={tweaks} apply={apply} T={T}/>}
    </>
  );
}

function TweaksPanel({ tweaks, apply, T }) {
  return (
    <div style={{
      position: 'fixed', bottom: 20, right: 20, width: 300, zIndex: 200,
      background: '#1D1815', color: '#fff', borderRadius: 14,
      padding: 16, boxShadow: '0 20px 60px rgba(0,0,0,.3)',
      fontFamily: 'Pretendard, sans-serif', fontSize: 12,
    }}>
      <div style={{ fontSize: 13, fontWeight: 800, marginBottom: 12, display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ color: T.primary }}>✦</span> Tweaks
      </div>

      <Row label="컬러 테마">
        <div style={{ display: 'flex', gap: 6 }}>
          {Object.keys(window.THEMES).map(k => {
            const th = window.THEMES[k];
            return (
              <button key={k} onClick={() => apply('theme', k)} style={{
                flex: 1, padding: 6, background: tweaks.theme === k ? '#fff' : '#2A251F',
                color: tweaks.theme === k ? '#000' : '#fff',
                border: 'none', borderRadius: 6, cursor: 'pointer',
                fontSize: 10, fontWeight: 700, fontFamily: 'inherit',
                display: 'flex', alignItems: 'center', gap: 4, justifyContent: 'center',
              }}>
                <span style={{ width: 10, height: 10, borderRadius: 5, background: th.primary }}/>
                {th.name}
              </button>
            );
          })}
        </div>
      </Row>

      <Row label="지도 뷰 모드">
        <div style={{ display: 'flex', gap: 6 }}>
          {[['pins','📍 판매점'],['heat','🔥 히트맵']].map(([k,l]) => (
            <button key={k} onClick={() => apply('mapMode', k)} style={{
              flex: 1, padding: 6, background: tweaks.mapMode === k ? '#fff' : '#2A251F',
              color: tweaks.mapMode === k ? '#000' : '#fff',
              border: 'none', borderRadius: 6, cursor: 'pointer',
              fontSize: 10, fontWeight: 700, fontFamily: 'inherit',
            }}>{l}</button>
          ))}
        </div>
      </Row>

      <Row label="현재 화면">
        <select value={tweaks.activeScreen} onChange={e => apply('activeScreen', e.target.value)} style={{
          width: '100%', padding: 6, background: '#2A251F', color: '#fff',
          border: '1px solid #3A3530', borderRadius: 6, fontSize: 11, fontFamily: 'inherit',
        }}>
          {ALL_SCREENS.map(s => <option key={s} value={s}>{SCREEN_LABEL[s]}</option>)}
          <option value="compare">🎨 테마 비교만 보기</option>
          <option value="all">전체 화면 보기</option>
        </select>
      </Row>

      <div style={{ fontSize: 10, fontWeight: 700, color: '#aaa', marginTop: 14, marginBottom: 4 }}>AI 추천 번호 (6개, 쉼표 구분)</div>
      {[['aiSetA','세트 A'],['aiSetB','세트 B'],['aiSetC','세트 C']].map(([k,l]) => (
        <div key={k} style={{ marginBottom: 6, display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ fontSize: 10, color: '#ccc', width: 40, fontWeight: 600 }}>{l}</div>
          <input value={tweaks[k]} onChange={e => apply(k, e.target.value)} style={{
            flex: 1, padding: 5, background: '#2A251F', color: '#fff',
            border: '1px solid #3A3530', borderRadius: 5, fontSize: 10,
            fontFamily: 'ui-monospace, monospace',
          }}/>
        </div>
      ))}
    </div>
  );
}

function Row({ label, children }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: '#aaa', marginBottom: 5 }}>{label}</div>
      {children}
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
