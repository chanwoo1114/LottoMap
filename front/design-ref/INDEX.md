# 복권지도 디자인 레퍼런스

Claude Design(claude.ai/design)에서 export한 시안. 이 폴더는 **참조용**이고 실제 구현은 `src/` 에서 진행한다.

---

## 확정 사항 (chat1.md 기준)

- **테마**: `cobalt` (코발트 럭키)
  - primary: `#2E5BD6` (코발트 블루 — AI/테크 느낌)
  - accent: `#F2C94C` (시트러스 옐로우 — 행운/당첨)
  - hot: `#EE6B4A` (오렌지 — HOT 명당, 1등 뱃지)
  - bg: `#EEF2F8`, surface: `#FFFFFF`
  - ink: `#15213D`, inkSoft: `#5C6E85`
- **타겟 플랫폼**: 웹 메인 + iOS/Android 비교 캔버스
- **마스코트**: 클로버 쓴 친근한 캐릭터 (`atoms.jsx > Mascot`)

전체 6개 테마 토큰은 `project/data.jsx`의 `THEMES` 객체 참고.

---

## 화면 6개

| # | 화면 | 핵심 요소 |
|---|---|---|
| 01 | **지도** | 판매점 핀 / 당첨 히트맵 전환, 상단 검색바, 사이드바 리스트 |
| 02 | **당첨번호** | 최신 회차 번호+보너스볼, LIVE 배지, 내 번호 자동 매칭, 패턴 분석, 지난 8회차 |
| 03 | **판매점 상세** | 당첨 이력, 리뷰, 정보 탭 |
| 04 | **AI 번호 추첨** | 5세트 추천(A~E) + 신뢰도/근거, 52회차 출현빈도 차트 |
| 05 | **내 번호 보관함** | 저장 번호, 회차별 매칭, 등수 배지, 일치 번호 하이라이트 |
| 06 | **지역별 랭킹** | 포디움(1~3등) + 인구 대비 당첨률, 8개 지역 |

---

## 파일 인덱스

### `project/` — React/JSX 프로토타입 소스

| 파일 | 내용 |
|---|---|
| `data.jsx` | **디자인 토큰 (THEMES)**, GAMES, STORES, REGIONS, AI_SETS, FREQ, MY_SAVED, WINNING_DRAWS, `numColor()` |
| `atoms.jsx` | 공용 컴포넌트: `Mascot`, `Ball`(번호공), `MapBackdrop`, `StorePin`, `Stars`, `Chip`, iOS/Android 상태바 |
| `frames.jsx` | 디바이스 프레임 (iPhone/Android/Web 외곽) |
| `web-screens.jsx` | **웹 전용 5개 화면** ← 우리가 가장 많이 참조할 파일 |
| `screens-1.jsx` | 모바일 화면 1 (지도, 판매점 상세) |
| `screens-2.jsx` | 모바일 화면 2 (AI, 보관함, 랭킹) |
| `winning-screens.jsx` | 당첨번호 화면 (web/iOS/Android) |
| `app.jsx` | 라우팅/탭 전환, WebShell, BottomTab |
| `design-canvas.jsx` | 캔버스(Web/iOS/Android 3열 비교 레이아웃) |
| `index.html` | 위 jsx를 inline해서 브라우저에서 바로 띄우는 통합본 (3337줄, 큼 — 가급적 개별 jsx 참조) |

### `chats/chat1.md`

사용자 ↔ Claude Design 대화 전문. 의사결정 맥락(왜 코발트인지, 어떤 화면 추가했는지) 다 여기 있음.

---

## 우리 프로젝트 적용 매핑 (예정)

| 디자인 컴포넌트 | 이 프로젝트의 위치 |
|---|---|
| `THEMES.cobalt` 토큰 | `src/index.css` 의 `@theme { --color-* }` |
| `WebShell` 상단 네비 | `src/components/layout/AppShell.tsx` 확장 |
| 웹 지도 화면 | `src/features/map/MapScreen.tsx` (현재 작업 중) |
| `Ball`, `Chip`, `Stars`, `Mascot` | `src/components/ui/` (새로 만들 예정) |
| `MapBackdrop`+`StorePin` | 실제 Mapbox 마커로 대체 (faux 지도 X) |
| `data.jsx` 더미 데이터 | 백엔드 API 연결 전 임시로 `src/lib/mockData.ts` 에 보관 |

---

## 메모

- 디자인 파일의 `MapBackdrop`은 **가짜 SVG 지도**다. 실제 구현은 Mapbox로 대체.
- 컬러 토큰은 디자인 파일이 inline style을 많이 쓰지만, 우리 프로젝트는 Tailwind v4 `@theme` 변수로 일원화한다.
- iOS/Android 시안은 참고용. 1차 구현은 **웹 반응형**으로 처리.
- 우선순위는 **A. 내 주변 판매점 + 명당 랭킹 → C. 지역 통계 → B. AI 예측** 순으로 합의됨.
