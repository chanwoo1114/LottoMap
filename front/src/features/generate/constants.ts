import type { Engine, StatStrategy, PensionStrategy } from './api'

export const ENGINE_TABS: { key: Engine; label: string }[] = [
  { key: 'statistical', label: '📊 통계' },
  { key: 'ai', label: '🤖 AI' },
  { key: 'pension', label: '💰 연금' },
]

export const ENGINE_DESC: Record<Engine, string> = {
  statistical: '💡 최근 회차의 번호별 출현 빈도·주기·패턴을 분석해서 추천해요.',
  ai: '💡 마르코프·주기·군집 등 7개 예측 모델을 합쳐 수천 개 후보 중 고른 조합이에요.',
  pension: '💡 연금복권 720+ 의 각 자리(6자리) 숫자 출현 빈도를 기반으로 조합해요.',
}

export interface StrategyMeta<T> {
  key: T
  label: string
  desc: string
}

export const STAT_STRATEGIES: StrategyMeta<StatStrategy>[] = [
  { key: 'balanced', label: '⚖️ 밸런스', desc: '자주 나온 번호와 뜸했던 번호를 고르게 섞어요. 가장 무난한 기본값이에요.' },
  { key: 'hot', label: '🔥 고빈도', desc: '요즘 자주 당첨된 "잘 나가는" 번호 위주로 뽑아요.' },
  { key: 'cold', label: '❄️ 저빈도', desc: '그동안 적게 나온 번호 위주로 뽑아요.' },
  { key: 'overdue', label: '⏳ 오버듀', desc: '한동안 안 나와서 "나올 때가 된" 번호를 노려요.' },
  { key: 'pattern_match', label: '🧩 패턴 매칭', desc: '역대 당첨번호의 합·홀짝 비율 같은 통계 패턴에 잘 맞는 조합을 골라요.' },
  { key: 'contrarian', label: '🙃 역발상', desc: '남들이 잘 안 고르는 비인기 번호로 채워요. 당첨되면 분배금이 커질 수 있어요.' },
  { key: 'streak_based', label: '📈 연속 흐름', desc: '최근 연속으로 나오는 흐름을 탄 번호를 골라요.' },
]

export const PENSION_STRATEGIES: StrategyMeta<PensionStrategy>[] = [
  { key: 'balanced', label: '⚖️ 밸런스', desc: '각 자리 숫자를 고르게 섞어요.' },
  { key: 'hot', label: '🔥 핫', desc: '자주 나온 자리 숫자 위주로 뽑아요.' },
  { key: 'cold', label: '❄️ 콜드', desc: '적게 나온 자리 숫자 위주로 뽑아요.' },
  { key: 'random', label: '🎲 랜덤', desc: '완전 무작위로 뽑아요.' },
]

/** AI 과감성(temperature) 구간별 사용자 설명 */
export function tempInfo(t: number): { label: string; desc: string } {
  if (t <= 1.2) return { label: '신중', desc: '모델이 가장 확신하는, 안정적인 번호 위주로 뽑아요.' }
  if (t <= 2.0) return { label: '균형', desc: '안정성과 다양성을 적당히 섞어요. 추천 기본값이에요.' }
  return { label: '과감', desc: '평소 잘 안 나오는 변칙적인 조합까지 폭넓게 시도해요.' }
}
