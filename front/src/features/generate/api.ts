import { api } from '@/lib/api'

export type Engine = 'statistical' | 'ai' | 'pension'
export type StatStrategy =
  'balanced' | 'hot' | 'cold' | 'overdue' | 'pattern_match' | 'contrarian' | 'streak_based'
export type PensionStrategy = 'balanced' | 'hot' | 'cold' | 'random'

export interface LottoSet {
  numbers: number[]
  sum: number
  ac_value: number
  odd_even: string
  consecutive_pairs: number
  pattern_score?: number   // 통계 생성기
  confidence?: number      // AI 생성기
}

export interface PensionSet {
  group: number
  number: string
  digits: number[]
  sum: number
  unique_digits: number
  max_repeat: number
}

interface GenResponse<T> {
  results: T[]
  count: number
}

export const genStatistical = (strategy: StatStrategy, count: number) =>
  api.get<GenResponse<LottoSet>>('/generator/statistical', { params: { strategy, count } })
    .then((r) => r.data.results)

export const genAI = (temperature: number, count: number) =>
  api.get<GenResponse<LottoSet>>('/generator/ai', { params: { temperature, count } })
    .then((r) => r.data.results)

export const genPension = (strategy: PensionStrategy, count: number, fixedGroup: number | null) =>
  api.get<GenResponse<PensionSet>>('/generator/pension', {
    params: { strategy, count, ...(fixedGroup ? { fixed_group: fixedGroup } : {}) },
  }).then((r) => r.data.results)
