import { api } from '@/lib/api'

export type Engine = 'statistical' | 'ai' | 'pension'
export type StatStrategy =
  'balanced' | 'hot' | 'cold' | 'overdue' | 'pattern_match' | 'contrarian' | 'streak_based'
export type PensionStrategy = 'balanced' | 'hot' | 'cold' | 'random'

export interface LottoSet {
  numbers: number[]
  confidence?: number      // AI 생성기
}

export interface PensionSet {
  group: number
  number: string
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
