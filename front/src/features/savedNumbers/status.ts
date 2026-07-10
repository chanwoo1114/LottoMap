import type { SavedNumber } from './api'

export const RANK_LABEL: Record<number, string> = { 1: '1등', 2: '2등', 3: '3등', 4: '4등', 5: '5등' }

export type Status = 'pending' | 'win' | 'lose'

/** 저장 항목의 채점 상태 */
export function statusOf(s: SavedNumber): Status {
  if (s.matched_rank == null) return 'pending'
  return s.matched_rank > 0 ? 'win' : 'lose'
}

export interface Summary {
  total: number
  wins: number
  pending: number
  bestRank: number | null
}

export function summarize(items: SavedNumber[]): Summary {
  let wins = 0
  let pending = 0
  let bestRank: number | null = null
  for (const s of items) {
    const st = statusOf(s)
    if (st === 'pending') pending++
    else if (st === 'win') {
      wins++
      if (bestRank == null || s.matched_rank! < bestRank) bestRank = s.matched_rank!
    }
  }
  return { total: items.length, wins, pending, bestRank }
}
