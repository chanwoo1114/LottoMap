import { api } from '@/lib/api'

export interface SavedNumber {
  id: number
  round_no: number
  numbers: number[]
  source: string
  matched_count: number | null
  matched_bonus: boolean | null
  matched_rank: number | null
  memo: string
  created_at: string
  draw_date: string | null
  winning_numbers: number[] | null
  winning_bonus: number | null
}

export const getSavedNumbers = () =>
  api.get<SavedNumber[]>('/saved-numbers').then((r) => r.data)

export const addSavedNumber = (numbers: number[], source = 'generated') =>
  api.post<SavedNumber>('/saved-numbers', { numbers, source }).then((r) => r.data)

export const removeSavedNumber = (id: number) =>
  api.delete(`/saved-numbers/${id}`).then(() => undefined)
