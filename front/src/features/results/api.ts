import { api } from '@/lib/api'

export interface LottoResult {
  round_no: number; draw_date: string; numbers: number[]; bonus: number;
  first_prize_amount: number; first_prize_winners: number; total_sales: number;
}
export interface PensionResult {
  round_no: number; draw_date: string;
  first_prize_group: number; first_prize_number: string; bonus_number: string;
}
export interface WinningStore {
  store_id: number; name: string; address: string;
  sido: string | null; sigungu: string | null; lat: number | null; lng: number | null;
  prize_rank: number; prize_amount: number; purchase_method: string;
}
export interface SpeettoGame {
  game_id: string; name: string;
  game_type: 'st2000' | 'st1000' | 'st500';
  round_no: number; price: number;
  sale_end_date: string | null; prize_claim_end_date: string | null;
  image_url: string | null;
  total_first_prizes: number; remaining_first_prizes: number;
  total_second_prizes: number; remaining_second_prizes: number;
  total_third_prizes: number; remaining_third_prizes: number;
  intake_rate: number; updated_at: string;
}

export const getLatestLotto = () =>
  api.get<LottoResult>('/lotto/results/latest').then((r) => r.data)

export const getLatestPension = () =>
  api.get<PensionResult>('/pension/results/latest').then((r) => r.data)

export const getLottoByRound = (round: number) =>
  api.get<LottoResult>(`/lotto/results/${round}`).then((r) => r.data)

export const getPensionByRound = (round: number) =>
  api.get<PensionResult>(`/pension/results/${round}`).then((r) => r.data)

export const getWinningStores = (lotteryType: string, roundNo: number, prizeRank = 1) =>
  api.get<WinningStore[]>('/winning-store', {
    params: { lottery_type: lotteryType, round_no: roundNo, prize_rank: prizeRank },
  }).then((r) => r.data)

export const getSpeettoGames = () =>
  api.get<SpeettoGame[]>('/speetto/games').then((r) => r.data)