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

export const getLatestLotto = () =>
  api.get<LottoResult>('/lotto/results/latest').then((r) => r.data);

export const getLatestPension = () =>
  api.get<PensionResult>('/pension/results/latest').then((r) => r.data);

export const getLottoByRound = (round: number) =>
  api.get<LottoResult>(`/lotto/results/${round}`).then((r) => r.data);

export const getPensionByRound = (round: number) =>
  api.get<PensionResult>(`/pension/results/${round}`).then((r) => r.data);

export const getWinningStores = (lotteryType: string, roundNo: number, prizeRank = 1) =>
  api.get<WinningStore[]>('/winning-store', {
    params: { lottery_type: lotteryType, round_no: roundNo, prize_rank: prizeRank },
  }).then((r) => r.data);