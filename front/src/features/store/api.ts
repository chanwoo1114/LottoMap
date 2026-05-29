import { api } from '@/lib/api';

export interface Store {
  id: number
  store_id: string
  name: string
  address: string
  phone: string
  sido: string | null
  sigungu: string | null
  dong: string | null
  sells_lotto: boolean
  sells_pension: boolean
  sells_speetto_2000: boolean
  sells_speetto_1000: boolean
  sells_speetto_500: boolean
  lat: number | null
  lng: number | null
}

export interface BoundsParams {
  min_lat: number
  min_lng: number
  max_lat: number
  max_lng: number
  limit?: number
}

export interface StoreSearchParams {
  sido?: string
  sigungu?: string
  address?: string
  sells_lotto?: boolean
  sells_pension?: boolean
  sells_speetto_2000?: boolean
  sells_speetto_1000?: boolean
  sells_speetto_500?: boolean
  page?: number
  size?: number
}

export const getStoresInBounds = (params: BoundsParams) =>
  api.get<Store[]>('/store/nearby', { params }).then(r => r.data)

export const searchStores = (params: StoreSearchParams) =>
  api.get<Store[]>('/store', { params }).then(r => r.data)

export const getStoreById = (id: number) =>
  api.get<Store>(`/store/${id}`).then(r => r.data)
