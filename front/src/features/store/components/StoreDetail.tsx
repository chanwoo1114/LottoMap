import type { Store } from '@/features/store/api';

interface StoreDetailProps {
  store: Store;
  onClose: () => void;
}

const PRODUCTS: { key: keyof Store; label: string }[] = [
  { key: 'sells_lotto', label: '로또6/45' },
  { key: 'sells_pension', label: '연금복권' },
  { key: 'sells_speetto_2000', label: '스피또2000' },
  { key: 'sells_speetto_1000', label: '스피또1000' },
  { key: 'sells_speetto_500', label: '스피또500' },
];

export function StoreDetail({ store, onClose }: StoreDetailProps) {

}