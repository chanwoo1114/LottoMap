import { useEffect, useState} from "react";
import { useNavigate } from 'react-router-dom';

import {
  getLatestLotto, getLatestPension, getLottoByRound, getPensionByRound,
  type LottoResult, type PensionResult, type WinningStore,
} from './api';

type Kind = 'lotto' | 'pension' | 'speetto';
const KIND_LABEL: Record<Kind, string> = {
  lotto: '로또 6/45',
  pension: '연금복권',
  speetto: '스피또',
};

export function ResultsPage() {
  const [kind, setKind] = useState<Kind>('lotto');

  const [lotto, setLotto] = useState<LottoResult | null>(null);
  const [pension, setPension] = useState<PensionResult | null>(null);


  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-3xl space-y-4 p-5">
        <h1 className="text-xl font-bold text-gray-900">
          당첨결과
        </h1>

        <div className="flex gap-1 rounded-xl bg-gray-200/70 p-1">
          {(['lotto', 'pension', 'speetto'] as Kind[]).map((k) => (
            <button
              key={k}
              className={`flex-1 rounded-lg px-4 py-2 text-sm font-semibold ${kind === k ? 'bg-white text-accent shadow-sm' : 'text-gray-500'}`}
            >
              {KIND_LABEL[k]}
            </button>
          ))}
        </div>

      </div>
    </div>
  );
}