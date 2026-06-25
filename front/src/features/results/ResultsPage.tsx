import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  getLatestLotto, getLatestPension, getLottoByRound, getPensionByRound,
  type LottoResult, type PensionResult, type WinningStore,
} from './api'

import { RoundSelector } from './components/RoundSelector'
import { LottoResultCard } from './components/LottoResultCard.tsx'
import { WinningStoreSection } from './components/WinningStoreSection'
import { PensionResultCard } from './components/PensionResultCard'

type Kind = 'lotto' | 'pension' | 'speetto'

export function ResultsPage() {
  const navigate = useNavigate()
  const [kind, setKind] = useState<Kind>('lotto')
  const [latest, setLatest] = useState({ lotto: 0, pension: 0 })
  const [round, setRound] = useState({ lotto: 0, pension: 0 })
  const [lotto, setLotto] = useState<LottoResult | null>(null)
  const [pension, setPension] = useState<PensionResult | null>(null)

  useEffect(() => {
    getLatestLotto()
      .then((r) => {
        setLatest((s) => ({ ...s, lotto: r.round_no }))
        setRound((s) => ({ ...s, lotto: r.round_no }))
        setLotto(r)
      })
      .catch(() => {})

    getLatestPension()
      .then((r) => {
        setLatest((s) => ({ ...s, pension: r.round_no }))
        setRound((s) => ({ ...s, pension: r.round_no }))
        setPension(r)
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    if (kind === 'lotto' && round.lotto) getLottoByRound(round.lotto).then(setLotto).catch(() => setLotto(null))
    if (kind === 'pension' && round.pension) getPensionByRound(round.pension).then(setPension).catch(() => setPension(null))
  }, [kind, round])

  const showOnMap = (s: WinningStore) =>
    navigate('/', { state: { focus: { lat: s.lat, lng: s.lng } } })

  const curRound = kind === 'lotto' ? round.lotto : round.pension
  const curLatest = kind === 'lotto' ? latest.lotto : latest.pension

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-3xl space-y-4 p-5">
        <h1 className="text-xl font-bold text-gray-900">당첨결과</h1>

        <div className="flex gap-1 rounded-xl bg-gray-200/70 p-1">
          {(['lotto', 'pension', 'speetto'] as Kind[]).map((k) => (
            <button
              key={k}
              onClick={() => setKind(k)}
              className={`flex-1 rounded-lg py-2 text-sm font-semibold ${kind === k ? 'bg-white text-accent shadow-sm' : 'text-gray-500'}`}
            >
              {k === 'lotto' ? '로또 6/45' : k === 'pension' ? '연금복권' : '스피또'}
            </button>
          ))}
        </div>

        <>
          <RoundSelector
            latest={kind === 'lotto' ? latest.lotto : latest.pension}
            value={kind === 'lotto' ? round.lotto : round.pension}
            onChange={(r) => setRound((s) => ({...s, [kind]: r}))}
          />

          {kind === 'lotto' && lotto && <LottoResultCard result={lotto} />}
          {kind === 'pension' && pension && <PensionResultCard result={pension} />}

          <WinningStoreSection
            lotteryType={kind}
            roundNo={curRound}
            onShowOnMap={showOnMap}
          />
        </>
      </div>
    </div>
  );
}