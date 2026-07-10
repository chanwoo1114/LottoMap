import { useState } from 'react'
import { Button } from '@/components/ui/Button'
import { getApiErrorMessage } from '@/lib/api'
import { useAuth } from '@/features/auth/AuthContext'
import { AuthModal } from '@/features/auth/components/AuthModal'
import { useSavedNumbers } from '@/features/savedNumbers/hooks/useSavedNumbers'
import {
  genStatistical, genAI, genPension,
  type Engine, type StatStrategy, type PensionStrategy, type LottoSet, type PensionSet,
} from './api'
import {
  ENGINE_TABS, ENGINE_DESC, STAT_STRATEGIES, PENSION_STRATEGIES, tempInfo,
} from './constants'
import { LottoSetCard, PensionSetCard } from './components/SetCards'

const PENSION_GROUPS: (number | null)[] = [null, 1, 2, 3, 4, 5]

export function GeneratePage() {
  const [engine, setEngine] = useState<Engine>('statistical')
  const [statStrategy, setStatStrategy] = useState<StatStrategy>('balanced')
  const [pensionStrategy, setPensionStrategy] = useState<PensionStrategy>('balanced')
  const [temperature, setTemperature] = useState(1.5)
  const [fixedGroup, setFixedGroup] = useState<number | null>(null)
  const [count, setCount] = useState(5)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [lotto, setLotto] = useState<LottoSet[]>([])
  const [pension, setPension] = useState<PensionSet[]>([])

  const { isAuthenticated } = useAuth()
  const [authOpen, setAuthOpen] = useState(false)
  const saved = useSavedNumbers(isAuthenticated)

  const toggleSave = (numbers: number[]) => {
    if (!isAuthenticated) { setAuthOpen(true); return }
    const id = saved.savedIdFor(numbers)
    if (id != null) saved.remove(id)
    else saved.save(numbers).catch(() => {})
  }

  const reset = () => { setLotto([]); setPension([]); setError('') }
  const switchEngine = (e: Engine) => { setEngine(e); reset() }

  async function generate() {
    setLoading(true)
    setError('')
    try {
      if (engine === 'pension') {
        setLotto([])
        setPension(await genPension(pensionStrategy, count, fixedGroup))
      } else {
        setPension([])
        setLotto(engine === 'ai'
          ? await genAI(temperature, count)
          : await genStatistical(statStrategy, count))
      }
    } catch (e) {
      setError(getApiErrorMessage(e))
    } finally {
      setLoading(false)
    }
  }

  const strategies = engine === 'pension' ? PENSION_STRATEGIES : STAT_STRATEGIES
  const curStrategy: string = engine === 'pension' ? pensionStrategy : statStrategy
  const selectStrategy = (key: string) =>
    engine === 'pension'
      ? setPensionStrategy(key as PensionStrategy)
      : setStatStrategy(key as StatStrategy)

  const temp = tempInfo(temperature)
  const empty = !loading && !error && lotto.length === 0 && pension.length === 0

  return (
    <div className='h-full overflow-y-auto'>
      <div className='mx-auto max-w-3xl space-y-4 p-5'>
        <h1 className='text-xl font-bold text-gray-900'>번호 생성</h1>

        {/* 생성기 종류 */}
        <div className='flex gap-1 rounded-xl bg-gray-200/70 p-1'>
          {ENGINE_TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => switchEngine(t.key)}
              className={`flex-1 rounded-lg py-2 text-sm font-semibold ${
                engine === t.key ? 'bg-white text-accent shadow-sm' : 'text-gray-500'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* 옵션 패널 */}
        <section className='rounded-2xl bg-white p-5 shadow-sm'>
          <p className='mb-4 rounded-lg bg-gray-50 p-3 text-sm text-gray-600'>{ENGINE_DESC[engine]}</p>

          {/* 전략 (통계/연금) */}
          {engine !== 'ai' && (
            <div className='mb-4'>
              <p className='mb-2 text-xs font-bold text-gray-400'>
                전략 선택 <span className='font-normal'>— 원하는 방식을 고르세요</span>
              </p>
              <div className='grid gap-2 sm:grid-cols-2'>
                {strategies.map((s) => {
                  const on = curStrategy === s.key
                  return (
                    <button
                      key={s.key}
                      onClick={() => selectStrategy(s.key)}
                      className={`rounded-xl border p-2.5 text-left transition ${
                        on
                          ? 'border-emerald-600 bg-emerald-50 ring-1 ring-emerald-600'
                          : 'border-gray-200 bg-white hover:bg-gray-50'
                      }`}
                    >
                      <div className={`text-sm font-bold ${on ? 'text-emerald-700' : 'text-gray-900'}`}>{s.label}</div>
                      <div className='mt-0.5 text-xs leading-snug text-gray-500'>{s.desc}</div>
                    </button>
                  )
                })}
              </div>
            </div>
          )}

          {/* 과감성 (AI) */}
          {engine === 'ai' && (
            <div className='mb-4'>
              <div className='mb-1 flex items-baseline justify-between'>
                <p className='text-xs font-bold text-gray-400'>과감성</p>
                <span className='text-sm font-bold text-accent'>{temp.label} ({temperature.toFixed(1)})</span>
              </div>
              <input
                type='range'
                min={0.5}
                max={3}
                step={0.1}
                value={temperature}
                onChange={(e) => setTemperature(+e.target.value)}
                className='w-full accent-emerald-600'
              />
              <div className='flex justify-between text-[11px] text-gray-400'>
                <span>◀ 신중·안정</span><span>과감·변칙 ▶</span>
              </div>
              <p className='mt-2 rounded-lg bg-gray-50 p-2.5 text-xs text-gray-500'>💡 {temp.desc}</p>
            </div>
          )}

          {/* 고정 조 (연금) */}
          {engine === 'pension' && (
            <div className='mb-4'>
              <p className='mb-2 text-xs font-bold text-gray-400'>
                고정 조 <span className='font-normal'>— 특정 조로 고정할 수 있어요</span>
              </p>
              <div className='flex flex-wrap gap-1.5'>
                {PENSION_GROUPS.map((g) => (
                  <button
                    key={g ?? 'auto'}
                    onClick={() => setFixedGroup(g)}
                    className={`rounded-full px-3.5 py-1.5 text-[13px] font-semibold ${
                      fixedGroup === g ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'
                    }`}
                  >
                    {g === null ? '자동' : `${g}조`}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* 세트 수 + 생성 버튼 */}
          <div className='flex items-center justify-between gap-3 border-t border-gray-100 pt-4'>
            <div className='flex items-center gap-2'>
              <span className='text-xs font-bold text-gray-400'>세트 수</span>
              <div className='flex items-center gap-1'>
                <button
                  onClick={() => setCount((c) => Math.max(1, c - 1))}
                  className='grid h-8 w-8 place-items-center rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50'
                >
                  −
                </button>
                <span className='w-8 text-center text-base font-bold text-gray-900'>{count}</span>
                <button
                  onClick={() => setCount((c) => Math.min(10, c + 1))}
                  className='grid h-8 w-8 place-items-center rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50'
                >
                  +
                </button>
              </div>
            </div>
            <Button onClick={generate} disabled={loading} className='flex-1'>
              {loading ? '생성 중…' : '🎲 번호 생성하기'}
            </Button>
          </div>
        </section>

        {/* 결과 */}
        {error && <p className='rounded-lg bg-red-50 p-3 text-center text-sm text-red-500'>{error}</p>}

        {lotto.length > 0 && (
          <ul className='space-y-2.5'>
            {lotto.map((s, i) => (
              <LottoSetCard
                key={i}
                set={s}
                index={i}
                saved={isAuthenticated && saved.savedIdFor(s.numbers) != null}
                onToggleSave={() => toggleSave(s.numbers)}
              />
            ))}
          </ul>
        )}
        {pension.length > 0 && (
          <ul className='space-y-2.5'>
            {pension.map((s, i) => <PensionSetCard key={i} set={s} index={i} />)}
          </ul>
        )}

        {empty && <p className='pb-4 text-center text-xs text-gray-400'>전략을 고르고 생성하기를 누르세요</p>}
      </div>

      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} />
    </div>
  )
}
