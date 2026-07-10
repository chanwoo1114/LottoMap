import { useMemo, useState } from 'react'
import { Button } from '@/components/ui/Button'
import { useAuth } from '@/features/auth/AuthContext'
import { AuthModal } from '@/features/auth/components/AuthModal'
import { useSavedNumbers } from './hooks/useSavedNumbers'
import { SavedNumberList } from './components/SavedNumberList'
import { SavedNumbersSummary } from './components/SavedNumbersSummary'
import { statusOf, summarize, type Status } from './status'

type Filter = 'all' | Status

const FILTERS: { key: Filter; label: string }[] = [
  { key: 'all', label: '전체' },
  { key: 'pending', label: '대기중' },
  { key: 'win', label: '당첨' },
  { key: 'lose', label: '낙첨' },
]

export function MyNumbersPage() {
  const { isAuthenticated } = useAuth()
  const saved = useSavedNumbers(isAuthenticated)
  const [authOpen, setAuthOpen] = useState(false)
  const [filter, setFilter] = useState<Filter>('all')

  const summary = useMemo(() => summarize(saved.items), [saved.items])
  const visible = useMemo(
    () => (filter === 'all' ? saved.items : saved.items.filter((s) => statusOf(s) === filter)),
    [saved.items, filter],
  )

  return (
    <div className='h-full overflow-y-auto'>
      <div className='mx-auto max-w-3xl space-y-4 p-5'>
        <h1 className='text-xl font-bold text-gray-900'>내 번호 기록</h1>

        {!isAuthenticated ? (
          <div className='rounded-2xl bg-white p-8 text-center shadow-sm'>
            <p className='mb-4 text-sm text-gray-500'>저장한 번호를 보려면 로그인이 필요해요.</p>
            <Button onClick={() => setAuthOpen(true)}>로그인</Button>
          </div>
        ) : saved.loading ? (
          <p className='py-10 text-center text-sm text-gray-400'>불러오는 중…</p>
        ) : saved.items.length === 0 ? (
          <div className='rounded-2xl bg-white p-8 text-center text-sm leading-relaxed text-gray-400 shadow-sm'>
            아직 저장한 번호가 없어요.
            <br />
            번호생성에서 ⭐를 눌러 저장해보세요.
          </div>
        ) : (
          <>
            <SavedNumbersSummary summary={summary} />

            <div className='flex gap-1 rounded-xl bg-gray-200/70 p-1'>
              {FILTERS.map((f) => (
                <button
                  key={f.key}
                  onClick={() => setFilter(f.key)}
                  className={`flex-1 rounded-lg py-1.5 text-sm font-semibold transition ${
                    filter === f.key ? 'bg-white text-accent shadow-sm' : 'text-gray-500'
                  }`}
                >
                  {f.label}
                </button>
              ))}
            </div>

            {visible.length === 0 ? (
              <p className='py-8 text-center text-sm text-gray-400'>해당하는 번호가 없어요.</p>
            ) : (
              <SavedNumberList items={visible} onRemove={saved.remove} />
            )}
          </>
        )}
      </div>

      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} />
    </div>
  )
}
