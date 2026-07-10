import { useCallback, useEffect, useMemo, useState } from 'react'
import { addSavedNumber, getSavedNumbers, removeSavedNumber, type SavedNumber } from '../api'

const keyOf = (nums: number[]) => [...nums].sort((a, b) => a - b).join(',')

/** 저장한 번호 목록을 관리하는 훅. enabled(=로그인 여부)가 켜질 때 서버에서 로드한다. */
export function useSavedNumbers(enabled: boolean) {
  const [items, setItems] = useState<SavedNumber[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!enabled) {
      setItems([])
      return
    }
    let cancelled = false
    setLoading(true)
    getSavedNumbers()
      .then((d) => { if (!cancelled) setItems(d) })
      .catch(() => { if (!cancelled) setItems([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [enabled])

  const savedIdByKey = useMemo(() => {
    const m = new Map<string, number>()
    for (const it of items) m.set(keyOf(it.numbers), it.id)
    return m
  }, [items])

  /** 해당 조합이 저장돼 있으면 저장 항목 id를, 아니면 undefined를 반환 */
  const savedIdFor = useCallback(
    (numbers: number[]) => savedIdByKey.get(keyOf(numbers)),
    [savedIdByKey],
  )

  const save = useCallback(async (numbers: number[]) => {
    const created = await addSavedNumber(numbers)
    setItems((prev) => [created, ...prev])
  }, [])

  const remove = useCallback(async (id: number) => {
    let rollback: SavedNumber[] = []
    setItems((prev) => {
      rollback = prev
      return prev.filter((i) => i.id !== id)
    })
    try {
      await removeSavedNumber(id)
    } catch {
      setItems(rollback)
    }
  }, [])

  return { items, loading, savedIdFor, save, remove }
}
