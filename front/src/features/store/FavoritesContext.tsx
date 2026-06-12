import {createContext, useCallback, useContext, useState, type ReactNode, useEffect} from 'react'
import { useAuth } from '@/features/auth/AuthContext'
import { getFavorites, addFavorite, removeFavorite} from "./api.ts";

interface FavoritesState {
  isFavorite: (storeId: number) => boolean
  toggle: (storeId: number) => void
}

const FavoritesContext = createContext<FavoritesState | null>(null)

export function FavoritesProvider({ children }: { children: ReactNode }) {
  const { isAuthenticated } = useAuth()
  const [ids, setIds] = useState<Set<number>>(new Set())
  const [prevAuth, setPrevAuth] = useState(isAuthenticated)

  if (isAuthenticated !== prevAuth) {
    setPrevAuth(isAuthenticated)
    setIds(new Set())
  }

  useEffect(() => {
    if (!isAuthenticated) return
    let cancelled = false

    getFavorites()
      .then((stores) => { if (!cancelled) setIds(new Set(stores.map((s) => s.id))) })
      .catch(() => { if (!cancelled) setIds(new Set()) })
    return () => { cancelled = true }
  }, [isAuthenticated])

  const isFavorite = useCallback((id: number) => ids.has(id), [ids])

  const toggle = useCallback((id: number) => {
    const adding = !ids.has(id)

    setIds((prev) => {
      const next = new Set(prev)
      if (adding) next.add(id)
      else next.delete(id)
      return next
    })

    const req = adding ? addFavorite(id) : removeFavorite(id)
    req.catch(() => {
      setIds((prev) => {
        const rb = new Set(prev)
        if (adding) rb.delete(id)
        else rb.add(id)
        return rb
      })
    })
  }, [ids])

  return <FavoritesContext.Provider value={{ isFavorite, toggle }}>{children}</FavoritesContext.Provider>
}

export function useFavorites() {
  const ctx = useContext(FavoritesContext)
  if (!ctx) throw new Error('useFavorites must be used within FavoritesProvider')
  return ctx
}