import { QueryClient, useQuery, type QueryKey } from '@tanstack/react-query'
import { useEffect } from 'react'
import { heartbeatFailed, heartbeatOk, useLiveState } from './live'

// The one TanStack Query cache every panel reads, so the drawer, tables, tiles, banners and
// badges show one copy of each thing. `poll` is for live numbers; space pauses every poll.

export const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false, staleTime: 500 } },
})

export type Load<T> = { data?: T; error?: unknown; loading: boolean; at?: number; reload: () => void }

export function useLive<T>(key: QueryKey, fn: () => Promise<T>, opts: { poll?: number | ((d?: T) => number | false); enabled?: boolean; heartbeat?: boolean } = {}): Load<T> {
  const { paused } = useLiveState()
  const q = useQuery({
    queryKey: key,
    queryFn: fn,
    enabled: opts.enabled ?? true,
    refetchInterval: paused ? false : typeof opts.poll === 'function' ? (x) => (opts.poll as (d?: T) => number | false)(x.state.data as T | undefined) : (opts.poll ?? false),
    placeholderData: (prev) => prev,
  })
  useEffect(() => {
    if (!opts.heartbeat) return
    if (q.isError) heartbeatFailed(q.error)
    else if (q.isSuccess) heartbeatOk()
  }, [opts.heartbeat, q.isError, q.isSuccess, q.error, q.dataUpdatedAt])
  return {
    data: q.data,
    error: q.isError ? q.error : undefined,
    loading: q.isPending && (opts.enabled ?? true),
    at: q.dataUpdatedAt || undefined,
    reload: () => void q.refetch(),
  }
}

export const invalidate = (...keys: QueryKey[]) => Promise.all(keys.map((k) => queryClient.invalidateQueries({ queryKey: k })))
