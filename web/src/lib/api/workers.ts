import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { api } from './client'
import type { WorkerInfo, WorkersPage } from './types'

const enc = encodeURIComponent

/** Registered workers in a namespace (workers are namespace-scoped
    server-side), a page at a time; `fetchNextPage` loads the next while
    `hasNextPage`. */
export function useWorkers(ns: string) {
  return useInfiniteQuery({
    queryKey: ['workers', ns],
    initialPageParam: '',
    queryFn: ({ pageParam }) =>
      api.get<WorkersPage>(`workers?namespace=${enc(ns)}${pageParam ? `&cursor=${enc(pageParam)}` : ''}`),
    getNextPageParam: (last) => (last.has_more && last.cursor ? last.cursor : undefined),
    // Heartbeats/health drift over time — keep the list reasonably fresh.
    refetchInterval: 10_000,
  })
}

/** One worker, in the namespace it registered in: a worker id is per namespace. */
export function useWorkerDetail(ns: string, id: string | null) {
  return useQuery({
    queryKey: ['workers', 'detail', ns, id],
    queryFn: () => api.get<WorkerInfo>(`workers/${enc(id!)}?namespace=${enc(ns)}`),
    enabled: !!id,
    refetchInterval: 10_000,
  })
}
