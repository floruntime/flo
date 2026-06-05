import { useQuery } from '@tanstack/react-query'
import { api } from './client'
import type { WorkerInfo } from './types'

const enc = encodeURIComponent

/** Registered workers in a namespace (workers are namespace-scoped server-side). */
export function useWorkers(ns: string) {
  return useQuery({
    queryKey: ['workers', ns],
    queryFn: () => api.get<WorkerInfo[]>(`workers?namespace=${enc(ns)}`),
    // Heartbeats/health drift over time — keep the list reasonably fresh.
    refetchInterval: 10_000,
  })
}

export function useWorkerDetail(id: string | null) {
  return useQuery({
    queryKey: ['workers', 'detail', id],
    queryFn: () => api.get<WorkerInfo>(`workers/${enc(id!)}`),
    enabled: !!id,
    refetchInterval: 10_000,
  })
}
