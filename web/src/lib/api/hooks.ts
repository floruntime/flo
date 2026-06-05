import { useQuery } from '@tanstack/react-query'
import { api } from './client'
import type { ClusterStats, NamespaceInfo, MetricsInfo } from './types'

/** Live list of namespaces (drives the namespace switcher). */
export function useNamespaces() {
  return useQuery({
    queryKey: ['namespaces'],
    queryFn: () => api.get<NamespaceInfo[]>('namespaces'),
  })
}

/** Cluster stats — powers the Overview cluster summary + node list + header health.
    Polls in the background too so the command-rate series + header stay live. */
export function useClusterStats() {
  return useQuery({
    queryKey: ['cluster', 'stats'],
    queryFn: () => api.get<ClusterStats>('cluster/stats'),
    refetchInterval: 5_000,
    refetchIntervalInBackground: true,
  })
}

/** Internal metrics — real entity counts + traffic + workflow activity. Polls. */
export function useMetrics() {
  return useQuery({
    queryKey: ['metrics'],
    queryFn: () => api.get<MetricsInfo>('metrics'),
    refetchInterval: 5_000,
  })
}
