import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type { KVKeysResponse, KVKeyValue, KVHistoryResponse, KVPutBody } from './types'

const enc = encodeURIComponent

export function useKVKeys(ns: string, search?: string) {
  return useQuery({
    queryKey: ['kv', ns, 'keys'],
    queryFn: () => api.get<KVKeysResponse>(`kv/namespaces/${enc(ns)}/keys?limit=1000`),
    select: (data) =>
      search
        ? { ...data, keys: data.keys.filter((k) => k.key.toLowerCase().includes(search.toLowerCase())) }
        : data,
  })
}

export function useKVKey(ns: string, key: string | null) {
  return useQuery({
    queryKey: ['kv', ns, 'key', key],
    queryFn: () => api.get<KVKeyValue>(`kv/namespaces/${enc(ns)}/keys/${enc(key!)}`),
    enabled: !!key,
  })
}

export function useKVHistory(ns: string, key: string | null) {
  return useQuery({
    queryKey: ['kv', ns, 'history', key],
    queryFn: () => api.get<KVHistoryResponse>(`kv/namespaces/${enc(ns)}/keys/${enc(key!)}/history`),
    enabled: !!key,
  })
}

export function usePutKVKey(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ key, body }: { key: string; body: KVPutBody }) =>
      api.put(`kv/namespaces/${enc(ns)}/keys/${enc(key)}`, body),
    onSuccess: (_d, { key }) => {
      qc.invalidateQueries({ queryKey: ['kv', ns] })
      qc.invalidateQueries({ queryKey: ['kv', ns, 'key', key] })
      qc.invalidateQueries({ queryKey: ['namespaces'] })
    },
  })
}

export function useDeleteKVKey(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (key: string) => api.del(`kv/namespaces/${enc(ns)}/keys/${enc(key)}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['kv', ns] })
      qc.invalidateQueries({ queryKey: ['namespaces'] })
    },
  })
}
