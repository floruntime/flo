import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type { KVKeysResponse, KVKeyValue, KVHistoryResponse, KVPutBody } from './types'

const enc = encodeURIComponent

/** Keys whose names start with `prefix`, a page at a time (the server's
    most per page); `fetchNextPage` loads the next while `hasNextPage`. */
export function useKVKeys(ns: string, prefix: string) {
  return useInfiniteQuery({
    queryKey: ['kv', ns, 'keys', prefix],
    initialPageParam: '',
    queryFn: ({ pageParam }) =>
      api.get<KVKeysResponse>(
        `kv/namespaces/${enc(ns)}/keys?limit=256&prefix=${enc(prefix)}${pageParam ? `&cursor=${pageParam}` : ''}`,
      ),
    getNextPageParam: (last) => (last.has_more && last.cursor ? last.cursor : undefined),
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
