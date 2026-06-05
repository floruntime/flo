import { useQuery } from '@tanstack/react-query'
import { api } from './client'
import type { StreamInfo, StreamDetailInfo, StreamMessagesResponse } from './types'

const enc = encodeURIComponent

export function useStreams(ns: string) {
  return useQuery({
    queryKey: ['streams', ns],
    queryFn: () => api.get<StreamInfo[]>(`streams?namespace=${enc(ns)}`),
  })
}

export function useStreamDetail(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['streams', ns, 'detail', name],
    queryFn: () => api.get<StreamDetailInfo>(`streams/${enc(name!)}?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

export function useStreamMessages(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['streams', ns, 'messages', name],
    queryFn: () => api.get<StreamMessagesResponse>(`streams/${enc(name!)}/messages?namespace=${enc(ns)}&limit=2000`),
    enabled: !!name,
  })
}
