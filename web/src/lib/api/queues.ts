import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type { QueueInfo, QueueDetailInfo, QueueDLQResponse, QueueMessagesResponse } from './types'

const enc = encodeURIComponent

/** All queues across namespaces. */
export function useQueues() {
  return useQuery({
    queryKey: ['queues'],
    queryFn: () => api.get<QueueInfo[]>('queues'),
  })
}

export function useQueueDetail(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['queues', ns, 'detail', name],
    queryFn: () => api.get<QueueDetailInfo>(`queues/${enc(name!)}?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

export function useQueueMessages(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['queues', ns, 'messages', name],
    queryFn: () => api.get<QueueMessagesResponse>(`queues/${enc(name!)}/messages?namespace=${enc(ns)}&limit=2000`),
    enabled: !!name,
  })
}

/** The queue's dead letters: the count, and the entries once an op lists them. */
export function useQueueDLQ(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['queues', ns, 'dlq', name],
    queryFn: () => api.get<QueueDLQResponse>(`queues/${enc(name!)}/dlq?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

/** Requeue a DLQ entry back to the queue (real loopback write). */
export function useRequeueDLQ(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ name, seq }: { name: string; seq: number }) =>
      api.post(`queues/${enc(name)}/dlq/${seq}/requeue?namespace=${enc(ns)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['queues'] }),
  })
}
