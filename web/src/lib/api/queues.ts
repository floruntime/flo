import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
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

/** The next ready messages in dequeue order, peeked (not taken). No paging:
    `more` only says others follow. */
export function useQueueMessages(ns: string, name: string | null, limit = 100) {
  return useQuery({
    queryKey: ['queues', ns, 'messages', name, limit],
    queryFn: () => api.get<QueueMessagesResponse>(`queues/${enc(name!)}/messages?namespace=${enc(ns)}&limit=${limit}`),
    enabled: !!name,
  })
}

/** The queue's dead letters, oldest dead first, a page at a time;
    `fetchNextPage` loads the next while `hasNextPage`. */
export function useQueueDLQ(ns: string, name: string | null) {
  return useInfiniteQuery({
    queryKey: ['queues', ns, 'dlq', name],
    initialPageParam: null as string | null,
    queryFn: ({ pageParam }) =>
      api.get<QueueDLQResponse>(
        `queues/${enc(name!)}/dlq?namespace=${enc(ns)}&limit=100${pageParam != null ? `&after=${enc(pageParam)}` : ''}`,
      ),
    getNextPageParam: (last) => (last.has_more && last.next_after != null ? last.next_after : undefined),
    enabled: !!name,
  })
}

/** Requeue one dead letter: ready again at once, its attempts reset. */
export function useRequeueDLQ(ns: string, name: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (seq: number) => api.post<{ ok: boolean }>(`queues/${enc(name)}/dlq/${seq}/requeue?namespace=${enc(ns)}`),
    onSettled: () => qc.invalidateQueries({ queryKey: ['queues'] }),
  })
}

/** Delete one dead letter for good. */
export function useDeleteDLQ(ns: string, name: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (seq: number) => api.del<{ ok: boolean }>(`queues/${enc(name)}/dlq/${seq}?namespace=${enc(ns)}`),
    onSettled: () => qc.invalidateQueries({ queryKey: ['queues'] }),
  })
}
