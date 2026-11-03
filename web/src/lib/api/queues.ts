import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
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

/** The queue's dead letters, oldest first, a page at a time; `fetchNextPage`
    loads the next while `hasNextPage`. */
export function useQueueDLQ(ns: string, name: string | null) {
  return useInfiniteQuery({
    queryKey: ['queues', ns, 'dlq', name],
    initialPageParam: null as number | null,
    queryFn: ({ pageParam }) =>
      api.get<QueueDLQResponse>(
        `queues/${enc(name!)}/dlq?namespace=${enc(ns)}&limit=100${pageParam != null ? `&after=${pageParam}` : ''}`,
      ),
    getNextPageParam: (last) => (last.has_more && last.next_after != null ? last.next_after : undefined),
    enabled: !!name,
  })
}
