import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type { ActionInfo, ActionDetailInfo, ActionRun, ActionInvokeResult } from './types'

const enc = encodeURIComponent

/** Registered actions in a namespace (actions are namespace-scoped server-side). */
export function useActions(ns: string) {
  return useQuery({
    queryKey: ['actions', ns],
    queryFn: () => api.get<ActionInfo[]>(`actions?namespace=${enc(ns)}`),
  })
}

export function useActionDetail(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['actions', ns, 'detail', name],
    queryFn: () => api.get<ActionDetailInfo>(`actions/${enc(name!)}?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

export function useActionRuns(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['actions', ns, 'runs', name],
    queryFn: () => api.get<ActionRun[]>(`actions/${enc(name!)}/runs?namespace=${enc(ns)}&limit=100`),
    enabled: !!name,
  })
}

/** Invoke an action (real loopback write — async, returns a run_id).
    `input` is sent as the JSON request body (stringified once by the client) and
    becomes the action's input payload. */
export function useInvokeAction(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ name, input }: { name: string; input: unknown }) =>
      api.post<ActionInvokeResult>(`actions/${enc(name)}/invoke?namespace=${enc(ns)}`, input),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['actions'] }),
  })
}
