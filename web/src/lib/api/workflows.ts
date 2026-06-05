import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type {
  WorkflowDefInfo,
  WorkflowDefDetail,
  WorkflowRunInfo,
  WorkflowRunDetail,
  WorkflowHistoryEvent,
  WorkflowMutationResult,
} from './types'

const enc = encodeURIComponent

/** Workflow definitions in a namespace (namespace-scoped server-side). */
export function useWorkflows(ns: string) {
  return useQuery({
    queryKey: ['workflows', ns],
    queryFn: () => api.get<WorkflowDefInfo[]>(`workflow/definitions?namespace=${enc(ns)}`),
  })
}

export function useWorkflowDef(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['workflows', ns, 'def', name],
    queryFn: () => api.get<WorkflowDefDetail>(`workflow/definitions/${enc(name!)}?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

/** All runs in a namespace (used for the list join + per-definition run lists). */
export function useRuns(ns: string) {
  return useQuery({
    queryKey: ['workflows', ns, 'runs'],
    queryFn: () => api.get<WorkflowRunInfo[]>(`workflow/runs?namespace=${enc(ns)}&limit=200`),
    refetchInterval: 5_000,
  })
}

/** A single run snapshot (run_id is globally unique — no namespace needed). */
export function useRunDetail(runId: string | null) {
  return useQuery({
    queryKey: ['workflows', 'run', runId],
    queryFn: () => api.get<WorkflowRunDetail>(`workflow/runs/${enc(runId!)}`),
    enabled: !!runId,
    refetchInterval: 5_000,
  })
}

export function useRunHistory(runId: string | null) {
  return useQuery({
    queryKey: ['workflows', 'run', runId, 'history'],
    queryFn: () => api.get<WorkflowHistoryEvent[]>(`workflow/runs/${enc(runId!)}/history`),
    enabled: !!runId,
    refetchInterval: 5_000,
  })
}

/** Start a run (real loopback write). `input` is sent as the raw JSON body. */
export function useStartRun(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ workflow, input }: { workflow: string; input: string }) =>
      api.postText<WorkflowMutationResult>(`workflow/runs?namespace=${enc(ns)}&workflow=${enc(workflow)}`, input),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workflows'] }),
  })
}

/** Cancel a run (real loopback write). */
export function useCancelRun(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) =>
      api.del<WorkflowMutationResult>(`workflow/runs/${enc(runId)}?namespace=${enc(ns)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workflows'] }),
  })
}

/** Enable/disable a workflow definition (real loopback write). */
export function useToggleWorkflow(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ name, enable }: { name: string; enable: boolean }) =>
      api.put<WorkflowMutationResult>(
        `workflow/definitions/${enc(name)}/${enable ? 'enable' : 'disable'}?namespace=${enc(ns)}`,
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['workflows'] }),
  })
}
