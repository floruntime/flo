import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from './client'
import type { ProcessingJobInfo, ProcessingJobDetail, ProcessingMutationResult } from './types'

const enc = encodeURIComponent

/** Processing jobs in a namespace (jobs are namespace-scoped server-side). */
export function useJobs(ns: string) {
  return useQuery({
    queryKey: ['processing', ns],
    queryFn: () => api.get<ProcessingJobInfo[]>(`processing/jobs?namespace=${enc(ns)}`),
    // records_processed climbs while a job runs — keep it reasonably fresh.
    refetchInterval: 5_000,
  })
}

/** One job. The server looks it up in `ns` (default "default"), so pass the job's own. */
export function useJobDetail(ns: string, id: string | null) {
  return useQuery({
    queryKey: ['processing', ns, 'detail', id],
    queryFn: () => api.get<ProcessingJobDetail>(`processing/jobs/${enc(id!)}?namespace=${enc(ns)}`),
    enabled: !!id,
    refetchInterval: 5_000,
  })
}

/** Submit a new pipeline (real loopback write — body is the YAML definition). */
export function useSubmitJob(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (yaml: string) =>
      api.postText<ProcessingMutationResult>(`processing/jobs?namespace=${enc(ns)}`, yaml),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['processing'] }),
  })
}

/** Gracefully stop a job (real loopback write). */
export function useStopJob(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) =>
      api.put<ProcessingMutationResult>(`processing/jobs/${enc(id)}/stop?namespace=${enc(ns)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['processing'] }),
  })
}

/** Force-cancel a job (real loopback write). */
export function useCancelJob(ns: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) =>
      api.del<ProcessingMutationResult>(`processing/jobs/${enc(id)}?namespace=${enc(ns)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['processing'] }),
  })
}
