import { useMutation, useQuery } from '@tanstack/react-query'
import { api } from './client'
import type { TsMeasurement, TsMeasurementDetail, TsDataResponse, TsFloqlResponse } from './types'

const enc = encodeURIComponent

/** All measurements (with per-measurement series/field/point counts).
    Namespace-scoped server-side via `?namespace=`. */
export function useMeasurements(ns: string) {
  return useQuery({
    queryKey: ['timeseries', ns],
    queryFn: () => api.get<TsMeasurement[]>(`timeseries?namespace=${enc(ns)}`),
  })
}

export function useMeasurementDetail(ns: string, name: string | null) {
  return useQuery({
    queryKey: ['timeseries', ns, 'detail', name],
    queryFn: () => api.get<TsMeasurementDetail>(`timeseries/${enc(name!)}?namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

/** Raw points for one measurement+field over the whole retained window. */
export function useSeriesData(ns: string, name: string | null, field: string) {
  return useQuery({
    queryKey: ['timeseries', ns, 'data', name, field],
    queryFn: () =>
      api.get<TsDataResponse>(`timeseries/${enc(name!)}/data?field=${enc(field)}&namespace=${enc(ns)}`),
    enabled: !!name,
  })
}

/** Run a FloQL query. The server parses it, resolves the source across shards
    and executes the pipeline, returning the computed series. Namespace-scoped
    server-side via `?namespace=`. */
export function useFloql(ns: string) {
  return useMutation({
    mutationFn: (q: string) =>
      api.get<TsFloqlResponse>(`timeseries/floql?q=${enc(q)}&namespace=${enc(ns)}`),
  })
}
