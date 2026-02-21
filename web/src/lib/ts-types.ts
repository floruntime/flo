/**
 * Flo-TS Time-Series Dashboard Types
 *
 * Types for the Time-Series primitive dashboard UI.
 * Matches the JSON shapes returned by /api/v1/timeseries endpoints.
 */

// =============================================================================
// Measurement List (GET /timeseries)
// =============================================================================

/** Measurement summary returned from GET /timeseries */
export interface TsMeasurement {
  name: string;
  namespace: string;
  field_count: number;
  fields: string[];
  series_count: number;
}

// =============================================================================
// Measurement Detail (GET /timeseries/:measurement)
// =============================================================================

/** Full measurement detail from GET /timeseries/:measurement */
export interface TsMeasurementDetail {
  name: string;
  namespace: string;
  field_count: number;
  fields: string[];
  series_count: number;
  series: TsSeriesInfo[];
  retention: string | null;
}

/** Individual series within a measurement */
export interface TsSeriesInfo {
  canonical_key: string;
  approx_count: number;
  last_write_ms: number;
  created_at_ms: number;
  tags: Record<string, string>;
}

// =============================================================================
// Data Point Types (for future data explorer)
// =============================================================================

/** A single data point (timestamp + value) */
export interface TsDataPoint {
  /** Timestamp in milliseconds (epoch) */
  timestamp_ms: number;
  /** Metric value */
  value: number;
}

/** Compact data point (from /data endpoint wire format) */
export interface TsCompactPoint {
  t: number;
  v: number;
}

/** Query result with aggregated data */
export interface TsQueryResult {
  measurement: string;
  field: string;
  series_key: string;
  points: TsDataPoint[];
  aggregation?: string;
  window_ms?: number;
}

// =============================================================================
// Data Response (GET /timeseries/:measurement/data)
// =============================================================================

/** Series data with points from the data endpoint */
export interface TsSeriesData {
  key: string;
  points: TsCompactPoint[];
}

/** Response from GET /timeseries/:measurement/data */
export interface TsDataResponse {
  measurement: string;
  field: string;
  aggregation: string;
  window_ms: number;
  from_ms: number;
  to_ms: number;
  series: TsSeriesData[];
}

// =============================================================================
// FloQL Response (POST /timeseries/floql)
// =============================================================================

/** A series returned from a FloQL query (includes field per series) */
export interface TsFloqlSeries {
  key: string;
  field: string;
  points: TsCompactPoint[];
}

/** Response from POST /timeseries/floql */
export interface TsFloqlResponse {
  query: string;
  series: TsFloqlSeries[];
}
