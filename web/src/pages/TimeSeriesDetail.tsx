import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import {
    ChevronLeft, BarChart3, Hash, TrendingUp,
    Clock, Tag, Database, Activity,
    Copy, Terminal, Filter, Search, Check, X
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card";
import { PageTabs } from "../components/ui/PageTabs";
import { DataTable, type TableColumn } from "../components/ui/DataTable";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import type { TsMeasurementDetail, TsSeriesInfo, TsDataResponse, TsSeriesData } from "../lib/ts-types";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { useNamespace } from "../lib/NamespaceContext";
import { TimeSeriesChart, SERIES_COLORS, shortSeriesKey, DEFAULT_MAX_SERIES } from "../components/ts/TimeSeriesChart";

// =============================================================================
// Helpers
// =============================================================================

function formatNumber(n: number | undefined | null): string {
    if (n == null) return '0';
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

function formatTimeAgo(ms: number): string {
    if (!ms) return 'never';
    const diff = Date.now() - ms;
    if (diff < 0) return 'just now';
    if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
    return `${Math.floor(diff / 86_400_000)}d ago`;
}

function formatDate(ms: number): string {
    if (!ms) return '—';
    return new Date(ms).toLocaleString();
}

// =============================================================================
// Metric Card (compact)
// =============================================================================

function MetricCard({ label, value, icon: Icon, iconColor, subtitle }: {
    label: string;
    value: React.ReactNode;
    icon: typeof Activity;
    iconColor: string;
    subtitle?: string;
}) {
    return (
        <Card>
            <CardContent className="p-4">
                <div className="flex items-center justify-between mb-2">
                    <span className="text-xs text-text-secondary uppercase tracking-wider">{label}</span>
                    <Icon className={cn("w-4 h-4", iconColor)} />
                </div>
                <p className="text-2xl font-semibold text-text-primary">{value}</p>
                {subtitle && <p className="text-xs text-text-secondary mt-1">{subtitle}</p>}
            </CardContent>
        </Card>
    );
}

// =============================================================================
// Tab: Overview
// =============================================================================

function OverviewTab({ detail }: { detail: TsMeasurementDetail }) {
    return (
        <div className="space-y-6">
            {/* Key Metrics */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <MetricCard
                    label="Fields"
                    value={detail.field_count}
                    icon={Hash}
                    iconColor="text-primary"
                    subtitle="schema-on-write"
                />
                <MetricCard
                    label="Series"
                    value={formatNumber(detail.series_count)}
                    icon={TrendingUp}
                    iconColor="text-text-secondary"
                    subtitle="unique tag combinations"
                />
                <MetricCard
                    label="Retention"
                    value={detail.retention || 'Infinite'}
                    icon={Clock}
                    iconColor="text-text-secondary"
                    subtitle={detail.retention ? 'configured' : 'no expiry set'}
                />
                <MetricCard
                    label="Namespace"
                    value={detail.namespace}
                    icon={Database}
                    iconColor="text-text-secondary"
                />
            </div>

            {/* Fields List */}
            <Card>
                <div className="p-4 border-b border-surface-border">
                    <h3 className="text-sm font-medium text-text-primary flex items-center gap-2">
                        <Hash className="w-4 h-4 text-text-secondary" />
                        Field Registry
                    </h3>
                    <p className="text-xs text-text-secondary mt-1">
                        Fields are discovered automatically via schema-on-write. Each field stores float64 values.
                    </p>
                </div>
                <div className="p-4">
                    {detail.fields.length === 0 ? (
                        <p className="text-sm text-text-secondary">No fields registered yet.</p>
                    ) : (
                        <div className="flex flex-wrap gap-2">
                            {detail.fields.map((field) => (
                                <span
                                    key={field}
                                    className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-surface-hover border border-surface-border text-sm font-mono text-text-primary"
                                >
                                    <Hash className="w-3 h-3 text-text-secondary" />
                                    {field}
                                    <span className="text-[10px] text-text-secondary ml-1">f64</span>
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            </Card>

            {/* Series Quick Summary */}
            {detail.series.length > 0 && (
                <Card>
                    <div className="p-4 border-b border-surface-border">
                        <h3 className="text-sm font-medium text-text-primary flex items-center gap-2">
                            <TrendingUp className="w-4 h-4 text-text-secondary" />
                            Top Series
                        </h3>
                        <p className="text-xs text-text-secondary mt-1">
                            Showing up to 10 series by data point count.
                        </p>
                    </div>
                    <div className="divide-y divide-surface-border">
                        {detail.series
                            .sort((a, b) => b.approx_count - a.approx_count)
                            .slice(0, 10)
                            .map((s) => (
                                <div key={s.canonical_key} className="px-4 py-3 flex items-center justify-between">
                                    <div className="flex items-center gap-3 min-w-0">
                                        <BarChart3 className="w-4 h-4 text-text-secondary shrink-0" />
                                        <div className="min-w-0">
                                            <p className="text-sm font-mono text-text-primary truncate">
                                                {s.canonical_key}
                                            </p>
                                            <div className="flex flex-wrap gap-1 mt-1">
                                                {Object.entries(s.tags).map(([k, v]) => (
                                                    <span
                                                        key={k}
                                                        className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-primary/10 text-primary"
                                                    >
                                                        {k}={v}
                                                    </span>
                                                ))}
                                            </div>
                                        </div>
                                    </div>
                                    <div className="text-right shrink-0 ml-4">
                                        <p className="text-sm tabular-nums text-text-primary">
                                            {formatNumber(s.approx_count)} pts
                                        </p>
                                        <p className="text-xs text-text-secondary">
                                            {formatTimeAgo(s.last_write_ms)}
                                        </p>
                                    </div>
                                </div>
                            ))}
                    </div>
                </Card>
            )}
        </div>
    );
}

// =============================================================================
// Tab: Series
// =============================================================================

const SERIES_COLUMNS: TableColumn<TsSeriesInfo>[] = [
    {
        key: 'canonical_key',
        label: 'Series Key',
        width: '40%',
        renderCell: (s) => (
            <div className="flex items-center gap-2">
                <BarChart3 className="w-4 h-4 text-text-secondary shrink-0" />
                <span className="font-mono text-sm text-text-primary truncate">{s.canonical_key}</span>
            </div>
        ),
    },
    {
        key: 'tags',
        label: 'Tags',
        renderCell: (s) => (
            <div className="flex flex-wrap gap-1">
                {Object.entries(s.tags).map(([k, v]) => (
                    <span
                        key={k}
                        className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-primary/10 text-primary"
                    >
                        {k}={v}
                    </span>
                ))}
            </div>
        ),
    },
    {
        key: 'approx_count',
        label: 'Points',
        width: '10%',
        renderCell: (s) => (
            <span className="tabular-nums text-text-secondary">{formatNumber(s.approx_count)}</span>
        ),
    },
    {
        key: 'last_write_ms',
        label: 'Last Write',
        width: '15%',
        renderCell: (s) => (
            <span className="text-text-secondary text-sm" title={formatDate(s.last_write_ms)}>
                {formatTimeAgo(s.last_write_ms)}
            </span>
        ),
    },
    {
        key: 'created_at_ms',
        label: 'Created',
        width: '15%',
        renderCell: (s) => (
            <span className="text-text-secondary text-sm" title={formatDate(s.created_at_ms)}>
                {formatTimeAgo(s.created_at_ms)}
            </span>
        ),
    },
];

function SeriesTab({ detail }: { detail: TsMeasurementDetail }) {
    const [searchTags, setSearchTags] = useState('');

    const filtered = searchTags
        ? detail.series.filter(s =>
            s.canonical_key.toLowerCase().includes(searchTags.toLowerCase()) ||
            Object.entries(s.tags).some(([k, v]) =>
                `${k}=${v}`.toLowerCase().includes(searchTags.toLowerCase())
            )
        )
        : detail.series;

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Tag className="w-4 h-4 text-text-secondary" />
                    <span className="text-sm text-text-secondary">
                        {detail.series_count} series total
                    </span>
                </div>
                <div className="relative">
                    <input
                        type="text"
                        value={searchTags}
                        onChange={e => setSearchTags(e.target.value)}
                        placeholder="Filter by tag..."
                        className="pl-3 pr-3 py-1.5 text-sm rounded-md border border-surface-border bg-background text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-primary w-64"
                    />
                </div>
            </div>

            <DataTable
                columns={SERIES_COLUMNS}
                rows={filtered}
                rowKey={s => s.canonical_key}
                emptyState={
                    <span className="text-sm">
                        {detail.series.length === 0
                            ? 'No series yet. Write data points to create series.'
                            : 'No series match this filter.'}
                    </span>
                }
            />
        </div>
    );
}

// =============================================================================
// Tab: Query (FloQL)
// =============================================================================

function QueryTab({ detail, namespace }: { detail: TsMeasurementDetail; namespace: string | null }) {
    const defaultField = detail.fields[0] ?? 'value';
    const [query, setQuery] = useState(
        `${detail.name}[1h] | window(5m) | avg("${defaultField}")`
    );
    const [copied, setCopied] = useState(false);
    const [executing, setExecuting] = useState(false);
    const [result, setResult] = useState<import('../lib/ts-types').TsFloqlResponse | null>(null);
    const [queryError, setQueryError] = useState<string | null>(null);

    const handleCopy = () => {
        navigator.clipboard.writeText(query);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    const handleExecute = async () => {
        if (!query.trim()) return;
        setExecuting(true);
        setQueryError(null);
        setResult(null);
        try {
            const res = await api.executeFloqlQuery(query.trim(), namespace || undefined);
            // Backend returns {"error":"..."} with HTTP 200 for parse errors
            if ('error' in res && (res as Record<string, unknown>).error) {
                setQueryError(String((res as Record<string, unknown>).error));
                return;
            }
            setResult(res);
        } catch (err: unknown) {
            setQueryError(err instanceof Error ? err.message : String(err));
        } finally {
            setExecuting(false);
        }
    };

    // Convert FloQL result to TsSeriesData[] for the chart
    const chartSeries = useMemo(() => {
        if (!result?.series?.length) return [];
        return result.series.map((s) => ({ key: s.key, points: s.points }));
    }, [result]);

    // Build a color map for query results
    const queryColorMap = useMemo(() => {
        const map: Record<string, string> = {};
        chartSeries.forEach((s, i) => {
            map[s.key] = SERIES_COLORS[i % SERIES_COLORS.length];
        });
        return map;
    }, [chartSeries]);

    // Example queries for this measurement (using actual FloQL syntax)
    const examples = [
        {
            label: 'Average (5min window)',
            query: `${detail.name}[1h] | window(5m) | avg("${defaultField}")`,
        },
        {
            label: 'Sum over 15m windows',
            query: `${detail.name}[6h] | window(15m) | sum("${defaultField}")`,
        },
        {
            label: 'Max value (1h window)',
            query: `${detail.name}[24h] | window(1h) | max("${defaultField}")`,
        },
        {
            label: 'Count per 5m bucket',
            query: `${detail.name}[1h] | window(5m) | count("${defaultField}")`,
        },
    ];

    return (
        <div className="space-y-6">
            {/* Query Editor */}
            <Card>
                <div className="p-4 border-b border-surface-border flex items-center justify-between">
                    <h3 className="text-sm font-medium text-text-primary flex items-center gap-2">
                        <Terminal className="w-4 h-4 text-text-secondary" />
                        FloQL Query Editor
                    </h3>
                    <button
                        onClick={handleCopy}
                        className="flex items-center gap-1.5 text-xs text-text-secondary hover:text-text-primary transition-colors"
                    >
                        <Copy className="w-3.5 h-3.5" />
                        {copied ? 'Copied!' : 'Copy'}
                    </button>
                </div>
                <div className="p-4">
                    <textarea
                        value={query}
                        onChange={e => setQuery(e.target.value)}
                        rows={3}
                        spellCheck={false}
                        className="w-full font-mono text-sm p-3 rounded-md border border-surface-border bg-background text-text-primary placeholder:text-text-secondary/50 focus:outline-none focus:ring-1 focus:ring-primary resize-none"
                        placeholder="Enter FloQL query..."
                        onKeyDown={e => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); handleExecute(); } }}
                    />
                    <div className="flex items-center justify-between mt-3">
                        <p className="text-xs text-text-secondary">
                            <kbd className="px-1 py-0.5 rounded bg-surface-hover font-mono text-[10px]">
                                {navigator.platform.includes('Mac') ? '\u2318' : 'Ctrl'}+Enter
                            </kbd>
                            {' '}to execute &middot; syntax:{' '}
                            <code className="px-1 py-0.5 rounded bg-surface-hover font-mono text-[10px]">
                                measurement[range] | stage | stage
                            </code>
                        </p>
                        <button
                            onClick={handleExecute}
                            disabled={executing || !query.trim()}
                            className={cn(
                                "px-4 py-1.5 rounded-md text-sm font-medium transition-colors",
                                executing || !query.trim()
                                    ? "bg-primary/50 text-background cursor-not-allowed"
                                    : "bg-primary text-background hover:bg-primary/90 cursor-pointer"
                            )}
                        >
                            {executing ? 'Executing\u2026' : 'Execute'}
                        </button>
                    </div>
                </div>
            </Card>

            {/* Query Error */}
            {queryError && (
                <Card>
                    <div className="p-4 border-l-4 border-red-500 bg-red-500/10">
                        <div className="flex items-start gap-2">
                            <X className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
                            <div>
                                <p className="text-sm font-medium text-red-400">Query Error</p>
                                <p className="text-xs text-text-secondary mt-1 font-mono">{queryError}</p>
                            </div>
                        </div>
                    </div>
                </Card>
            )}

            {/* Query Results Chart */}
            {chartSeries.length > 0 && (
                <Card>
                    <div className="p-4 border-b border-surface-border flex items-center justify-between">
                        <div className="flex items-center gap-2">
                            <BarChart3 className="w-4 h-4 text-text-secondary" />
                            <h3 className="text-sm font-medium text-text-primary">
                                Query Results
                                <span className="text-text-secondary font-normal ml-1">
                                    &middot; {chartSeries.length} series &middot; {chartSeries.reduce((sum, s) => sum + s.points.length, 0)} points
                                </span>
                            </h3>
                        </div>
                    </div>
                    <div className="p-4">
                        <TimeSeriesChart series={chartSeries} height={320} colorMap={queryColorMap} />
                    </div>
                </Card>
            )}

            {/* Query Results Table */}
            {result && result.series.length > 0 && (
                <Card>
                    <div className="p-4 border-b border-surface-border flex items-center justify-between">
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-text-secondary" />
                            <h3 className="text-sm font-medium text-text-primary">Result Data</h3>
                        </div>
                        <span className="text-xs text-text-secondary">
                            {result.series.length} series
                        </span>
                    </div>
                    <div className="overflow-x-auto max-h-96">
                        <table className="w-full text-xs">
                            <thead className="sticky top-0 bg-surface">
                                <tr className="border-b border-surface-border">
                                    <th className="text-left px-4 py-2 text-text-secondary font-medium">Series</th>
                                    <th className="text-left px-4 py-2 text-text-secondary font-medium">Field</th>
                                    <th className="text-left px-4 py-2 text-text-secondary font-medium">Timestamp</th>
                                    <th className="text-right px-4 py-2 text-text-secondary font-medium">Value</th>
                                </tr>
                            </thead>
                            <tbody>
                                {result.series.flatMap((s) => {
                                    const pts = s.points.slice(-100);
                                    return pts.map((pt, i) => (
                                        <tr
                                            key={`${s.key}-${pt.t}`}
                                            className="border-b border-surface-border/50"
                                        >
                                            {i === 0 ? (
                                                <td className="px-4 py-1.5 font-mono text-text-primary" rowSpan={pts.length}>
                                                    <div className="flex items-center gap-1.5">
                                                        <span
                                                            className="w-2 h-2 rounded-sm flex-shrink-0"
                                                            style={{ backgroundColor: queryColorMap[s.key] ?? SERIES_COLORS[0] }}
                                                        />
                                                        {shortSeriesKey(s.key)}
                                                    </div>
                                                </td>
                                            ) : null}
                                            {i === 0 ? (
                                                <td className="px-4 py-1.5 font-mono text-text-secondary" rowSpan={pts.length}>
                                                    {s.field}
                                                </td>
                                            ) : null}
                                            <td className="px-4 py-1.5 text-text-secondary font-mono">
                                                {new Date(pt.t).toLocaleString()}
                                            </td>
                                            <td className="px-4 py-1.5 text-right font-mono text-text-primary tabular-nums">
                                                {pt.v.toFixed(2)}
                                            </td>
                                        </tr>
                                    ));
                                })}
                            </tbody>
                        </table>
                    </div>
                </Card>
            )}

            {/* No results message */}
            {result && result.series.length === 0 && (
                <Card>
                    <div className="p-8 text-center">
                        <p className="text-sm text-text-secondary">Query returned no results.</p>
                    </div>
                </Card>
            )}

            {/* Example Queries */}
            <Card>
                <div className="p-4 border-b border-surface-border">
                    <h3 className="text-sm font-medium text-text-primary">Example Queries</h3>
                    <p className="text-xs text-text-secondary mt-1">
                        Click to load into the editor above.
                    </p>
                </div>
                <div className="divide-y divide-surface-border">
                    {examples.map((ex) => (
                        <button
                            key={ex.label}
                            onClick={() => setQuery(ex.query)}
                            className="w-full text-left px-4 py-3 hover:bg-surface-hover transition-colors"
                        >
                            <p className="text-sm font-medium text-text-primary">{ex.label}</p>
                            <p className="text-xs font-mono text-text-secondary mt-1">{ex.query}</p>
                        </button>
                    ))}
                </div>
            </Card>

            {/* CLI Reference */}
            <Card>
                <div className="p-4 border-b border-surface-border">
                    <h3 className="text-sm font-medium text-text-primary">CLI Reference</h3>
                </div>
                <div className="p-4 space-y-3">
                    <div>
                        <p className="text-xs text-text-secondary mb-1">Write data (InfluxDB line protocol)</p>
                        <code className="block text-xs font-mono p-2 rounded bg-surface-hover text-text-primary">
                            flo ts write '{detail.name},host=web-01 {defaultField}=42.5'
                        </code>
                    </div>
                    <div>
                        <p className="text-xs text-text-secondary mb-1">Read latest values</p>
                        <code className="block text-xs font-mono p-2 rounded bg-surface-hover text-text-primary">
                            flo ts read {detail.name}
                        </code>
                    </div>
                    <div>
                        <p className="text-xs text-text-secondary mb-1">Execute FloQL query</p>
                        <code className="block text-xs font-mono p-2 rounded bg-surface-hover text-text-primary">
                            flo ts floql '{detail.name}[1h] | field({defaultField}) | window(5m) | avg()'
                        </code>
                    </div>
                </div>
            </Card>
        </div>
    );
}

// =============================================================================
// Tab: Explorer (Chart)
// =============================================================================

/** Time range presets */
const TIME_RANGES = [
    { label: '15m', ms: 15 * 60_000 },
    { label: '1h', ms: 60 * 60_000 },
    { label: '6h', ms: 6 * 60 * 60_000 },
    { label: '24h', ms: 24 * 60 * 60_000 },
    { label: '7d', ms: 7 * 24 * 60 * 60_000 },
] as const;

/** Auto-pick a useful window for a given time range (finer granularity to avoid collapsing) */
function autoWindow(rangeMs: number): number {
    if (rangeMs <= 15 * 60_000) return 5_000;           // 15m → 5s  windows (180 max buckets)
    if (rangeMs <= 60 * 60_000) return 10_000;          // 1h  → 10s windows (360 max buckets)
    if (rangeMs <= 6 * 60 * 60_000) return 60_000;      // 6h  → 1m  windows (360 max buckets)
    if (rangeMs <= 24 * 60 * 60_000) return 5 * 60_000;  // 24h → 5m  windows (288 max buckets)
    return 30 * 60_000;                                  // 7d  → 30m windows (336 max buckets)
}

/** Window size presets for manual selection */
const WINDOW_PRESETS = [
    { label: '1s',  ms: 1_000 },
    { label: '5s',  ms: 5_000 },
    { label: '10s', ms: 10_000 },
    { label: '30s', ms: 30_000 },
    { label: '1m',  ms: 60_000 },
    { label: '5m',  ms: 5 * 60_000 },
    { label: '15m', ms: 15 * 60_000 },
    { label: '1h',  ms: 60 * 60_000 },
] as const;

const AGGREGATIONS = ['avg', 'sum', 'min', 'max', 'count'] as const;

function ExplorerTab({ detail, namespace }: { detail: TsMeasurementDetail; namespace: string | null }) {
    const [rangeMs, setRangeMs] = useState(60 * 60_000); // default 1h
    const [field, setField] = useState(detail.fields[0] ?? 'value');
    const [aggregation, setAggregation] = useState<string>('avg');
    const [customWindowMs, setCustomWindowMs] = useState<number | null>(null); // null = auto

    // Series filter state — pre-populate from detail.series immediately
    const initialKeys = useMemo(() => {
        const sorted = [...detail.series]
            .sort((a, b) => b.approx_count - a.approx_count)
            .slice(0, DEFAULT_MAX_SERIES);
        return new Set(sorted.map((s) => s.canonical_key));
    }, [detail.series]);
    const [selectedKeys, setSelectedKeys] = useState<Set<string>>(initialKeys);
    const [filterOpen, setFilterOpen] = useState(false);
    const [filterSearch, setFilterSearch] = useState('');
    const filterRef = useRef<HTMLDivElement>(null);

    const windowMs = useMemo(() => customWindowMs ?? autoWindow(rangeMs), [rangeMs, customWindowMs]);

    const fetcher = useCallback(() => {
        const now = Date.now();
        return api.getTimeSeriesData(detail.name, {
            namespace: namespace || undefined,
            field,
            from: now - rangeMs,
            to: now,
            window: windowMs,
            aggregation,
        });
    }, [detail.name, namespace, field, rangeMs, windowMs, aggregation]);

    const { data, loading, error, refetch } = useApi<TsDataResponse>(
        fetcher,
        [detail.name, namespace, field, rangeMs, windowMs, aggregation],
        0, // no auto-refresh; user controls refresh
    );

    // When data arrives, auto-select top N series by magnitude
    useEffect(() => {
        if (!data?.series?.length) return;
        const ranked = [...data.series]
            .map((s) => ({
                key: s.key,
                magnitude: s.points.reduce((sum, p) => sum + Math.abs(p.v), 0),
            }))
            .sort((a, b) => b.magnitude - a.magnitude);
        const top = ranked.slice(0, DEFAULT_MAX_SERIES).map((r) => r.key);
        setSelectedKeys(new Set(top));
    }, [data]);

    // Close dropdown on click outside
    useEffect(() => {
        function handleClick(e: MouseEvent) {
            if (filterRef.current && !filterRef.current.contains(e.target as Node)) {
                setFilterOpen(false);
            }
        }
        if (filterOpen) {
            document.addEventListener('mousedown', handleClick);
            return () => document.removeEventListener('mousedown', handleClick);
        }
    }, [filterOpen]);

    // Pre-populate allKeys from detail.series (instant), update when data arrives
    const allKeys = useMemo(() => {
        if (data?.series?.length) return data.series.map((s) => s.key);
        return detail.series.map((s) => s.canonical_key);
    }, [data, detail.series]);

    // Stable color map: series key → color (indexed by position in allKeys)
    const colorMap = useMemo(() => {
        const map: Record<string, string> = {};
        allKeys.forEach((key, i) => {
            map[key] = SERIES_COLORS[i % SERIES_COLORS.length];
        });
        return map;
    }, [allKeys]);
    const filteredDropdownKeys = useMemo(
        () =>
            filterSearch
                ? allKeys.filter((k) => k.toLowerCase().includes(filterSearch.toLowerCase()))
                : allKeys,
        [allKeys, filterSearch]
    );

    const filteredSeries: TsSeriesData[] = useMemo(
        () => (data?.series ?? []).filter((s) => selectedKeys.has(s.key)),
        [data, selectedKeys]
    );

    const totalPoints = filteredSeries.reduce((sum, s) => sum + s.points.length, 0);

    const toggleKey = (key: string) => {
        setSelectedKeys((prev) => {
            const next = new Set(prev);
            if (next.has(key)) next.delete(key);
            else next.add(key);
            return next;
        });
    };

    // When search is active, Select All / Clear apply only to visible (filtered) keys
    const selectAll = () => {
        if (filterSearch) {
            // Replace entire selection with only the filtered keys
            setSelectedKeys(new Set(filteredDropdownKeys));
        } else {
            setSelectedKeys(new Set(allKeys));
        }
    };
    const clearAll = () => {
        if (filterSearch) {
            // Remove only the filtered keys from current selection
            setSelectedKeys((prev) => {
                const next = new Set(prev);
                for (const k of filteredDropdownKeys) next.delete(k);
                return next;
            });
        } else {
            setSelectedKeys(new Set());
        }
    };

    return (
        <div className="space-y-4">
            {/* Controls */}
            <Card>
                <CardContent className="p-4">
                    <div className="flex flex-wrap items-center gap-4">
                        {/* Time range */}
                        <div className="flex items-center gap-1.5">
                            <Clock className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary mr-1">Range</span>
                            <div className="flex rounded-md border border-surface-border overflow-hidden">
                                {TIME_RANGES.map((r) => (
                                    <button
                                        key={r.label}
                                        onClick={() => setRangeMs(r.ms)}
                                        className={cn(
                                            "px-2.5 py-1 text-xs font-medium transition-colors",
                                            rangeMs === r.ms
                                                ? "bg-primary text-background"
                                                : "bg-surface hover:bg-surface-hover text-text-secondary"
                                        )}
                                    >
                                        {r.label}
                                    </button>
                                ))}
                            </div>
                        </div>

                        {/* Field selector */}
                        {detail.fields.length > 1 && (
                            <div className="flex items-center gap-1.5">
                                <Hash className="w-4 h-4 text-text-secondary" />
                                <select
                                    value={field}
                                    onChange={(e) => setField(e.target.value)}
                                    className="pl-2 pr-6 py-1 text-xs rounded-md border border-surface-border bg-surface text-text-primary focus:outline-none focus:ring-1 focus:ring-primary"
                                >
                                    {detail.fields.map((f) => (
                                        <option key={f} value={f}>{f}</option>
                                    ))}
                                </select>
                            </div>
                        )}

                        {/* Aggregation */}
                        <div className="flex items-center gap-1.5">
                            <Activity className="w-4 h-4 text-text-secondary" />
                            <select
                                value={aggregation}
                                onChange={(e) => setAggregation(e.target.value)}
                                className="pl-2 pr-6 py-1 text-xs rounded-md border border-surface-border bg-surface text-text-primary focus:outline-none focus:ring-1 focus:ring-primary"
                            >
                                {AGGREGATIONS.map((a) => (
                                    <option key={a} value={a}>{a}</option>
                                ))}
                            </select>
                        </div>

                        {/* Window size */}
                        <div className="flex items-center gap-1.5">
                            <Clock className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary mr-1">Window</span>
                            <select
                                value={customWindowMs ?? 0}
                                onChange={(e) => {
                                    const v = Number(e.target.value);
                                    setCustomWindowMs(v === 0 ? null : v);
                                }}
                                className="pl-2 pr-6 py-1 text-xs rounded-md border border-surface-border bg-surface text-text-primary focus:outline-none focus:ring-1 focus:ring-primary"
                            >
                                <option value={0}>auto ({autoWindow(rangeMs) / 1000}s)</option>
                                {WINDOW_PRESETS.map((w) => (
                                    <option key={w.ms} value={w.ms}>{w.label}</option>
                                ))}
                            </select>
                        </div>

                        {/* Series Filter Dropdown */}
                        {allKeys.length > 0 && (
                            <div className="relative" ref={filterRef}>
                                <button
                                    onClick={() => setFilterOpen((v) => !v)}
                                    className={cn(
                                        "flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border transition-colors",
                                        filterOpen
                                            ? "border-primary bg-primary/10 text-primary"
                                            : "border-surface-border bg-surface hover:bg-surface-hover text-text-secondary"
                                    )}
                                >
                                    <Filter className="w-3.5 h-3.5" />
                                    <span>{selectedKeys.size} of {allKeys.length} series</span>
                                </button>

                                {filterOpen && (
                                    <div className="absolute top-full left-0 mt-1 z-50 w-72 rounded-lg border border-surface-border bg-surface shadow-xl">
                                        {/* Search */}
                                        <div className="p-2 border-b border-surface-border">
                                            <div className="relative">
                                                <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-secondary" />
                                                <input
                                                    type="text"
                                                    placeholder="Search series..."
                                                    value={filterSearch}
                                                    onChange={(e) => setFilterSearch(e.target.value)}
                                                    className="w-full pl-7 pr-2 py-1.5 text-xs rounded-md border border-surface-border bg-background text-text-primary placeholder:text-text-secondary focus:outline-none focus:ring-1 focus:ring-primary"
                                                    autoFocus
                                                />
                                            </div>
                                        </div>
                                        {/* Select All / Clear — scoped to visible keys when searching */}
                                        <div className="flex items-center justify-between px-3 py-1.5 border-b border-surface-border">
                                            <button
                                                onClick={selectAll}
                                                className="text-xs text-primary hover:underline"
                                            >
                                                {filterSearch ? `Select Only These ${filteredDropdownKeys.length}` : 'Select All'}
                                            </button>
                                            <button
                                                onClick={clearAll}
                                                className="text-xs text-text-secondary hover:text-text-primary"
                                            >
                                                Clear
                                            </button>
                                        </div>
                                        {/* List */}
                                        <div className="max-h-64 overflow-y-auto py-1">
                                            {filteredDropdownKeys.length === 0 ? (
                                                <p className="text-xs text-text-secondary text-center py-3">No matching series</p>
                                            ) : (
                                                filteredDropdownKeys.map((key) => {
                                                    const idx = allKeys.indexOf(key);
                                                    const color = SERIES_COLORS[idx % SERIES_COLORS.length];
                                                    const checked = selectedKeys.has(key);
                                                    return (
                                                        <button
                                                            key={key}
                                                            onClick={() => toggleKey(key)}
                                                            className="w-full flex items-center gap-2 px-3 py-1.5 text-left hover:bg-surface-hover transition-colors"
                                                        >
                                                            <span
                                                                className={cn(
                                                                    "flex-shrink-0 w-4 h-4 rounded border flex items-center justify-center",
                                                                    checked
                                                                        ? "border-primary bg-primary"
                                                                        : "border-surface-border bg-surface"
                                                                )}
                                                            >
                                                                {checked && <Check className="w-3 h-3 text-background" />}
                                                            </span>
                                                            <span
                                                                className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                                                                style={{ backgroundColor: color }}
                                                            />
                                                            <span className="text-xs text-text-primary truncate">
                                                                {shortSeriesKey(key)}
                                                            </span>
                                                        </button>
                                                    );
                                                })
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Refresh */}
                        <button
                            onClick={refetch}
                            className="ml-auto px-3 py-1 text-xs rounded-md border border-surface-border bg-surface hover:bg-surface-hover text-text-primary transition-colors"
                        >
                            Refresh
                        </button>
                    </div>
                </CardContent>
            </Card>

            {/* Chart */}
            <Card>
                <div className="p-4 border-b border-surface-border flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <BarChart3 className="w-4 h-4 text-text-secondary" />
                        <h3 className="text-sm font-medium text-text-primary">
                            {detail.name}
                            <span className="text-text-secondary font-normal ml-1">
                                · {field} · {aggregation}
                            </span>
                        </h3>
                    </div>
                    {data && (
                        <span className="text-xs text-text-secondary">
                            showing {selectedKeys.size} of {allKeys.length} series · {totalPoints} points
                        </span>
                    )}
                </div>
                <div className="p-4">
                    {loading && !data ? (
                        <div className="flex items-center justify-center h-[320px]">
                            <div className="w-6 h-6 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                        </div>
                    ) : error && !data ? (
                        <div className="flex items-center justify-center h-[320px] text-sm text-text-secondary">
                            {error}
                        </div>
                    ) : data ? (
                        <TimeSeriesChart series={filteredSeries} height={320} colorMap={colorMap} />
                    ) : null}
                </div>
            </Card>

            {/* Raw Data Table */}
            {filteredSeries.length > 0 && (
                <Card>
                    <div className="p-4 border-b border-surface-border flex items-center justify-between">
                        <div className="flex items-center gap-2">
                            <Database className="w-4 h-4 text-text-secondary" />
                            <h3 className="text-sm font-medium text-text-primary">Raw Data</h3>
                        </div>
                        <span className="text-xs text-text-secondary">
                            {filteredSeries.length} series · last 100 points each
                        </span>
                    </div>
                    <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="border-b border-surface-border">
                                    <th className="text-left px-4 py-2 text-text-secondary font-medium">Series</th>
                                    <th className="text-left px-4 py-2 text-text-secondary font-medium">Timestamp</th>
                                    <th className="text-right px-4 py-2 text-text-secondary font-medium">Value</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredSeries.flatMap((s) => {
                                    const pts = s.points.slice(-100);
                                    return pts.map((pt, i) => (
                                        <tr
                                            key={`${s.key}-${pt.t}`}
                                            className={cn(
                                                "border-b border-surface-border/50",
                                                i === 0 ? "bg-surface-hover/30" : ""
                                            )}
                                        >
                                            {i === 0 ? (
                                                <td className="px-4 py-1.5 font-mono text-text-primary" rowSpan={pts.length}>
                                                    <div className="flex items-center gap-1.5">
                                                        <span
                                                            className="w-2 h-2 rounded-sm flex-shrink-0"
                                                            style={{ backgroundColor: colorMap[s.key] ?? SERIES_COLORS[0] }}
                                                        />
                                                        {shortSeriesKey(s.key)}
                                                    </div>
                                                </td>
                                            ) : null}
                                            <td className="px-4 py-1.5 text-text-secondary font-mono">
                                                {new Date(pt.t).toLocaleString()}
                                            </td>
                                            <td className="px-4 py-1.5 text-right font-mono text-text-primary tabular-nums">
                                                {pt.v.toFixed(2)}
                                            </td>
                                        </tr>
                                    ));
                                })}
                            </tbody>
                        </table>
                    </div>
                </Card>
            )}

        </div>
    );
}

// =============================================================================
// Main Page
// =============================================================================

type TabId = 'overview' | 'explorer' | 'series' | 'query';

const TABS = [
    { id: 'overview', label: 'Overview' },
    { id: 'explorer', label: 'Explorer' },
    { id: 'series', label: 'Series' },
    { id: 'query', label: 'FloQL' },
];

export function TimeSeriesDetail() {
    const { measurement } = useParams<{ measurement: string }>();
    const { selected: namespace } = useNamespace();
    const [activeTab, setActiveTab] = useState<TabId>('overview');

    const { data: detail, loading, error, refetch } = useApi(
        () => api.getTimeSeriesDetail(measurement!, namespace || undefined),
        [measurement, namespace],
        5000
    );

    if (loading && !detail) return <LoadingState />;
    if (error && !detail) return <ErrorState message={error} onRetry={refetch} />;
    if (!detail) return <ErrorState message="Measurement not found" onRetry={refetch} />;

    const tabsWithCounts = TABS.map(t => ({
        ...t,
        count: t.id === 'series' ? detail.series_count : undefined,
    }));

    return (
        <div className="space-y-6">
            {/* Back Link + Header */}
            <div>
                <Link
                    to="/timeseries"
                    className="inline-flex items-center gap-1 text-sm text-text-secondary hover:text-text-primary transition-colors mb-3"
                >
                    <ChevronLeft className="w-4 h-4" />
                    Back to Time Series
                </Link>

                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="p-2 rounded-lg bg-primary/10">
                            <BarChart3 className="w-5 h-5 text-primary" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-semibold text-text-primary">{detail.name}</h1>
                            <p className="text-sm text-text-secondary mt-0.5">
                                {detail.field_count} fields · {formatNumber(detail.series_count)} series · {detail.namespace}
                            </p>
                        </div>
                    </div>
                </div>
            </div>

            {/* Quick Stats Strip */}
            <div className="flex items-center gap-6 px-4 py-3 rounded-md border border-surface-border bg-surface">
                <div className="flex items-center gap-2">
                    <Hash className="w-4 h-4 text-text-secondary" />
                    <span className="text-sm text-text-secondary">Fields</span>
                    <span className="text-sm font-semibold text-text-primary">{detail.field_count}</span>
                </div>
                <div className="w-px h-4 bg-surface-border" />
                <div className="flex items-center gap-2">
                    <TrendingUp className="w-4 h-4 text-text-secondary" />
                    <span className="text-sm text-text-secondary">Series</span>
                    <span className="text-sm font-semibold text-text-primary">{formatNumber(detail.series_count)}</span>
                </div>
                <div className="w-px h-4 bg-surface-border" />
                <div className="flex items-center gap-2">
                    <Clock className="w-4 h-4 text-text-secondary" />
                    <span className="text-sm text-text-secondary">Retention</span>
                    <span className="text-sm font-semibold text-text-primary">{detail.retention || '∞'}</span>
                </div>
                <div className="w-px h-4 bg-surface-border" />
                <div className="flex items-center gap-2">
                    <Database className="w-4 h-4 text-text-secondary" />
                    <span className="text-sm text-text-secondary">Namespace</span>
                    <span className="text-sm font-semibold text-text-primary">{detail.namespace}</span>
                </div>
            </div>

            {/* Tabs */}
            <div>
                <PageTabs tabs={tabsWithCounts} activeTab={activeTab} onChange={id => setActiveTab(id as TabId)} />
                <div className="pt-6">
                    {activeTab === 'overview' && <OverviewTab detail={detail} />}
                    {activeTab === 'explorer' && <ExplorerTab detail={detail} namespace={namespace} />}
                    {activeTab === 'series' && <SeriesTab detail={detail} />}
                    {activeTab === 'query' && <QueryTab detail={detail} namespace={namespace} />}
                </div>
            </div>
        </div>
    );
}
