import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
    Activity, TrendingUp, Hash, Database,
    Filter, ChevronRight, Plus, BarChart3
} from "lucide-react";
import { DataTable, type TableColumn } from "../components/ui/DataTable";
import { FilterBar } from "../components/ui/FilterBar";
import { StatCard } from "../components/ui/StatCard";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import type { TsMeasurement } from "../lib/ts-types";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { useNamespace } from "../lib/NamespaceContext";

// =============================================================================
// Helpers
// =============================================================================

function formatNumber(n: number | undefined | null): string {
    if (n == null) return '0';
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

// =============================================================================
// Activity Badge
// =============================================================================

function MeasurementActivityBadge({ measurement }: { measurement: TsMeasurement }) {
    if (measurement.series_count === 0 && measurement.field_count === 0) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium text-text-secondary bg-surface-hover">
                Empty
            </span>
        );
    }
    if (measurement.series_count > 100) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium text-primary bg-primary/10">
                <Activity className="w-3 h-3" /> High Cardinality
            </span>
        );
    }
    return (
        <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium text-text-secondary bg-surface-hover">
            Active
        </span>
    );
}

// =============================================================================
// Column Definitions
// =============================================================================

function buildColumns(): TableColumn<TsMeasurement>[] {
    return [
        {
            key: 'name',
            label: 'Measurement',
            width: '35%',
            renderCell: (m) => (
                <div className="flex items-center gap-2.5">
                    <div className="p-1.5 rounded bg-surface-hover text-text-secondary group-hover:text-primary transition-colors">
                        <BarChart3 className="w-4 h-4" />
                    </div>
                    <div className="flex flex-col">
                        <span className="font-medium text-text-primary group-hover:text-primary transition-colors">
                            {m.name}
                        </span>
                    </div>
                </div>
            ),
        },
        {
            key: 'status',
            label: 'Status',
            width: '14%',
            renderCell: (m) => <MeasurementActivityBadge measurement={m} />,
        },
        {
            key: 'fields',
            label: 'Fields',
            width: '12%',
            renderCell: (m) => (
                <span className="tabular-nums text-text-secondary">{m.field_count}</span>
            ),
        },
        {
            key: 'series_count',
            label: 'Series',
            width: '12%',
            renderCell: (m) => (
                <span className="tabular-nums text-text-secondary">{formatNumber(m.series_count)}</span>
            ),
        },
        {
            key: 'field_list',
            label: 'Field Names',
            renderCell: (m) => (
                <div className="flex flex-wrap gap-1">
                    {m.fields.slice(0, 4).map((f) => (
                        <span
                            key={f}
                            className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-surface-hover text-text-secondary"
                        >
                            {f}
                        </span>
                    ))}
                    {m.fields.length > 4 && (
                        <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-surface-hover text-text-secondary">
                            +{m.fields.length - 4}
                        </span>
                    )}
                </div>
            ),
        },
        {
            key: '_arrow',
            label: '',
            width: '40px',
            renderCell: () => (
                <ChevronRight className="w-4 h-4 text-text-secondary/40 group-hover:text-primary transition-colors" />
            ),
        },
    ];
}

// =============================================================================
// Filter Options
// =============================================================================

type StatusFilter = 'all' | 'active' | 'empty' | 'high-cardinality';

const STATUS_OPTIONS = [
    { value: 'all', label: 'Status' },
    { value: 'active', label: 'Active' },
    { value: 'empty', label: 'Empty' },
    { value: 'high-cardinality', label: 'High Cardinality' },
];

function applyStatusFilter(measurements: TsMeasurement[], filter: StatusFilter): TsMeasurement[] {
    if (filter === 'active') return measurements.filter(m => m.series_count > 0);
    if (filter === 'empty') return measurements.filter(m => m.series_count === 0 && m.field_count === 0);
    if (filter === 'high-cardinality') return measurements.filter(m => m.series_count > 100);
    return measurements;
}

// =============================================================================
// Main Page
// =============================================================================

const COLUMNS = buildColumns();

export function TimeSeriesList() {
    const navigate = useNavigate();
    const { selected: namespace } = useNamespace();
    const [search, setSearch] = useState('');
    const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');

    const { data: measurements, loading, error, refetch } = useApi(
        () => api.getTimeSeries(namespace || undefined),
        [namespace],
        5000
    );

    const allMeasurements = measurements ?? [];

    const filtered = useMemo(() => {
        let result = applyStatusFilter(allMeasurements, statusFilter);
        if (search) {
            const q = search.toLowerCase();
            result = result.filter(m => m.name.toLowerCase().includes(q));
        }
        return result;
    }, [allMeasurements, statusFilter, search]);

    const totalFields = allMeasurements.reduce((a, m) => a + m.field_count, 0);
    const totalSeries = allMeasurements.reduce((a, m) => a + m.series_count, 0);
    const highCardinality = allMeasurements.filter(m => m.series_count > 100).length;

    if (loading && !measurements) return <LoadingState />;
    if (error && !measurements) return <ErrorState message={error} onRetry={refetch} />;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-semibold text-text-primary">Time Series</h1>
                <p className="text-text-secondary mt-1 text-sm">
                    InfluxDB-compatible time-series measurements with schema-on-write, tag indexing, and FloQL queries.
                </p>
            </div>

            {/* Stats */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard
                    label="Measurements"
                    value={allMeasurements.length}
                    subtitle="in this namespace"
                    icon={Database}
                />
                <StatCard
                    label="Total Fields"
                    value={formatNumber(totalFields)}
                    subtitle="across all measurements"
                    icon={Hash}
                />
                <StatCard
                    label="Total Series"
                    value={formatNumber(totalSeries)}
                    subtitle="unique tag combinations"
                    icon={TrendingUp}
                />
                <StatCard
                    label="High Cardinality"
                    value={highCardinality}
                    subtitle={highCardinality > 0
                        ? `${highCardinality} measurement${highCardinality !== 1 ? 's' : ''} with 100+ series`
                        : 'all measurements normal'}
                    icon={Activity}
                    alert={highCardinality > 0}
                />
            </div>

            {/* Filter bar + table */}
            <div className="flex flex-col gap-4">
                <FilterBar
                    search={search}
                    onSearchChange={setSearch}
                    searchPlaceholder="Search measurements..."
                    selects={[
                        {
                            icon: Filter,
                            value: statusFilter,
                            onChange: v => setStatusFilter(v as StatusFilter),
                            options: STATUS_OPTIONS,
                        },
                    ]}
                    actions={
                        <button className={cn(
                            "flex items-center gap-2 bg-primary hover:bg-primary/90",
                            "text-background font-medium px-4 py-1.5 rounded-md transition-colors text-sm"
                        )}>
                            <Plus className="w-4 h-4" /> Write Data
                        </button>
                    }
                    resultCount={filtered.length}
                    resultLabel="measurement"
                />

                <DataTable
                    columns={COLUMNS}
                    rows={filtered}
                    rowKey={m => `${m.namespace}/${m.name}`}
                    onRowClick={m => navigate(`/timeseries/${m.name}`)}
                    emptyState={
                        <span className="text-sm">
                            {allMeasurements.length === 0
                                ? 'No measurements yet. Write time-series data via ts_write or InfluxDB line protocol to get started.'
                                : 'No measurements match this filter.'}
                        </span>
                    }
                />
            </div>
        </div>
    );
}
