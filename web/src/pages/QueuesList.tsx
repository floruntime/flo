import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { AlertCircle, Activity, Inbox, MessageSquare, Plus, Filter, ChevronRight } from "lucide-react";
import { DataTable, type TableColumn } from "../components/ui/DataTable";
import { FilterBar } from "../components/ui/FilterBar";
import { StatCard } from "../components/ui/StatCard";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import type { QueueInfo } from "../lib/api";
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
// Health Badge
// =============================================================================

function QueueHealthBadge({ queue }: { queue: QueueInfo }) {
    const dlq = queue.dlq_count ?? 0;
    if (dlq > 0 && queue.pending > 50) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium bg-error/10 text-error">
                <AlertCircle className="w-3 h-3" /> Unhealthy
            </span>
        );
    }
    if (dlq > 0) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium bg-error/10 text-error">
                <AlertCircle className="w-3 h-3" /> DLQ
            </span>
        );
    }
    if (queue.pending > 50) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium text-text-secondary bg-surface-hover">
                Backlog
            </span>
        );
    }
    if (queue.available === 0 && queue.pending === 0) {
        return (
            <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium text-text-secondary bg-surface-hover">
                Idle
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
// Column definitions
// =============================================================================

function buildColumns(): TableColumn<QueueInfo>[] {
    return [
        {
            key: 'name',
            label: 'Queue',
            width: '35%',
            renderCell: (q) => (
                <div className="flex items-center gap-2.5">
                    <div className="p-1.5 rounded bg-surface-hover text-text-secondary group-hover:text-primary transition-colors">
                        <MessageSquare className="w-4 h-4" />
                    </div>
                    <span className="font-medium text-text-primary group-hover:text-primary transition-colors">
                        {q.name}
                    </span>
                </div>
            ),
        },
        {
            key: 'health',
            label: 'Health',
            width: '12%',
            renderCell: (q) => <QueueHealthBadge queue={q} />,
        },
        {
            key: 'available',
            label: 'Available',
            width: '10%',
            renderCell: (q) => (
                <span className="tabular-nums text-text-secondary">{formatNumber(q.available)}</span>
            ),
        },
        {
            key: 'pending',
            label: 'In-Flight',
            width: '10%',
            renderCell: (q) => (
                <span className="tabular-nums text-text-secondary">{formatNumber(q.pending)}</span>
            ),
        },
        {
            key: 'enqueued',
            label: 'Enqueued / Dequeued',
            renderCell: (q) => (
                <span className="tabular-nums text-text-secondary">
                    {formatNumber(q.enqueued)} / {formatNumber(q.dequeued)}
                </span>
            ),
        },
        {
            key: 'dlq_count',
            label: 'DLQ',
            width: '8%',
            renderCell: (q) => {
                const dlq = q.dlq_count ?? 0;
                return dlq > 0
                    ? <span className="tabular-nums font-medium text-error">{dlq}</span>
                    : <span className="text-text-secondary">—</span>;
            },
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
// Filter options
// =============================================================================

type HealthFilter = 'all' | 'active' | 'dlq' | 'idle';

const HEALTH_OPTIONS = [
    { value: 'all',    label: 'Status' },
    { value: 'active', label: 'Active' },
    { value: 'dlq',    label: 'Has DLQ' },
    { value: 'idle',   label: 'Idle' },
];

function applyHealthFilter(queues: QueueInfo[], filter: HealthFilter): QueueInfo[] {
    if (filter === 'active') return queues.filter(q => q.available > 0 || q.pending > 0);
    if (filter === 'dlq')    return queues.filter(q => (q.dlq_count ?? 0) > 0);
    if (filter === 'idle')   return queues.filter(q => q.available === 0 && q.pending === 0);
    return queues;
}

// =============================================================================
// Main Page
// =============================================================================

const COLUMNS = buildColumns();

export function QueuesList() {
    const navigate = useNavigate();
    const { selected: namespace } = useNamespace();
    const [search, setSearch] = useState('');
    const [healthFilter, setHealthFilter] = useState<HealthFilter>('all');

    const { data: queues, loading, error, refetch } = useApi(
        () => api.getQueues(namespace || undefined),
        [namespace],
        5000
    );

    const allQueues = queues ?? [];

    const filtered = useMemo(() => {
        let result = applyHealthFilter(allQueues, healthFilter);
        if (search) {
            const q = search.toLowerCase();
            result = result.filter(queue => queue.name.toLowerCase().includes(q));
        }
        return result;
    }, [allQueues, healthFilter, search]);

    const totalAvailable = allQueues.reduce((a, q) => a + q.available, 0);
    const totalPending   = allQueues.reduce((a, q) => a + q.pending, 0);
    const totalDLQ       = allQueues.reduce((a, q) => a + (q.dlq_count ?? 0), 0);

    if (loading && !queues) return <LoadingState />;
    if (error && !queues) return <ErrorState message={error} onRetry={refetch} />;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-semibold text-text-primary">Queues</h1>
                <p className="text-text-secondary mt-1 text-sm">
                    Priority-based worker queues with leases, delays, and dead-letter handling.
                </p>
            </div>

            {/* Stats */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard label="Queues"       value={allQueues.length}          subtitle="in this namespace"    icon={MessageSquare} />
                <StatCard label="Available"    value={formatNumber(totalAvailable)} subtitle="ready for processing" icon={Inbox} />
                <StatCard label="In-Flight"    value={formatNumber(totalPending)}   subtitle="leased to workers"    icon={Activity} />
                <StatCard
                    label="Dead Letters"
                    value={formatNumber(totalDLQ)}
                    subtitle={totalDLQ > 0
                        ? `across ${allQueues.filter(q => (q.dlq_count ?? 0) > 0).length} queues`
                        : 'all queues clean'}
                    icon={AlertCircle}
                    alert={totalDLQ > 0}
                />
            </div>

            {/* Filter bar + table */}
            <div className="flex flex-col gap-4">
                <FilterBar
                    search={search}
                    onSearchChange={setSearch}
                    searchPlaceholder="Search queues..."
                    selects={[
                        {
                            icon: Filter,
                            value: healthFilter,
                            onChange: v => setHealthFilter(v as HealthFilter),
                            options: HEALTH_OPTIONS,
                        },
                    ]}
                    actions={
                        <button className={cn(
                            "flex items-center gap-2 bg-primary hover:bg-primary/90",
                            "text-background font-medium px-4 py-1.5 rounded-md transition-colors text-sm"
                        )}>
                            <Plus className="w-4 h-4" /> New Queue
                        </button>
                    }
                    resultCount={filtered.length}
                    resultLabel="queue"
                />

                <DataTable
                    columns={COLUMNS}
                    rows={filtered}
                    rowKey={q => `${q.namespace}/${q.name}`}
                    onRowClick={q => navigate(`/queues/${q.name}`)}
                    emptyState={
                        <span className="text-sm">
                            {allQueues.length === 0
                                ? 'No queues yet. Create a queue to start processing messages.'
                                : 'No queues match this filter.'}
                        </span>
                    }
                />
            </div>
        </div>
    );
}

