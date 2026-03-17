import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Activity, AlertCircle, ChevronRight, Filter, Users, Zap } from "lucide-react";
import { DataTable, type TableColumn } from "../components/ui/DataTable";
import { FilterBar } from "../components/ui/FilterBar";
import { StatCard } from "../components/ui/StatCard";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import type { ActionInfo } from "../lib/api";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { useNamespace } from "../lib/NamespaceContext";

// =============================================================================
// Helpers
// =============================================================================

function TypeBadge({ type }: { type: string }) {
    const isWasm = type === 'wasm';
    return (
        <span className={cn(
            "text-[10px] px-1.5 py-0.5 rounded font-medium uppercase tracking-wide",
            isWasm ? "bg-purple-400/10 text-purple-400" : "bg-zinc-400/10 text-zinc-400"
        )}>
            {isWasm ? "WASM" : "USER"}
        </span>
    );
}

function StatusBadge({ enabled }: { enabled: boolean }) {
    return (
        <span className={cn(
            "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium",
            enabled ? "bg-emerald-400/10 text-emerald-400" : "bg-zinc-400/10 text-zinc-500"
        )}>
            {enabled ? "Active" : "Disabled"}
        </span>
    );
}

// =============================================================================
// Column definitions
// =============================================================================

function buildColumns(): TableColumn<ActionInfo>[] {
    return [
        {
            key: 'name',
            label: 'Action',
            width: '28%',
            renderCell: (a) => (
                <div className="flex items-center gap-2.5">
                    <div className="p-1.5 rounded bg-surface-hover text-text-secondary group-hover:text-primary transition-colors">
                        <Zap className="w-4 h-4" />
                    </div>
                    <div className="min-w-0">
                        <span className="font-medium text-text-primary group-hover:text-primary transition-colors">
                            {a.name}
                        </span>
                        {a.description && (
                            <p className="text-[11px] text-text-secondary truncate max-w-[200px]">{a.description}</p>
                        )}
                    </div>
                </div>
            ),
        },
        {
            key: 'type',
            label: 'Type',
            width: '8%',
            renderCell: (a) => <TypeBadge type={a.type} />,
        },
        {
            key: 'enabled',
            label: 'Status',
            width: '10%',
            renderCell: (a) => <StatusBadge enabled={a.enabled} />,
        },
        {
            key: 'runs',
            label: 'Runs',
            width: '8%',
            align: 'right',
            renderCell: (a) => (
                <span className="tabular-nums text-text-secondary">{a.runs.total}</span>
            ),
        },
        {
            key: 'success',
            label: 'Success',
            width: '8%',
            align: 'right',
            renderCell: (a) => {
                if (a.runs.total === 0) return <span className="text-text-secondary">—</span>;
                const rate = Math.round((a.runs.completed / a.runs.total) * 100);
                return (
                    <span className={cn(
                        "tabular-nums",
                        rate < 70 ? "text-red-400" : rate < 90 ? "text-yellow-400" : "text-text-secondary"
                    )}>
                        {rate}%
                    </span>
                );
            },
        },
        {
            key: 'workers',
            label: 'Workers',
            width: '8%',
            align: 'right',
            renderCell: (a) => (
                <span className="tabular-nums text-text-secondary">{a.worker_count}</span>
            ),
        },
        {
            key: 'timeout',
            label: 'Timeout',
            width: '8%',
            renderCell: (a) => (
                <span className="text-text-secondary text-xs">
                    {a.timeout_ms >= 1000 ? `${a.timeout_ms / 1000}s` : `${a.timeout_ms}ms`}
                </span>
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
// Filter options
// =============================================================================

type TypeFilter = 'all' | 'user' | 'wasm';
type StatusFilter = 'all' | 'active' | 'disabled';

const TYPE_OPTIONS = [
    { value: 'all',  label: 'All Types' },
    { value: 'user', label: 'User' },
    { value: 'wasm', label: 'WASM' },
];

const STATUS_OPTIONS = [
    { value: 'all',      label: 'Status' },
    { value: 'active',   label: 'Active' },
    { value: 'disabled', label: 'Disabled' },
];

// =============================================================================
// Main Page
// =============================================================================

export function ActionsList() {
    const navigate = useNavigate();
    const { selected: namespace } = useNamespace();
    const { data: actions, loading, error, refetch } = useApi(() => api.getActions(namespace), [namespace], 5000);
    const [search, setSearch] = useState('');
    const [typeFilter, setTypeFilter] = useState<TypeFilter>('all');
    const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');

    const columns = useMemo(() => buildColumns(), []);

    const allActions = actions ?? [];

    const filtered = useMemo(() => {
        let result = allActions;
        if (typeFilter !== 'all') result = result.filter(a => a.type === typeFilter);
        if (statusFilter === 'active') result = result.filter(a => a.enabled);
        if (statusFilter === 'disabled') result = result.filter(a => !a.enabled);
        if (search) {
            const q = search.toLowerCase();
            result = result.filter(a => a.name.toLowerCase().includes(q));
        }
        return result;
    }, [actions, typeFilter, statusFilter, search]);

    const totalRuns = allActions.reduce((sum, a) => sum + a.runs.total, 0);
    const totalFailed = allActions.reduce((sum, a) => sum + a.runs.failed, 0);
    const totalRunning = allActions.reduce((sum, a) => sum + a.runs.running, 0);
    const totalWorkers = allActions.reduce((sum, a) => sum + a.worker_count, 0);

    // All hooks above — early returns below
    if (loading && !actions) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-semibold text-text-primary">Actions</h1>
                <p className="text-text-secondary mt-1 text-sm">
                    Callable units of business logic — user functions and WASM modules.
                </p>
            </div>

            {/* Stat cards */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard label="Actions"  value={allActions.length}  subtitle="registered"     icon={Zap} />
                <StatCard label="Running"  value={totalRunning}       subtitle="in progress"    icon={Activity} />
                <StatCard label="Workers"  value={totalWorkers}       subtitle="connected"      icon={Users} />
                <StatCard
                    label="Failed"
                    value={totalFailed}
                    subtitle={totalFailed > 0 ? `${Math.round((totalFailed / (totalRuns || 1)) * 100)}% failure rate` : 'all runs clean'}
                    icon={AlertCircle}
                    alert={totalFailed > 0}
                />
            </div>

            {/* Filter bar + table */}
            <div className="flex flex-col gap-4">
                <FilterBar
                    search={search}
                    onSearchChange={setSearch}
                    searchPlaceholder="Search actions..."
                    selects={[
                        { icon: Filter, value: typeFilter, onChange: (v) => setTypeFilter(v as TypeFilter), options: TYPE_OPTIONS },
                        { value: statusFilter, onChange: (v) => setStatusFilter(v as StatusFilter), options: STATUS_OPTIONS },
                    ]}
                    resultCount={filtered.length}
                    resultLabel="action"
                />

                <DataTable
                    columns={columns}
                    rows={filtered}
                    rowKey={(a) => `${a.namespace}:${a.name}`}
                    onRowClick={(a) => navigate(`/actions/${encodeURIComponent(a.name)}`)}
                    emptyState={
                        allActions.length === 0
                            ? <span className="text-sm">No actions registered yet. Register one via CLI: <code>flo action register &lt;name&gt; --type user</code></span>
                            : <span className="text-sm">No matching actions found.</span>
                    }
                />
            </div>
        </div>
    );
}
