import { Users, Activity, Heart, RefreshCw, Cpu, AlertTriangle } from "lucide-react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";
import { cn } from "../lib/utils";

function formatTimeAgo(ms: number): string {
    if (ms <= 0) return "—";
    const now = Date.now();
    const diff = now - ms;
    if (diff < 0) return "just now";
    if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
    return `${Math.floor(diff / 86_400_000)}d ago`;
}

function formatDate(ms: number): string {
    if (ms <= 0) return "—";
    return new Date(ms).toLocaleString();
}

function LoadBar({ value, max }: { value: number; max: number }) {
    const pct = max > 0 ? Math.min(100, Math.round((value / max) * 100)) : 0;
    return (
        <div className="flex items-center gap-2 min-w-[120px]">
            <div className="flex-1 h-2 rounded-full bg-surface-hover overflow-hidden">
                <div
                    className={cn(
                        "h-full rounded-full transition-all",
                        pct > 80 ? "bg-red-400" : pct > 50 ? "bg-yellow-400" : "bg-emerald-400"
                    )}
                    style={{ width: `${pct}%` }}
                />
            </div>
            <span className="text-xs text-text-secondary w-8 text-right">{pct}%</span>
        </div>
    );
}

function StatusBadge({ status }: { status: string }) {
    const styles: Record<string, string> = {
        active: "bg-emerald-500/20 text-emerald-400",
        idle: "bg-gray-500/20 text-gray-400",
        draining: "bg-yellow-500/20 text-yellow-400",
        unhealthy: "bg-red-500/20 text-red-400",
    };
    return (
        <span className={cn("text-xs px-2 py-0.5 rounded-full font-medium", styles[status] ?? styles.idle)}>
            {status}
        </span>
    );
}

function TypeBadge({ type }: { type: string }) {
    return (
        <span className={cn(
            "text-xs px-2 py-0.5 rounded font-mono",
            type === "action" ? "bg-blue-500/15 text-blue-400" : "bg-purple-500/15 text-purple-400"
        )}>
            {type}
        </span>
    );
}

export function WorkersListPage() {
    const { data: workers, loading, error, refetch } = useApi(
        () => api.getWorkers(),
        [],
        5000
    );

    if (loading && !workers) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!workers || workers.length === 0) {
        return <EmptyState title="No Workers" description="No workers are currently registered." />;
    }

    const activeCount = workers.filter(w => w.status === "active" || w.status === "idle").length;
    const drainingCount = workers.filter(w => w.status === "draining").length;
    const unhealthyCount = workers.filter(w => w.status === "unhealthy").length;
    const totalLoad = workers.reduce((sum, w) => sum + w.current_load, 0);
    const totalCapacity = workers.reduce((sum, w) => sum + w.max_concurrent, 0);

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary">Workers</h1>
                    <p className="text-sm text-text-secondary mt-1">
                        {workers.length} worker{workers.length !== 1 ? 's' : ''} registered
                    </p>
                </div>
                <button
                    onClick={refetch}
                    className="p-2 rounded-md hover:bg-surface-hover transition-colors text-text-secondary"
                    title="Refresh"
                >
                    <RefreshCw className="w-4 h-4" />
                </button>
            </div>

            {/* Summary */}
            <div className="grid gap-4 md:grid-cols-4">
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Users className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary">Total Workers</span>
                        </div>
                        <p className="text-xl font-bold">{workers.length}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Heart className="w-4 h-4 text-emerald-400" />
                            <span className="text-xs text-text-secondary">Active</span>
                        </div>
                        <p className="text-xl font-bold text-emerald-400">{activeCount}</p>
                        {drainingCount > 0 && (
                            <p className="text-xs text-yellow-400 mt-1">{drainingCount} draining</p>
                        )}
                        {unhealthyCount > 0 && (
                            <p className="text-xs text-red-400 mt-1">{unhealthyCount} unhealthy</p>
                        )}
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Activity className="w-4 h-4 text-blue-400" />
                            <span className="text-xs text-text-secondary">Current Load</span>
                        </div>
                        <p className="text-xl font-bold">{totalLoad}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Cpu className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary">Cluster Utilization</span>
                        </div>
                        <LoadBar value={totalLoad} max={totalCapacity} />
                    </CardContent>
                </Card>
            </div>

            {/* Workers Table */}
            <Card>
                <CardHeader>
                    <CardTitle>Worker Pool</CardTitle>
                </CardHeader>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="bg-surface-hover/50 text-text-secondary font-medium border-b border-surface-border">
                            <tr>
                                <th className="px-4 py-2.5">Status</th>
                                <th className="px-4 py-2.5">Worker ID</th>
                                <th className="px-4 py-2.5">Type</th>
                                <th className="px-4 py-2.5">Machine</th>
                                <th className="px-4 py-2.5">Load</th>
                                <th className="px-4 py-2.5">Completed</th>
                                <th className="px-4 py-2.5">Failed</th>
                                <th className="px-4 py-2.5">Processes</th>
                                <th className="px-4 py-2.5">Last Seen</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-surface-border">
                            {workers.map((w) => (
                                <tr key={w.worker_id} className="hover:bg-surface-hover/30 transition-colors">
                                    <td className="px-4 py-3">
                                        <StatusBadge status={w.status} />
                                    </td>
                                    <td className="px-4 py-3">
                                        <Link
                                            to={`/workers/${encodeURIComponent(w.worker_id)}`}
                                            className="font-mono text-xs text-accent hover:underline"
                                        >
                                            {w.worker_id}
                                        </Link>
                                    </td>
                                    <td className="px-4 py-3">
                                        <TypeBadge type={w.worker_type} />
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary text-xs">
                                        {w.machine_id ?? "—"}
                                    </td>
                                    <td className="px-4 py-3">
                                        <LoadBar value={w.current_load} max={w.max_concurrent} />
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary">{w.tasks_completed}</td>
                                    <td className="px-4 py-3">
                                        {w.tasks_failed > 0 ? (
                                            <span className="flex items-center gap-1 text-red-400">
                                                <AlertTriangle className="w-3 h-3" />
                                                {w.tasks_failed}
                                            </span>
                                        ) : (
                                            <span className="text-text-secondary">0</span>
                                        )}
                                    </td>
                                    <td className="px-4 py-3">
                                        {w.processes.length > 0 ? (
                                            <div className="flex flex-wrap gap-1">
                                                {w.processes.map((p) => (
                                                    <span key={p.name} className="text-xs bg-surface-hover px-1.5 py-0.5 rounded">
                                                        {p.name}
                                                    </span>
                                                ))}
                                            </div>
                                        ) : (
                                            <span className="text-text-secondary text-xs">—</span>
                                        )}
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary" title={formatDate(w.last_seen)}>
                                        {formatTimeAgo(w.last_seen)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}
