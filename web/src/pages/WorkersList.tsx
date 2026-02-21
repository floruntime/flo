import { Users, Activity, Heart, RefreshCw, Cpu } from "lucide-react";
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

    const healthyCount = workers.filter(w => w.healthy).length;
    const unhealthyCount = workers.length - healthyCount;
    const totalActiveTasks = workers.reduce((sum, w) => sum + w.active_tasks, 0);
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
                            <span className="text-xs text-text-secondary">Healthy</span>
                        </div>
                        <p className="text-xl font-bold text-emerald-400">{healthyCount}</p>
                        {unhealthyCount > 0 && (
                            <p className="text-xs text-red-400 mt-1">{unhealthyCount} unhealthy</p>
                        )}
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Activity className="w-4 h-4 text-blue-400" />
                            <span className="text-xs text-text-secondary">Active Tasks</span>
                        </div>
                        <p className="text-xl font-bold">{totalActiveTasks}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Cpu className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary">Cluster Utilization</span>
                        </div>
                        <LoadBar value={totalActiveTasks} max={totalCapacity} />
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
                                <th className="px-4 py-2.5">Health</th>
                                <th className="px-4 py-2.5">Worker ID</th>
                                <th className="px-4 py-2.5">Namespace</th>
                                <th className="px-4 py-2.5">Task Types</th>
                                <th className="px-4 py-2.5">Load</th>
                                <th className="px-4 py-2.5">Tasks</th>
                                <th className="px-4 py-2.5">Last Seen</th>
                                <th className="px-4 py-2.5">Registered</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-surface-border">
                            {workers.map((w) => (
                                <tr key={w.worker_id} className="hover:bg-surface-hover/30 transition-colors">
                                    <td className="px-4 py-3">
                                        {w.healthy ? (
                                            <span className="flex items-center gap-1.5 text-emerald-400">
                                                <span className="w-2 h-2 rounded-full bg-emerald-400" />
                                                <span className="text-xs">Healthy</span>
                                            </span>
                                        ) : (
                                            <span className="flex items-center gap-1.5 text-red-400">
                                                <span className="w-2 h-2 rounded-full bg-red-400" />
                                                <span className="text-xs">Unhealthy</span>
                                            </span>
                                        )}
                                    </td>
                                    <td className="px-4 py-3 font-mono text-xs">{w.worker_id}</td>
                                    <td className="px-4 py-3 text-text-secondary">{w.namespace || "default"}</td>
                                    <td className="px-4 py-3">
                                        <span className="text-xs bg-surface-hover px-2 py-0.5 rounded">
                                            {w.task_types || "—"}
                                        </span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <LoadBar value={w.current_load} max={100} />
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary">
                                        {w.active_tasks} / {w.max_concurrent}
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary" title={formatDate(w.last_seen)}>
                                        {formatTimeAgo(w.last_seen)}
                                    </td>
                                    <td className="px-4 py-3 text-text-secondary" title={formatDate(w.registered_at)}>
                                        {formatTimeAgo(w.registered_at)}
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
