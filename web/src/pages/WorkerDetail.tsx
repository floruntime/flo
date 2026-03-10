import { useParams, Link } from "react-router-dom";
import {
    ArrowLeft, Cpu, Activity, RefreshCw,
    AlertTriangle, CheckCircle2, XCircle
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";
import { api, type ProcessInfo } from "../lib/api";
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

function KindBadge({ kind }: { kind: string }) {
    return (
        <span className={cn(
            "text-xs px-2 py-0.5 rounded font-mono",
            kind === "action" ? "bg-blue-500/15 text-blue-400" : "bg-purple-500/15 text-purple-400"
        )}>
            {kind}
        </span>
    );
}

function ProcessesTable({ processes }: { processes: ProcessInfo[] }) {
    if (processes.length === 0) {
        return (
            <div className="text-center py-8 text-text-secondary text-sm">
                No processes registered for this worker.
            </div>
        );
    }

    return (
        <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
                <thead className="bg-surface-hover/50 text-text-secondary font-medium border-b border-surface-border">
                    <tr>
                        <th className="px-4 py-2.5">Name</th>
                        <th className="px-4 py-2.5">Kind</th>
                        <th className="px-4 py-2.5">Runs</th>
                        <th className="px-4 py-2.5">Failures</th>
                        <th className="px-4 py-2.5">Last Run</th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-surface-border">
                    {processes.map((p) => (
                        <tr key={p.name} className="hover:bg-surface-hover/30 transition-colors">
                            <td className="px-4 py-3 font-mono text-xs">{p.name}</td>
                            <td className="px-4 py-3"><KindBadge kind={p.kind} /></td>
                            <td className="px-4 py-3 text-text-secondary">{p.run_count}</td>
                            <td className="px-4 py-3">
                                {p.fail_count > 0 ? (
                                    <span className="flex items-center gap-1 text-red-400">
                                        <AlertTriangle className="w-3 h-3" />
                                        {p.fail_count}
                                    </span>
                                ) : (
                                    <span className="text-text-secondary">0</span>
                                )}
                            </td>
                            <td className="px-4 py-3 text-text-secondary" title={formatDate(p.last_run_at)}>
                                {formatTimeAgo(p.last_run_at)}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export function WorkerDetailPage() {
    const { workerId } = useParams<{ workerId: string }>();
    const decodedId = workerId ? decodeURIComponent(workerId) : '';

    const { data: worker, loading, error, refetch } = useApi(
        () => api.getWorkerDetail(decodedId),
        [decodedId],
        5000
    );

    if (loading && !worker) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!worker) return <EmptyState title="Worker Not Found" description={`No worker with ID "${decodedId}".`} />;

    const totalRuns = worker.processes.reduce((s, p) => s + p.run_count, 0);
    const totalFails = worker.processes.reduce((s, p) => s + p.fail_count, 0);

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-3">
                <Link to="/workers" className="p-1.5 rounded hover:bg-surface-hover transition-colors text-text-secondary">
                    <ArrowLeft className="w-4 h-4" />
                </Link>
                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                        <h1 className="text-2xl font-semibold text-text-primary truncate">{worker.worker_id}</h1>
                        <StatusBadge status={worker.status} />
                        <span className={cn(
                            "text-xs px-2 py-0.5 rounded font-mono",
                            worker.worker_type === "action" ? "bg-blue-500/15 text-blue-400" : "bg-purple-500/15 text-purple-400"
                        )}>
                            {worker.worker_type}
                        </span>
                    </div>
                    {worker.machine_id && (
                        <p className="text-sm text-text-secondary mt-0.5">Machine: {worker.machine_id}</p>
                    )}
                </div>
                <button
                    onClick={refetch}
                    className="p-2 rounded-md hover:bg-surface-hover transition-colors text-text-secondary"
                    title="Refresh"
                >
                    <RefreshCw className="w-4 h-4" />
                </button>
            </div>

            {/* Summary Cards */}
            <div className="grid gap-4 md:grid-cols-4">
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Activity className="w-4 h-4 text-blue-400" />
                            <span className="text-xs text-text-secondary">Current Load</span>
                        </div>
                        <p className="text-xl font-bold">{worker.current_load} / {worker.max_concurrent}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                            <span className="text-xs text-text-secondary">Completed</span>
                        </div>
                        <p className="text-xl font-bold text-emerald-400">{worker.tasks_completed}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <XCircle className="w-4 h-4 text-red-400" />
                            <span className="text-xs text-text-secondary">Failed</span>
                        </div>
                        <p className="text-xl font-bold text-red-400">{worker.tasks_failed}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-2 mb-1">
                            <Cpu className="w-4 h-4 text-text-secondary" />
                            <span className="text-xs text-text-secondary">Processes</span>
                        </div>
                        <p className="text-xl font-bold">{worker.processes.length}</p>
                    </CardContent>
                </Card>
            </div>

            {/* Info Card */}
            <Card>
                <CardHeader><CardTitle>Details</CardTitle></CardHeader>
                <CardContent>
                    <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div>
                            <dt className="text-text-secondary mb-0.5">Status</dt>
                            <dd><StatusBadge status={worker.status} /></dd>
                        </div>
                        <div>
                            <dt className="text-text-secondary mb-0.5">Last Seen</dt>
                            <dd title={formatDate(worker.last_seen)}>{formatTimeAgo(worker.last_seen)}</dd>
                        </div>
                        <div>
                            <dt className="text-text-secondary mb-0.5">Registered</dt>
                            <dd title={formatDate(worker.registered_at)}>{formatTimeAgo(worker.registered_at)}</dd>
                        </div>
                        <div>
                            <dt className="text-text-secondary mb-0.5">Machine</dt>
                            <dd>{worker.machine_id ?? "—"}</dd>
                        </div>
                    </dl>
                    {worker.metadata && (
                        <div className="mt-4 p-3 rounded bg-surface-hover text-xs font-mono break-all">
                            {worker.metadata}
                        </div>
                    )}
                </CardContent>
            </Card>

            {/* Process Stats */}
            <Card>
                <CardHeader>
                    <CardTitle>
                        Processes ({worker.processes.length})
                        <span className="ml-3 text-sm font-normal text-text-secondary">
                            {totalRuns} runs, {totalFails} failures
                        </span>
                    </CardTitle>
                </CardHeader>
                <ProcessesTable processes={worker.processes} />
            </Card>
        </div>
    );
}
