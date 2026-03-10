import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import {
    Cpu, Zap, Play, ArrowLeft, Users, CheckCircle2,
    Settings, RefreshCw, Activity, HardDrive, Box
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card";
import { api, type ActionRunInfo, type WorkerInfo } from "../lib/api";
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

function StatusBadge({ status }: { status: ActionRunInfo['status'] }) {
    const styles: Record<string, string> = {
        pending: "bg-yellow-400/10 text-yellow-400",
        running: "bg-blue-400/10 text-blue-400",
        completed: "bg-emerald-400/10 text-emerald-400",
        failed: "bg-red-400/10 text-red-400",
        cancelled: "bg-zinc-400/10 text-zinc-400",
        timed_out: "bg-orange-400/10 text-orange-400",
    };
    return (
        <span className={cn("text-xs px-2 py-0.5 rounded-full font-medium", styles[status] || "bg-zinc-400/10 text-zinc-400")}>
            {status.replace('_', ' ')}
        </span>
    );
}

function RunsTable({ runs }: { runs: ActionRunInfo[] }) {
    if (runs.length === 0) {
        return (
            <div className="text-center py-8 text-text-secondary text-sm">
                No runs recorded yet. Trigger the action to see execution history.
            </div>
        );
    }

    return (
        <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
                <thead className="bg-surface-hover/50 text-text-secondary font-medium border-b border-surface-border">
                    <tr>
                        <th className="px-4 py-2.5">Status</th>
                        <th className="px-4 py-2.5">Run ID</th>
                        <th className="px-4 py-2.5">Worker</th>
                        <th className="px-4 py-2.5">Attempt</th>
                        <th className="px-4 py-2.5">Created</th>
                        <th className="px-4 py-2.5">Duration</th>
                        <th className="px-4 py-2.5">Outcome</th>
                    </tr>
                </thead>
                <tbody className="divide-y divide-surface-border">
                    {runs.map((run) => {
                        const duration = run.completed_at && run.started_at
                            ? `${run.completed_at - run.started_at}ms`
                            : run.started_at
                                ? "running..."
                                : "—";

                        return (
                            <tr key={run.run_id} className="hover:bg-surface-hover/30 transition-colors">
                                <td className="px-4 py-3">
                                    <StatusBadge status={run.status} />
                                </td>
                                <td className="px-4 py-3 font-mono text-xs text-text-secondary">
                                    {run.run_id.length > 30 ? run.run_id.slice(0, 30) + "…" : run.run_id}
                                </td>
                                <td className="px-4 py-3 text-text-secondary">
                                    {run.worker_id || "—"}
                                </td>
                                <td className="px-4 py-3 text-text-secondary">{run.attempt}</td>
                                <td className="px-4 py-3 text-text-secondary">{formatTimeAgo(run.created_at)}</td>
                                <td className="px-4 py-3 text-text-secondary">{duration}</td>
                                <td className="px-4 py-3">
                                    {run.outcome ? (
                                        <span className={cn(
                                            "text-xs",
                                            run.outcome === 'success' ? "text-emerald-400" :
                                                run.outcome === 'failure' ? "text-red-400" :
                                                    "text-text-secondary"
                                        )}>
                                            {run.outcome}
                                        </span>
                                    ) : "—"}
                                    {run.error && (
                                        <span className="block text-xs text-red-400/70 mt-0.5 truncate max-w-[200px]" title={run.error}>
                                            {run.error}
                                        </span>
                                    )}
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
}

function WorkersPanel({ workers }: { workers: WorkerInfo[] }) {
    if (workers.length === 0) {
        return (
            <div className="text-center py-8 text-text-secondary text-sm">
                No workers connected for this action.
            </div>
        );
    }

    return (
        <div className="space-y-3">
            {workers.map((w) => (
                <div key={w.worker_id} className="flex items-center justify-between p-3 rounded-lg bg-surface-hover/30 border border-surface-border">
                    <div className="flex items-center gap-3">
                        <div className={cn(
                            "w-2 h-2 rounded-full",
                            w.healthy ? "bg-emerald-400" : "bg-red-400"
                        )} />
                        <div>
                            <p className="text-sm font-medium">{w.worker_id}</p>
                            <p className="text-xs text-text-secondary">{w.task_types}</p>
                        </div>
                    </div>
                    <div className="flex items-center gap-4 text-xs text-text-secondary">
                        <div className="text-right">
                            <p>Load: {w.current_load}%</p>
                            <p>Tasks: {w.active_tasks}/{w.max_concurrent}</p>
                        </div>
                        <div className="text-right">
                            <p>Last seen</p>
                            <p>{formatTimeAgo(w.last_seen)}</p>
                        </div>
                    </div>
                </div>
            ))}
        </div>
    );
}

export function ActionDetailPage() {
    const { actionName } = useParams<{ actionName: string }>();
    const { data: action, loading, error, refetch } = useApi(
        () => api.getActionDetail(actionName!),
        [actionName],
        5000
    );
    const [invokeResult, setInvokeResult] = useState<string | null>(null);
    const [tab, setTab] = useState<'runs' | 'workers' | 'config'>('runs');

    const tabs = action?.type === 'wasm'
        ? [
            { key: 'runs' as const, label: 'Recent Runs', icon: Activity, count: action?.recent_runs.length },
            { key: 'workers' as const, label: 'WASM Module', icon: Box },
            { key: 'config' as const, label: 'Configuration', icon: Settings },
        ]
        : [
            { key: 'runs' as const, label: 'Recent Runs', icon: Activity, count: action?.recent_runs.length },
            { key: 'workers' as const, label: 'Workers', icon: Users, count: action?.workers.length },
            { key: 'config' as const, label: 'Configuration', icon: Settings },
        ];

    const handleInvoke = async () => {
        if (!actionName) return;
        try {
            const result = await api.invokeAction(actionName);
            setInvokeResult(result.run_id);
            setTimeout(() => setInvokeResult(null), 5000);
            refetch();
        } catch {
            // handled
        }
    };

    if (loading && !action) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!action) return <EmptyState title="Action not found" />;

    const successRate = action.runs.total > 0
        ? Math.round((action.runs.completed / action.runs.total) * 100)
        : null;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <Link to="/actions" className="p-1.5 rounded-md hover:bg-surface-hover transition-colors">
                        <ArrowLeft className="w-5 h-5 text-text-secondary" />
                    </Link>
                    <div className="flex items-center gap-3">
                        {action.type === 'wasm' ? (
                            <div className="p-2 rounded-lg bg-purple-400/10">
                                <Cpu className="w-5 h-5 text-purple-400" />
                            </div>
                        ) : (
                            <div className="p-2 rounded-lg bg-yellow-400/10">
                                <Zap className="w-5 h-5 text-yellow-400" />
                            </div>
                        )}
                        <div>
                            <h1 className="text-2xl font-semibold text-text-primary">{action.name}</h1>
                            <p className="text-sm text-text-secondary">
                                {action.namespace} · v{action.version} · {action.type.toUpperCase()} · by {action.owner}
                            </p>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <button
                        onClick={refetch}
                        className="p-2 rounded-md hover:bg-surface-hover transition-colors text-text-secondary"
                        title="Refresh"
                    >
                        <RefreshCw className="w-4 h-4" />
                    </button>
                    {action.enabled && (
                        <button
                            onClick={handleInvoke}
                            className="flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-background font-medium hover:bg-primary/90 transition-colors"
                        >
                            <Play className="w-4 h-4" />
                            Trigger
                        </button>
                    )}
                </div>
            </div>

            {action.description && (
                <p className="text-text-secondary">{action.description}</p>
            )}

            {/* Invoke success */}
            {invokeResult && (
                <div className="rounded-md bg-emerald-400/10 border border-emerald-400/20 px-4 py-3 flex items-center gap-2 text-sm text-emerald-400">
                    <CheckCircle2 className="w-4 h-4" />
                    Triggered. Run ID: <code className="text-xs bg-surface px-1 py-0.5 rounded">{invokeResult}</code>
                </div>
            )}

            {/* Stats Cards */}
            <div className="grid gap-4 md:grid-cols-5">
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <p className="text-xs text-text-secondary mb-1">Total Runs</p>
                        <p className="text-xl font-bold">{action.runs.total.toLocaleString()}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <p className="text-xs text-text-secondary mb-1">Success Rate</p>
                        <p className={cn("text-xl font-bold",
                            successRate !== null && successRate < 90 ? "text-yellow-400" :
                                successRate !== null && successRate < 70 ? "text-red-400" : ""
                        )}>
                            {successRate !== null ? `${successRate}%` : "—"}
                        </p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <p className="text-xs text-text-secondary mb-1">Running Now</p>
                        <p className="text-xl font-bold text-blue-400">{action.runs.running}</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <p className="text-xs text-text-secondary mb-1">Failed</p>
                        <p className={cn("text-xl font-bold", action.runs.failed > 0 ? "text-red-400" : "")}>{action.runs.failed}</p>
                    </CardContent>
                </Card>
                {action.type === 'wasm' ? (
                    <Card>
                        <CardContent className="pt-4 pb-4">
                            <p className="text-xs text-text-secondary mb-1">Module Size</p>
                            <p className="text-xl font-bold text-purple-400">
                                {action.wasm_module_size != null
                                    ? action.wasm_module_size > 1024 * 1024
                                        ? `${(action.wasm_module_size / (1024 * 1024)).toFixed(1)}MB`
                                        : action.wasm_module_size > 1024
                                            ? `${(action.wasm_module_size / 1024).toFixed(1)}KB`
                                            : `${action.wasm_module_size}B`
                                    : "—"}
                            </p>
                        </CardContent>
                    </Card>
                ) : (
                    <Card>
                        <CardContent className="pt-4 pb-4">
                            <p className="text-xs text-text-secondary mb-1">Workers</p>
                            <p className="text-xl font-bold">{action.workers.length}</p>
                        </CardContent>
                    </Card>
                )}
            </div>

            {/* Run status breakdown bar */}
            {action.runs.total > 0 && (
                <Card>
                    <CardContent className="pt-4 pb-4">
                        <div className="flex items-center gap-4 mb-2">
                            <span className="text-xs text-text-secondary">Run Distribution</span>
                        </div>
                        <div className="w-full h-3 rounded-full bg-surface-hover overflow-hidden flex">
                            {action.runs.completed > 0 && (
                                <div
                                    className="h-full bg-emerald-400"
                                    style={{ width: `${(action.runs.completed / action.runs.total) * 100}%` }}
                                    title={`${action.runs.completed} completed`}
                                />
                            )}
                            {action.runs.running > 0 && (
                                <div
                                    className="h-full bg-blue-400"
                                    style={{ width: `${(action.runs.running / action.runs.total) * 100}%` }}
                                    title={`${action.runs.running} running`}
                                />
                            )}
                            {action.runs.pending > 0 && (
                                <div
                                    className="h-full bg-yellow-400"
                                    style={{ width: `${(action.runs.pending / action.runs.total) * 100}%` }}
                                    title={`${action.runs.pending} pending`}
                                />
                            )}
                            {action.runs.failed > 0 && (
                                <div
                                    className="h-full bg-red-400"
                                    style={{ width: `${(action.runs.failed / action.runs.total) * 100}%` }}
                                    title={`${action.runs.failed} failed`}
                                />
                            )}
                            {action.runs.timed_out > 0 && (
                                <div
                                    className="h-full bg-orange-400"
                                    style={{ width: `${(action.runs.timed_out / action.runs.total) * 100}%` }}
                                    title={`${action.runs.timed_out} timed out`}
                                />
                            )}
                        </div>
                        <div className="flex items-center gap-4 mt-2 text-xs text-text-secondary">
                            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-emerald-400" /> Completed ({action.runs.completed})</span>
                            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-blue-400" /> Running ({action.runs.running})</span>
                            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-yellow-400" /> Pending ({action.runs.pending})</span>
                            <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-red-400" /> Failed ({action.runs.failed})</span>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Tabs */}
            <div className="flex items-center gap-1 border-b border-surface-border">
                {tabs.map(t => (
                    <button
                        key={t.key}
                        onClick={() => setTab(t.key)}
                        className={cn(
                            "flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px",
                            tab === t.key
                                ? "border-primary text-primary"
                                : "border-transparent text-text-secondary hover:text-text-primary"
                        )}
                    >
                        <t.icon className="w-4 h-4" />
                        {t.label}
                        {t.count !== undefined && (
                            <span className="text-xs bg-surface-hover px-1.5 py-0.5 rounded-full">{t.count}</span>
                        )}
                    </button>
                ))}
            </div>

            {/* Tab Content */}
            <Card>
                {tab === 'runs' && <RunsTable runs={action.recent_runs} />}
                {tab === 'workers' && action.type === 'wasm' && (
                    <CardContent className="pt-4">
                        <div className="space-y-4">
                            <div className="flex items-center gap-3 p-4 rounded-lg bg-purple-400/5 border border-purple-400/20">
                                <div className="p-2 rounded-lg bg-purple-400/10">
                                    <HardDrive className="w-5 h-5 text-purple-400" />
                                </div>
                                <div>
                                    <p className="text-sm font-medium">Server-Side Execution</p>
                                    <p className="text-xs text-text-secondary">
                                        This action runs inline on the Flo server via WebAssembly. No external workers needed.
                                    </p>
                                </div>
                            </div>
                            <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-2 text-sm">
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Module Size</span>
                                        <span>
                                            {action.wasm_module_size != null
                                                ? action.wasm_module_size > 1024 * 1024
                                                    ? `${(action.wasm_module_size / (1024 * 1024)).toFixed(1)} MB`
                                                    : action.wasm_module_size > 1024
                                                        ? `${(action.wasm_module_size / 1024).toFixed(1)} KB`
                                                        : `${action.wasm_module_size} bytes`
                                                : "No module deployed"}
                                        </span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Execution Model</span>
                                        <span>Synchronous (inline)</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Concurrency Limit</span>
                                        <span>4 per shard</span>
                                    </div>
                                </div>
                                <div className="space-y-2 text-sm">
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Runtime</span>
                                        <span>zware (WASM 2.0)</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">ABI</span>
                                        <span className="font-mono text-xs">handle / alloc / dealloc</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Host Functions</span>
                                        <span>log, kv_get, kv_set, kv_delete</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </CardContent>
                )}
                {tab === 'workers' && action.type !== 'wasm' && (
                    <CardContent className="pt-4">
                        <WorkersPanel workers={action.workers} />
                    </CardContent>
                )}
                {tab === 'config' && (
                    <CardContent className="pt-4">
                        <div className="grid grid-cols-2 gap-6">
                            <div className="space-y-3">
                                <h3 className="text-sm font-medium text-text-primary">Execution Settings</h3>
                                <div className="space-y-2 text-sm">
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Timeout</span>
                                        <span>{action.timeout_ms >= 1000 ? `${action.timeout_ms / 1000}s` : `${action.timeout_ms}ms`}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Max Retries</span>
                                        <span>{action.max_retries}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Retry Delay</span>
                                        <span>{action.retry_delay_ms}ms</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Status</span>
                                        <span className={action.enabled ? "text-emerald-400" : "text-zinc-400"}>
                                            {action.enabled ? "Enabled" : "Disabled"}
                                        </span>
                                    </div>
                                </div>
                            </div>
                            <div className="space-y-3">
                                <h3 className="text-sm font-medium text-text-primary">Metadata</h3>
                                <div className="space-y-2 text-sm">
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Version</span>
                                        <span>{action.version}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Owner</span>
                                        <span>{action.owner}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Created</span>
                                        <span>{formatDate(action.created_at)}</span>
                                    </div>
                                    <div className="flex justify-between">
                                        <span className="text-text-secondary">Updated</span>
                                        <span>{formatDate(action.updated_at)}</span>
                                    </div>
                                    {action.trigger_stream && (
                                        <div className="flex justify-between">
                                            <span className="text-text-secondary">Trigger Stream</span>
                                            <span>{action.trigger_stream}</span>
                                        </div>
                                    )}
                                    {action.trigger_group && (
                                        <div className="flex justify-between">
                                            <span className="text-text-secondary">Trigger Group</span>
                                            <span>{action.trigger_group}</span>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </CardContent>
                )}
            </Card>
        </div>
    );
}
