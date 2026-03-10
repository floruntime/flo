import { useState } from "react";
import { Link } from "react-router-dom";
import { Cpu, Zap, Play, ChevronRight, Users, Clock, RotateCcw, AlertTriangle, CheckCircle2, XCircle, Timer, Ban, HardDrive } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";
import { api, type ActionInfo } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";
import { cn } from "../lib/utils";

function StatusBadge({ enabled }: { enabled: boolean }) {
    return (
        <span className={cn(
            "text-xs px-2 py-0.5 rounded-full font-medium",
            enabled ? "bg-emerald-400/10 text-emerald-400" : "bg-zinc-400/10 text-zinc-400"
        )}>
            {enabled ? "Active" : "Disabled"}
        </span>
    );
}

function TypeBadge({ type }: { type: string }) {
    const isWasm = type === 'wasm';
    return (
        <span className={cn(
            "text-xs px-2 py-0.5 rounded-full font-medium",
            isWasm ? "bg-purple-400/10 text-purple-400" : "bg-yellow-400/10 text-yellow-400"
        )}>
            {isWasm ? "WASM" : "USER"}
        </span>
    );
}

function RunStats({ runs }: { runs: ActionInfo['runs'] }) {
    const items = [
        { label: "Completed", value: runs.completed, icon: CheckCircle2, color: "text-emerald-400" },
        { label: "Running", value: runs.running, icon: Play, color: "text-blue-400" },
        { label: "Pending", value: runs.pending, icon: Clock, color: "text-yellow-400" },
        { label: "Failed", value: runs.failed, icon: XCircle, color: "text-red-400" },
        { label: "Timed Out", value: runs.timed_out, icon: Timer, color: "text-orange-400" },
        { label: "Cancelled", value: runs.cancelled, icon: Ban, color: "text-zinc-400" },
    ];

    const activeItems = items.filter(i => i.value > 0);
    if (activeItems.length === 0) return <span className="text-xs text-text-secondary">No runs yet</span>;

    return (
        <div className="flex flex-wrap gap-3">
            {activeItems.map(item => (
                <div key={item.label} className="flex items-center gap-1">
                    <item.icon className={cn("w-3 h-3", item.color)} />
                    <span className="text-xs text-text-secondary">{item.value}</span>
                </div>
            ))}
        </div>
    );
}

function ActionCard({ action, onInvoke }: { action: ActionInfo; onInvoke: (name: string) => void }) {
    const successRate = action.runs.total > 0
        ? Math.round((action.runs.completed / action.runs.total) * 100)
        : null;

    return (
        <Card className="hover:border-primary/50 transition-colors group">
            <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2">
                <div className="space-y-1 min-w-0 flex-1">
                    <CardTitle className="text-base font-medium flex items-center gap-2">
                        {action.type === 'wasm' ? (
                            <div className="p-1 rounded bg-purple-400/10">
                                <Cpu className="w-4 h-4 text-purple-400" />
                            </div>
                        ) : (
                            <div className="p-1 rounded bg-yellow-400/10">
                                <Zap className="w-4 h-4 text-yellow-400" />
                            </div>
                        )}
                        <Link
                            to={`/actions/${encodeURIComponent(action.name)}`}
                            className="hover:text-primary transition-colors truncate"
                        >
                            {action.name}
                        </Link>
                    </CardTitle>
                    {action.description && (
                        <p className="text-xs text-text-secondary pl-8 truncate">{action.description}</p>
                    )}
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                    <TypeBadge type={action.type} />
                    <StatusBadge enabled={action.enabled} />
                </div>
            </CardHeader>
            <CardContent>
                {/* Stats Grid */}
                <div className="grid grid-cols-4 gap-4 mt-2">
                    <div>
                        <p className="text-xs text-text-secondary">Total Runs</p>
                        <p className="text-lg font-bold">{action.runs.total.toLocaleString()}</p>
                    </div>
                    <div>
                        <p className="text-xs text-text-secondary">Success Rate</p>
                        <p className={cn("text-lg font-bold", successRate !== null && successRate < 90 ? "text-yellow-400" : successRate !== null && successRate < 70 ? "text-red-400" : "")}>
                            {successRate !== null ? `${successRate}%` : "—"}
                        </p>
                    </div>
                    <div>
                        {action.type === 'wasm' ? (
                            <>
                                <p className="text-xs text-text-secondary">Module</p>
                                <p className="text-lg font-bold flex items-center gap-1">
                                    <HardDrive className="w-3.5 h-3.5 text-purple-400" />
                                    {action.wasm_module_size != null
                                        ? action.wasm_module_size > 1024
                                            ? `${(action.wasm_module_size / 1024).toFixed(0)}KB`
                                            : `${action.wasm_module_size}B`
                                        : "—"}
                                </p>
                            </>
                        ) : (
                            <>
                                <p className="text-xs text-text-secondary">Workers</p>
                                <p className="text-lg font-bold flex items-center gap-1">
                                    <Users className="w-3.5 h-3.5 text-text-secondary" />
                                    {action.worker_count}
                                </p>
                            </>
                        )}
                    </div>
                    <div>
                        <p className="text-xs text-text-secondary">Timeout</p>
                        <p className="text-lg font-bold">{action.timeout_ms >= 1000 ? `${action.timeout_ms / 1000}s` : `${action.timeout_ms}ms`}</p>
                    </div>
                </div>

                {/* Run breakdown */}
                <div className="mt-3 pt-3 border-t border-surface-border flex items-center justify-between">
                    <RunStats runs={action.runs} />
                    <div className="flex items-center gap-2">
                        {action.enabled && (
                            <button
                                onClick={(e) => { e.preventDefault(); onInvoke(action.name); }}
                                className="flex items-center gap-1 text-xs px-2.5 py-1 rounded-md bg-primary/10 text-primary hover:bg-primary/20 transition-colors"
                                title="Trigger action"
                            >
                                <Play className="w-3 h-3" />
                                Trigger
                            </button>
                        )}
                        <Link
                            to={`/actions/${encodeURIComponent(action.name)}`}
                            className="flex items-center gap-1 text-xs text-text-secondary hover:text-primary transition-colors"
                        >
                            Detail
                            <ChevronRight className="w-3 h-3" />
                        </Link>
                    </div>
                </div>

                {/* Warnings */}
                {action.runs.failed > 0 && (
                    <div className="mt-2 flex items-center gap-1.5 text-xs text-red-400">
                        <AlertTriangle className="w-3 h-3" />
                        {action.runs.failed} failed run{action.runs.failed > 1 ? "s" : ""}
                    </div>
                )}
                {action.trigger_stream && (
                    <div className="mt-1 flex items-center gap-1.5 text-xs text-text-secondary">
                        <RotateCcw className="w-3 h-3" />
                        Triggered by stream: {action.trigger_stream}
                    </div>
                )}
            </CardContent>
        </Card>
    );
}

export function ActionsList() {
    const { data: actions, loading, error, refetch } = useApi(() => api.getActions(), [], 5000);
    const [invokeResult, setInvokeResult] = useState<{ name: string; runId: string } | null>(null);
    const [filter, setFilter] = useState<'all' | 'user' | 'wasm'>('all');

    const handleInvoke = async (name: string) => {
        try {
            const result = await api.invokeAction(name);
            setInvokeResult({ name, runId: result.run_id });
            setTimeout(() => setInvokeResult(null), 5000);
            refetch();
        } catch {
            // Error handled by UI
        }
    };

    if (loading && !actions) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!actions || actions.length === 0) return (
        <EmptyState
            title="No actions registered"
            description="Register an action via the CLI: flo action register <name> --type user --owner you@example.com"
        />
    );

    const filtered = filter === 'all' ? actions : actions.filter(a => a.type === filter);

    // Summary stats
    const totalRuns = actions.reduce((sum, a) => sum + a.runs.total, 0);
    const totalFailed = actions.reduce((sum, a) => sum + a.runs.failed, 0);
    const totalRunning = actions.reduce((sum, a) => sum + a.runs.running, 0);
    const totalWorkers = actions.reduce((sum, a) => sum + a.worker_count, 0);

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary">Actions</h1>
                    <p className="text-text-secondary mt-1">Registered callable units of business logic.</p>
                </div>
                <Link
                    to="/workers"
                    className="px-3 py-1.5 text-sm bg-surface border border-surface-border text-text-primary font-medium rounded-md hover:bg-surface-hover transition-colors flex items-center gap-2"
                >
                    <Users className="w-4 h-4" />
                    Workers
                </Link>
            </div>

            {/* Summary Cards */}
            <div className="grid gap-4 md:grid-cols-4">
                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-xs text-text-secondary font-medium uppercase tracking-wider">Actions</p>
                                <p className="text-2xl font-bold">{actions.length}</p>
                            </div>
                            <div className="p-2 rounded-lg bg-primary/10">
                                <Zap className="w-5 h-5 text-primary" />
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-xs text-text-secondary font-medium uppercase tracking-wider">Total Runs</p>
                                <p className="text-2xl font-bold">{totalRuns.toLocaleString()}</p>
                            </div>
                            <div className="p-2 rounded-lg bg-blue-400/10">
                                <Play className="w-5 h-5 text-blue-400" />
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-xs text-text-secondary font-medium uppercase tracking-wider">Running</p>
                                <p className="text-2xl font-bold">{totalRunning}</p>
                            </div>
                            <div className="p-2 rounded-lg bg-emerald-400/10">
                                <Clock className="w-5 h-5 text-emerald-400" />
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <p className="text-xs text-text-secondary font-medium uppercase tracking-wider">Workers</p>
                                <p className="text-2xl font-bold">{totalWorkers}</p>
                            </div>
                            <div className="p-2 rounded-lg bg-purple-400/10">
                                <Users className="w-5 h-5 text-purple-400" />
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Invoke success banner */}
            {invokeResult && (
                <div className="rounded-md bg-emerald-400/10 border border-emerald-400/20 px-4 py-3 flex items-center gap-2 text-sm text-emerald-400">
                    <CheckCircle2 className="w-4 h-4" />
                    Action &ldquo;{invokeResult.name}&rdquo; triggered. Run ID: <code className="text-xs bg-surface px-1 py-0.5 rounded">{invokeResult.runId}</code>
                </div>
            )}

            {/* Filter */}
            <div className="flex items-center gap-2">
                {(['all', 'user', 'wasm'] as const).map((f) => (
                    <button
                        key={f}
                        onClick={() => setFilter(f)}
                        className={cn(
                            "px-3 py-1.5 text-xs rounded-md font-medium transition-colors",
                            filter === f
                                ? "bg-primary text-background"
                                : "bg-surface-hover text-text-secondary hover:text-text-primary"
                        )}
                    >
                        {f === 'all' ? `All (${actions.length})` : f === 'user' ? `User (${actions.filter(a => a.type === 'user').length})` : `WASM (${actions.filter(a => a.type === 'wasm').length})`}
                    </button>
                ))}
                {totalFailed > 0 && (
                    <span className="ml-auto text-xs text-red-400 flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3" />
                        {totalFailed} total failures
                    </span>
                )}
            </div>

            {/* Actions Grid */}
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                {filtered.map((action) => (
                    <ActionCard
                        key={`${action.namespace}:${action.name}`}
                        action={action}
                        onInvoke={handleInvoke}
                    />
                ))}
            </div>
            {filtered.length === 0 && (
                <div className="text-center py-12 text-text-secondary text-sm">
                    No {filter} actions found.
                </div>
            )}
        </div>
    );
}
