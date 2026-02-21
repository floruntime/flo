import { Database, Zap, HardDrive, GitGraph, RefreshCw, Activity } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";

export function ClusterOverview() {
    const { data: stats, loading: statsLoading, error: statsError, refetch: refetchStats } = useApi(
        () => api.getClusterStats(), [], 5000
    );
    const { data: streams } = useApi(() => api.getStreams(), [], 5000);
    const { data: actions } = useApi(() => api.getActions(), [], 5000);
    const { data: workflows } = useApi(() => api.getWorkflows(), [], 5000);

    if (statsLoading && !stats) return <LoadingState />;
    if (statsError) return <ErrorState message={statsError} onRetry={refetchStats} />;

    const totalIngestRate = streams?.reduce((acc, s) => acc + s.ingest_rate, 0) ?? 0;
    const totalInvocations = actions?.reduce((acc, a) => acc + a.invocations, 0) ?? 0;
    const totalErrors = actions?.reduce((acc, a) => acc + a.errors, 0) ?? 0;
    const activeWorkflows = Array.isArray(workflows) ? workflows.filter(w => w.status === 'running').length : 0;

    return (
        <div className="space-y-8">
            {/* Header */}
            <div className="flex items-center justify-between">
                <h2 className="text-2xl font-normal text-text-primary">Project Overview</h2>
                <div className="flex items-center gap-4">
                    <div className="flex items-center gap-2 text-sm text-text-secondary">
                        <div className="w-2 h-2 rounded-full bg-success" />
                        <span>Project Status: Healthy</span>
                    </div>
                    <button
                        onClick={refetchStats}
                        className="flex items-center gap-2 px-3 py-1.5 rounded-md border border-surface-border bg-surface text-sm text-text-secondary hover:text-text-primary hover:border-text-secondary transition-colors"
                    >
                        Refresh
                        <RefreshCw className="w-3 h-3" />
                    </button>
                </div>
            </div>

            {/* 4-Column Grid */}
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
                <StatusCard
                    title="Cluster Health"
                    icon={Database}
                    metricLabel="Global RPS"
                    metricValue={stats?.rps?.toLocaleString() ?? '0'}
                    subItems={[
                        { label: 'Connections', value: stats?.active_connections?.toLocaleString() ?? '0' },
                        { label: 'Shards', value: stats?.num_shards?.toLocaleString() ?? '0' },
                        { label: 'Uptime', value: stats?.uptime ?? '-' },
                    ]}
                />
                <StatusCard
                    title="Data Ingest"
                    icon={HardDrive}
                    metricLabel="Events / Sec"
                    metricValue={totalIngestRate.toLocaleString()}
                    subItems={[
                        { label: 'Streams', value: streams?.length?.toLocaleString() ?? '0' },
                        { label: 'Bytes In', value: formatBytes(stats?.bytes_received ?? 0) },
                        { label: 'Bytes Out', value: formatBytes(stats?.bytes_sent ?? 0) },
                    ]}
                />
                <StatusCard
                    title="Compute Load"
                    icon={Zap}
                    metricLabel="Total Invocations"
                    metricValue={totalInvocations.toLocaleString()}
                    subItems={[
                        { label: 'Actions', value: actions?.length?.toLocaleString() ?? '0' },
                        { label: 'Errors', value: totalErrors.toLocaleString() },
                    ]}
                />
                <StatusCard
                    title="Orchestration"
                    icon={GitGraph}
                    metricLabel="Active Workflows"
                    metricValue={activeWorkflows.toLocaleString()}
                    subItems={[
                        { label: 'Total', value: workflows?.length?.toLocaleString() ?? '0' },
                    ]}
                />
            </div>

            {/* Nodes / Shards Section */}
            {stats?.nodes && stats.nodes.length > 0 && (
                <div className="pt-4">
                    <div className="flex items-center justify-between mb-4">
                        <h3 className="text-lg font-medium text-text-primary">Nodes &amp; Shards</h3>
                        <span className="text-xs text-text-secondary">{stats.nodes.length} shard{stats.nodes.length !== 1 ? 's' : ''} across 1 node</span>
                    </div>

                    {/* This Node */}
                    <Card className="bg-surface border-surface-border mb-4">
                        <CardContent className="p-4">
                            <div className="flex items-center justify-between mb-3">
                                <div className="flex items-center gap-2">
                                    <Database className="w-4 h-4 text-text-secondary" />
                                    <span className="text-sm font-medium text-text-primary">Local Node</span>
                                </div>
                                <div className="flex items-center gap-1.5">
                                    <div className="w-2 h-2 rounded-full bg-success" />
                                    <span className="text-xs text-text-secondary">Healthy</span>
                                </div>
                            </div>
                            <div className="grid grid-cols-4 gap-4 text-xs">
                                <div>
                                    <span className="text-text-secondary">Shards</span>
                                    <p className="text-text-primary font-medium mt-0.5">{stats.num_shards}</p>
                                </div>
                                <div>
                                    <span className="text-text-secondary">Connections</span>
                                    <p className="text-text-primary font-medium mt-0.5">{stats.active_connections}</p>
                                </div>
                                <div>
                                    <span className="text-text-secondary">Uptime</span>
                                    <p className="text-text-primary font-medium mt-0.5">{stats.uptime}</p>
                                </div>
                                <div>
                                    <span className="text-text-secondary">Version</span>
                                    <p className="text-text-primary font-medium mt-0.5">{stats.version || '-'}</p>
                                </div>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Per-shard grid */}
                    <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
                        {stats.nodes.map((node) => (
                            <Card key={node.id} className="bg-surface border-surface-border">
                                <CardContent className="p-3 flex items-center justify-between">
                                    <div className="flex items-center gap-2.5">
                                        <div className={`w-2 h-2 rounded-full ${node.status === 'healthy' ? 'bg-success' : node.status === 'unhealthy' ? 'bg-error' : 'bg-yellow-500'}`} />
                                        <div>
                                            <p className="text-sm font-medium text-text-primary">{node.id}</p>
                                            <p className="text-[11px] text-text-secondary">{node.role}</p>
                                        </div>
                                    </div>
                                    {(node.cpu != null || node.mem != null) && (
                                        <div className="text-right text-[11px] text-text-secondary">
                                            {node.cpu != null && <p>CPU {node.cpu}%</p>}
                                            {node.mem != null && <p>Mem {node.mem}%</p>}
                                        </div>
                                    )}
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                </div>
            )}

            {/* Issues Section */}
            {totalErrors > 0 && (
                <div className="pt-4">
                    <h3 className="text-lg font-medium text-text-primary mb-4">
                        {totalErrors} issue{totalErrors !== 1 ? 's' : ''} need <span className="text-error">attention</span>
                    </h3>
                    <div className="grid gap-4 md:grid-cols-2">
                        {actions?.filter(a => a.errors > 0).map(a => (
                            <Card key={a.name} className="bg-surface border-surface-border">
                                <CardHeader className="flex flex-row items-center justify-between py-3 border-b border-surface-border">
                                    <div className="flex items-center gap-2">
                                        <Activity className="w-3 h-3 text-error" />
                                        <span className="text-xs font-bold text-text-secondary uppercase tracking-wider">{a.name}</span>
                                        <span className="bg-error text-white text-xs px-1.5 rounded">{a.errors}</span>
                                    </div>
                                </CardHeader>
                                <CardContent className="p-4">
                                    <p className="text-sm text-text-secondary">{a.errors} errors detected - Avg latency {a.avg_latency}ms</p>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

interface SubItem { label: string; value: string; }

function StatusCard({ title, icon: Icon, metricLabel, metricValue, subItems }: {
    title: string; icon: typeof Database; metricLabel: string; metricValue: string; subItems?: SubItem[];
}) {
    return (
        <Card className="h-64 flex flex-col">
            <CardHeader className="flex flex-row items-center gap-2 pb-2">
                <Icon className="w-4 h-4 text-text-secondary" />
                <CardTitle className="text-sm font-medium text-text-primary">{title}</CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col justify-between">
                <div>
                    <p className="text-xs text-text-secondary">{metricLabel}</p>
                    <p className="text-2xl font-normal text-text-primary mt-1">{metricValue}</p>
                </div>
                {subItems && subItems.length > 0 && (
                    <div className="mt-4 space-y-2">
                        {subItems.map((item) => (
                            <div key={item.label} className="flex items-center justify-between text-xs">
                                <span className="text-text-secondary">{item.label}</span>
                                <span className="text-text-primary font-medium">{item.value}</span>
                            </div>
                        ))}
                    </div>
                )}
            </CardContent>
        </Card>
    );
}
