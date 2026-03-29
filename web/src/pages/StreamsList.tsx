import { Layers, Activity } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import { Card } from "../components/ui/Card";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { useNamespace } from "../lib/NamespaceContext";

export function StreamsList() {
    const navigate = useNavigate();
    const { selected: namespace } = useNamespace();
    const { data: streams, loading, error, refetch } = useApi(() => api.getStreams(namespace), [namespace], 5000);

    if (loading && !streams) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;

    const allStreams = streams || [];

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary">Streams</h1>
                    <p className="text-text-secondary mt-1 text-sm">Manage your data streams and retention policies.</p>
                </div>
                <button className="flex items-center gap-2 bg-primary hover:bg-primary/90 text-background font-medium px-4 py-2 rounded-md transition-colors text-sm">
                    New Stream
                </button>
            </div>

            {/* Stats Overview */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                    <div className="p-4">
                        <div className="flex items-center justify-between mb-2">
                            <span className="text-xs text-text-secondary uppercase tracking-wider">Streams</span>
                            <Layers className="w-4 h-4 text-primary" />
                        </div>
                        <p className="text-2xl font-semibold text-text-primary">{allStreams.length}</p>
                        <p className="text-xs text-text-secondary mt-1">{allStreams.length} total</p>
                    </div>
                </Card>
                <Card>
                    <div className="p-4">
                        <div className="flex items-center justify-between mb-2">
                            <span className="text-xs text-text-secondary uppercase tracking-wider">Partitions</span>
                            <Layers className="w-4 h-4 text-blue-400" />
                        </div>
                        <p className="text-2xl font-semibold text-text-primary">{allStreams.reduce((sum, s) => sum + s.partitions, 0)}</p>
                        <p className="text-xs text-text-secondary mt-1">across all streams</p>
                    </div>
                </Card>
                <Card>
                    <div className="p-4">
                        <div className="flex items-center justify-between mb-2">
                            <span className="text-xs text-text-secondary uppercase tracking-wider">Ingest Rate</span>
                            <Activity className="w-4 h-4 text-emerald-400" />
                        </div>
                        <p className="text-2xl font-semibold text-text-primary">{allStreams.reduce((sum, s) => sum + s.ingest_rate, 0).toLocaleString()}/s</p>
                        <p className="text-xs text-text-secondary mt-1">combined throughput</p>
                    </div>
                </Card>
                <Card>
                    <div className="p-4">
                        <div className="flex items-center justify-between mb-2">
                            <span className="text-xs text-text-secondary uppercase tracking-wider">Namespaces</span>
                            <Layers className="w-4 h-4 text-purple-400" />
                        </div>
                        <p className="text-2xl font-semibold text-text-primary">{new Set(allStreams.map(s => s.namespace)).size}</p>
                        <p className="text-xs text-text-secondary mt-1">with streams</p>
                    </div>
                </Card>
            </div>

            {/* Table */}
            <Card>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="bg-surface-hover/50 text-text-secondary font-medium border-b border-surface-border">
                            <tr>
                                <th className="px-6 py-3">Name</th>
                                <th className="px-6 py-3">Namespace</th>
                                <th className="px-6 py-3">Partitions</th>
                                <th className="px-6 py-3">Ingest Rate</th>
                                <th className="px-6 py-3">Retention</th>
                                <th className="px-6 py-3 text-right">Actions</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-surface-border">
                            {allStreams.length === 0 ? (
                                <tr>
                                    <td colSpan={6} className="px-6 py-12 text-center text-text-secondary text-sm">
                                        No streams yet. Create a stream to get started.
                                    </td>
                                </tr>
                            ) : (
                                allStreams.map((stream) => (
                                    <tr
                                        key={stream.name}
                                        className="hover:bg-surface-hover/50 transition-colors group cursor-pointer"
                                        onClick={() => navigate(`/streams/${stream.name}`)}
                                    >
                                        <td className="px-6 py-4 font-medium text-text-primary flex items-center gap-3">
                                            <div className="p-1.5 rounded bg-surface-hover text-text-secondary group-hover:text-primary transition-colors">
                                                <Layers className="w-4 h-4" />
                                            </div>
                                            {stream.name}
                                        </td>
                                        <td className="px-6 py-4 text-text-secondary">
                                            {stream.namespace}
                                        </td>
                                        <td className="px-6 py-4 text-text-secondary">
                                            {stream.partitions}
                                        </td>
                                        <td className="px-6 py-4 text-text-secondary">
                                            <div className="flex items-center gap-2">
                                                <Activity className="w-3 h-3 text-primary" />
                                                {stream.ingest_rate.toLocaleString()} /s
                                            </div>
                                        </td>
                                        <td className="px-6 py-4 text-text-secondary">
                                            {stream.retention}
                                        </td>
                                        <td className="px-6 py-4 text-right">
                                            <Link
                                                to={`/streams/${stream.name}`}
                                                className="text-text-secondary hover:text-text-primary transition-colors text-xs font-medium border border-surface-border rounded px-2 py-1 hover:bg-surface-hover inline-block"
                                            >
                                                Manage
                                            </Link>
                                        </td>
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}
