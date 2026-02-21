import { Layers, Activity } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import { Card } from "../components/ui/Card";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";

export function StreamsList() {
    const navigate = useNavigate();
    const { data: streams, loading, error, refetch } = useApi(() => api.getStreams(), [], 5000);

    if (loading && !streams) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!streams || streams.length === 0) return <EmptyState title="No streams" description="Create a stream to get started." />;

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary">Streams</h1>
                    <p className="text-text-secondary mt-1">Manage your data streams and retention policies.</p>
                </div>
                <button className="px-3 py-1.5 text-sm bg-primary text-background font-medium rounded-md hover:bg-primary/90 transition-colors">
                    New Stream
                </button>
            </div>

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
                            {streams.map((stream) => (
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
                            ))}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}
