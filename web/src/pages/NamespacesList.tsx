import { Database, Plus, Layers, HardDrive, MessageSquare } from "lucide-react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";

export function NamespacesList() {
    const { data: namespaces, loading, error, refetch } = useApi(() => api.getNamespaces(), [], 5000);

    if (loading && !namespaces) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!namespaces || namespaces.length === 0) return <EmptyState title="No namespaces" description="Create a namespace to organize your data." />;

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary flex items-center gap-3">
                        <Database className="w-6 h-6 text-primary" />
                        Namespaces
                    </h1>
                    <p className="text-text-secondary mt-1 text-sm">Manage data isolation and storage policies.</p>
                </div>
                <button className="flex items-center gap-2 px-3 py-1.5 text-sm bg-primary text-background font-medium rounded-md hover:bg-primary/90 transition-colors">
                    <Plus className="w-4 h-4" />
                    New Namespace
                </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {namespaces.map((ns) => (
                    <Link key={ns.name} to={`/namespaces/${ns.name}`} className="block group">
                        <Card className="h-full transition-all duration-200 group-hover:border-primary/50 group-hover:shadow-md overflow-hidden">
                            <CardHeader className="pb-2">
                                <CardTitle className="flex items-center gap-2">
                                    <Database className="w-4 h-4 text-primary" />
                                    <span className="font-semibold">{ns.name}</span>
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="grid grid-cols-3 gap-4">
                                    <div className="space-y-1">
                                        <div className="text-xs text-text-secondary flex items-center gap-1">
                                            <Layers className="w-3 h-3" /> Streams
                                        </div>
                                        <div className="text-xl font-semibold">{ns.stream_count}</div>
                                    </div>
                                    <div className="space-y-1">
                                        <div className="text-xs text-text-secondary flex items-center gap-1">
                                            <MessageSquare className="w-3 h-3" /> Queues
                                        </div>
                                        <div className="text-xl font-semibold">{ns.queue_count}</div>
                                    </div>
                                    <div className="space-y-1">
                                        <div className="text-xs text-text-secondary flex items-center gap-1">
                                            <HardDrive className="w-3 h-3" /> KV Keys
                                        </div>
                                        <div className="text-xl font-semibold">{ns.kv_count.toLocaleString()}</div>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    </Link>
                ))}
            </div>
        </div>
    );
}
