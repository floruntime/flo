import { Database, Plus } from "lucide-react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import { useApi, LoadingState, ErrorState, EmptyState } from "../lib/useApi";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/Card";

export function KVBrowser() {
    const { data: kvNamespaces, loading, error, refetch } = useApi(() => api.getKVNamespaces(), [], 5000);

    if (loading && !kvNamespaces) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!kvNamespaces || kvNamespaces.length === 0) return <EmptyState title="No KV data" description="Set a key-value pair to get started." />;

    const totalKeys = kvNamespaces.reduce((acc, ns) => acc + ns.key_count, 0);
    const totalOps = kvNamespaces.reduce((acc, ns) => acc + ns.get_ops + ns.set_ops + ns.delete_ops, 0);

    return (
        <div className="flex flex-col h-[calc(100vh-64px)]">
            {/* Page Header */}
            <div className="flex items-center gap-4 mb-6">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary flex items-center gap-3">
                        <Database className="w-6 h-6 text-primary" />
                        KV Store
                        <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-primary/10 text-primary border border-primary/20">
                            Live
                        </span>
                    </h1>
                    <div className="flex items-center gap-4 mt-1 text-sm text-text-secondary">
                        <span>{totalKeys.toLocaleString()} Keys</span>
                        <span>-</span>
                        <span>{kvNamespaces.length} Namespace{kvNamespaces.length !== 1 ? 's' : ''}</span>
                        <span>-</span>
                        <span>{totalOps.toLocaleString()} Ops Total</span>
                    </div>
                </div>

                <div className="ml-auto flex items-center gap-3">
                    <button className="flex items-center gap-2 px-3 py-1.5 text-sm bg-primary text-background font-medium rounded-md hover:bg-primary/90 transition-colors">
                        <Plus className="w-4 h-4" />
                        New Key
                    </button>
                </div>
            </div>

            {/* Namespace Cards */}
            <div className="flex-1 min-h-0 overflow-auto">
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                    {kvNamespaces.map((ns) => (
                        <Link key={ns.name} to={`/kv/${encodeURIComponent(ns.name)}`}>
                            <Card className="hover:border-primary/50 transition-colors cursor-pointer">
                                <CardHeader className="pb-2">
                                    <CardTitle className="flex items-center gap-2 text-base">
                                        <Database className="w-4 h-4 text-primary" />
                                        {ns.name}
                                    </CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="space-y-1">
                                            <p className="text-xs text-text-secondary">Keys</p>
                                            <p className="text-lg font-semibold">{ns.key_count.toLocaleString()}</p>
                                        </div>
                                        <div className="space-y-1">
                                            <p className="text-xs text-text-secondary">Storage</p>
                                            <p className="text-lg font-semibold">{formatBytes(ns.bytes_stored)}</p>
                                        </div>
                                    </div>
                                    <div className="mt-3 pt-3 border-t border-surface-border text-xs text-text-secondary">
                                        {(ns.get_ops + ns.set_ops + ns.delete_ops).toLocaleString()} total ops
                                    </div>
                                </CardContent>
                            </Card>
                        </Link>
                    ))}
                </div>
            </div>
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
