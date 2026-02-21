import { cn } from "../../lib/utils";
import type { KVNamespace } from "../../lib/types";
import { Database, HardDrive, Layers, Activity } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "../ui/Card";

interface NamespaceStatsProps {
    namespace: KVNamespace;
    className?: string;
}

export function NamespaceStats({ namespace, className }: NamespaceStatsProps) {
    const formatBytes = (bytes: number) => {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    return (
        <Card className={cn("overflow-hidden", className)}>
            <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <Database className="w-4 h-4 text-primary" />
                        <span className="font-semibold">{namespace.name}</span>
                    </div>
                    <span className="text-xs font-mono text-text-secondary bg-surface-hover px-2 py-1 rounded">
                        LSN: {namespace.current_lsn}
                    </span>
                </CardTitle>
            </CardHeader>
            <CardContent>
                <div className="grid grid-cols-3 gap-4 mb-6">
                    <div className="space-y-1">
                        <div className="text-xs text-text-secondary flex items-center gap-1">
                            <Layers className="w-3 h-3" /> Keys
                        </div>
                        <div className="text-xl font-semibold">{namespace.key_count.toLocaleString()}</div>
                    </div>
                    <div className="space-y-1">
                        <div className="text-xs text-text-secondary flex items-center gap-1">
                            <Activity className="w-3 h-3" /> Versions
                        </div>
                        <div className="text-xl font-semibold">{namespace.version_count.toLocaleString()}</div>
                    </div>
                    <div className="space-y-1">
                        <div className="text-xs text-text-secondary flex items-center gap-1">
                            <HardDrive className="w-3 h-3" /> Size
                        </div>
                        <div className="text-xl font-semibold">{formatBytes(namespace.size_bytes)}</div>
                    </div>
                </div>

                {/* Key Distribution Mockup */}
                <div className="space-y-2">
                    <div className="text-xs font-medium text-text-secondary">Key Distribution</div>
                    <div className="space-y-1.5">
                        <div className="flex items-center gap-2 text-xs">
                            <span className="w-20 truncate text-text-secondary">user:*</span>
                            <div className="flex-1 h-2 bg-surface-hover rounded-full overflow-hidden">
                                <div className="h-full bg-primary w-[45%]" />
                            </div>
                            <span className="w-8 text-right">45%</span>
                        </div>
                        <div className="flex items-center gap-2 text-xs">
                            <span className="w-20 truncate text-text-secondary">session:*</span>
                            <div className="flex-1 h-2 bg-surface-hover rounded-full overflow-hidden">
                                <div className="h-full bg-primary/70 w-[32%]" />
                            </div>
                            <span className="w-8 text-right">32%</span>
                        </div>
                        <div className="flex items-center gap-2 text-xs">
                            <span className="w-20 truncate text-text-secondary">other</span>
                            <div className="flex-1 h-2 bg-surface-hover rounded-full overflow-hidden">
                                <div className="h-full bg-primary/40 w-[23%]" />
                            </div>
                            <span className="w-8 text-right">23%</span>
                        </div>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}
