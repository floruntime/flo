import { useEffect, useState } from "react";
import type { PartitionStat } from "../../lib/types";
import { cn } from "../../lib/utils";

interface PartitionEqualizerProps {
    partitions: PartitionStat[];
}

export function PartitionEqualizer({ partitions }: PartitionEqualizerProps) {
    // We'll use a simple CSS animation for the "pulse" effect
    // In a real app, this might be driven by WebSocket updates
    const [tick, setTick] = useState(0);

    useEffect(() => {
        const interval = setInterval(() => {
            setTick(t => t + 1);
        }, 1000);
        return () => clearInterval(interval);
    }, []);

    const maxCount = Math.max(...partitions.map(p => p.message_count), 1);

    return (
        <div className="bg-surface border border-surface-border rounded-lg p-4">
            <div className="flex items-center justify-between mb-4">
                <h3 className="text-sm font-medium text-text-primary">Partition Equalizer</h3>
                <div className="flex items-center gap-4 text-xs text-text-secondary">
                    <div className="flex items-center gap-1.5">
                        <div className="w-2 h-2 rounded-full bg-primary/50" />
                        <span>Healthy</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                        <div className="w-2 h-2 rounded-full bg-white" />
                        <span>Hot Shard</span>
                    </div>
                </div>
            </div>

            <div className="space-y-1">
                {partitions.map((p) => {
                    const jitter = Math.sin(tick + p.id) * 0.05;
                    const widthPercent = Math.min(100, ((p.message_count / maxCount) + jitter) * 100);

                    return (
                        <div key={p.id} className="flex items-center gap-3 h-6 group">
                            <span className="text-[10px] font-mono text-text-secondary w-6 text-right">
                                {p.id}
                            </span>
                            <div className="flex-1 h-full bg-surface-hover/30 rounded-sm overflow-hidden relative">
                                <div
                                    className={cn(
                                        "h-full transition-all duration-1000 ease-linear rounded-r-sm",
                                        p.status === 'hot' ? "bg-white shadow-[0_0_10px_rgba(255,255,255,0.5)]" : "bg-primary/60"
                                    )}
                                    style={{ width: `${widthPercent}%` }}
                                />
                            </div>
                            <span className="text-[10px] font-mono text-text-secondary w-12 text-right">
                                {p.message_count}
                            </span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
