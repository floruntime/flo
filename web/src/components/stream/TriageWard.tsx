import { useState, useEffect } from "react";
import type { PendingMessage } from "../../lib/types";
import { cn } from "../../lib/utils";
import { RefreshCw, X, Hand } from "lucide-react";

interface TriageWardProps {
    pendingMessages: PendingMessage[];
    onReclaimNow?: (messageId: string) => void;
    onTouch?: (messageId: string) => void;
    onNack?: (messageId: string) => void;
}

export function TriageWard({ pendingMessages: initialMessages, onReclaimNow, onTouch, onNack }: TriageWardProps) {
    const [messages, setMessages] = useState(initialMessages);
    const [selectedGroup] = useState<string | null>(null);

    // Update countdown timers every 100ms
    useEffect(() => {
        const interval = setInterval(() => {
            setMessages(prevMessages =>
                prevMessages.map(msg => ({
                    ...msg,
                    idleTime: msg.idleTime + 100,
                    reclaimIn: Math.max(0, msg.reclaimIn - 100)
                }))
            );
        }, 100);

        return () => clearInterval(interval);
    }, []);

    // Filter messages by selected group
    const filteredMessages = selectedGroup
        ? messages.filter(m => m.groupName === selectedGroup)
        : messages;

    // Get unique consumer groups
    // const groups = Array.from(new Set(messages.map(m => m.groupName)));

    // Calculate stats
    // const totalPending = messages.length;
    // const uniqueConsumers = new Set(messages.map(m => m.consumer)).size;
    // const avgIdleTime = messages.reduce((acc, m) => acc + m.idleTime, 0) / messages.length;

    const formatTime = (ms: number) => {
        if (ms < 1000) return `${ms}ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
        return `${(ms / 60000).toFixed(1)}m`;
    };

    const getDoomBarColor = (reclaimIn: number, totalTime: number = 30000) => {
        const percentage = (reclaimIn / totalTime) * 100;
        if (percentage > 60) return 'from-green-500 to-emerald-400';
        if (percentage > 30) return 'from-yellow-500 to-orange-400';
        return 'from-orange-500 to-red-500';
    };

    const getDoomBarWidth = (reclaimIn: number, totalTime: number = 30000) => {
        return Math.max(0, Math.min(100, (reclaimIn / totalTime) * 100));
    };

    return (
        <div>
            {/* Messages Table */}
            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead className="bg-surface-hover/30 border-b border-surface-border">
                        <tr>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Message</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Key</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Owner</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Group</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Deliveries</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Idle</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase min-w-[200px]">Doom Timer</th>
                            <th className="px-4 py-3 text-left text-[10px] font-bold text-text-secondary uppercase">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-surface-border">
                        {filteredMessages.length === 0 ? (
                            <tr>
                                <td colSpan={8} className="px-4 py-8 text-center text-sm text-text-secondary">
                                    No pending messages
                                </td>
                            </tr>
                        ) : (
                            filteredMessages.map((msg) => (
                                <tr key={msg.id} className="hover:bg-surface-hover/50 transition-colors">
                                    <td className="px-4 py-3">
                                        <div className="flex flex-col">
                                            <span className="text-xs font-mono text-text-primary">{msg.id}</span>
                                            <span className="text-[10px] text-text-secondary">Seq: {msg.seq}</span>
                                        </div>
                                    </td>
                                    <td className="px-4 py-3">
                                        <span className="text-xs font-mono text-text-secondary">{msg.key}</span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <span className="text-xs font-medium text-text-primary">{msg.consumer}</span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <span className="text-xs text-text-secondary">{msg.groupName}</span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <span className={cn(
                                            "text-xs font-bold",
                                            msg.deliveryCount > 3 ? "text-red-400" : "text-text-secondary"
                                        )}>
                                            {msg.deliveryCount}x
                                        </span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <span className="text-xs text-text-secondary">{formatTime(msg.idleTime)}</span>
                                    </td>
                                    <td className="px-4 py-3">
                                        <div className="flex flex-col gap-1">
                                            {/* Countdown bar */}
                                            <div className="w-full h-2 bg-surface-border/30 rounded-full overflow-hidden">
                                                <div
                                                    className={cn(
                                                        "h-full bg-gradient-to-r transition-all duration-100",
                                                        getDoomBarColor(msg.reclaimIn)
                                                    )}
                                                    style={{ width: `${getDoomBarWidth(msg.reclaimIn)}%` }}
                                                />
                                            </div>
                                            {/* Label */}
                                            <span className={cn(
                                                "text-[10px] font-mono",
                                                msg.reclaimIn < 5000 ? "text-red-400 font-bold" : "text-text-secondary"
                                            )}>
                                                {msg.reclaimIn > 0 ? `Reclaiming in ${formatTime(msg.reclaimIn)}` : 'Reclaimed!'}
                                            </span>
                                        </div>
                                    </td>
                                    <td className="px-4 py-3">
                                        <div className="flex items-center gap-1.5">
                                            {/* Reclaim Now */}
                                            <button
                                                onClick={() => onReclaimNow?.(msg.id)}
                                                className="p-1.5 bg-orange-500/20 text-orange-400 rounded hover:bg-orange-500/30 transition-colors"
                                                title="Reclaim Now"
                                            >
                                                <RefreshCw className="w-3 h-3" />
                                            </button>

                                            {/* Touch (Extend Lease) */}
                                            <button
                                                onClick={() => onTouch?.(msg.id)}
                                                className="p-1.5 bg-blue-500/20 text-blue-400 rounded hover:bg-blue-500/30 transition-colors"
                                                title="Touch - Extend Lease"
                                            >
                                                <Hand className="w-3 h-3" />
                                            </button>

                                            {/* NACK (Return to Queue) */}
                                            <button
                                                onClick={() => onNack?.(msg.id)}
                                                className="p-1.5 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-colors"
                                                title="NACK - Return to Queue"
                                            >
                                                <X className="w-3 h-3" />
                                            </button>
                                        </div>
                                    </td>
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
