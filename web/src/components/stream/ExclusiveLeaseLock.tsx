import { useState, useEffect } from "react";
import type { ExclusiveGroup } from "../../lib/types";
import { cn } from "../../lib/utils";
import { Crown, AlertTriangle, ChevronDown, ChevronRight } from "lucide-react";

interface ExclusiveLeaseLockProps {
    groups: ExclusiveGroup[];
}

export function ExclusiveLeaseLock({ groups }: ExclusiveLeaseLockProps) {
    const [exclusiveGroups, setExclusiveGroups] = useState(groups);
    const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

    // Update lease countdown timers every 100ms
    useEffect(() => {
        const interval = setInterval(() => {
            setExclusiveGroups(prevGroups =>
                prevGroups.map(group => ({
                    ...group,
                    leaseRemaining: Math.max(0, group.leaseRemaining - 100),
                    // Simulate failover when lease expires
                    ...(group.leaseRemaining <= 0 && group.leader && {
                        leader: null,
                        lastFailover: Date.now()
                    })
                }))
            );
        }, 100);

        return () => clearInterval(interval);
    }, []);

    const toggleExpanded = (groupName: string) => {
        setExpandedGroups(prev => {
            const next = new Set(prev);
            if (next.has(groupName)) {
                next.delete(groupName);
            } else {
                next.add(groupName);
            }
            return next;
        });
    };

    const formatTime = (ms: number) => {
        if (ms < 1000) return `${ms}ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
        return `${(ms / 60000).toFixed(1)}m`;
    };

    const getLeaseColor = (remaining: number, hasLeader: boolean) => {
        if (!hasLeader) return 'text-red-400';
        const percentage = (remaining / 60000) * 100; // Assume 60s max lease
        if (percentage > 30) return 'text-green-400';
        if (percentage > 10) return 'text-yellow-400';
        return 'text-orange-400';
    };

    const getLeaseBarColor = (remaining: number) => {
        const percentage = (remaining / 60000) * 100;
        if (percentage > 30) return 'from-green-500 to-emerald-400';
        if (percentage > 10) return 'from-yellow-500 to-orange-400';
        return 'from-orange-500 to-red-500';
    };

    const getLeasePercentage = (remaining: number) => {
        return Math.max(0, Math.min(100, (remaining / 60000) * 100));
    };

    // Stats
    // const totalGroups = exclusiveGroups.length;
    // const withLeader = exclusiveGroups.filter(g => g.leader !== null).length;
    // const noLeader = totalGroups - withLeader;

    return (
        <div>
            {/* Groups */}
            <div className="divide-y divide-surface-border">
                {exclusiveGroups.map((group) => {
                    const isExpanded = expandedGroups.has(group.groupName);
                    const hasLeader = group.leader !== null;
                    const leaseColor = getLeaseColor(group.leaseRemaining, hasLeader);

                    return (
                        <div key={group.groupName} className="p-4">
                            {/* Group Header - Clickable */}
                            <button
                                onClick={() => toggleExpanded(group.groupName)}
                                className="w-full flex items-center justify-between hover:bg-surface-hover/30 -m-2 p-2 rounded transition-colors"
                            >
                                <div className="flex items-center gap-3">
                                    {isExpanded ? (
                                        <ChevronDown className="w-4 h-4 text-text-secondary" />
                                    ) : (
                                        <ChevronRight className="w-4 h-4 text-text-secondary" />
                                    )}
                                    <span className="text-sm font-bold text-text-primary">{group.groupName}</span>
                                    <span className="text-[10px] px-2 py-0.5 rounded-full bg-purple-500/20 text-purple-400 border border-purple-500/30">
                                        Partition {group.partitionId}
                                    </span>
                                </div>

                                {/* Leader Status Badge */}
                                <div className={cn("text-xs font-medium", leaseColor)}>
                                    {hasLeader ? `👑 ${group.leader}` : '⚠️ No Leader'}
                                </div>
                            </button>

                            {/* Expanded Content */}
                            {isExpanded && (
                                <div className="mt-4 pl-7 space-y-4">
                                    {/* Leader Info */}
                                    {hasLeader ? (
                                        <div className="space-y-2">
                                            <div className="flex items-center gap-2">
                                                <Crown className={cn("w-4 h-4", leaseColor)} />
                                                <span className="text-sm font-medium text-text-primary">
                                                    Leader: <strong className={leaseColor}>{group.leader}</strong>
                                                </span>
                                            </div>

                                            {/* Lease Progress Bar */}
                                            <div className="space-y-1">
                                                <div className="flex items-center justify-between text-[10px] text-text-secondary">
                                                    <span>Lease Remaining</span>
                                                    <span className={cn("font-mono", leaseColor)}>
                                                        {Math.round(getLeasePercentage(group.leaseRemaining))}% ({formatTime(group.leaseRemaining)} left)
                                                    </span>
                                                </div>
                                                <div className="w-full h-2 bg-surface-border/30 rounded-full overflow-hidden">
                                                    <div
                                                        className={cn(
                                                            "h-full bg-gradient-to-r transition-all duration-100",
                                                            getLeaseBarColor(group.leaseRemaining)
                                                        )}
                                                        style={{ width: `${getLeasePercentage(group.leaseRemaining)}%` }}
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                    ) : (
                                        <div className="flex items-center gap-2 text-red-400">
                                            <AlertTriangle className="w-4 h-4" />
                                            <span className="text-sm font-medium">
                                                Lease Expired {formatTime(Math.abs(group.leaseRemaining))} ago
                                            </span>
                                        </div>
                                    )}

                                    {/* Standby Queue */}
                                    <div className="space-y-2">
                                        <div className="text-xs font-bold text-text-secondary uppercase">
                                            Standby Queue ({group.standbys.length})
                                        </div>
                                        {group.standbys.length === 0 ? (
                                            <div className="text-xs text-text-secondary italic">No standby consumers</div>
                                        ) : (
                                            <div className="space-y-1">
                                                {group.standbys.map((standby, idx) => (
                                                    <div
                                                        key={standby.id}
                                                        className="flex items-center justify-between px-3 py-2 bg-surface-hover/30 rounded border border-surface-border"
                                                    >
                                                        <div className="flex items-center gap-2">
                                                            <span className="text-xs font-mono text-text-secondary">
                                                                {idx + 1}.
                                                            </span>
                                                            <span className="text-xs font-medium text-text-primary">
                                                                {standby.id}
                                                            </span>
                                                        </div>
                                                        <span
                                                            className={cn(
                                                                "text-[10px] px-2 py-0.5 rounded-full font-medium",
                                                                standby.status === 'ready' && "bg-blue-500/20 text-blue-400",
                                                                standby.status === 'acquiring' && "bg-yellow-500/20 text-yellow-400 animate-pulse",
                                                                standby.status === 'unavailable' && "bg-gray-500/20 text-gray-400"
                                                            )}
                                                        >
                                                            {standby.status === 'ready' && 'Ready'}
                                                            {standby.status === 'acquiring' && 'Acquiring...'}
                                                            {standby.status === 'unavailable' && 'Unavailable'}
                                                        </span>
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>

                                    {/* Last Failover Info */}
                                    <div className="text-[10px] text-text-secondary">
                                        Last failover: {formatTime(Date.now() - group.lastFailover)} ago
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
