import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import {
    ChevronLeft, MessageSquare, Inbox, Clock, Activity,
    ArrowDownRight, ArrowUpRight, SkullIcon, CheckCircle,
    XCircle, AlertTriangle, Package, Timer, RefreshCw,
    Trash2, RotateCcw, Eye, Copy, ChevronDown, ChevronRight,
    Gauge, Shield, Tag, Hash
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card";
import { PageTabs } from "../components/ui/PageTabs";
import { Button } from "../components/ui/Button";
import { cn } from "../lib/utils";
import { api } from "../lib/api";
import type { QueueDetail as QueueDetailType, QueueMessage, QueueDLQEntry } from "../lib/api";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";

// =============================================================================
// Helpers
// =============================================================================

function formatNumber(n: number | undefined | null): string {
    if (!n) return '0';
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

function formatBytes(b: number): string {
    if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB`;
    if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB`;
    if (b >= 1_024) return `${(b / 1_024).toFixed(1)} KB`;
    return `${b} B`;
}

function formatTimeAgo(ms: number): string {
    const diff = Date.now() - ms;
    if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
    if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
    return `${Math.floor(diff / 86_400_000)}d ago`;
}

function formatDate(ms: number): string {
    return new Date(ms).toLocaleString();
}

function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
    return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
}

function formatRate(n: number): string {
    return `${formatNumber(n)}/s`;
}

// =============================================================================
// Message Status Badge
// =============================================================================

function MessageStatusBadge({ status }: { status: QueueMessage['status'] }) {
    const config = {
        available: { icon: Inbox, className: 'bg-surface-hover text-text-secondary', label: 'Available' },
        leased: { icon: Clock, className: 'bg-surface-hover text-text-secondary', label: 'Leased' },
        delayed: { icon: Timer, className: 'bg-surface-hover text-text-secondary', label: 'Delayed' },
    };
    const s = config[status];
    const Icon = s.icon;
    return (
        <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", s.className)}>
            <Icon className="w-3 h-3" /> {s.label}
        </span>
    );
}

// =============================================================================
// Priority Badge
// =============================================================================

function PriorityBadge({ priority }: { priority: number }) {
    const color = priority <= 10 ? 'text-error bg-error/10' :
                  'text-text-secondary bg-surface-hover';
    return (
        <span className={cn("text-[10px] font-mono font-semibold px-1.5 py-0.5 rounded", color)}>
            P{priority}
        </span>
    );
}

// =============================================================================
// Metric Card
// =============================================================================

function MetricCard({ label, value, icon: Icon, iconColor, subtitle }: {
    label: string;
    value: React.ReactNode;
    icon: typeof Activity;
    iconColor: string;
    subtitle?: string;
}) {
    return (
        <Card>
            <CardContent className="p-4">
                <div className="flex items-center justify-between mb-2">
                    <span className="text-xs text-text-secondary uppercase tracking-wider">{label}</span>
                    <Icon className={cn("w-4 h-4", iconColor)} />
                </div>
                <p className="text-2xl font-semibold text-text-primary">{value}</p>
                {subtitle && <p className="text-xs text-text-secondary mt-1">{subtitle}</p>}
            </CardContent>
        </Card>
    );
}

// =============================================================================
// Tab: Overview
// =============================================================================

function OverviewTab({ detail }: { detail: QueueDetailType }) {
    const completionRate = detail.enqueued > 0
        ? ((detail.acked / detail.enqueued) * 100).toFixed(1)
        : '0.0';
    const failureRate = detail.enqueued > 0
        ? ((detail.nacked / detail.enqueued) * 100).toFixed(2)
        : '0.00';

    return (
        <div className="space-y-6">
            {/* DLQ Warning Banner */}
            {detail.dlq_count > 0 && (
                <div className="flex items-start gap-3 p-4 rounded-md border border-error/30 bg-error/5">
                    <AlertTriangle className="w-5 h-5 text-error shrink-0 mt-0.5" />
                    <div>
                        <p className="text-sm font-medium text-error">Dead Letter Queue Active</p>
                        <p className="text-sm text-text-secondary mt-1">
                            {detail.dlq_count} message{detail.dlq_count !== 1 ? 's' : ''} in the dead letter queue.
                            Switch to the DLQ tab to inspect and reprocess failed messages.
                        </p>
                    </div>
                </div>
            )}

            {/* Key Metrics */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <MetricCard
                    label="Enqueue Rate"
                    value={formatRate(detail.enqueue_rate ?? 0)}
                    icon={ArrowDownRight}
                    iconColor="text-text-secondary"
                    subtitle="messages per second"
                />
                <MetricCard
                    label="Dequeue Rate"
                    value={formatRate(detail.dequeue_rate ?? 0)}
                    icon={ArrowUpRight}
                    iconColor="text-text-secondary"
                    subtitle="messages per second"
                />
                <MetricCard
                    label="Completion"
                    value={`${completionRate}%`}
                    icon={CheckCircle}
                    iconColor="text-text-secondary"
                    subtitle={`${formatNumber(detail.acked)} acked`}
                />
                <MetricCard
                    label="Failure Rate"
                    value={`${failureRate}%`}
                    icon={XCircle}
                    iconColor="text-text-secondary"
                    subtitle={`${formatNumber(detail.nacked)} nacked`}
                />
            </div>

            {/* Queue Stats Grid */}
            <div className="grid gap-6 lg:grid-cols-2">
                {/* Message Breakdown */}
                <Card>
                    <CardContent className="p-6">
                        <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                            <Package className="w-4 h-4 text-text-secondary" /> Message Breakdown
                        </h3>
                        <div className="space-y-3">
                            <StatRow label="Available" value={formatNumber(detail.available)} />
                            <StatRow label="In-Flight (Leased)" value={formatNumber(detail.pending)} />
                            <StatRow label="Delayed" value={formatNumber(detail.delayed ?? 0)} />
                            <div className="border-t border-surface-border my-2" />
                            <StatRow label="Total Enqueued" value={formatNumber(detail.enqueued)} />
                            <StatRow label="Total Dequeued" value={formatNumber(detail.dequeued)} />
                            <StatRow label="Acknowledged" value={formatNumber(detail.acked)} />
                            <StatRow label="Negative Acked" value={formatNumber(detail.nacked)} color={detail.nacked > 0 ? "text-error" : undefined} />
                            <StatRow label="Dead Letters" value={formatNumber(detail.dlq_count)} color={detail.dlq_count > 0 ? "text-error" : undefined} />
                        </div>
                    </CardContent>
                </Card>

                {/* Queue Configuration */}
                <Card>
                    <CardContent className="p-6">
                        <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                            <Shield className="w-4 h-4 text-text-secondary" /> Configuration
                        </h3>
                        <div className="space-y-3">
                            <StatRow label="Lease Timeout" value={formatDuration(detail.lease_timeout_ms ?? 30000)} />
                            <StatRow label="Max Retries" value={String(detail.max_retries ?? 3)} />
                            <StatRow label="Total Bytes" value={formatBytes(detail.bytes_total)} />
                            <StatRow label="Created" value={detail.created_at_ms ? formatDate(detail.created_at_ms) : '—'} />
                        </div>

                        {/* Visual: Message Lifecycle */}
                        <div className="mt-6 p-4 bg-surface-hover/50 rounded-md">
                            <p className="text-xs text-text-secondary uppercase tracking-wider mb-3">Message Lifecycle</p>
                            <div className="flex items-center gap-1 text-xs">
                                <span className="px-2 py-1 bg-primary/10 text-primary rounded font-medium">Enqueue</span>
                                <ChevronRight className="w-3 h-3 text-text-secondary" />
                                <span className="px-2 py-1 bg-surface-border/50 text-text-primary rounded font-medium">Available</span>
                                <ChevronRight className="w-3 h-3 text-text-secondary" />
                                <span className="px-2 py-1 bg-surface-border/50 text-text-primary rounded font-medium">Leased</span>
                                <ChevronRight className="w-3 h-3 text-text-secondary" />
                                <span className="px-2 py-1 bg-surface-border/50 text-text-primary rounded font-medium">Ack</span>
                            </div>
                            <div className="flex items-center gap-1 text-xs mt-2 ml-[170px]">
                                <span className="text-text-secondary">└</span>
                                <span className="px-2 py-1 bg-surface-border/50 text-text-secondary rounded font-medium">Retry</span>
                                <span className="text-text-secondary">→</span>
                                <span className="px-2 py-1 bg-error/10 text-error rounded font-medium">DLQ</span>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}

function StatRow({ label, value, color }: { label: string; value: string; color?: string }) {
    return (
        <div className="flex items-center justify-between">
            <span className="text-sm text-text-secondary">{label}</span>
            <span className={cn("text-sm font-medium tabular-nums", color || "text-text-primary")}>{value}</span>
        </div>
    );
}

// =============================================================================
// Tab: Messages
// =============================================================================

function MessagesTab({ queueName, detail }: { queueName: string; detail: QueueDetailType }) {
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [expandedMsg, setExpandedMsg] = useState<number | null>(null);

    // Fetch messages from API
    const { data: msgResponse } = useApi(
        () => api.getQueueMessages(queueName, statusFilter === 'all' ? undefined : statusFilter),
        [queueName, statusFilter],
        5000
    );

    const messages: QueueMessage[] = msgResponse?.messages ?? [];

    const statusCounts = {
        all: detail.available + detail.pending,
        available: detail.available,
        leased: detail.pending,
        delayed: 0,
    };

    return (
        <div className="space-y-4">
            {/* Filter bar */}
            <div className="flex items-center gap-2">
                {(['all', 'available', 'leased', 'delayed'] as const).map(s => (
                    <button
                        key={s}
                        onClick={() => setStatusFilter(s)}
                        className={cn(
                            "px-3 py-1.5 rounded-md text-xs font-medium transition-colors capitalize",
                            statusFilter === s
                                ? "bg-primary text-white"
                                : "bg-surface-hover text-text-secondary hover:text-text-primary"
                        )}
                    >
                        {s} {statusCounts[s] > 0 && <span className="ml-1 opacity-60">{statusCounts[s]}</span>}
                    </button>
                ))}
            </div>

            {/* Messages Table */}
            <Card>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="bg-surface-hover/50 text-text-secondary text-xs uppercase tracking-wider">
                            <tr>
                                <th className="px-4 py-3 font-medium w-8" />
                                <th className="px-4 py-3 font-medium">Seq</th>
                                <th className="px-4 py-3 font-medium">Priority</th>
                                <th className="px-4 py-3 font-medium">Status</th>
                                <th className="px-4 py-3 font-medium">Type</th>
                                <th className="px-4 py-3 font-medium">Consumer</th>
                                <th className="px-4 py-3 font-medium">Enqueued</th>
                                <th className="px-4 py-3 font-medium">Deliveries</th>
                            </tr>
                        </thead>
                        <tbody>
                            {messages.length === 0 ? (
                                <tr>
                                    <td colSpan={8} className="px-4 py-12 text-center text-text-secondary text-sm">
                                        No messages match this filter.
                                    </td>
                                </tr>
                            ) : (
                                messages.map(msg => (
                                    <MessageRow
                                        key={msg.seq}
                                        message={msg}
                                        isExpanded={expandedMsg === msg.seq}
                                        onToggle={() => setExpandedMsg(expandedMsg === msg.seq ? null : msg.seq)}
                                    />
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}

function MessageRow({ message, isExpanded, onToggle }: {
    message: QueueMessage;
    isExpanded: boolean;
    onToggle: () => void;
}) {
    return (
        <>
            <tr
                onClick={onToggle}
                className="border-b border-surface-border hover:bg-surface-hover/50 cursor-pointer transition-colors group"
            >
                <td className="px-4 py-3">
                    <ChevronRight className={cn(
                        "w-3.5 h-3.5 text-text-secondary transition-transform",
                        isExpanded && "rotate-90"
                    )} />
                </td>
                <td className="px-4 py-3 font-mono text-xs text-text-primary">
                    #{message.seq}
                </td>
                <td className="px-4 py-3">
                    <PriorityBadge priority={message.priority} />
                </td>
                <td className="px-4 py-3">
                    <MessageStatusBadge status={message.status} />
                </td>
                <td className="px-4 py-3">
                    {message.message_type ? (
                        <span className="text-xs font-mono text-text-secondary bg-surface-hover px-1.5 py-0.5 rounded">
                            {message.message_type}
                        </span>
                    ) : (
                        <span className="text-text-secondary text-xs">—</span>
                    )}
                </td>
                <td className="px-4 py-3 text-xs text-text-secondary">
                    {message.consumer || '—'}
                </td>
                <td className="px-4 py-3 text-xs text-text-secondary">
                    {formatTimeAgo(message.enqueued_at_ms)}
                </td>
                <td className="px-4 py-3">
                    {message.delivery_count > 0 ? (
                        <span className={cn(
                            "text-xs font-mono px-1.5 py-0.5 rounded",
                            message.delivery_count >= 3 ? "bg-error/10 text-error" :
                            message.delivery_count >= 2 ? "bg-warning/10 text-warning" :
                            "bg-text-secondary/10 text-text-secondary"
                        )}>
                            {message.delivery_count}×
                        </span>
                    ) : (
                        <span className="text-text-secondary text-xs">—</span>
                    )}
                </td>
            </tr>
            {/* Expanded Payload */}
            {isExpanded && (
                <tr className="bg-surface-hover/30">
                    <td colSpan={8} className="px-4 py-4">
                        <div className="space-y-3">
                            {/* Metadata row */}
                            <div className="flex flex-wrap gap-4 text-xs">
                                {message.dedup_key && (
                                    <div className="flex items-center gap-1">
                                        <Hash className="w-3 h-3 text-text-secondary" />
                                        <span className="text-text-secondary">Dedup:</span>
                                        <span className="font-mono text-text-primary">{message.dedup_key}</span>
                                    </div>
                                )}
                                {message.correlation_id && (
                                    <div className="flex items-center gap-1">
                                        <Tag className="w-3 h-3 text-text-secondary" />
                                        <span className="text-text-secondary">Correlation:</span>
                                        <span className="font-mono text-text-primary">{message.correlation_id}</span>
                                    </div>
                                )}
                                {message.lease_expires_ms && (
                                    <div className="flex items-center gap-1">
                                        <Clock className="w-3 h-3 text-text-secondary" />
                                        <span className="text-text-secondary">Lease expires:</span>
                                        <span className="font-mono text-text-primary">
                                            {formatDuration(Math.max(0, message.lease_expires_ms - Date.now()))}
                                        </span>
                                    </div>
                                )}
                                {message.delay_until_ms && (
                                    <div className="flex items-center gap-1">
                                        <Timer className="w-3 h-3 text-text-secondary" />
                                        <span className="text-text-secondary">Available in:</span>
                                        <span className="font-mono text-text-primary">
                                            {formatDuration(Math.max(0, message.delay_until_ms - Date.now()))}
                                        </span>
                                    </div>
                                )}
                            </div>
                            {/* Payload */}
                            <div>
                                <div className="flex items-center justify-between mb-1.5">
                                    <span className="text-xs text-text-secondary uppercase tracking-wider">Payload</span>
                                    <button
                                        onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(message.payload); }}
                                        className="text-xs text-text-secondary hover:text-text-primary flex items-center gap-1 transition-colors"
                                    >
                                        <Copy className="w-3 h-3" /> Copy
                                    </button>
                                </div>
                                <pre className="text-xs font-mono bg-background p-3 rounded-md border border-surface-border overflow-x-auto max-h-48 text-text-primary">
                                    {(() => {
                                        try { return JSON.stringify(JSON.parse(message.payload), null, 2); }
                                        catch { return message.payload; }
                                    })()}
                                </pre>
                            </div>
                        </div>
                    </td>
                </tr>
            )}
        </>
    );
}

// =============================================================================
// Tab: Dead Letter Queue
// =============================================================================

function DLQTab({ queueName, dlqCount }: { queueName: string; dlqCount: number }) {
    const [expandedEntry, setExpandedEntry] = useState<number | null>(null);

    // Fetch DLQ entries from API
    const { data: dlqResponse } = useApi(
        () => api.getQueueDLQ(queueName),
        [queueName],
        5000
    );

    const entries: QueueDLQEntry[] = dlqResponse?.entries ?? [];

    if (entries.length === 0) {
        return (
            <Card>
                <CardContent className="p-12 text-center">
                    <CheckCircle className="w-12 h-12 text-success mx-auto mb-3 opacity-50" />
                    <p className="text-text-primary font-medium">No Dead Letters</p>
                    <p className="text-sm text-text-secondary mt-1">All messages are being processed successfully.</p>
                </CardContent>
            </Card>
        );
    }

    return (
        <div className="space-y-4">
            {/* DLQ Actions bar */}
            <div className="flex items-center justify-between">
                <p className="text-sm text-text-secondary">
                    {entries.length} failed message{entries.length !== 1 ? 's' : ''} in dead letter queue
                </p>
                <div className="flex items-center gap-2">
                    <Button variant="secondary" size="sm">
                        <RotateCcw className="w-3 h-3" /> Requeue All
                    </Button>
                    <Button variant="danger" size="sm">
                        <Trash2 className="w-3 h-3" /> Purge DLQ
                    </Button>
                </div>
            </div>

            {/* DLQ Table */}
            <Card>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="bg-surface-hover/50 text-text-secondary text-xs uppercase tracking-wider">
                            <tr>
                                <th className="px-4 py-3 font-medium w-8" />
                                <th className="px-4 py-3 font-medium">Seq</th>
                                <th className="px-4 py-3 font-medium">Error</th>
                                <th className="px-4 py-3 font-medium">Type</th>
                                <th className="px-4 py-3 font-medium">Attempts</th>
                                <th className="px-4 py-3 font-medium">Failed At</th>
                                <th className="px-4 py-3 font-medium">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {entries.map(entry => (
                                <DLQRow
                                    key={entry.seq}
                                    entry={entry}
                                    isExpanded={expandedEntry === entry.seq}
                                    onToggle={() => setExpandedEntry(expandedEntry === entry.seq ? null : entry.seq)}
                                />
                            ))}
                        </tbody>
                    </table>
                </div>
            </Card>
        </div>
    );
}

function DLQRow({ entry, isExpanded, onToggle }: {
    entry: QueueDLQEntry;
    isExpanded: boolean;
    onToggle: () => void;
}) {
    return (
        <>
            <tr
                onClick={onToggle}
                className="border-b border-surface-border hover:bg-surface-hover/50 cursor-pointer transition-colors group"
            >
                <td className="px-4 py-3">
                    <ChevronRight className={cn(
                        "w-3.5 h-3.5 text-text-secondary transition-transform",
                        isExpanded && "rotate-90"
                    )} />
                </td>
                <td className="px-4 py-3 font-mono text-xs text-text-primary">
                    #{entry.seq}
                </td>
                <td className="px-4 py-3">
                    <div className="flex items-start gap-2 max-w-sm">
                        <XCircle className="w-3.5 h-3.5 text-error shrink-0 mt-0.5" />
                        <span className="text-xs text-text-primary truncate">{entry.error_msg}</span>
                    </div>
                </td>
                <td className="px-4 py-3">
                    {entry.message_type ? (
                        <span className="text-xs font-mono text-text-secondary bg-surface-hover px-1.5 py-0.5 rounded">
                            {entry.message_type}
                        </span>
                    ) : (
                        <span className="text-text-secondary text-xs">—</span>
                    )}
                </td>
                <td className="px-4 py-3">
                    <span className={cn(
                        "text-xs font-mono px-1.5 py-0.5 rounded",
                        entry.attempts >= 3 ? "bg-error/10 text-error" : "bg-warning/10 text-warning"
                    )}>
                        {entry.attempts}×
                    </span>
                </td>
                <td className="px-4 py-3 text-xs text-text-secondary">
                    {formatTimeAgo(entry.dlq_at_ms)}
                </td>
                <td className="px-4 py-3">
                    <div className="flex items-center gap-1" onClick={e => e.stopPropagation()}>
                        <Button variant="ghost" size="sm" title="Requeue">
                            <RotateCcw className="w-3 h-3" />
                        </Button>
                        <Button variant="ghost" size="sm" title="Delete">
                            <Trash2 className="w-3 h-3" />
                        </Button>
                    </div>
                </td>
            </tr>
            {/* Expanded Row */}
            {isExpanded && (
                <tr className="bg-surface-hover/30">
                    <td colSpan={7} className="px-4 py-4">
                        <div className="space-y-3">
                            {/* Error Detail */}
                            <div className="flex items-start gap-3 p-3 rounded-md border border-error/20 bg-error/5">
                                <AlertTriangle className="w-4 h-4 text-error shrink-0 mt-0.5" />
                                <div>
                                    <p className="text-xs font-medium text-error">Failure Reason</p>
                                    <p className="text-xs text-text-secondary mt-1">{entry.error_msg}</p>
                                    <p className="text-xs text-text-secondary mt-1">
                                        Failed after {entry.attempts} attempt{entry.attempts !== 1 ? 's' : ''} •
                                        Partition {entry.partition} •
                                        {formatDate(entry.dlq_at_ms)}
                                    </p>
                                </div>
                            </div>
                            {/* Payload */}
                            <div>
                                <div className="flex items-center justify-between mb-1.5">
                                    <span className="text-xs text-text-secondary uppercase tracking-wider">Original Payload</span>
                                    <button
                                        onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(entry.payload); }}
                                        className="text-xs text-text-secondary hover:text-text-primary flex items-center gap-1 transition-colors"
                                    >
                                        <Copy className="w-3 h-3" /> Copy
                                    </button>
                                </div>
                                <pre className="text-xs font-mono bg-background p-3 rounded-md border border-surface-border overflow-x-auto max-h-48 text-text-primary">
                                    {(() => {
                                        try { return JSON.stringify(JSON.parse(entry.payload), null, 2); }
                                        catch { return entry.payload; }
                                    })()}
                                </pre>
                            </div>
                            {/* Requeue actions */}
                            <div className="flex items-center gap-2">
                                <Button variant="primary" size="sm">
                                    <RotateCcw className="w-3 h-3" /> Requeue to Main Queue
                                </Button>
                                <Button variant="danger" size="sm">
                                    <Trash2 className="w-3 h-3" /> Delete Permanently
                                </Button>
                            </div>
                        </div>
                    </td>
                </tr>
            )}
        </>
    );
}

// =============================================================================
// Main Page
// =============================================================================

export function QueueDetail() {
    const { queueName } = useParams();
    const [activeTab, setActiveTab] = useState('overview');

    // Try real API, fall back to mock
    const { data: apiDetail, loading, error, refetch } = useApi(
        () => api.getQueueDetail(queueName || ''),
        [queueName],
        5000
    );

    const detail: QueueDetailType | null = apiDetail ?? null;

    if (loading && !detail) return <LoadingState />;
    if (!detail) return <ErrorState message="Queue not found" onRetry={refetch} />;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-4">
                <Link to="/queues" className="p-2 hover:bg-surface-hover rounded-md text-text-secondary transition-colors">
                    <ChevronLeft className="w-5 h-5" />
                </Link>
                <div className="flex-1 min-w-0">
                    <h1 className="text-2xl font-semibold text-text-primary flex items-center gap-3">
                        <MessageSquare className="w-6 h-6 text-primary" />
                        {detail.name}
                        {detail.available > 0 || detail.pending > 0 ? (
                            <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-primary/10 text-primary border border-primary/20">
                                Active
                            </span>
                        ) : (
                            <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-text-secondary/10 text-text-secondary">
                                Idle
                            </span>
                        )}
                    </h1>
                    <p className="text-text-secondary text-sm mt-0.5">
                        {detail.namespace}
                        {' • '}{formatNumber(detail.available)} available
                        {' • '}{formatNumber(detail.pending)} in-flight
                        {' • '}{formatBytes(detail.bytes_total)}
                    </p>
                </div>
                <div className="flex items-center gap-2">
                    <Button variant="secondary" size="sm">
                        <RefreshCw className="w-3.5 h-3.5" /> Refresh
                    </Button>
                    <Button variant="danger" size="sm">
                        <Trash2 className="w-3.5 h-3.5" /> Purge
                    </Button>
                </div>
            </div>

            {/* Quick Stats Strip */}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                <QuickStat label="Available" value={formatNumber(detail.available)} />
                <QuickStat label="In-Flight" value={formatNumber(detail.pending)} />
                <QuickStat label="Delayed" value={formatNumber(detail.delayed ?? 0)} />
                <QuickStat label="Acked" value={formatNumber(detail.acked)} />
                <QuickStat label="Nacked" value={formatNumber(detail.nacked)} highlight={detail.nacked > 0} />
                <QuickStat label="DLQ" value={formatNumber(detail.dlq_count)} highlight={detail.dlq_count > 0} />
            </div>

            {/* Tabs */}
            <PageTabs
                tabs={[
                    { id: 'overview', label: 'Overview' },
                    { id: 'messages', label: 'Messages', count: detail.available + detail.pending },
                    { id: 'dlq', label: 'Dead Letters', count: detail.dlq_count },
                ]}
                activeTab={activeTab}
                onChange={setActiveTab}
            />

            {/* Tab Content */}
            {activeTab === 'overview' && <OverviewTab detail={detail} />}
            {activeTab === 'messages' && <MessagesTab queueName={detail.name} detail={detail} />}
            {activeTab === 'dlq' && <DLQTab queueName={detail.name} dlqCount={detail.dlq_count} />}
        </div>
    );
}

function QuickStat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
    return (
        <div className="bg-surface border border-surface-border rounded-md px-3 py-2">
            <p className="text-[10px] text-text-secondary uppercase tracking-wider">{label}</p>
            <p className={cn("text-lg font-semibold tabular-nums text-text-primary", highlight && "text-error")}>{value}</p>
        </div>
    );
}
