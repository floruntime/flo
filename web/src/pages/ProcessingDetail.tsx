import { useState, useCallback } from "react";
import { useParams, Link } from "react-router-dom";
import {
    ChevronLeft, Play, Pause, Square, RotateCcw, Save,
    Activity, ArrowDownRight, ArrowUpRight, Clock, Gauge,
    CheckCircle, XCircle, AlertTriangle, Layers,
    FileCode, History, BarChart3, Shield, Tag, Loader2
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card";
import { PageTabs } from "../components/ui/PageTabs";
import { cn } from "../lib/utils";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { api } from "../lib/api";
import type { ProcessingJobDetail, JobState, JobEvent } from "../lib/processing-types";

// =============================================================================
// Helpers
// =============================================================================

function formatNumber(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

function formatRate(n: number): string {
    return `${formatNumber(n)}/s`;
}

function formatDuration(ns: number): string {
    const ms = ns / 1_000_000;
    if (ms < 1) return `${(ns / 1_000).toFixed(0)}µs`;
    if (ms < 1000) return `${ms.toFixed(1)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
}

function formatBytes(b: number): string {
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

function formatUptime(ms: number): string {
    const sec = Math.floor(ms / 1000);
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ${sec % 60}s`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ${min % 60}m`;
    const days = Math.floor(hr / 24);
    return `${days}d ${hr % 24}h`;
}

// =============================================================================
// Status Badge
// =============================================================================

function StateBadge({ state }: { state: JobState }) {
    const map: Record<JobState, { icon: typeof Play; className: string; label: string }> = {
        CREATED: { icon: Clock, className: 'bg-text-secondary/10 text-text-secondary', label: 'Created' },
        RUNNING: { icon: Play, className: 'bg-blue-500/10 text-blue-500', label: 'Running' },
        FINISHED: { icon: CheckCircle, className: 'bg-success/10 text-success', label: 'Finished' },
        STOPPED: { icon: Pause, className: 'bg-warning/10 text-warning', label: 'Stopped' },
        CANCELLED: { icon: XCircle, className: 'bg-text-secondary/10 text-text-secondary', label: 'Cancelled' },
        FAILED: { icon: XCircle, className: 'bg-error/10 text-error', label: 'Failed' },
    };
    const s = map[state];
    const Icon = s.icon;
    return (
        <span className={cn("inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-sm font-medium", s.className)}>
            <Icon className="w-3.5 h-3.5" /> {s.label}
        </span>
    );
}

// =============================================================================
// Operator Type Badge
// =============================================================================

function OperatorTypeBadge({ type }: { type: string }) {
    const colors: Record<string, string> = {
        filter: 'text-yellow-400 bg-yellow-400/10',
        map: 'text-blue-400 bg-blue-400/10',
        flatmap: 'text-indigo-400 bg-indigo-400/10',
        keyby: 'text-emerald-400 bg-emerald-400/10',
        aggregate: 'text-purple-400 bg-purple-400/10',
        passthrough: 'text-text-secondary bg-text-secondary/10',
        wasm: 'text-orange-400 bg-orange-400/10',
    };
    return (
        <span className={cn("text-[10px] uppercase font-semibold px-1.5 py-0.5 rounded", colors[type] || 'text-text-secondary bg-text-secondary/10')}>
            {type}
        </span>
    );
}

// =============================================================================
// Endpoint Badge
// =============================================================================

function EndpointBadge({ type }: { type: string }) {
    const color = type === 'stream' ? 'text-blue-400 bg-blue-400/10' :
                  type === 'kv' ? 'text-emerald-400 bg-emerald-400/10' :
                  'text-purple-400 bg-purple-400/10';
    return (
        <span className={cn("text-[10px] uppercase font-semibold px-1.5 py-0.5 rounded", color)}>
            {type}
        </span>
    );
}

// =============================================================================
// Checkpoint Status Badge
// =============================================================================

function CheckpointStatusBadge({ status }: { status: string }) {
    if (status === 'completed') return <span className="text-xs text-success font-medium">Completed</span>;
    if (status === 'failed') return <span className="text-xs text-error font-medium">Failed</span>;
    return <span className="text-xs text-warning font-medium">In Progress</span>;
}

// =============================================================================
// BackpressureBar
// =============================================================================

function BackpressureBar({ ratio, large }: { ratio: number; large?: boolean }) {
    const pct = Math.round(ratio * 100);
    const color = ratio > 0.8 ? 'bg-error' : ratio > 0.5 ? 'bg-warning' : 'bg-success';
    return (
        <div className="flex items-center gap-2">
            <div className={cn("flex-1 bg-surface-hover rounded-full overflow-hidden", large ? "h-2.5 max-w-[120px]" : "h-1.5 max-w-[80px]")}>
                <div className={cn("h-full rounded-full transition-all", color)} style={{ width: `${pct}%` }} />
            </div>
            <span className={cn("tabular-nums font-mono", large ? "text-sm" : "text-xs", ratio > 0.8 ? 'text-error' : 'text-text-secondary')}>{pct}%</span>
        </div>
    );
}

// =============================================================================
// Tab: Overview
// =============================================================================

function OverviewTab({ job }: { job: ProcessingJobDetail }) {
    const isRunning = job.state === 'RUNNING';
    const uptime = isRunning
        ? Date.now() - job.created_at_ms
        : (job.completed_at_ms ?? job.updated_at_ms) - job.created_at_ms;

    return (
        <div className="space-y-6">
            {/* Error Banner */}
            {job.error_message && (
                <div className="flex items-start gap-3 p-4 rounded-md border border-error/30 bg-error/5">
                    <AlertTriangle className="w-5 h-5 text-error shrink-0 mt-0.5" />
                    <div>
                        <p className="text-sm font-medium text-error">Pipeline Error</p>
                        <p className="text-sm text-text-secondary mt-1">{job.error_message}</p>
                    </div>
                </div>
            )}

            {/* Key Metrics Grid */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <MetricCard label="Input Rate" value={isRunning ? formatRate(job.metrics.input_throughput) : '—'} icon={ArrowDownRight} iconColor="text-blue-400" />
                <MetricCard label="Output Rate" value={isRunning ? formatRate(job.metrics.output_throughput) : '—'} icon={ArrowUpRight} iconColor="text-emerald-400" />
                <MetricCard label="E2E Latency" value={isRunning ? `${job.metrics.e2e_latency_ms.toFixed(1)}ms` : '—'} icon={Clock} iconColor="text-warning" />
                <MetricCard label="Backpressure" value={<BackpressureBar ratio={job.metrics.backpressure.busy_ratio} large />} icon={Gauge} iconColor={job.metrics.backpressure.is_backpressured ? "text-error" : "text-success"} />
            </div>

            {/* Pipeline Topology (Visual DAG) */}
            <Card>
                <CardContent className="p-6">
                    <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                        <Layers className="w-4 h-4 text-text-secondary" /> Pipeline Topology
                    </h3>
                    <div className="flex items-center gap-0 overflow-x-auto pb-2">
                        {/* Source */}
                        <TopologyNode
                            label={job.source.name}
                            sublabel={`${job.source.namespace}/${job.source.target}`}
                            type="source"
                            badge={<EndpointBadge type={job.source.type} />}
                        />
                        <TopologyArrow records={job.operators[0]?.records_in} />

                        {/* Operators */}
                        {job.operators.map((op, i) => (
                            <div key={op.name} className="flex items-center">
                                <TopologyNode
                                    label={op.name}
                                    sublabel={`${formatNumber(op.records_in)} → ${formatNumber(op.records_out)}`}
                                    type="operator"
                                    badge={<OperatorTypeBadge type={op.type} />}
                                    stateful={op.stateful}
                                    hasErrors={op.errors > 0}
                                />
                                {i < job.operators.length - 1 && (
                                    <TopologyArrow records={job.operators[i + 1].records_in} />
                                )}
                            </div>
                        ))}

                        <TopologyArrow records={job.operators[job.operators.length - 1]?.records_out} />

                        {/* Sink */}
                        <TopologyNode
                            label={job.sink.name}
                            sublabel={`${job.sink.namespace}/${job.sink.target}`}
                            type="sink"
                            badge={<EndpointBadge type={job.sink.type} />}
                        />
                    </div>
                </CardContent>
            </Card>

            {/* Two-Column Details */}
            <div className="grid gap-6 md:grid-cols-2">
                {/* Job Info */}
                <Card>
                    <CardContent className="p-6">
                        <h3 className="text-sm font-medium text-text-primary mb-4">Job Details</h3>
                        <dl className="space-y-3 text-sm">
                            <DetailRow label="Job ID" value={<span className="font-mono text-xs">{job.job_id}</span>} />
                            <DetailRow label="Namespace" value={job.namespace} />
                            <DetailRow label="Parallelism" value={`×${job.parallelism}`} />
                            <DetailRow label="Records Processed" value={formatNumber(job.records_processed)} />
                            <DetailRow label="Uptime" value={formatUptime(uptime)} />
                            <DetailRow label="Created" value={formatDate(job.created_at_ms)} />
                            <DetailRow label="Last Updated" value={formatTimeAgo(job.updated_at_ms)} />
                            {job.savepoint_id && (
                                <DetailRow label="Last Savepoint" value={<span className="font-mono text-xs">{job.savepoint_id}</span>} />
                            )}
                        </dl>
                    </CardContent>
                </Card>

                {/* Watermark & Checkpointing */}
                <Card>
                    <CardContent className="p-6">
                        <h3 className="text-sm font-medium text-text-primary mb-4">Time & Checkpointing</h3>
                        <dl className="space-y-3 text-sm">
                            {job.watermark && (
                                <>
                                    <DetailRow label="Watermark Strategy" value={
                                        <span className="capitalize">{job.watermark.strategy.replace(/_/g, ' ')}</span>
                                    } />
                                    {job.watermark.strategy !== 'none' && (
                                        <DetailRow label="Current Watermark" value={formatTimeAgo(job.watermark.current_watermark_ms)} />
                                    )}
                                    {job.watermark.max_delay_ms && (
                                        <DetailRow label="Max Delay" value={`${job.watermark.max_delay_ms}ms`} />
                                    )}
                                    <DetailRow label="Watermark Lag" value={`${job.metrics.watermark_lag_ms}ms`} />
                                </>
                            )}
                            <DetailRow label="Checkpoints Completed" value={job.metrics.checkpoints_completed.toLocaleString()} />
                            <DetailRow label="Last Checkpoint" value={job.metrics.last_checkpoint_duration_ms > 0 ? `${job.metrics.last_checkpoint_duration_ms}ms` : '—'} />
                            <DetailRow label="Records Dropped" value={
                                <span className={job.metrics.records_dropped > 0 ? 'text-warning' : ''}>
                                    {job.metrics.records_dropped.toLocaleString()}
                                </span>
                            } />
                        </dl>

                        {/* Side Outputs */}
                        {job.side_outputs && job.side_outputs.length > 0 && (
                            <div className="mt-6 pt-4 border-t border-surface-border">
                                <h4 className="text-xs text-text-secondary uppercase tracking-wider mb-3 flex items-center gap-1.5">
                                    <Tag className="w-3 h-3" /> Side Outputs
                                </h4>
                                <div className="space-y-2">
                                    {job.side_outputs.map(so => (
                                        <div key={so.tag} className="flex items-center justify-between text-sm">
                                            <span className="font-mono text-xs text-text-secondary">{so.tag}</span>
                                            <span className="tabular-nums text-text-primary">{formatNumber(so.total_emitted)}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}

// =============================================================================
// Tab: Operators
// =============================================================================

function OperatorsTab({ job }: { job: ProcessingJobDetail }) {
    return (
        <div className="space-y-4">
            {job.operators.map((op, i) => (
                <Card key={op.name}>
                    <CardContent className="p-5">
                        <div className="flex items-center justify-between mb-4">
                            <div className="flex items-center gap-3">
                                <span className="text-sm text-text-secondary font-mono">#{i + 1}</span>
                                <h4 className="font-medium text-text-primary">{op.name}</h4>
                                <OperatorTypeBadge type={op.type} />
                                {op.stateful && (
                                    <span className="text-[10px] uppercase font-semibold px-1.5 py-0.5 rounded text-amber-400 bg-amber-400/10">
                                        stateful
                                    </span>
                                )}
                            </div>
                            {op.errors > 0 && (
                                <span className="flex items-center gap-1 text-xs text-error">
                                    <AlertTriangle className="w-3 h-3" /> {op.errors} errors
                                </span>
                            )}
                        </div>
                        <div className="grid gap-4 md:grid-cols-5 text-sm">
                            <div>
                                <span className="text-xs text-text-secondary">Records In</span>
                                <p className="font-medium text-text-primary mt-0.5 tabular-nums">{formatNumber(op.records_in)}</p>
                            </div>
                            <div>
                                <span className="text-xs text-text-secondary">Records Out</span>
                                <p className="font-medium text-text-primary mt-0.5 tabular-nums">{formatNumber(op.records_out)}</p>
                            </div>
                            <div>
                                <span className="text-xs text-text-secondary">Selectivity</span>
                                <p className="font-medium text-text-primary mt-0.5 tabular-nums">
                                    {op.records_in > 0 ? `${((op.records_out / op.records_in) * 100).toFixed(1)}%` : '—'}
                                </p>
                            </div>
                            <div>
                                <span className="text-xs text-text-secondary">Processing Time</span>
                                <p className="font-medium text-text-primary mt-0.5">{formatDuration(op.processing_time_ns)}</p>
                            </div>
                            <div>
                                <span className="text-xs text-text-secondary">Last Active</span>
                                <p className="font-medium text-text-primary mt-0.5">{formatTimeAgo(op.last_processed_ms)}</p>
                            </div>
                        </div>
                        {/* Config */}
                        {op.config && op.config.length > 0 && (
                            <div className="mt-4 pt-3 border-t border-surface-border">
                                <p className="text-xs text-text-secondary mb-2">Configuration</p>
                                <div className="flex flex-wrap gap-2">
                                    {op.config.map(c => (
                                        <span key={c.key} className="text-xs font-mono bg-surface-hover px-2 py-1 rounded text-text-secondary">
                                            {c.key}=<span className="text-text-primary">{c.value}</span>
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                    </CardContent>
                </Card>
            ))}
        </div>
    );
}

// =============================================================================
// Tab: Metrics
// =============================================================================

function MetricsTab({ job }: { job: ProcessingJobDetail }) {
    const m = job.metrics;
    const maxBucket = Math.max(...m.latency_histogram.map(b => b.count));

    return (
        <div className="space-y-6">
            {/* Summary Row */}
            <div className="grid gap-4 md:grid-cols-3">
                <Card>
                    <CardContent className="p-4">
                        <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Throughput</p>
                        <div className="flex items-baseline gap-3">
                            <div>
                                <span className="text-lg font-semibold text-text-primary tabular-nums">{formatRate(m.input_throughput)}</span>
                                <span className="text-xs text-text-secondary ml-1">in</span>
                            </div>
                            <span className="text-text-secondary">→</span>
                            <div>
                                <span className="text-lg font-semibold text-text-primary tabular-nums">{formatRate(m.output_throughput)}</span>
                                <span className="text-xs text-text-secondary ml-1">out</span>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="p-4">
                        <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">End-to-End Latency</p>
                        <span className="text-lg font-semibold text-text-primary tabular-nums">{m.e2e_latency_ms.toFixed(1)}</span>
                        <span className="text-sm text-text-secondary ml-1">ms avg</span>
                    </CardContent>
                </Card>
                <Card>
                    <CardContent className="p-4">
                        <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Backpressure</p>
                        <BackpressureBar ratio={m.backpressure.busy_ratio} large />
                        {m.backpressure.is_backpressured && (
                            <p className="text-xs text-error mt-1">⚠ Pipeline is backpressured</p>
                        )}
                    </CardContent>
                </Card>
            </div>

            {/* Latency Histogram */}
            <Card>
                <CardContent className="p-6">
                    <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                        <BarChart3 className="w-4 h-4 text-text-secondary" /> Latency Distribution
                    </h3>
                    <div className="space-y-2">
                        {m.latency_histogram.map(bucket => (
                            <div key={bucket.label} className="flex items-center gap-3">
                                <span className="text-xs text-text-secondary w-16 text-right font-mono">{bucket.label}</span>
                                <div className="flex-1 h-6 bg-surface-hover rounded overflow-hidden relative">
                                    <div
                                        className="h-full bg-primary/60 rounded transition-all"
                                        style={{ width: maxBucket > 0 ? `${(bucket.count / maxBucket) * 100}%` : '0%' }}
                                    />
                                    <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs tabular-nums text-text-primary">
                                        {formatNumber(bucket.count)}
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>

            {/* Throughput Timeline (simple bar chart) */}
            {m.throughput_history.length > 0 && (
                <Card>
                    <CardContent className="p-6">
                        <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                            <Activity className="w-4 h-4 text-text-secondary" /> Throughput Over Time
                        </h3>
                        <div className="flex items-end gap-px h-32">
                            {m.throughput_history.map((point, i) => {
                                const maxThroughput = Math.max(...m.throughput_history.map(p => p.records_per_sec));
                                const height = maxThroughput > 0 ? (point.records_per_sec / maxThroughput) * 100 : 0;
                                return (
                                    <div
                                        key={i}
                                        className="flex-1 bg-primary/40 hover:bg-primary/70 rounded-t transition-colors cursor-default group relative"
                                        style={{ height: `${Math.max(2, height)}%` }}
                                        title={`${Math.round(point.records_per_sec)} rec/s`}
                                    />
                                );
                            })}
                        </div>
                        <div className="flex justify-between text-[10px] text-text-secondary mt-1">
                            <span>{formatTimeAgo(m.throughput_history[0]?.timestamp)}</span>
                            <span>now</span>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Per-Operator Metrics Table */}
            <Card>
                <CardContent className="p-6">
                    <h3 className="text-sm font-medium text-text-primary mb-4">Operator Metrics</h3>
                    <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                            <thead className="text-text-secondary text-xs uppercase tracking-wider border-b border-surface-border">
                                <tr>
                                    <th className="pb-2 pr-4 font-medium">Operator</th>
                                    <th className="pb-2 pr-4 font-medium">Type</th>
                                    <th className="pb-2 pr-4 font-medium text-right">In</th>
                                    <th className="pb-2 pr-4 font-medium text-right">Out</th>
                                    <th className="pb-2 pr-4 font-medium text-right">Selectivity</th>
                                    <th className="pb-2 pr-4 font-medium text-right">Proc Time</th>
                                    <th className="pb-2 font-medium text-right">Errors</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-surface-border">
                                {job.operators.map(op => (
                                    <tr key={op.name} className="hover:bg-surface-hover/30">
                                        <td className="py-2.5 pr-4 font-medium text-text-primary">{op.name}</td>
                                        <td className="py-2.5 pr-4"><OperatorTypeBadge type={op.type} /></td>
                                        <td className="py-2.5 pr-4 text-right tabular-nums">{formatNumber(op.records_in)}</td>
                                        <td className="py-2.5 pr-4 text-right tabular-nums">{formatNumber(op.records_out)}</td>
                                        <td className="py-2.5 pr-4 text-right tabular-nums">
                                            {op.records_in > 0 ? `${((op.records_out / op.records_in) * 100).toFixed(1)}%` : '—'}
                                        </td>
                                        <td className="py-2.5 pr-4 text-right">{formatDuration(op.processing_time_ns)}</td>
                                        <td className={cn("py-2.5 text-right tabular-nums", op.errors > 0 ? 'text-error font-medium' : 'text-text-secondary')}>
                                            {op.errors}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}

// =============================================================================
// Tab: Checkpoints
// =============================================================================

function CheckpointsTab({ job }: { job: ProcessingJobDetail }) {
    const checkpoints = job.checkpoints;

    return (
        <div className="space-y-6">
            {/* Summary */}
            <div className="grid gap-4 md:grid-cols-4">
                <Card><CardContent className="p-4">
                    <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Total Completed</p>
                    <p className="text-xl font-semibold text-text-primary tabular-nums">{job.metrics.checkpoints_completed}</p>
                </CardContent></Card>
                <Card><CardContent className="p-4">
                    <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Last Duration</p>
                    <p className="text-xl font-semibold text-text-primary tabular-nums">{job.metrics.last_checkpoint_duration_ms > 0 ? `${job.metrics.last_checkpoint_duration_ms}ms` : '—'}</p>
                </CardContent></Card>
                <Card><CardContent className="p-4">
                    <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Failed</p>
                    <p className={cn("text-xl font-semibold tabular-nums", checkpoints.filter(c => c.status === 'failed').length > 0 ? 'text-error' : 'text-text-primary')}>
                        {checkpoints.filter(c => c.status === 'failed').length}
                    </p>
                </CardContent></Card>
                <Card><CardContent className="p-4">
                    <p className="text-xs text-text-secondary uppercase tracking-wider mb-1">Avg Size</p>
                    <p className="text-xl font-semibold text-text-primary tabular-nums">
                        {checkpoints.filter(c => c.size_bytes).length > 0
                            ? formatBytes(checkpoints.filter(c => c.size_bytes).reduce((a, c) => a + (c.size_bytes || 0), 0) / checkpoints.filter(c => c.size_bytes).length)
                            : '—'}
                    </p>
                </CardContent></Card>
            </div>

            {/* Checkpoint History Table */}
            <Card>
                <CardContent className="p-6">
                    <h3 className="text-sm font-medium text-text-primary mb-4 flex items-center gap-2">
                        <Shield className="w-4 h-4 text-text-secondary" /> Checkpoint History
                    </h3>
                    <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                            <thead className="text-text-secondary text-xs uppercase tracking-wider border-b border-surface-border">
                                <tr>
                                    <th className="pb-2 pr-4 font-medium">ID</th>
                                    <th className="pb-2 pr-4 font-medium">Status</th>
                                    <th className="pb-2 pr-4 font-medium">Time</th>
                                    <th className="pb-2 pr-4 font-medium text-right">Duration</th>
                                    <th className="pb-2 pr-4 font-medium text-right">Size</th>
                                    <th className="pb-2 pr-4 font-medium text-center">Operators</th>
                                    <th className="pb-2 font-medium text-center">Offsets</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-surface-border">
                                {checkpoints.map(cp => (
                                    <tr key={cp.checkpoint_id} className={cn("hover:bg-surface-hover/30", cp.status === 'failed' && 'bg-error/5')}>
                                        <td className="py-2.5 pr-4 font-mono text-xs text-text-secondary">#{cp.checkpoint_id}</td>
                                        <td className="py-2.5 pr-4"><CheckpointStatusBadge status={cp.status} /></td>
                                        <td className="py-2.5 pr-4 text-text-secondary">{formatTimeAgo(cp.timestamp_ms)}</td>
                                        <td className="py-2.5 pr-4 text-right tabular-nums">{cp.duration_ms != null ? `${cp.duration_ms}ms` : '—'}</td>
                                        <td className="py-2.5 pr-4 text-right tabular-nums">{cp.size_bytes != null ? formatBytes(cp.size_bytes) : '—'}</td>
                                        <td className="py-2.5 pr-4 text-center">
                                            <span className={cn("tabular-nums", cp.acked_operators < cp.total_operators ? 'text-warning' : '')}>
                                                {cp.acked_operators}/{cp.total_operators}
                                            </span>
                                        </td>
                                        <td className="py-2.5 text-center">
                                            {cp.offsets_saved ? (
                                                <CheckCircle className="w-4 h-4 text-success inline" />
                                            ) : (
                                                <XCircle className="w-4 h-4 text-error inline" />
                                            )}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}

// =============================================================================
// Tab: YAML Definition
// =============================================================================

function YamlTab({ job }: { job: ProcessingJobDetail }) {
    return (
        <Card>
            <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-sm font-medium text-text-primary flex items-center gap-2">
                        <FileCode className="w-4 h-4 text-text-secondary" /> Pipeline Definition
                    </h3>
                    <button
                        onClick={() => navigator.clipboard.writeText(job.definition_yaml)}
                        className="text-xs text-text-secondary hover:text-text-primary transition-colors px-2 py-1 rounded hover:bg-surface-hover"
                    >
                        Copy YAML
                    </button>
                </div>
                <pre className="bg-background border border-surface-border rounded-md p-4 overflow-x-auto text-sm font-mono text-text-primary leading-relaxed whitespace-pre">
                    {job.definition_yaml}
                </pre>
            </CardContent>
        </Card>
    );
}

// =============================================================================
// Tab: Timeline
// =============================================================================

function TimelineTab({ job }: { job: ProcessingJobDetail }) {
    // Timeline events are not yet returned by the backend API.
    // Synthesize basic events from job state data.
    const events: JobEvent[] = [];
    events.push({ timestamp_ms: job.created_at_ms, event_type: 'created', description: 'Pipeline created' });
    if (job.state === 'RUNNING' || job.state === 'FINISHED' || job.state === 'STOPPED') {
        events.push({ timestamp_ms: job.created_at_ms + 100, event_type: 'started', description: 'Pipeline started' });
    }
    if (job.savepoint_id) {
        events.push({ timestamp_ms: job.updated_at_ms - 1000, event_type: 'savepoint', description: `Savepoint created: ${job.savepoint_id}`, metadata: { savepoint_id: job.savepoint_id } });
    }
    if (job.state === 'STOPPED' && job.completed_at_ms) {
        events.push({ timestamp_ms: job.completed_at_ms, event_type: 'stopped', description: 'Pipeline stopped gracefully' });
    }
    if (job.state === 'FAILED' && job.error_message) {
        events.push({ timestamp_ms: job.completed_at_ms ?? job.updated_at_ms, event_type: 'failed', description: job.error_message });
    }
    if (job.state === 'FINISHED' && job.completed_at_ms) {
        events.push({ timestamp_ms: job.completed_at_ms, event_type: 'finished', description: 'Pipeline finished successfully' });
    }
    if (job.state === 'CANCELLED' && job.completed_at_ms) {
        events.push({ timestamp_ms: job.completed_at_ms, event_type: 'cancelled', description: 'Pipeline cancelled' });
    }

    const eventIcons: Record<string, { icon: typeof Play; color: string }> = {
        created: { icon: Activity, color: 'text-text-secondary' },
        started: { icon: Play, color: 'text-blue-400' },
        checkpoint: { icon: Shield, color: 'text-primary' },
        savepoint: { icon: Save, color: 'text-emerald-400' },
        rescaled: { icon: Layers, color: 'text-purple-400' },
        stopped: { icon: Pause, color: 'text-warning' },
        restored: { icon: RotateCcw, color: 'text-cyan-400' },
        failed: { icon: XCircle, color: 'text-error' },
        finished: { icon: CheckCircle, color: 'text-success' },
        cancelled: { icon: Square, color: 'text-text-secondary' },
    };

    return (
        <Card>
            <CardContent className="p-6">
                <h3 className="text-sm font-medium text-text-primary mb-6 flex items-center gap-2">
                    <History className="w-4 h-4 text-text-secondary" /> Job Timeline
                </h3>
                <div className="relative">
                    {/* Vertical line */}
                    <div className="absolute left-4 top-0 bottom-0 w-px bg-surface-border" />

                    <div className="space-y-6">
                        {events.map((event, i) => {
                            const ei = eventIcons[event.event_type] || eventIcons.created;
                            const Icon = ei.icon;
                            return (
                                <div key={i} className="flex items-start gap-4 relative">
                                    <div className={cn("w-8 h-8 rounded-full flex items-center justify-center bg-surface border border-surface-border shrink-0 z-10", ei.color)}>
                                        <Icon className="w-4 h-4" />
                                    </div>
                                    <div className="flex-1 pt-1">
                                        <div className="flex items-center justify-between">
                                            <p className="text-sm text-text-primary">{event.description}</p>
                                            <span className="text-xs text-text-secondary shrink-0 ml-4">{formatDate(event.timestamp_ms)}</span>
                                        </div>
                                        {event.metadata && (
                                            <div className="flex flex-wrap gap-2 mt-1.5">
                                                {Object.entries(event.metadata).map(([k, v]) => (
                                                    <span key={k} className="text-xs font-mono bg-surface-hover px-2 py-0.5 rounded text-text-secondary">
                                                        {k}=<span className="text-text-primary">{v}</span>
                                                    </span>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}

// =============================================================================
// Topology Visualization Helpers
// =============================================================================

function TopologyNode({ label, sublabel, type, badge, stateful, hasErrors }: {
    label: string; sublabel: string; type: 'source' | 'operator' | 'sink';
    badge: React.ReactNode; stateful?: boolean; hasErrors?: boolean;
}) {
    const borderColor = hasErrors ? 'border-error/50' :
        type === 'source' ? 'border-blue-400/30' :
        type === 'sink' ? 'border-emerald-400/30' :
        'border-surface-border';
    const bgColor = type === 'source' ? 'bg-blue-400/5' :
        type === 'sink' ? 'bg-emerald-400/5' :
        'bg-surface';

    return (
        <div className={cn("border rounded-lg px-4 py-3 min-w-[140px] shrink-0", borderColor, bgColor)}>
            <div className="flex items-center gap-2 mb-1">
                {badge}
                {stateful && (
                    <span className="w-1.5 h-1.5 rounded-full bg-amber-400" title="Stateful operator" />
                )}
            </div>
            <p className="text-sm font-medium text-text-primary truncate">{label}</p>
            <p className="text-[11px] text-text-secondary truncate">{sublabel}</p>
        </div>
    );
}

function TopologyArrow({ records }: { records?: number }) {
    return (
        <div className="flex flex-col items-center px-2 shrink-0">
            <div className="w-8 h-px bg-text-secondary/30" />
            {records != null && (
                <span className="text-[10px] text-text-secondary tabular-nums mt-0.5">{formatNumber(records)}</span>
            )}
        </div>
    );
}

// =============================================================================
// Shared Widgets
// =============================================================================

function MetricCard({ label, value, icon: Icon, iconColor }: {
    label: string; value: React.ReactNode; icon: typeof Activity; iconColor: string;
}) {
    return (
        <Card>
            <CardContent className="p-4">
                <div className="flex items-center justify-between mb-2">
                    <span className="text-xs text-text-secondary uppercase tracking-wider">{label}</span>
                    <Icon className={cn("w-4 h-4", iconColor)} />
                </div>
                <div className="text-lg font-semibold text-text-primary">{value}</div>
            </CardContent>
        </Card>
    );
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
    return (
        <div className="flex items-center justify-between">
            <dt className="text-text-secondary">{label}</dt>
            <dd className="text-text-primary">{value}</dd>
        </div>
    );
}

// =============================================================================
// Main Detail Page
// =============================================================================

export function ProcessingDetail() {
    const { jobId } = useParams();
    const [activeTab, setActiveTab] = useState('overview');
    const [actionLoading, setActionLoading] = useState<string | null>(null);
    const [actionError, setActionError] = useState<string | null>(null);

    const { data: job, loading, error, refetch } = useApi(
        () => api.getProcessingJobDetail(jobId!),
        [jobId],
        3000
    );

    const performAction = useCallback(async (action: string, fn: () => Promise<unknown>) => {
        setActionLoading(action);
        setActionError(null);
        try {
            await fn();
            refetch();
        } catch (err) {
            setActionError(err instanceof Error ? err.message : 'Action failed');
        } finally {
            setActionLoading(null);
        }
    }, [refetch]);

    if (loading && !job) return <LoadingState message="Loading job details..." />;
    if (error && !job) return (
        <div className="space-y-6">
            <Link to="/processing" className="flex items-center gap-1 text-sm text-text-secondary hover:text-text-primary transition-colors">
                <ChevronLeft className="w-4 h-4" /> Back to Processing
            </Link>
            <ErrorState message={error} onRetry={refetch} />
        </div>
    );

    if (!job) {
        return (
            <div className="space-y-6">
                <Link to="/processing" className="flex items-center gap-1 text-sm text-text-secondary hover:text-text-primary transition-colors">
                    <ChevronLeft className="w-4 h-4" /> Back to Processing
                </Link>
                <div className="text-center py-20">
                    <p className="text-text-secondary">Job not found: {jobId}</p>
                </div>
            </div>
        );
    }

    const isRunning = job.state === 'RUNNING';

    return (
        <div className="space-y-6">
            {/* Action Error Banner */}
            {actionError && (
                <div className="flex items-center gap-3 p-3 rounded-md border border-error/30 bg-error/5 text-sm">
                    <AlertTriangle className="w-4 h-4 text-error shrink-0" />
                    <span className="text-error">{actionError}</span>
                    <button onClick={() => setActionError(null)} className="ml-auto text-xs text-text-secondary hover:text-text-primary">Dismiss</button>
                </div>
            )}

            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <Link to="/processing" className="p-2 hover:bg-surface-hover rounded-md text-text-secondary transition-colors">
                        <ChevronLeft className="w-5 h-5" />
                    </Link>
                    <div>
                        <div className="flex items-center gap-3">
                            <h1 className="text-2xl font-semibold text-text-primary">{job.job_name}</h1>
                            <StateBadge state={job.state} />
                        </div>
                        <p className="text-sm text-text-secondary mt-0.5">
                            <span className="font-mono text-xs">{job.job_id}</span>
                            <span className="mx-2">·</span>
                            {job.namespace}
                            <span className="mx-2">·</span>
                            ×{job.parallelism} parallelism
                        </p>
                    </div>
                </div>

                {/* Action Buttons */}
                <div className="flex items-center gap-2">
                    {isRunning && (
                        <>
                            <button
                                onClick={() => performAction('savepoint', () => api.createProcessingSavepoint(job.job_id))}
                                disabled={!!actionLoading}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-surface-border rounded-md text-text-secondary hover:text-text-primary hover:border-text-secondary transition-colors disabled:opacity-50"
                            >
                                {actionLoading === 'savepoint' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />} Savepoint
                            </button>
                            <button
                                onClick={() => performAction('stop', () => api.stopProcessingJob(job.job_id))}
                                disabled={!!actionLoading}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-surface-border rounded-md text-text-secondary hover:text-warning hover:border-warning transition-colors disabled:opacity-50"
                            >
                                {actionLoading === 'stop' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Pause className="w-3.5 h-3.5" />} Stop
                            </button>
                            <button
                                onClick={() => performAction('cancel', () => api.cancelProcessingJob(job.job_id))}
                                disabled={!!actionLoading}
                                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-surface-border rounded-md text-text-secondary hover:text-error hover:border-error transition-colors disabled:opacity-50"
                            >
                                {actionLoading === 'cancel' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Square className="w-3.5 h-3.5" />} Cancel
                            </button>
                        </>
                    )}
                    {(job.state === 'STOPPED' || job.state === 'FAILED') && (
                        <button
                            onClick={() => performAction('restore', () => api.restoreProcessingJob(job.job_id, job.savepoint_id))}
                            disabled={!!actionLoading}
                            className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-primary text-background font-medium rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50"
                        >
                            {actionLoading === 'restore' ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <RotateCcw className="w-3.5 h-3.5" />} Restore
                        </button>
                    )}
                </div>
            </div>

            {/* Tabs */}
            <PageTabs
                tabs={[
                    { id: 'overview', label: 'Overview' },
                    { id: 'operators', label: 'Operators', count: job.operators.length },
                    { id: 'metrics', label: 'Metrics' },
                    { id: 'checkpoints', label: 'Checkpoints', count: job.checkpoints.length },
                    { id: 'yaml', label: 'YAML' },
                    { id: 'timeline', label: 'Timeline' },
                ]}
                activeTab={activeTab}
                onChange={setActiveTab}
            />

            {/* Tab Content */}
            <div>
                {activeTab === 'overview' && <OverviewTab job={job} />}
                {activeTab === 'operators' && <OperatorsTab job={job} />}
                {activeTab === 'metrics' && <MetricsTab job={job} />}
                {activeTab === 'checkpoints' && <CheckpointsTab job={job} />}
                {activeTab === 'yaml' && <YamlTab job={job} />}
                {activeTab === 'timeline' && <TimelineTab job={job} />}
            </div>
        </div>
    );
}
