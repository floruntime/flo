import { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
    Play, CheckCircle, XCircle, Pause, Activity, Clock,
    Plus, ChevronRight, Layers, AlertTriangle, Cpu
} from "lucide-react";
import { Card, CardContent } from "../components/ui/Card";
import { PageTabs } from "../components/ui/PageTabs";
import { cn } from "../lib/utils";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import { api } from "../lib/api";
import { useNamespace } from "../lib/NamespaceContext";
import type { ProcessingJobInfo } from "../lib/api";
import type { JobState } from "../lib/processing-types";
import { ProcessingSubmitModal } from "../components/processing/SubmitModal";

// =============================================================================
// Helpers
// =============================================================================

function formatUptime(ms: number): string {
    if (ms <= 0) return "—";
    const sec = Math.floor(ms / 1000);
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m`;
    const hr = Math.floor(min / 60);
    if (hr < 24) return `${hr}h ${min % 60}m`;
    const days = Math.floor(hr / 24);
    return `${days}d ${hr % 24}h`;
}

function formatNumber(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

function parseOperatorCount(operators?: string): number {
    if (!operators) return 0;
    return operators.split(',').filter(s => s.trim()).length;
}

// =============================================================================
// Status Badge
// =============================================================================

function JobStateBadge({ state }: { state: JobState }) {
    const styles: Record<JobState, { icon: typeof Play; className: string; label: string }> = {
        CREATED: { icon: Clock, className: 'bg-text-secondary/10 text-text-secondary', label: 'Created' },
        RUNNING: { icon: Play, className: 'bg-blue-500/10 text-blue-500', label: 'Running' },
        FINISHED: { icon: CheckCircle, className: 'bg-success/10 text-success', label: 'Finished' },
        STOPPED: { icon: Pause, className: 'bg-warning/10 text-warning', label: 'Stopped' },
        CANCELLED: { icon: XCircle, className: 'bg-text-secondary/10 text-text-secondary', label: 'Cancelled' },
        FAILED: { icon: XCircle, className: 'bg-error/10 text-error', label: 'Failed' },
    };
    const s = styles[state];
    const Icon = s.icon;
    return (
        <span className={cn("inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium", s.className)}>
            <Icon className="w-3 h-3" /> {s.label}
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
// Stats Cards (Flink-inspired overview)
// =============================================================================

function StatsCards({ jobs }: { jobs: ProcessingJobInfo[] }) {
    const running = jobs.filter(j => j.state === 'RUNNING').length;
    const failed = jobs.filter(j => j.state === 'FAILED').length;
    const stopped = jobs.filter(j => j.state === 'STOPPED').length;
    const totalRecords = jobs.reduce((acc, j) => acc + j.records_processed, 0);
    const totalParallelism = jobs.filter(j => j.state === 'RUNNING').reduce((acc, j) => acc + j.parallelism, 0);

    return (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <Card className="bg-surface">
                <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-text-secondary uppercase tracking-wider">Jobs</span>
                        <Activity className="w-4 h-4 text-primary" />
                    </div>
                    <p className="text-2xl font-semibold text-text-primary">{running}<span className="text-sm text-text-secondary font-normal">/{jobs.length}</span></p>
                    <p className="text-xs text-text-secondary mt-1">{running} running · {stopped} stopped</p>
                </CardContent>
            </Card>

            <Card className="bg-surface">
                <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-text-secondary uppercase tracking-wider">Records</span>
                        <Layers className="w-4 h-4 text-emerald-400" />
                    </div>
                    <p className="text-2xl font-semibold text-text-primary">{formatNumber(totalRecords)}</p>
                    <p className="text-xs text-text-secondary mt-1">total processed across all jobs</p>
                </CardContent>
            </Card>

            <Card className="bg-surface">
                <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-text-secondary uppercase tracking-wider">Parallelism</span>
                        <Cpu className="w-4 h-4 text-blue-400" />
                    </div>
                    <p className="text-2xl font-semibold text-text-primary">×{totalParallelism}</p>
                    <p className="text-xs text-text-secondary mt-1">active across {running} pipelines</p>
                </CardContent>
            </Card>

            <Card className={cn("bg-surface", failed > 0 && "border-error/30")}>
                <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-text-secondary uppercase tracking-wider">Issues</span>
                        <AlertTriangle className={cn("w-4 h-4", failed > 0 ? "text-error" : "text-success")} />
                    </div>
                    <p className={cn("text-2xl font-semibold", failed > 0 ? "text-error" : "text-text-primary")}>
                        {failed}
                    </p>
                    <p className="text-xs text-text-secondary mt-1">{failed > 0 ? `${failed} failed pipelines` : 'all pipelines healthy'}</p>
                </CardContent>
            </Card>
        </div>
    );
}

// =============================================================================
// Job Row (Flink-style with inline metrics)
// =============================================================================

function JobRow({ job, onClick }: { job: ProcessingJobInfo; onClick: () => void }) {
    const opCount = parseOperatorCount(job.operators);
    const isRunning = job.state === 'RUNNING';

    return (
        <tr
            onClick={onClick}
            className="border-b border-surface-border hover:bg-surface-hover/50 cursor-pointer transition-colors group"
        >
            {/* Name + Namespace */}
            <td className="px-4 py-3.5">
                <div className="flex items-center gap-2.5">
                    <div className={cn(
                        "w-2 h-2 rounded-full shrink-0",
                        job.state === 'RUNNING' ? 'bg-blue-500 animate-pulse' :
                        job.state === 'FAILED' ? 'bg-error' :
                        job.state === 'STOPPED' ? 'bg-warning' :
                        job.state === 'FINISHED' ? 'bg-success' : 'bg-text-secondary'
                    )} />
                    <div>
                        <p className="font-medium text-text-primary group-hover:text-primary transition-colors">{job.name}</p>
                        <p className="text-xs text-text-secondary">{job.namespace}</p>
                    </div>
                </div>
            </td>

            {/* State */}
            <td className="px-4 py-3.5">
                <JobStateBadge state={job.state as JobState} />
            </td>

            {/* Topology: source → operators → sink */}
            <td className="px-4 py-3.5">
                <div className="flex items-center gap-1 text-xs">
                    {job.source && <EndpointBadge type={job.source.kind} />}
                    <span className="text-text-secondary">→</span>
                    <span className="text-text-secondary">{opCount} ops</span>
                    <span className="text-text-secondary">→</span>
                    {job.sink && <EndpointBadge type={job.sink.kind} />}
                </div>
            </td>

            {/* Parallelism */}
            <td className="px-4 py-3.5 text-center">
                <span className="text-xs font-mono text-text-secondary bg-surface-hover px-2 py-0.5 rounded">
                    ×{job.parallelism}
                </span>
            </td>

            {/* Records */}
            <td className="px-4 py-3.5 tabular-nums text-sm text-text-secondary">
                {formatNumber(job.records_processed)}
            </td>

            {/* Uptime */}
            <td className="px-4 py-3.5 text-xs text-text-secondary">
                {formatUptime(isRunning ? Date.now() - job.created_at_ms : (job.completed_at_ms ?? job.updated_at_ms) - job.created_at_ms)}
            </td>

            {/* Arrow */}
            <td className="px-2 py-3.5">
                <ChevronRight className="w-4 h-4 text-text-secondary/40 group-hover:text-primary transition-colors" />
            </td>
        </tr>
    );
}

// =============================================================================
// Main Page
// =============================================================================

export function ProcessingList() {
    const navigate = useNavigate();
    const [activeTab, setActiveTab] = useState('all');
    const [showSubmitModal, setShowSubmitModal] = useState(false);
    const { selected: namespace } = useNamespace();
    const { data: jobs, loading, error, refetch } = useApi(() => api.getProcessingJobs(namespace), [namespace], 5000);

    if (loading && !jobs) return <LoadingState message="Loading processing jobs..." />;
    if (error && !jobs) return <ErrorState message={error} onRetry={refetch} />;

    const allJobs = jobs || [];

    const filteredJobs = activeTab === 'all' ? allJobs :
        activeTab === 'running' ? allJobs.filter(j => j.state === 'RUNNING') :
        activeTab === 'stopped' ? allJobs.filter(j => j.state === 'STOPPED') :
        activeTab === 'failed' ? allJobs.filter(j => j.state === 'FAILED' || j.state === 'CANCELLED') :
        allJobs;

    const tabCounts = {
        all: allJobs.length,
        running: allJobs.filter(j => j.state === 'RUNNING').length,
        stopped: allJobs.filter(j => j.state === 'STOPPED').length,
        failed: allJobs.filter(j => j.state === 'FAILED' || j.state === 'CANCELLED').length,
    };

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary">Processing</h1>
                    <p className="text-text-secondary mt-1 text-sm">Real-time stream processing pipelines — define, deploy, monitor.</p>
                </div>
                <button
                    onClick={() => setShowSubmitModal(true)}
                    className="flex items-center gap-2 bg-primary hover:bg-primary/90 text-background font-medium px-4 py-2 rounded-md transition-colors text-sm"
                >
                    <Plus className="w-4 h-4" /> Submit Pipeline
                </button>
            </div>

            {/* Stats Overview */}
            <StatsCards jobs={allJobs} />

            {/* Tabs + Table */}
            <div>
                <PageTabs
                    tabs={[
                        { id: 'all', label: 'All Jobs', count: tabCounts.all },
                        { id: 'running', label: 'Running', count: tabCounts.running },
                        { id: 'stopped', label: 'Stopped', count: tabCounts.stopped },
                        { id: 'failed', label: 'Failed', count: tabCounts.failed },
                    ]}
                    activeTab={activeTab}
                    onChange={setActiveTab}
                />

                <Card className="rounded-t-none border-t-0">
                    <div className="overflow-x-auto">
                        <table className="w-full text-left text-sm">
                            <thead className="bg-surface-hover/50 text-text-secondary text-xs uppercase tracking-wider">
                                <tr>
                                    <th className="px-4 py-3 font-medium">Pipeline</th>
                                    <th className="px-4 py-3 font-medium">State</th>
                                    <th className="px-4 py-3 font-medium">Topology</th>
                                    <th className="px-4 py-3 font-medium text-center">Par.</th>
                                    <th className="px-4 py-3 font-medium">Records</th>
                                    <th className="px-4 py-3 font-medium">Uptime</th>
                                    <th className="w-8" />
                                </tr>
                            </thead>
                            <tbody>
                                {filteredJobs.length === 0 ? (
                                    <tr>
                                        <td colSpan={7} className="px-4 py-12 text-center text-text-secondary text-sm">
                                            {allJobs.length === 0 ? 'No processing jobs yet. Submit a pipeline to get started.' : 'No jobs match this filter.'}
                                        </td>
                                    </tr>
                                ) : (
                                    filteredJobs.map(job => (
                                        <JobRow
                                            key={job.job_id}
                                            job={job}
                                            onClick={() => navigate(`/processing/${job.job_id}`)}
                                        />
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>
                </Card>
            </div>

            {showSubmitModal && (
                <ProcessingSubmitModal
                    onClose={() => setShowSubmitModal(false)}
                    onSubmitted={refetch}
                />
            )}
        </div>
    );
}
