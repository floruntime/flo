import { useParams } from "react-router-dom";
import { PartitionEqualizer } from "../components/stream/PartitionEqualizer";
import { InfiniteTape } from "../components/stream/InfiniteTape";
import { OperationalViews } from "../components/stream/OperationalViews";
import { ChevronLeft } from "lucide-react";
import { useState, useMemo } from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import type { StreamMessagesResponse, GroupPendingResponse } from "../lib/api";
import { useApi, LoadingState, ErrorState } from "../lib/useApi";
import type { StreamMessage, ConsumerGroup, PendingMessage } from "../lib/types";
import { useNamespace } from "../lib/NamespaceContext";

/** Map API stream message to the component's StreamMessage shape */
function toStreamMessage(msg: StreamMessagesResponse['messages'][0]): StreamMessage {
    const streamId = `${msg.id_ms}-${msg.id_seq}`;
    return {
        id: streamId,
        id_ms: msg.id_ms,
        id_seq: msg.id_seq,
        key: '',
        size: msg.size,
        payload: '',
    };
}

/** Map API pending entry to component PendingMessage shape */
function toPendingMessage(entry: GroupPendingResponse['pending'][0], groupName: string): PendingMessage {
    const now = Date.now();
    return {
        id: `${entry.id_ms}-${entry.id_seq}`,
        id_seq: entry.id_seq,
        key: '',
        consumer: entry.consumer,
        groupName,
        deliveryCount: entry.delivery_count,
        idleTime: now - entry.delivered_at_ms,
        reclaimIn: 0,
        payload: '',
    };
}

export function StreamDetail() {
    const { streamId } = useParams();
    const { selected: namespace } = useNamespace();
    const [selectedPartition, setSelectedPartition] = useState<number | undefined>(undefined);

    const { data: detail, loading, error, refetch } = useApi(
        () => api.getStreamDetail(streamId || '', namespace), [streamId, namespace], 5000
    );
    const { data: messagesResp } = useApi(
        () => api.getStreamMessages(streamId || '', undefined, 2000, selectedPartition, namespace),
        [streamId, selectedPartition, namespace], 5000
    );

    // Fetch pending messages for first consumer group (if any)
    const firstGroup = detail?.consumer_groups?.[0]?.name;
    const { data: pendingResp } = useApi(
        () => firstGroup ? api.getGroupPending(streamId || '', firstGroup, namespace) : Promise.resolve({ pending: [], count: 0 }),
        [streamId, firstGroup, namespace], 10000
    );

    // Seeker range state (shared between InfiniteTape and MessagesList)
    const [seekerRange, setSeekerRange] = useState({ start: 0, end: 0 });

    // All hooks MUST be above early returns (React rules of hooks)
    const pendingMessages: PendingMessage[] = useMemo(() => {
        if (!pendingResp?.pending || !firstGroup) return [];
        return pendingResp.pending.map(e => toPendingMessage(e, firstGroup));
    }, [pendingResp, firstGroup]);

    if (loading && !detail) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={refetch} />;
    if (!detail) return <ErrorState message="Stream not found" />;

    const partitions = detail.partitions.map(p => ({
        id: p.id,
        message_count: (p as any).message_count ?? 0,
        bytes: (p as any).bytes ?? 0,
        status: p.status as 'healthy' | 'hot' | 'error',
    }));

    const messages: StreamMessage[] = (messagesResp?.messages || []).map(toStreamMessage);

    const groups: ConsumerGroup[] = detail.consumer_groups.map((g, i) => ({
        name: g.name,
        current_id: '0-0',
        lag: g.lag,
        color: ['#10B981', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899'][i % 6],
    }));

    return (
        <div className="space-y-6 h-full flex flex-col">
            {/* Header */}
            <div className="flex items-center gap-4">
                <Link to="/streams" className="p-2 hover:bg-surface-hover rounded-md text-text-secondary transition-colors">
                    <ChevronLeft className="w-5 h-5" />
                </Link>
                <div>
                    <h1 className="text-2xl font-semibold text-text-primary flex items-center gap-3">
                        {detail.name}
                        <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-primary/10 text-primary border border-primary/20">
                            Live
                        </span>
                    </h1>
                    <p className="text-text-secondary text-sm">
                        {detail.partitions.length} Partition{detail.partitions.length !== 1 ? 's' : ''}
                        {detail.namespace && ` • ${detail.namespace}`}
                        {detail.total_count > 0 && ` • ${detail.total_count.toLocaleString()} messages`}
                        {detail.total_bytes > 0 && ` • ${(detail.total_bytes / 1024).toFixed(1)} KB`}
                    </p>
                </div>
            </div>

            {/* Partition Selector (only for multi-partition streams) */}
            {detail.partitions.length > 1 && (
                <div className="flex items-center gap-2 text-sm">
                    <span className="text-text-secondary font-medium">Partition:</span>
                    <button
                        onClick={() => setSelectedPartition(undefined)}
                        className={`px-3 py-1 rounded-md transition-colors ${
                            selectedPartition === undefined
                                ? 'bg-primary text-white'
                                : 'bg-surface-hover text-text-secondary hover:text-text-primary'
                        }`}
                    >
                        All
                    </button>
                    {detail.partitions.map(p => (
                        <button
                            key={p.id}
                            onClick={() => setSelectedPartition(p.id)}
                            className={`px-3 py-1 rounded-md transition-colors ${
                                selectedPartition === p.id
                                    ? 'bg-primary text-white'
                                    : 'bg-surface-hover text-text-secondary hover:text-text-primary'
                            }`}
                        >
                            P{p.id}
                        </button>
                    ))}
                </div>
            )}

            {/* Main Content Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 flex-1 min-h-0">
                {/* Left Column: Equalizer & Stats */}
                <div className="space-y-6">
                    <PartitionEqualizer partitions={partitions} />

                </div>

                {/* Center/Right: The Infinite Tape */}
                <div className="lg:col-span-2 flex flex-col gap-6 min-h-0">
                    <InfiniteTape
                        messages={messages}
                        groups={groups}
                        onSeekerRangeChange={setSeekerRange}
                    />

                    {/* Operational Views: Messages, Triage Ward & Exclusive Lease Lock */}
                    <OperationalViews
                        messages={messages}
                        seekerRange={seekerRange}
                        pendingMessages={pendingMessages}
                        exclusiveGroups={[]}
                        onReclaimNow={(id) => console.log('Reclaim:', id)}
                        onTouch={(id) => console.log('Touch:', id)}
                        onNack={(id) => console.log('NACK:', id)}

                    />
                </div>
            </div>
        </div>
    );
}
