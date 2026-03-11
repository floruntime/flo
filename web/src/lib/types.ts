export interface KVVersion {
    version: number; // Per-key sequential version (1, 2, 3...)
    lsn: number; // Global WAL sequence number (for debugging)
    timestamp: number; // ms since epoch
    value: string | object; // In real app this would be bytes/buffer
    deleted: boolean;
    ttl?: number; // expiration timestamp
    metadata?: Record<string, any>;
}

export interface KVKey {
    key: string;
    namespace: string;
    current_version: number; // Latest per-key version number
    current_lsn: number; // Latest LSN (for reference)
    version_count: number;
    size: number; // bytes
    ttl_expiry?: number; // timestamp, undefined if no TTL
    last_modified: number; // timestamp
    versions?: KVVersion[]; // Optional, loaded on demand
}

export interface KVNamespace {
    name: string;
    key_count: number;
    version_count: number;
    current_lsn: number;
    size_bytes: number;
}

export interface KVTransactionOp {
    type: 'PUT' | 'DELETE';
    key: string;
    value?: string | object;
    ttl?: number;
}

export interface KVTransaction {
    id: string;
    status: 'active' | 'committed' | 'rolled_back' | 'failed';
    started_at: number;
    operations: KVTransactionOp[];
}

export interface KVSnapshot {
    lsn: number;
    timestamp: number;
    namespace?: string;
}

// Stream component types
export interface PartitionStat {
    id: number;
    message_count: number;
    bytes: number;
    status: 'healthy' | 'hot' | 'error';
}

export interface StreamMessage {
    id: string;
    id_ms: number;
    id_seq: number;
    key: string;
    size: number;
    payload: string;
}

export interface ConsumerGroup {
    name: string;
    current_id: string;
    lag: number;
    color: string;
}

export interface PendingMessage {
    id: string;
    id_seq: number;
    key: string;
    consumer: string;
    groupName: string;
    deliveryCount: number;
    idleTime: number;
    reclaimIn: number;
    payload: string;
}

export interface StandbyConsumer {
    id: string;
    status: 'ready' | 'acquiring' | 'unavailable';
}

export interface ExclusiveGroup {
    groupName: string;
    mode: 'exclusive' | 'shared';
    leader: string | null;
    leaseExpiry: number;
    leaseRemaining: number;
    standbys: StandbyConsumer[];
    lastFailover: number;
    partitionId: number;
}
