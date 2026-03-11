import type { KVKey, KVNamespace, KVTransaction, KVVersion } from "./types";

export const MOCK_CLUSTER_STATS = {
    rps: 12500,
    active_connections: 450,
    uptime: "14d 2h 15m",
    version: "0.9.0",
    nodes: [
        { id: "node-1", status: "healthy", role: "leader", cpu: 45, mem: 60 },
        { id: "node-2", status: "healthy", role: "follower", cpu: 30, mem: 45 },
        { id: "node-3", status: "healthy", role: "follower", cpu: 32, mem: 48 },
    ]
};

export const MOCK_STREAMS = [
    { name: "user-clicks", partitions: 32, ingest_rate: 4500, lag: 0, retention: "7d" },
    { name: "orders", partitions: 8, ingest_rate: 120, lag: 0, retention: "30d" },
    { name: "payments", partitions: 8, ingest_rate: 115, lag: 0, retention: "30d" },
    { name: "logs-app", partitions: 64, ingest_rate: 8000, lag: 150, retention: "24h" },
    { name: "notifications", partitions: 16, ingest_rate: 50, lag: 0, retention: "7d" },
];

export const MOCK_ACTIONS = [
    { name: "send-email", type: "user", invocations: 15420, errors: 12, avg_latency: 240 },
    { name: "resize-image", type: "wasm", invocations: 45000, errors: 0, avg_latency: 45 },
    { name: "fraud-check", type: "wasm", invocations: 120, errors: 0, avg_latency: 12 },
    { name: "charge-card", type: "user", invocations: 115, errors: 2, avg_latency: 850 },
];

export const MOCK_WORKFLOWS = [
    { id: "wf-1023", name: "order-fulfillment", status: "running", step: "ship-order", started: "2m ago" },
    { id: "wf-1022", name: "order-fulfillment", status: "completed", step: "-", started: "5m ago" },
    { id: "wf-1021", name: "user-signup", status: "completed", step: "-", started: "10m ago" },
    { id: "wf-1020", name: "order-fulfillment", status: "failed", step: "charge-card", started: "15m ago" },
    { id: "wf-1019", name: "user-signup", status: "completed", step: "-", started: "20m ago" },
];

export interface PartitionStat {
    id: number;
    rate: number; // msg/sec
    status: 'healthy' | 'hot' | 'error';
}

export const generateMockPartitions = (count: number): PartitionStat[] => {
    return Array.from({ length: count }).map((_, i) => {
        // Simulate a "Hot Shard" scenario: Partition 0 gets 80% of traffic
        const isHot = i === 0;
        const baseRate = isHot ? 850 : Math.floor(Math.random() * 50) + 10;

        return {
            id: i,
            rate: baseRate,
            status: isHot ? 'hot' : 'healthy'
        };
    });
};

export interface StreamMessage {
    id: string;
    id_ms: number;
    id_seq: number;
    key: string;
    size: number; // bytes
    payload: string;
}

export interface ConsumerGroup {
    name: string;
    current_id: string;
    lag: number;
    color: string;
}

export const generateMockMessages = (count: number, startSeq: number = 10000): StreamMessage[] => {
    return Array.from({ length: count }).map((_, i) => {
        const seq = startSeq + i;
        const ts = Date.now() - (count - i) * 1000;
        return {
            id: `${ts}-${seq}`,
            id_ms: ts,
            id_seq: seq,
            key: `user:${Math.floor(Math.random() * 1000)}`,
            size: Math.floor(Math.random() * 500) + 50,
            payload: `{"event": "click", "x": ${Math.random()}, "y": ${Math.random()}}`
        };
    });
};

export const MOCK_CONSUMER_GROUPS: ConsumerGroup[] = [
    // Critical lag (> 1000)
    { name: "audit-log", current_id: "0-12000", lag: 3000, color: "#EF4444" }, // Red
    { name: "data-warehouse", current_id: "0-11500", lag: 3500, color: "#DC2626" }, // Dark Red
    { name: "backup-service", current_id: "0-12800", lag: 2200, color: "#F87171" }, // Light Red

    // Warning lag (100-1000)
    { name: "analytics-batch", current_id: "0-14200", lag: 800, color: "#F59E0B" }, // Yellow
    { name: "reporting-engine", current_id: "0-13900", lag: 1100, color: "#FBBF24" }, // Light Yellow
    { name: "ml-pipeline", current_id: "0-14100", lag: 900, color: "#F97316" }, // Orange
    { name: "search-indexer", current_id: "0-14500", lag: 500, color: "#FB923C" }, // Light Orange

    // Healthy lag (< 100)
    { name: "payment-processor", current_id: "0-14995", lag: 5, color: "#10B981" }, // Green
    { name: "notification-sender", current_id: "0-14990", lag: 10, color: "#34D399" }, // Light Green
    { name: "fraud-detector", current_id: "0-14980", lag: 20, color: "#059669" }, // Dark Green
    { name: "cache-warmer", current_id: "0-14970", lag: 30, color: "#6EE7B7" }, // Very Light Green
    { name: "webhook-dispatcher", current_id: "0-14960", lag: 40, color: "#14B8A6" }, // Teal
];

export interface PendingMessage {
    id: string;
    id_seq: number;
    key: string;
    consumer: string;
    groupName: string;
    deliveryCount: number;
    idleTime: number; // milliseconds since last delivery
    reclaimIn: number; // milliseconds until auto-reclaim
    payload: string;
}

export const generateMockPendingMessages = (count: number = 15): PendingMessage[] => {
    const consumers = ['worker-1', 'worker-2', 'worker-3', 'worker-4', 'worker-5'];
    const groups = ['payment-processor', 'notification-sender', 'analytics-batch', 'audit-log'];
    const keys = ['order', 'payment', 'user', 'event', 'notification'];

    return Array.from({ length: count }).map((_, i) => {
        const seq = 14000 + i * 10;
        const deliveryCount = Math.floor(Math.random() * 5) + 1;
        const idleTime = Math.floor(Math.random() * 25000) + 2000; // 2-27 seconds
        const maxReclaimTime = 30000; // 30 seconds max
        const reclaimIn = Math.max(1000, maxReclaimTime - idleTime + Math.random() * 5000);

        return {
            id: `0-${seq}`,
            id_seq: seq,
            key: `${keys[Math.floor(Math.random() * keys.length)]}:${Math.floor(Math.random() * 10000)}`,
            consumer: consumers[Math.floor(Math.random() * consumers.length)],
            groupName: groups[Math.floor(Math.random() * groups.length)],
            deliveryCount,
            idleTime,
            reclaimIn,
            payload: `{"action": "process", "data": {...}}`
        };
    });
};

export interface StandbyConsumer {
    id: string;
    status: 'ready' | 'acquiring' | 'unavailable';
}

export interface ExclusiveGroup {
    groupName: string;
    mode: 'exclusive' | 'shared';
    leader: string | null;
    leaseExpiry: number; // timestamp
    leaseRemaining: number; // milliseconds
    standbys: StandbyConsumer[];
    lastFailover: number; // timestamp
    partitionId: number;
}

export const generateMockExclusiveGroups = (): ExclusiveGroup[] => {
    const now = Date.now();

    return [
        {
            groupName: 'payment-processor',
            mode: 'exclusive',
            leader: 'worker-2',
            leaseExpiry: now + 25000,
            leaseRemaining: 25000,
            standbys: [
                { id: 'worker-5', status: 'ready' },
                { id: 'worker-1', status: 'ready' }
            ],
            lastFailover: now - 120000,
            partitionId: 0
        },
        {
            groupName: 'order-fulfillment',
            mode: 'exclusive',
            leader: 'worker-3',
            leaseExpiry: now + 8000,
            leaseRemaining: 8000,
            standbys: [
                { id: 'worker-4', status: 'ready' },
                { id: 'worker-6', status: 'ready' },
                { id: 'worker-7', status: 'unavailable' }
            ],
            lastFailover: now - 45000,
            partitionId: 1
        },
        {
            groupName: 'audit-log',
            mode: 'exclusive',
            leader: null,
            leaseExpiry: now - 2000,
            leaseRemaining: 0,
            standbys: [
                { id: 'worker-3', status: 'acquiring' },
                { id: 'worker-4', status: 'ready' }
            ],
            lastFailover: now - 5000,
            partitionId: 2
        },
        {
            groupName: 'notification-sender',
            mode: 'exclusive',
            leader: 'worker-1',
            leaseExpiry: now + 42000,
            leaseRemaining: 42000,
            standbys: [
                { id: 'worker-8', status: 'ready' }
            ],
            lastFailover: now - 180000,
            partitionId: 0
        }
    ];
};



// --- KV Mock Data ---

export const generateMockVersions = (key: string, count: number): KVVersion[] => {
    const versions: KVVersion[] = [];
    let currentLsn = 10000 + Math.floor(Math.random() * 5000);
    let currentTime = Date.now();

    for (let i = 0; i < count; i++) {
        const isDeleted = i > 0 && Math.random() < 0.2; // 20% chance of being a tombstone if not latest
        const valueObj = {
            name: key.split(':')[1] || "unknown",
            updated_at: new Date(currentTime).toISOString(),
            count: Math.floor(Math.random() * 100),
            tags: ["mock", "data", "kv"]
        };

        versions.push({
            version: count - i,
            lsn: currentLsn,
            timestamp: currentTime,
            value: isDeleted ? "(tombstone)" : JSON.stringify(valueObj, null, 2),
            deleted: isDeleted,
            ttl: Math.random() < 0.3 ? currentTime + 3600000 : undefined // 30% chance of TTL
        });

        currentLsn -= Math.floor(Math.random() * 100) + 1;
        currentTime -= Math.floor(Math.random() * 86400000); // Go back up to 1 day
    }
    return versions;
};

export const generateMockKeys = (count: number, namespace: string): KVKey[] => {
    const prefixes = ['user', 'session', 'cache', 'analytics', 'config', 'data'];
    const subPrefixes = ['profile', 'settings', 'auth', 'temp', 'metrics', 'events'];
    const keys: KVKey[] = [];

    for (let i = 0; i < count; i++) {
        const prefix = prefixes[Math.floor(Math.random() * prefixes.length)];
        const subPrefix = subPrefixes[Math.floor(Math.random() * subPrefixes.length)];
        const id = Math.floor(Math.random() * 1000);

        // Create hierarchical keys like: user:123:profile, session:abc:auth, etc.
        const keyName = `${prefix}:${id}:${subPrefix}`;
        const versionCount = Math.floor(Math.random() * 10) + 1;
        const versions = generateMockVersions(keyName, versionCount);
        const latest = versions[0];

        // TTL logic: 20% expiring soon, 20% expired, 20% fresh, 40% no TTL
        const now = Date.now();
        let ttl_expiry: number | undefined = undefined;
        const ttlRoll = Math.random();

        if (ttlRoll < 0.2) {
            ttl_expiry = now + Math.random() * 60000; // Expiring in < 1 min
        } else if (ttlRoll < 0.4) {
            ttl_expiry = now - Math.random() * 60000; // Expired recently
        } else if (ttlRoll < 0.6) {
            ttl_expiry = now + 86400000; // Fresh (1 day)
        }

        keys.push({
            key: keyName,
            namespace,
            current_version: versionCount,
            current_lsn: latest.lsn,
            version_count: versionCount,
            size: JSON.stringify(latest.value).length,
            ttl_expiry,
            last_modified: latest.timestamp,
            versions
        });
    }

    return keys;
};

export const MOCK_NAMESPACES: KVNamespace[] = [
    { name: "sessions", key_count: 15420, version_count: 45000, current_lsn: 15600, size_bytes: 1024 * 1024 * 45 },
    { name: "users", key_count: 5200, version_count: 12000, current_lsn: 15500, size_bytes: 1024 * 1024 * 120 },
    { name: "config", key_count: 150, version_count: 4500, current_lsn: 15400, size_bytes: 1024 * 50 },
    { name: "cache:api", key_count: 85000, version_count: 85000, current_lsn: 15800, size_bytes: 1024 * 1024 * 500 },
];

export const MOCK_KV_KEYS = [
    ...generateMockKeys(50, "sessions"),
    ...generateMockKeys(20, "users"),
    ...generateMockKeys(10, "config")
];

export const MOCK_TRANSACTION: KVTransaction = {
    id: "txn-12345",
    status: "active",
    started_at: Date.now() - 1000 * 60 * 5, // 5 mins ago
    operations: [
        { type: "PUT", key: "sessions:user-123", value: { active: true } },
        { type: "PUT", key: "users:123:last_login", value: new Date().toISOString() },
        { type: "DELETE", key: "cache:api:user-123" }
    ]
};

// =============================================================================
// Queue Mock Data
// =============================================================================

import type {
    QueueInfo, QueueDetail, QueueMessage, QueueDLQEntry
} from "./api";

export const MOCK_QUEUES: QueueInfo[] = [
    { name: "order-processing", namespace: "production", pending: 23, available: 1450, enqueued: 89420, dequeued: 87947, acked: 87900, nacked: 47, dlq_count: 12, bytes_total: 1024 * 1024 * 230 },
    { name: "email-notifications", namespace: "production", pending: 5, available: 320, enqueued: 45000, dequeued: 44675, acked: 44670, nacked: 5, dlq_count: 3, bytes_total: 1024 * 1024 * 95 },
    { name: "image-resize", namespace: "production", pending: 142, available: 8900, enqueued: 125000, dequeued: 115958, acked: 115900, nacked: 58, dlq_count: 0, bytes_total: 1024 * 1024 * 1500 },
    { name: "payment-webhooks", namespace: "production", pending: 0, available: 0, enqueued: 12300, dequeued: 12300, acked: 12300, nacked: 0, dlq_count: 0, bytes_total: 1024 * 1024 * 18 },
    { name: "analytics-events", namespace: "analytics", pending: 3, available: 52000, enqueued: 980000, dequeued: 927997, acked: 927900, nacked: 97, dlq_count: 45, bytes_total: 1024 * 1024 * 4200 },
    { name: "video-transcode", namespace: "media", pending: 87, available: 234, enqueued: 4500, dequeued: 4179, acked: 4170, nacked: 9, dlq_count: 8, bytes_total: 1024 * 1024 * 850 },
    { name: "user-signup-flow", namespace: "production", pending: 0, available: 15, enqueued: 3200, dequeued: 3185, acked: 3184, nacked: 1, dlq_count: 1, bytes_total: 1024 * 1024 * 6 },
    { name: "inventory-sync", namespace: "production", pending: 12, available: 890, enqueued: 67000, dequeued: 66098, acked: 66090, nacked: 8, dlq_count: 6, bytes_total: 1024 * 1024 * 310 },
];

export const MOCK_QUEUE_DETAIL: Record<string, QueueDetail> = {
    "order-processing": {
        name: "order-processing",
        namespace: "production",
        pending: 23,
        available: 1450,
        enqueued: 89420,
        dequeued: 87947,
        acked: 87900,
        nacked: 47,
        dlq_count: 12,
        bytes_total: 45 * 1024 * 1024,
        delayed: 5,
        lease_timeout_ms: 30000,
        max_retries: 3,
        created_at_ms: Date.now() - 30 * 24 * 3600 * 1000,
        enqueue_rate: 125,
        dequeue_rate: 118,
    },
    "email-notifications": {
        name: "email-notifications",
        namespace: "production",
        pending: 5,
        available: 320,
        enqueued: 45000,
        dequeued: 44675,
        acked: 44650,
        nacked: 25,
        dlq_count: 3,
        bytes_total: 12 * 1024 * 1024,
        delayed: 0,
        lease_timeout_ms: 60000,
        max_retries: 5,
        created_at_ms: Date.now() - 60 * 24 * 3600 * 1000,
        enqueue_rate: 50,
        dequeue_rate: 48,
    },
    "analytics-events": {
        name: "analytics-events",
        namespace: "analytics",
        pending: 3,
        available: 52000,
        enqueued: 980000,
        dequeued: 927997,
        acked: 927800,
        nacked: 197,
        dlq_count: 45,
        bytes_total: 890 * 1024 * 1024,
        delayed: 200,
        lease_timeout_ms: 10000,
        max_retries: 2,
        created_at_ms: Date.now() - 90 * 24 * 3600 * 1000,
        enqueue_rate: 4500,
        dequeue_rate: 4200,
    },
};

export function generateMockQueueMessages(count: number, queueName: string): QueueMessage[] {
    const statuses: QueueMessage['status'][] = ['available', 'available', 'available', 'leased', 'delayed'];
    const types = ['order.created', 'order.updated', 'payment.received', 'user.signup', 'email.send'];
    const consumers = ['worker-1', 'worker-2', 'worker-3', 'worker-4'];
    const now = Date.now();

    return Array.from({ length: count }).map((_, i) => {
        const seq = 10000 + i;
        const status = statuses[Math.floor(Math.random() * statuses.length)];
        const priority = Math.floor(Math.random() * 100);
        const enqueued_at = now - (count - i) * 2000;

        return {
            seq,
            priority,
            status,
            header: types[Math.floor(Math.random() * types.length)],
            payload: JSON.stringify({
                id: `${queueName}-${seq}`,
                action: types[Math.floor(Math.random() * types.length)],
                data: { user_id: Math.floor(Math.random() * 10000), amount: (Math.random() * 500).toFixed(2) },
                timestamp: new Date(enqueued_at).toISOString(),
            }),
            enqueued_at_ms: enqueued_at,
            lease_expires_ms: status === 'leased' ? now + Math.floor(Math.random() * 30000) : undefined,
            consumer: status === 'leased' ? consumers[Math.floor(Math.random() * consumers.length)] : undefined,
            delivery_count: status === 'leased' ? Math.floor(Math.random() * 3) + 1 : 0,
            delay_until_ms: status === 'delayed' ? now + Math.floor(Math.random() * 60000) : undefined,
            dedup_key: Math.random() < 0.3 ? `dedup-${queueName}-${seq}` : undefined,
            message_type: types[Math.floor(Math.random() * types.length)],
            correlation_id: Math.random() < 0.5 ? `corr-${Math.floor(Math.random() * 10000)}` : undefined,
        };
    });
}

export function generateMockDLQEntries(count: number, queueName: string): QueueDLQEntry[] {
    const errors = [
        'Timeout: processing exceeded 30s limit',
        'HTTP 503: Payment gateway unavailable',
        'JSON parse error: unexpected token at position 42',
        'Database constraint violation: duplicate key',
        'Connection refused: downstream service unreachable',
        'Rate limit exceeded: 429 Too Many Requests',
        'Invalid payload: missing required field "user_id"',
    ];
    const types = ['order.created', 'payment.charge', 'email.send', 'webhook.dispatch'];
    const now = Date.now();

    return Array.from({ length: count }).map((_, i) => {
        const seq = 5000 + i * 7;
        const dlq_at = now - Math.floor(Math.random() * 86400000 * 7); // up to 7 days ago

        return {
            seq,
            header: types[Math.floor(Math.random() * types.length)],
            payload: JSON.stringify({
                id: `${queueName}-dlq-${seq}`,
                original_data: { user_id: Math.floor(Math.random() * 10000) },
            }),
            error_msg: errors[Math.floor(Math.random() * errors.length)],
            attempts: Math.floor(Math.random() * 5) + 1,
            dlq_at_ms: dlq_at,
            partition: Math.floor(Math.random() * 4),
            message_type: types[Math.floor(Math.random() * types.length)],
        };
    });
}
