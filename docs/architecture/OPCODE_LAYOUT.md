# Flo OpCode Layout — v2

> **Status**: Active
> **OpCode type**: `enum(u16)`
> **Dispatch table**: 1024 slots (8 KB per shard)
> **Date**: 2026-03-30

## Design Principles

1. **Three layers**: Infra (`0x0__`), Data (`0x1__`–`0x2__`), Compute (`0x3__`)
2. **`op >> 8`** gives the layer: `0` = infra, `1`/`2` = data, `3` = compute
3. Each subsystem gets a 32- or 48-slot block at `0x10`-aligned boundaries
4. Every block has room to grow without spilling into neighbors
5. Workers are fully decoupled from Actions (generic task runners)
6. Vectors and Emit have reserved ranges but are not yet implemented

## Layout Summary

```
Page 0x0__ — Infrastructure (256 slots)
Page 0x1__ — Data, lower half (256 slots)
Page 0x2__ — Data, upper half (256 slots)
Page 0x3__ — Compute (256 slots)
```

| Range | Subsystem | Slots | Used | Free | Layer |
|-------|-----------|-------|------|------|-------|
| **`0x000–0x0FF`** | **Infrastructure** | **256** | **28** | **228** | |
| `0x000–0x00F` | System | 16 | 6 | 10 | Infra |
| `0x010–0x02F` | Namespace | 32 | 12 | 20 | Infra |
| `0x030–0x04F` | Cluster | 32 | 10 | 22 | Infra |
| `0x050–0x0FF` | *(infra reserve)* | 176 | 0 | 176 | Infra |
| | | | | | |
| **`0x100–0x2FF`** | **Data** | **512** | **87** | **425** | |
| `0x100–0x12F` | KV + Txn + Snap | 48 | 18 | 30 | Data |
| `0x130–0x14F` | Streams | 32 | 16 | 16 | Data |
| `0x150–0x16F` | Consumer Groups | 32 | 13 | 19 | Data |
| `0x170–0x19F` | Queues | 48 | 26 | 22 | Data |
| `0x1A0–0x1BF` | Time-Series | 32 | 14 | 18 | Data |
| `0x1C0–0x2FF` | *(data reserve)* | 320 | 0 | 320 | Data |
| | | | | | |
| **`0x300–0x3FF`** | **Compute** | **256** | **62** | **194** | |
| `0x300–0x31F` | Actions | 32 | 16 | 16 | Compute |
| `0x320–0x33F` | Workers | 32 | 10 | 22 | Compute |
| `0x340–0x35F` | Workflows | 32 | 20 | 12 | Compute |
| `0x360–0x37F` | Processing | 32 | 16 | 16 | Compute |
| `0x380–0x3FF` | *(compute reserve)* | 128 | 0 | 128 | Compute |
| | | | | | |
| | **Total** | **1024** | **177** | **847** | |

## Layer Classification

| Layer | Page(s) | Purpose | Growth Expectation |
|-------|---------|---------|-------------------|
| Infra | `0x0__` | System bootstrap, namespacing, cluster management, auth (future) | Low — stable primitives |
| Data | `0x1__`–`0x2__` | All storage: KV, streams, queues, time-series, future (vectors, documents, geo, counters) | High — new storage engines |
| Compute | `0x3__` | All execution: actions, workers, workflows, processing, future (emit) | Low — new capabilities fold into existing subsystems |

## Auth Subsystem Mapping

`opcodeSubsystem()` uses `op >> 8` for layer, then range checks within:

```
0x000–0x00F → "system"
0x010–0x02F → "namespace"
0x030–0x04F → "cluster"
0x100–0x12F → "kv"
0x130–0x16F → "stream"       (streams + consumer groups)
0x170–0x19F → "queue"
0x1A0–0x1BF → "ts"
0x300–0x31F → "actions"
0x320–0x33F → "worker"
0x340–0x35F → "workflow"
0x360–0x37F → "processing"
```

## Reserved Space — Expected Future Use

| Reserve | Likely Candidates |
|---------|------------------|
| Infra (`0x050–0x0FF`) | Auth/RBAC, rate limiting, telemetry config, audit log |
| Data (`0x1C0–0x2FF`) | Vectors (kNN/ANN), document/secondary indexes, geospatial, counters/CRDTs |
| Compute (`0x380–0x3FF`) | Emit (outbound webhooks), headroom for existing subsystems to grow |

## Wire Format

```
RequestHeader (24 bytes, extern struct):
  magic:          u32   [0:3]     0x004F4C46 ("FLO\0")
  payload_length: u32   [4:7]
  request_id:     u64   [8:15]
  crc32:          u32   [16:19]
  op_code:        u16   [20:21]   ← indexes into dispatch table
  version:        u8    [22]
  flags:          u8    [23]
```

## Dispatch Table Cost

`1024 × @sizeOf(?*const fn) = 1024 × 8 = 8 KB per shard`

On a 16-shard machine: 128 KB total. Negligible.


```
/// ============================================================================
/// OpCode v2 — Final Layout (1024 slots)
/// ============================================================================
///
/// Three layers: Infra (0x0__), Data (0x1__–0x2__), Compute (0x3__)
/// `op >> 8` → 0 = infra, 1/2 = data, 3 = compute
///
/// See docs/architecture/OPCODE_LAYOUT.md for full rationale.
///
///   Range           Subsystem               Slots  Used  Free
///   ─────────────   ──────────────────────   ─────  ────  ────
///   0x000 – 0x00F   System                    16      6    10
///   0x010 – 0x02F   Namespace                 32     12    20
///   0x030 – 0x04F   Cluster                   32     10    22
///   0x050 – 0x0FF   (infra reserve)          176      0   176
///   0x100 – 0x12F   KV + Txn + Snapshots      48     18    30
///   0x130 – 0x14F   Streams                   32     16    16
///   0x150 – 0x16F   Consumer Groups           32     13    19
///   0x170 – 0x19F   Queues                    48     26    22
///   0x1A0 – 0x1BF   Time-Series               32     14    18
///   0x1C0 – 0x2FF   (data reserve)           320      0   320
///   0x300 – 0x31F   Actions                   32     16    16
///   0x320 – 0x33F   Workers                   32     10    22
///   0x340 – 0x35F   Workflows                 32     20    12
///   0x360 – 0x37F   Processing                32     16    16
///   0x380 – 0x3FF   (compute reserve)        128      0   128
///                                           ────   ───   ───
///                   Total                   1024    177   847
///
pub const OpCode = enum(u16) {

    // =========================================================================
    // INFRASTRUCTURE — Page 0x0__ (256 slots)
    // =========================================================================

    // ── System (0x000 – 0x00F) — 16 slots ───────────────────────────────────
    ping = 0x000,
    pong = 0x001,
    error_response = 0x002,
    auth = 0x003,
    set_durability = 0x004,
    ok = 0x005,
    // 0x006–0x00F reserved

    // ── Namespace (0x010 – 0x02F) — 32 slots ────────────────────────────────
    namespace_create = 0x010,
    namespace_delete = 0x011,
    namespace_list = 0x012,
    namespace_info = 0x013,
    namespace_config_set = 0x014,
    namespace_config_get = 0x015,

    namespace_create_response = 0x020,
    namespace_delete_response = 0x021,
    namespace_list_response = 0x022,
    namespace_info_response = 0x023,
    namespace_config_set_response = 0x024,
    namespace_config_get_response = 0x025,
    // 0x026–0x02F reserved

    // ── Cluster (0x030 – 0x04F) — 32 slots ──────────────────────────────────
    cluster_status = 0x030,
    cluster_members = 0x031,
    cluster_join = 0x032,
    cluster_leave = 0x033,
    cluster_transfer_leader = 0x034,
    cluster_add_node = 0x035,
    cluster_remove_node = 0x036,

    cluster_status_response = 0x040,
    cluster_members_response = 0x041,
    cluster_join_response = 0x042,
    // 0x043–0x04F reserved

    // 0x050–0x0FF: infra reserve (176 slots — auth, rate-limit, telemetry, audit)

    // =========================================================================
    // DATA — Pages 0x1__ + 0x2__ (512 slots)
    // =========================================================================

    // ── KV + Transactions + Snapshots (0x100 – 0x12F) — 48 slots ────────────

    // Core KV (0x100 – 0x10F)
    kv_put = 0x100,
    kv_get = 0x101,
    kv_mget = 0x102,
    kv_delete = 0x103,
    kv_scan = 0x104,
    kv_history = 0x105,
    kv_get_response = 0x106,
    kv_mget_response = 0x107,
    kv_put_response = 0x108,
    kv_scan_response = 0x109,
    kv_history_response = 0x10A,

    // Transactions (0x110 – 0x11F)
    kv_begin_txn = 0x110,
    kv_commit_txn = 0x111,
    kv_rollback_txn = 0x112,

    // Snapshots (0x120 – 0x12F)
    kv_snapshot_create = 0x120,
    kv_snapshot_get = 0x121,
    kv_snapshot_release = 0x122,
    kv_snapshot_create_response = 0x123,
    // 0x124–0x12F reserved

    // ── Streams (0x130 – 0x14F) — 32 slots ──────────────────────────────────
    stream_append = 0x130,
    stream_read = 0x131,
    stream_trim = 0x132,
    stream_info = 0x133,
    stream_append_response = 0x134,
    stream_read_response = 0x135,
    stream_event = 0x136,
    stream_subscribe = 0x137,
    stream_unsubscribe = 0x138,
    stream_subscribed = 0x139,
    stream_unsubscribed = 0x13A,
    stream_list = 0x13B,
    stream_list_response = 0x13C,
    stream_create = 0x13D,
    stream_create_response = 0x13E,
    stream_alter = 0x13F,
    // 0x140–0x14F reserved for stream extensions

    // ── Stream Consumer Groups (0x150 – 0x16F) — 32 slots ───────────────────
    stream_group_create = 0x150,
    stream_group_join = 0x151,
    stream_group_leave = 0x152,
    stream_group_read = 0x153,
    stream_group_ack = 0x154,
    stream_group_claim = 0x155,
    stream_group_pending = 0x156,
    stream_group_configure_sweeper = 0x157,
    stream_group_read_response = 0x158,
    stream_group_nack = 0x159,
    stream_group_touch = 0x15A,
    stream_group_info = 0x15B,
    stream_group_delete = 0x15C,
    // 0x15D–0x16F reserved

    // ── Queues (0x170 – 0x19F) — 48 slots ───────────────────────────────────

    // Queue operations (0x170 – 0x17F)
    queue_enqueue = 0x170,
    queue_dequeue = 0x171,
    queue_complete = 0x172,
    queue_extend_lease = 0x173,
    queue_fail = 0x174,
    queue_fail_auto = 0x175,
    queue_dlq_list = 0x176,
    queue_dlq_delete = 0x177,
    queue_dlq_requeue = 0x178,
    queue_dlq_stats = 0x179,
    queue_promote_due = 0x17A,
    queue_stats = 0x17B,
    queue_peek = 0x17C,
    queue_touch = 0x17D,
    queue_batch_enqueue = 0x17E,
    queue_purge = 0x17F,

    // Queue responses (0x190 – 0x19F)
    queue_enqueue_response = 0x190,
    queue_dequeue_response = 0x191,
    queue_dlq_list_response = 0x192,
    queue_stats_response = 0x193,
    queue_peek_response = 0x194,
    queue_touch_response = 0x195,
    queue_batch_enqueue_response = 0x196,
    queue_purge_response = 0x197,
    queue_list = 0x198,
    queue_list_response = 0x199,
    // 0x19A–0x19F reserved

    // ── Time-Series (0x1A0 – 0x1BF) — 32 slots ─────────────────────────────
    ts_write = 0x1A0,
    ts_read = 0x1A1,
    ts_query = 0x1A2,
    ts_floql = 0x1A3,
    ts_list = 0x1A4,
    ts_delete = 0x1A5,
    ts_retention = 0x1A6,
    ts_write_response = 0x1A7,
    ts_read_response = 0x1A8,
    ts_query_response = 0x1A9,
    ts_floql_response = 0x1AA,
    ts_list_response = 0x1AB,
    ts_delete_response = 0x1AC,
    ts_retention_response = 0x1AD,
    // 0x1AE–0x1BF reserved

    // 0x1C0–0x2FF: data reserve (320 slots — vectors, documents, geospatial, counters)

    // =========================================================================
    // COMPUTE — Page 0x3__ (256 slots)
    // =========================================================================

    // ── Actions (0x300 – 0x31F) — 32 slots ──────────────────────────────────
    action_register = 0x300,
    action_invoke = 0x301,
    action_status = 0x302,
    action_list = 0x303,
    action_list_runs = 0x304,
    action_delete = 0x305,
    action_await = 0x306,
    action_complete = 0x307,
    action_fail = 0x308,
    action_touch = 0x309,

    action_register_response = 0x310,
    action_invoke_response = 0x311,
    action_status_response = 0x312,
    action_list_response = 0x313,
    action_list_runs_response = 0x314,
    action_task_assignment = 0x315,
    // 0x316–0x31F reserved

    // ── Workers (0x320 – 0x33F) — 32 slots ──────────────────────────────────
    //
    // Generic task runners. Fully decoupled from Actions.
    // A worker can serve actions, stream processing, or any subsystem.
    //
    worker_register = 0x320,
    worker_heartbeat = 0x321,
    worker_deregister = 0x322,
    worker_list = 0x323,
    worker_info = 0x324,
    worker_drain = 0x325,
    // 0x326–0x32F reserved

    worker_register_response = 0x330,
    worker_list_response = 0x331,
    worker_info_response = 0x332,
    worker_drain_response = 0x333,
    // 0x334–0x33F reserved

    // ── Workflows (0x340 – 0x35F) — 32 slots ────────────────────────────────
    workflow_create = 0x340,
    workflow_start = 0x341,
    workflow_signal = 0x342,
    workflow_cancel = 0x343,
    workflow_status = 0x344,
    workflow_history = 0x345,
    workflow_list_runs = 0x346,
    workflow_get_definition = 0x347,
    workflow_disable = 0x348,
    workflow_enable = 0x349,
    workflow_list_definitions = 0x34A,

    workflow_create_response = 0x350,
    workflow_start_response = 0x351,
    workflow_status_response = 0x352,
    workflow_history_response = 0x353,
    workflow_list_runs_response = 0x354,
    workflow_get_definition_response = 0x355,
    workflow_disable_response = 0x356,
    workflow_enable_response = 0x357,
    workflow_list_definitions_response = 0x358,
    // 0x359–0x35F reserved

    // ── Processing (0x360 – 0x37F) — 32 slots ───────────────────────────────
    processing_submit = 0x360,
    processing_stop = 0x361,
    processing_cancel = 0x362,
    processing_status = 0x363,
    processing_list = 0x364,
    processing_savepoint = 0x365,
    processing_restore = 0x366,
    processing_rescale = 0x367,

    processing_submit_response = 0x370,
    processing_stop_response = 0x371,
    processing_cancel_response = 0x372,
    processing_status_response = 0x373,
    processing_list_response = 0x374,
    processing_savepoint_response = 0x375,
    processing_restore_response = 0x376,
    processing_rescale_response = 0x377,
    // 0x378–0x37F reserved

    // 0x380–0x3FF: compute reserve (128 slots — emit, future compute)

    /// Non-exhaustive — allows unknown opcodes on the wire
    _,
};
```