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
RequestHeader (32 bytes, extern struct):
  magic:          u32   [0:3]     0x004F4C46 ("FLO\0")
  payload_length: u32   [4:7]
  request_id:     u64   [8:15]
  crc32:          u32   [16:19]   covers bytes [0:15] + [20:31]
  op_code:        u16   [20:21]   ← indexes into dispatch table
  version:        u8    [22]
  flags:          u8    [23]
  reserved:       [8]u8 [24:31]   must be zero; future: trace_id, timeout_ms, etc.
```

## Dispatch Table Cost

`1024 × @sizeOf(?*const fn) = 1024 × 8 = 8 KB per shard`

On a 16-shard machine: 128 KB total. Negligible.

## Canonical OpCode Definition

The single source of truth for all opcode values is
[`src/protocol/proto.zig`](../../src/protocol/proto.zig) — `pub const OpCode = enum(u16) { … }`.

Do **not** duplicate the enum here. The Layout Summary table above documents
the block assignments and slot counts; `proto.zig` has the authoritative values.
```