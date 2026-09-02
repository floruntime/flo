//! VOPR Scenario — swarm-synthesized simulation configuration
//!
//! A Scenario is the complete, deterministic description of one simulation
//! run: cluster shape, fault probabilities, network behavior, durability
//! model, and workload profile. Every field derives from a single seed
//! (swarm testing, per TigerBeetle), so seed → scenario → run is fully
//! reproducible.
//!
//! Scenarios serialize to JSON (`writeJson`) so a run can be pinned and
//! replayed exactly: a pinned bare seed would silently become a
//! different, passing scenario the moment the sampling in `fromSeed`
//! changes.

const std = @import("std");
const PRNG = @import("stdx").PRNG;

/// How Raft hard state (current_term, voted_for) behaves across a crash.
/// `.persisted` is the Raft spec — the target semantics and the default.
/// `.volatile_state` models production today (nothing persisted); running
/// in that mode demonstrates the split-brain hazard rather than testing
/// toward the target, so `fromSeed` never selects it — a scenario must
/// set it explicitly.
pub const HardStateMode = enum {
    persisted,
    volatile_state,

    pub fn name(self: HardStateMode) []const u8 {
        return switch (self) {
            .persisted => "persisted",
            .volatile_state => "volatile",
        };
    }
};

/// When appended log entries become durable relative to the ack.
/// Mirrors the server `Durability` config: `.sync` before responding,
/// `.async_flush` on an interval — so a crash inside the window loses
/// acked entries. That loss is `.async_flush`'s documented contract —
/// loss inside the window is the mode's semantics, not a finding.
pub const DurabilityMode = enum {
    sync,
    async_flush,

    pub fn name(self: DurabilityMode) []const u8 {
        return switch (self) {
            .sync => "sync",
            .async_flush => "async_flush",
        };
    }
};

/// AppendEntries batch ceiling — matches the `max_entries_per_batch`
/// default in `raft/node.zig`. Payload and ring-capacity sampling are
/// constrained against this so a single batch can never evict its own
/// head mid-apply.
pub const MAX_BATCH: u32 = 64;

pub const Scenario = struct {
    seed: u64,

    // ── Cluster shape ──────────────────────────────────────────────────
    /// 3, 5, or 7 nodes (RaftNode MAX_PEERS = 7 caps total at 8).
    node_count: u8,
    /// Raft log hot-ring capacity in bytes per node.
    log_capacity: usize,
    /// True when this scenario deliberately violates the ring-floor
    /// constraint to exercise eviction with no catch-up path.
    small_ring: bool,

    // ── Phases (simulated milliseconds; 1 tick = 1 ms) ─────────────────
    ticks_safety: u64,
    ticks_convergence: u64,

    // ── Durability model ───────────────────────────────────────────────
    hard_state: HardStateMode,
    durability: DurabilityMode,
    /// Flush interval for `.async_flush`, in ticks.
    flush_interval_ms: u64,

    // ── Network faults ─────────────────────────────────────────────────
    msg_delay_min_ms: u64,
    msg_delay_max_ms: u64,
    /// Percent chance an individual message is dropped.
    drop_percent: u8,
    /// Percent chance an individual message is duplicated.
    duplicate_percent: u8,
    /// Per-tick per-mille chance a partition forms when none is active.
    partition_permille: u16,
    partition_min_ms: u64,
    partition_max_ms: u64,

    // ── Process faults ─────────────────────────────────────────────────
    /// Per-tick per-node crash chance (per-mille).
    crash_permille: u16,
    /// Per-tick per-crashed-node restart chance (per-mille).
    restart_permille: u16,

    // ── Workload ───────────────────────────────────────────────────────
    /// Percent chance per tick that a client submits an op.
    request_percent: u8,
    payload_min: u32,
    payload_max: u32,

    // ── Raft timing ────────────────────────────────────────────────────
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
    heartbeat_interval_ms: u64,
    /// Resend an inflight AppendEntries after this many ticks w/o reply.
    rpc_timeout_ms: u64,

    /// Derive a complete scenario from a seed. Every run samples a
    /// different point in fault space; the constraints below keep the
    /// main swarm out of fails-by-design territory (see `smallRing`).
    pub fn fromSeed(seed: u64) Scenario {
        var prng = PRNG.init(seed);
        const r = prng.random();

        const node_count: u8 = switch (r.uintLessThan(u8, 3)) {
            0 => 3,
            1 => 5,
            else => 7,
        };

        const election_min: u64 = r.intRangeAtMost(u64, 100, 300);
        const election_max: u64 = election_min + r.intRangeAtMost(u64, 100, 300);

        const delay_min: u64 = r.intRangeAtMost(u64, 1, 10);
        const delay_max: u64 = delay_min + r.intRangeAtMost(u64, 1, 40);

        // 64 KiB .. 1 MiB ring; payload capped so MAX_BATCH max-size
        // entries fill at most half the ring (batch cannot evict its own
        // head, EntryTooLarge unreachable from protocol-legal input).
        const log_capacity = @as(usize, 64 * 1024) << @intCast(r.uintLessThan(u8, 5));
        const payload_ceiling: u32 = @intCast(log_capacity / (2 * MAX_BATCH));
        const payload_max = @min(r.intRangeAtMost(u32, 64, 2048), payload_ceiling);

        return .{
            .seed = seed,
            .node_count = node_count,
            .log_capacity = log_capacity,
            .small_ring = false,
            .ticks_safety = r.intRangeAtMost(u64, 10_000, 40_000),
            .ticks_convergence = 60_000,
            .hard_state = .persisted,
            .durability = if (r.uintLessThan(u8, 100) < 70) DurabilityMode.sync else .async_flush,
            .flush_interval_ms = 1000,
            .msg_delay_min_ms = delay_min,
            .msg_delay_max_ms = delay_max,
            .drop_percent = r.intRangeAtMost(u8, 0, 20),
            .duplicate_percent = r.intRangeAtMost(u8, 0, 5),
            .partition_permille = r.intRangeAtMost(u16, 0, 10),
            .partition_min_ms = r.intRangeAtMost(u64, 200, 1000),
            .partition_max_ms = r.intRangeAtMost(u64, 1000, 5000),
            .crash_permille = r.intRangeAtMost(u16, 0, 5),
            .restart_permille = r.intRangeAtMost(u16, 5, 50),
            .request_percent = r.intRangeAtMost(u8, 5, 60),
            .payload_min = 16,
            .payload_max = @max(16, payload_max),
            .election_timeout_min_ms = election_min,
            .election_timeout_max_ms = election_max,
            .heartbeat_interval_ms = @max(10, election_min / 4),
            .rpc_timeout_ms = @max(50, delay_max * 3),
        };
    }

    /// The deliberately-small-ring slice: same sampling as `fromSeed` but
    /// the ring is shrunk below the floor so eviction outruns replication.
    /// Until a snapshot/catch-up path exists these seeds fail convergence
    /// *by design* — they are that work's standing acceptance test — and
    /// the truncate/evict interaction can hang inside `ual.append`, so
    /// anything running these seeds needs a per-seed timeout.
    pub fn smallRing(seed: u64) Scenario {
        var s = fromSeed(seed);
        s.small_ring = true;
        s.log_capacity = 8 * 1024;
        // Eviction pressure needs traffic: an idle seed never outruns even an
        // 8 KiB ring, so force the request rate to the sampling maximum.
        s.request_percent = 60;
        // Keep single entries appendable — the point is eviction pressure,
        // not EntryTooLarge.
        s.payload_max = 256;
        return s;
    }

    /// No faults, small cluster, roomy ring — the sanity baseline:
    /// if calm fails, the harness is broken, not Flo.
    pub fn calm(seed: u64) Scenario {
        var s = fromSeed(seed);
        s.node_count = 3;
        s.ticks_safety = 5_000;
        s.drop_percent = 0;
        s.duplicate_percent = 0;
        s.partition_permille = 0;
        s.crash_permille = 0;
        s.durability = .sync;
        s.log_capacity = 4 * 1024 * 1024;
        s.payload_max = 2048;
        return s;
    }

    /// Serialize to JSON. Hand-rolled: stable field order, no dependence
    /// on std.json output shape — a serialized scenario is re-loadable
    /// and replayable exactly.
    pub fn writeJson(self: *const Scenario, writer: anytype) !void {
        try writer.print(
            \\{{
            \\  "seed": {d},
            \\  "node_count": {d},
            \\  "log_capacity": {d},
            \\  "small_ring": {},
            \\  "ticks_safety": {d},
            \\  "ticks_convergence": {d},
            \\  "hard_state": "{s}",
            \\  "durability": "{s}",
            \\  "flush_interval_ms": {d},
            \\  "msg_delay_min_ms": {d},
            \\  "msg_delay_max_ms": {d},
            \\  "drop_percent": {d},
            \\  "duplicate_percent": {d},
            \\  "partition_permille": {d},
            \\  "partition_min_ms": {d},
            \\  "partition_max_ms": {d},
            \\  "crash_permille": {d},
            \\  "restart_permille": {d},
            \\  "request_percent": {d},
            \\  "payload_min": {d},
            \\  "payload_max": {d},
            \\  "election_timeout_min_ms": {d},
            \\  "election_timeout_max_ms": {d},
            \\  "heartbeat_interval_ms": {d},
            \\  "rpc_timeout_ms": {d}
            \\}}
            \\
        , .{
            self.seed,                    self.node_count,              self.log_capacity,
            self.small_ring,              self.ticks_safety,            self.ticks_convergence,
            self.hard_state.name(),       self.durability.name(),       self.flush_interval_ms,
            self.msg_delay_min_ms,        self.msg_delay_max_ms,        self.drop_percent,
            self.duplicate_percent,       self.partition_permille,      self.partition_min_ms,
            self.partition_max_ms,        self.crash_permille,          self.restart_permille,
            self.request_percent,         self.payload_min,             self.payload_max,
            self.election_timeout_min_ms, self.election_timeout_max_ms, self.heartbeat_interval_ms,
            self.rpc_timeout_ms,
        });
    }

    /// Parse a scenario back from `writeJson` output — the pin format.
    pub fn fromJsonSlice(allocator: std.mem.Allocator, bytes: []const u8) !Scenario {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidField;
        const obj = parsed.value.object;
        // A pin is hand-editable: an unrecognised mode must fail loudly, never
        // fall through to a default and run a different scenario.
        const hard_state: HardStateMode = if (try jsonEnum(obj, "hard_state", &.{ "persisted", "volatile" })) .persisted else .volatile_state;
        const durability: DurabilityMode = if (try jsonEnum(obj, "durability", &.{ "sync", "async_flush" })) .sync else .async_flush;
        const small_ring_v = obj.get("small_ring") orelse return error.MissingField;
        if (small_ring_v != .bool) return error.InvalidField;
        return .{
            .seed = try jsonU64(obj, "seed"),
            .node_count = try jsonInt(u8, obj, "node_count"),
            .log_capacity = try jsonInt(usize, obj, "log_capacity"),
            .small_ring = small_ring_v.bool,
            .ticks_safety = try jsonU64(obj, "ticks_safety"),
            .ticks_convergence = try jsonU64(obj, "ticks_convergence"),
            .hard_state = hard_state,
            .durability = durability,
            .flush_interval_ms = try jsonU64(obj, "flush_interval_ms"),
            .msg_delay_min_ms = try jsonU64(obj, "msg_delay_min_ms"),
            .msg_delay_max_ms = try jsonU64(obj, "msg_delay_max_ms"),
            .drop_percent = try jsonInt(u8, obj, "drop_percent"),
            .duplicate_percent = try jsonInt(u8, obj, "duplicate_percent"),
            .partition_permille = try jsonInt(u16, obj, "partition_permille"),
            .partition_min_ms = try jsonU64(obj, "partition_min_ms"),
            .partition_max_ms = try jsonU64(obj, "partition_max_ms"),
            .crash_permille = try jsonInt(u16, obj, "crash_permille"),
            .restart_permille = try jsonInt(u16, obj, "restart_permille"),
            .request_percent = try jsonInt(u8, obj, "request_percent"),
            .payload_min = try jsonInt(u32, obj, "payload_min"),
            .payload_max = try jsonInt(u32, obj, "payload_max"),
            .election_timeout_min_ms = try jsonU64(obj, "election_timeout_min_ms"),
            .election_timeout_max_ms = try jsonU64(obj, "election_timeout_max_ms"),
            .heartbeat_interval_ms = try jsonU64(obj, "heartbeat_interval_ms"),
            .rpc_timeout_ms = try jsonU64(obj, "rpc_timeout_ms"),
        };
    }
};

fn jsonU64(obj: std.json.ObjectMap, key: []const u8) !u64 {
    const v = obj.get(key) orelse return error.MissingField;
    if (v != .integer or v.integer < 0) return error.InvalidField;
    return @intCast(v.integer);
}

/// Range-checked narrowing: an out-of-range pin value is an error, not a
/// panic.
fn jsonInt(comptime T: type, obj: std.json.ObjectMap, key: []const u8) !T {
    return std.math.cast(T, try jsonU64(obj, key)) orelse error.InvalidField;
}

/// True when the field equals `names[0]`, false for `names[1]`, error for
/// anything else.
fn jsonEnum(obj: std.json.ObjectMap, key: []const u8, names: []const []const u8) !bool {
    const v = obj.get(key) orelse return error.MissingField;
    if (v != .string) return error.InvalidField;
    if (std.mem.eql(u8, v.string, names[0])) return true;
    if (std.mem.eql(u8, v.string, names[1])) return false;
    return error.InvalidField;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "vopr scenario: deterministic from seed" {
    const a = Scenario.fromSeed(42);
    const b = Scenario.fromSeed(42);
    try testing.expectEqual(a, b);
}

test "vopr scenario: different seeds differ beyond the seed field" {
    // Normalize the embedded seed first — otherwise this passes even if
    // fromSeed ignored its input entirely.
    var a = Scenario.fromSeed(1);
    const b = Scenario.fromSeed(2);
    a.seed = b.seed;
    try testing.expect(!std.meta.eql(a, b));
}

test "vopr scenario: swarm bounds and batch/ring constraint" {
    for (0..500) |seed| {
        const s = Scenario.fromSeed(seed);
        try testing.expect(s.node_count == 3 or s.node_count == 5 or s.node_count == 7);
        try testing.expect(s.election_timeout_max_ms > s.election_timeout_min_ms);
        try testing.expect(s.msg_delay_max_ms >= s.msg_delay_min_ms);
        try testing.expect(s.payload_min >= 16 and s.payload_max >= s.payload_min);
        // A full batch of max-size payloads must fit in half the ring.
        try testing.expect(@as(usize, s.payload_max) * MAX_BATCH <= s.log_capacity / 2);
        try testing.expect(!s.small_ring);
    }
}

test "vopr scenario: small-ring slice breaks the floor on purpose" {
    const s = Scenario.smallRing(7);
    try testing.expect(s.small_ring);
    try testing.expect(s.log_capacity < 64 * 1024);
    // Single entries still fit comfortably.
    try testing.expect(@as(usize, s.payload_max) * 4 < s.log_capacity);
}

test "vopr scenario: calm has no faults" {
    const s = Scenario.calm(123);
    try testing.expectEqual(@as(u8, 0), s.drop_percent);
    try testing.expectEqual(@as(u16, 0), s.partition_permille);
    try testing.expectEqual(@as(u16, 0), s.crash_permille);
    try testing.expectEqual(DurabilityMode.sync, s.durability);
}

test "vopr scenario: json round-trips through fromJsonSlice" {
    var buf: [4096]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const original = Scenario.smallRing(321);
    try original.writeJson(&w);
    const loaded = try Scenario.fromJsonSlice(testing.allocator, w.buffered());
    try testing.expectEqual(original, loaded);
}

test "vopr scenario: pin parsing rejects bad values instead of guessing" {
    const bad_enum =
        \\{"seed":1,"node_count":3,"log_capacity":65536,"small_ring":false,"ticks_safety":1,"ticks_convergence":1,"hard_state":"persisted","durability":"async","flush_interval_ms":1,"msg_delay_min_ms":1,"msg_delay_max_ms":1,"drop_percent":0,"duplicate_percent":0,"partition_permille":0,"partition_min_ms":1,"partition_max_ms":1,"crash_permille":0,"restart_permille":1,"request_percent":1,"payload_min":16,"payload_max":64,"election_timeout_min_ms":1,"election_timeout_max_ms":2,"heartbeat_interval_ms":1,"rpc_timeout_ms":1}
    ;
    try testing.expectError(error.InvalidField, Scenario.fromJsonSlice(testing.allocator, bad_enum));
    const out_of_range =
        \\{"seed":1,"node_count":3,"log_capacity":65536,"small_ring":false,"ticks_safety":1,"ticks_convergence":1,"hard_state":"persisted","durability":"sync","flush_interval_ms":1,"msg_delay_min_ms":1,"msg_delay_max_ms":1,"drop_percent":300,"duplicate_percent":0,"partition_permille":0,"partition_min_ms":1,"partition_max_ms":1,"crash_permille":0,"restart_permille":1,"request_percent":1,"payload_min":16,"payload_max":64,"election_timeout_min_ms":1,"election_timeout_max_ms":2,"heartbeat_interval_ms":1,"rpc_timeout_ms":1}
    ;
    try testing.expectError(error.InvalidField, Scenario.fromJsonSlice(testing.allocator, out_of_range));
}

test "vopr scenario: json emit parses and fields pair correctly" {
    var buf: [2048]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    const s = Scenario.smallRing(99);
    try s.writeJson(&w);
    const out = w.buffered();

    // Parse for real — a substring check would pass with swapped
    // format/argument pairing or invalid JSON.
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, out, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try testing.expectEqual(@as(usize, 25), obj.count());
    try testing.expectEqual(@as(i64, 99), obj.get("seed").?.integer);
    try testing.expectEqual(@as(i64, @intCast(s.log_capacity)), obj.get("log_capacity").?.integer);
    try testing.expect(obj.get("small_ring").?.bool);
    try testing.expectEqualStrings("persisted", obj.get("hard_state").?.string);
    try testing.expectEqual(@as(i64, @intCast(s.partition_min_ms)), obj.get("partition_min_ms").?.integer);
    try testing.expectEqual(@as(i64, @intCast(s.partition_max_ms)), obj.get("partition_max_ms").?.integer);
    try testing.expectEqual(@as(i64, @intCast(s.rpc_timeout_ms)), obj.get("rpc_timeout_ms").?.integer);
}
