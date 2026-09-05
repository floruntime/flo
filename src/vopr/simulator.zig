//! VOPR Simulator — a deterministic cluster of real RaftNodes under
//! fault injection and invariant checking
//!
//! The unit under test is the production `RaftNode`, driven through its
//! public API plus the seams a wired runtime must own anyway: per-peer
//! replication state (`peers[]`), the election deadline, hard-state
//! restore on restart, and `last_applied`. The harness supplies what the
//! runtime doesn't have yet — a replication pump, timer arming, stable
//! storage — as reference implementations.
//!
//! One run is two-phase: a safety phase under full fault injection, then
//! a liveness phase where a random quorum-sized "core" is healed, its
//! faults frozen, and every non-core node permanently isolated — bugs
//! that random healing would mask must now surface as convergence
//! failures. The seed is the whole repro.

const std = @import("std");
const stdx = @import("stdx");
const PRNG = @import("stdx").PRNG;
const raft_node = @import("../raft/node.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const scenario_mod = @import("scenario.zig");
const workload_mod = @import("workload.zig");
const network_mod = @import("network.zig");

const Allocator = std.mem.Allocator;
const RaftNode = raft_node.RaftNode;
const Scenario = scenario_mod.Scenario;
const Workload = workload_mod.Workload;
const SimNetwork = network_mod.SimNetwork;
const Message = network_mod.Message;
const Body = network_mod.Body;
const OwnedEntry = network_mod.OwnedEntry;
const NodeId = network_mod.NodeId;

pub const MAX_NODES = network_mod.MAX_NODES;
const MAX_PEERS = raft_node.MAX_PEERS;
const MAX_BATCH = scenario_mod.MAX_BATCH;
const HASH_SEED: u64 = 0x5EED;

// ═══════════════════════════════════════════════════════════════════════════════
// Violations
// ═══════════════════════════════════════════════════════════════════════════════

pub const Invariant = enum {
    election_safety,
    state_machine_safety,
    leader_completeness,
    durability,
    term_monotonicity,
    applied_integrity,
    convergence,
    api_error, // error return from a public API on protocol-legal input
};

pub const Violation = struct {
    invariant: Invariant,
    node: NodeId,
    index: u64,
    tick: u64,
    detail: []const u8, // static string

    pub fn format(self: Violation, writer: anytype) !void {
        try writer.print("[{s}] node={d} index={d} tick={d}: {s}", .{
            @tagName(self.invariant), self.node, self.index, self.tick, self.detail,
        });
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// SimDisk — the stable-storage model
// ═══════════════════════════════════════════════════════════════════════════════

/// What a correct node persists. Log appends arrive via the UAL
/// `on_append` hook; hard state is synced once per tick, before fault
/// injection, which is equivalent to persist-before-respond because
/// crashes only happen at the fault stage.
const SimDisk = struct {
    allocator: Allocator,
    term: u64 = 0,
    voted_for: u32 = 0,
    entries: std.ArrayListUnmanaged(DiskEntry) = .empty,
    /// entries[0..durable_len] survive a crash.
    durable_len: usize = 0,
    last_flush: u64 = 0,

    const DiskEntry = struct {
        entry_type: u8,
        flags: u16,
        term: u64,
        timestamp_ns: u64,
        payload: []u8,
    };

    fn deinit(self: *SimDisk) void {
        for (self.entries.items) |e| self.allocator.free(e.payload);
        self.entries.deinit(self.allocator);
    }

    /// UAL fires this on every log append. An append at an index at or
    /// below the tip is an implicit truncate-to-(index-1): `truncateAfter`
    /// bypasses UAL and fires no hook, so truncation must be inferred —
    /// and it clamps durability, because the truncated suffix is gone
    /// from the log a correct disk would persist.
    fn onAppend(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        const idx = entry.header.index;
        if (idx <= self.entries.items.len) {
            var i = self.entries.items.len;
            while (i >= idx) : (i -= 1) {
                self.allocator.free(self.entries.items[i - 1].payload);
            }
            self.entries.shrinkRetainingCapacity(idx - 1);
            self.durable_len = @min(self.durable_len, idx - 1);
        }
        const payload = self.allocator.dupe(u8, entry.payload) catch @panic("sim disk OOM");
        self.entries.append(self.allocator, .{
            .entry_type = entry.header.entry_type,
            .flags = entry.header.flags,
            .term = entry.header.term,
            .timestamp_ns = entry.header.timestamp_ns,
            .payload = payload,
        }) catch @panic("sim disk OOM");
    }

    fn crash(self: *SimDisk) void {
        var i = self.entries.items.len;
        while (i > self.durable_len) : (i -= 1) {
            self.allocator.free(self.entries.items[i - 1].payload);
        }
        self.entries.shrinkRetainingCapacity(self.durable_len);
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// SimNode
// ═══════════════════════════════════════════════════════════════════════════════

const SimNode = struct {
    id: NodeId,
    up: bool,
    raft: RaftNode,
    disk: SimDisk,
    /// Highest term this node has ever held — restarting below it is the
    /// term-monotonicity violation (fires only in volatile mode).
    max_term_seen: u64,
    // Pump state, parallel to raft.peer_ids.
    sent_at: [MAX_PEERS]u64,
    last_heartbeat: [MAX_PEERS]u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Checker
// ═══════════════════════════════════════════════════════════════════════════════

const Canon = struct { term: u64, hash: u64, op_id: ?u64, recorded_at: u64 };

const Checker = struct {
    allocator: Allocator,
    /// Leader completeness assumes committed entries survive on a quorum.
    /// Under async_flush a crashed quorum legally loses its unflushed
    /// suffix (the mode's documented contract, the same grace the final
    /// durability check gets). Asserted until the first async-mode crash
    /// makes erasure possible; sync mode asserts it throughout.
    check_completeness: bool = true,
    /// Under async_flush a cluster-wide crash inside the flush window can
    /// legally erase applied history, and a newer term rewrites those
    /// indices — cross-term canonical immutability doesn't hold. What
    /// always holds is same-term immutability: one leader wrote that
    /// index exactly once. Sync mode asserts full immutability, and even
    /// async mode only permits a rewrite when a crash actually happened
    /// after the canonical entry was recorded — without one, nothing
    /// could have legally erased it. A legal rewrite can carry an OLDER
    /// term, not just a newer one: when the newer quorum loses its
    /// unflushed entries, a survivor's older flushed suffix can win the
    /// next election and be re-committed at the same indices.
    allow_cross_term_rewrite: bool = false,
    /// canonical[i] is the first-applied (term, payload-hash) at index i+1.
    /// First applier sets it; every later applier must match. A mismatch
    /// is a violation whichever side is "right" — two nodes applied
    /// different entries at one index.
    canonical: std.ArrayListUnmanaged(?Canon) = .empty,
    /// term → leader for election safety, checked on the become-leader
    /// event (a per-tick sweep misses a leader elected and deposed within
    /// one tick's message processing).
    leaders: std.AutoHashMapUnmanaged(u64, NodeId) = .empty,
    violations: std.ArrayListUnmanaged(Violation) = .empty,

    fn deinit(self: *Checker) void {
        self.canonical.deinit(self.allocator);
        self.leaders.deinit(self.allocator);
        self.violations.deinit(self.allocator);
    }

    fn fail(self: *Checker, v: Violation) void {
        self.violations.append(self.allocator, v) catch @panic("checker OOM");
    }

    fn onLeader(self: *Checker, node: *const SimNode, tick: u64) void {
        const term = node.raft.current_term;
        const prev = self.leaders.get(term);
        if (prev) |p| {
            if (p != node.id) self.fail(.{
                .invariant = .election_safety,
                .node = node.id,
                .index = term,
                .tick = tick,
                .detail = "second leader elected in the same term",
            });
        } else {
            self.leaders.put(self.allocator, term, node.id) catch @panic("checker OOM");
        }
        // Leader completeness: the new leader's log must contain every
        // canonically committed entry, at the committed term.
        if (!self.check_completeness) return;
        for (self.canonical.items, 1..) |maybe, idx| {
            const canon = maybe orelse continue;
            const t = node.raft.log.entryTerm(idx) orelse {
                self.fail(.{
                    .invariant = .leader_completeness,
                    .node = node.id,
                    .index = idx,
                    .tick = tick,
                    .detail = "new leader's log is missing a committed entry",
                });
                continue;
            };
            if (t != canon.term) self.fail(.{
                .invariant = .leader_completeness,
                .node = node.id,
                .index = idx,
                .tick = tick,
                .detail = "new leader holds a different term at a committed index",
            });
        }
    }

    fn onApply(
        self: *Checker,
        workload: *const Workload,
        node: NodeId,
        idx: u64,
        term: u64,
        payload: []const u8,
        tick: u64,
        latest_crash: u64,
    ) void {
        const hash = std.hash.Wyhash.hash(HASH_SEED, payload);
        const op_id = Workload.opIdFromPayload(payload);
        while (self.canonical.items.len < idx) {
            self.canonical.append(self.allocator, null) catch @panic("checker OOM");
        }
        if (self.canonical.items[idx - 1]) |canon| {
            if (canon.term == term and canon.hash != hash) {
                self.fail(.{
                    .invariant = .state_machine_safety,
                    .node = node,
                    .index = idx,
                    .tick = tick,
                    .detail = "two different entries applied at one index in the same term",
                });
            } else if (canon.term != term) {
                const erasable = self.allow_cross_term_rewrite and latest_crash >= canon.recorded_at;
                if (erasable) {
                    // History at this index was legally erased; whatever
                    // is now being applied is the surviving version.
                    self.canonical.items[idx - 1] = .{ .term = term, .hash = hash, .op_id = op_id, .recorded_at = tick };
                } else {
                    self.fail(.{
                        .invariant = .state_machine_safety,
                        .node = node,
                        .index = idx,
                        .tick = tick,
                        .detail = "applied entry differs from the canonical entry at this index",
                    });
                }
            }
        } else {
            self.canonical.items[idx - 1] = .{ .term = term, .hash = hash, .op_id = op_id, .recorded_at = tick };
        }
        // Applied-entry integrity: a workload payload must hash to what
        // the oracle recorded when it was synthesized.
        if (op_id) |id| {
            if (id >= workload.ops.items.len or workload.ops.items[id].payload_hash != hash) {
                self.fail(.{
                    .invariant = .applied_integrity,
                    .node = node,
                    .index = idx,
                    .tick = tick,
                    .detail = "applied payload does not match its synthesized content",
                });
            }
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Simulator
// ═══════════════════════════════════════════════════════════════════════════════

pub const Options = struct {
    /// Model production's unpersisted hard state instead of the spec.
    volatile_hard_state: bool = false,
    /// Disable the harness's timer re-arming on valid AppendEntries and
    /// vote grants — demonstrates the livelock the in-node reset gap
    /// causes. Timers are still armed once at start/restart.
    no_timer_reset: bool = false,
    /// Print per-phase progress.
    verbose: bool = false,
    /// Emit `progress tick=N` every N ticks (0 = never) — a swarm parent
    /// uses the last such line to tell a slow child from a hung one.
    progress_every: u64 = 0,
};

pub const Summary = struct {
    seed: u64,
    ok: bool,
    ticks: u64,
    ops_submitted: u64,
    ops_acked: u64,
    ops_lost: u64,
    max_committed: u64,
    elections_won: u64,
    crashes: u64,
    restarts: u64,
    messages_delivered: u64,
    messages_dropped: u64,
    apply_stalls: u64,
    eviction_stalls: u64,
    /// Wyhash over the canonical history — two runs of one seed must agree.
    canonical_hash: u64,
    violation_count: usize,
};

pub const Simulator = struct {
    allocator: Allocator,
    scenario: Scenario,
    options: Options,
    prng: PRNG,
    nodes: []SimNode,
    net: SimNetwork,
    workload: Workload,
    checker: Checker,
    now: u64,
    phase: enum { safety, convergence },
    core: [MAX_NODES]bool,
    pending_ops: std.ArrayListUnmanaged(u64),
    acked_at: std.AutoHashMapUnmanaged(u64, u64),
    /// Every crash tick, in order — the async_flush durability grace must
    /// ask "did any crash land inside this op's flush window", not
    /// "did the last crash".
    crash_ticks: std.ArrayListUnmanaged(u64),
    /// The erasability gate for cross-term canonical rewrites: a rewrite
    /// is legal only if some crash happened after the entry was recorded.
    latest_crash_at: u64,
    /// Wall-clock at init, for progress lines only.
    started_ms: i64,
    /// Highest log index of any acked op — the convergence target.
    /// Tracked incrementally; recomputing it from all ops every tick is
    /// O(ops x ticks) and dominates the whole run.
    max_acked_index: u64,
    // Scratch, allocated once.
    range_entries: []entry_mod.Entry,
    range_arena: []u8,
    apply_buf: []u8,
    delivery: std.ArrayListUnmanaged(Message),

    crashes: u64 = 0,
    restarts: u64 = 0,
    elections_won: u64 = 0,
    apply_stalls: u64 = 0,
    eviction_stalls: u64 = 0,

    const RAFT_GROUP: u32 = 1;

    pub fn init(allocator: Allocator, scenario: Scenario, options: Options) !Simulator {
        std.debug.assert(scenario.node_count >= 3 and scenario.node_count <= MAX_NODES);
        var self = Simulator{
            .allocator = allocator,
            .scenario = scenario,
            .options = options,
            .prng = PRNG.init(scenario.seed ^ 0x51D0_2A7E),
            .nodes = try allocator.alloc(SimNode, scenario.node_count),
            .net = SimNetwork.init(allocator),
            .workload = try Workload.init(allocator, &scenario),
            .checker = .{
                .allocator = allocator,
                .allow_cross_term_rewrite = scenario.durability == .async_flush,
            },
            .now = 0,
            .phase = .safety,
            .core = @splat(false),
            .pending_ops = .empty,
            .acked_at = .empty,
            .crash_ticks = .empty,
            .latest_crash_at = 0,
            .started_ms = stdx.time.milliTimestamp(),
            .max_acked_index = 0,
            .range_entries = try allocator.alloc(entry_mod.Entry, MAX_BATCH),
            .range_arena = try allocator.alloc(u8, @as(usize, scenario.payload_max) * MAX_BATCH + 64),
            .apply_buf = try allocator.alloc(u8, @as(usize, scenario.payload_max) + 64),
            .delivery = .empty,
        };
        for (self.nodes, 0..) |*node, i| {
            const id: NodeId = @intCast(i + 1);
            node.* = .{
                .id = id,
                .up = true,
                .raft = try RaftNode.init(allocator, id, RAFT_GROUP, scenario.log_capacity, self.raftConfig()),
                .disk = .{ .allocator = allocator },
                .max_term_seen = 0,
                .sent_at = @splat(0),
                .last_heartbeat = @splat(0),
            };
            for (self.nodes, 0..) |_, j| {
                if (i != j) node.raft.addPeer(@intCast(j + 1));
            }
        }
        // Hooks attach after (empty) replay, mirroring production wiring.
        for (self.nodes) |*node| self.attachDisk(node);
        for (self.nodes) |*node| self.armTimer(node);
        return self;
    }

    pub fn deinit(self: *Simulator) void {
        for (self.nodes) |*node| {
            node.raft.deinit();
            node.disk.deinit();
        }
        self.allocator.free(self.nodes);
        self.net.deinit();
        self.workload.deinit();
        self.checker.deinit();
        self.pending_ops.deinit(self.allocator);
        self.acked_at.deinit(self.allocator);
        self.crash_ticks.deinit(self.allocator);
        self.allocator.free(self.range_entries);
        self.allocator.free(self.range_arena);
        self.allocator.free(self.apply_buf);
        for (self.delivery.items) |*m| self.net.release(m);
        self.delivery.deinit(self.allocator);
    }

    fn raftConfig(self: *const Simulator) raft_node.Config {
        return .{
            .election_timeout_min_ms = self.scenario.election_timeout_min_ms,
            .election_timeout_max_ms = self.scenario.election_timeout_max_ms,
            .heartbeat_interval_ms = self.scenario.heartbeat_interval_ms,
            .max_entries_per_batch = MAX_BATCH,
            .enable_pre_vote = false,
        };
    }

    fn attachDisk(self: *Simulator, node: *SimNode) void {
        _ = self;
        node.raft.log.ual.on_append_ctx = @ptrCast(&node.disk);
        node.raft.log.ual.on_append = SimDisk.onAppend;
    }

    /// Timers are harness-owned: RaftNode never arms its own (a fresh
    /// deadline of 0 means disabled), so a wired runtime must do this too.
    /// Jitter comes from the run PRNG, not the node's deterministic
    /// per-id formula, so identical-log candidates don't interleave
    /// adversarially for many terms.
    fn armTimer(self: *Simulator, node: *SimNode) void {
        const r = self.prng.random();
        const jitter = r.intRangeAtMost(
            u64,
            self.scenario.election_timeout_min_ms,
            self.scenario.election_timeout_max_ms,
        );
        node.raft.election_deadline_ms = self.now + jitter;
        node.raft.current_time_ms = self.now;
    }

    fn node_(self: *Simulator, id: NodeId) *SimNode {
        return &self.nodes[id - 1];
    }

    fn peerSlot(node: *const SimNode, peer: NodeId) ?usize {
        for (0..node.raft.peer_count) |i| {
            if (node.raft.peer_ids[i] == peer) return i;
        }
        return null;
    }

    // ── Fault injection ────────────────────────────────────────────────

    fn crashNode(self: *Simulator, node: *SimNode) void {
        node.up = false;
        node.disk.crash();
        self.crashes += 1;
        self.latest_crash_at = self.now;
        self.crash_ticks.append(self.allocator, self.now) catch @panic("sim OOM");
        // From here on an async-mode quorum can legally lose committed
        // entries, so leader completeness stops being assertable.
        if (self.scenario.durability == .async_flush) self.checker.check_completeness = false;
    }

    fn restartNode(self: *Simulator, node: *SimNode) !void {
        const prev_term = node.max_term_seen;
        node.raft.deinit();
        node.raft = try RaftNode.init(
            self.allocator,
            node.id,
            RAFT_GROUP,
            self.scenario.log_capacity,
            self.raftConfig(),
        );
        for (self.nodes, 0..) |_, j| {
            if (j + 1 != node.id) node.raft.addPeer(@intCast(j + 1));
        }
        if (self.options.volatile_hard_state) {
            node.disk.term = 0;
            node.disk.voted_for = 0;
        }
        // Replay the durable log before attaching the hook, exactly as
        // production wires persistence after segment replay — a hook
        // active during replay would re-feed the disk.
        for (node.disk.entries.items[0..node.disk.durable_len], 1..) |e, idx| {
            var entry = entry_mod.buildEntry(
                @enumFromInt(e.entry_type),
                e.flags,
                e.term,
                @intCast(idx),
                e.timestamp_ns,
                e.payload,
            );
            entry.header.crc32c = entry.computeCrc();
            _ = node.raft.log.append(&entry) catch @panic("sim replay append");
        }
        node.raft.current_term = node.disk.term;
        node.raft.voted_for = node.disk.voted_for;
        self.attachDisk(node);
        node.up = true;
        node.sent_at = @splat(0);
        node.last_heartbeat = @splat(0);
        self.armTimer(node);
        self.restarts += 1;
        if (node.raft.current_term < prev_term) {
            self.checker.fail(.{
                .invariant = .term_monotonicity,
                .node = node.id,
                .index = node.raft.current_term,
                .tick = self.now,
                .detail = "node restarted below its highest seen term",
            });
        }
    }

    fn injectFaults(self: *Simulator) !void {
        if (self.phase == .convergence) return;
        const r = self.prng.random();
        for (self.nodes) |*node| {
            if (node.up) {
                if (r.uintLessThan(u16, 1000) < self.scenario.crash_permille) {
                    self.crashNode(node);
                }
            } else {
                if (r.uintLessThan(u16, 1000) < self.scenario.restart_permille) {
                    try self.restartNode(node);
                }
            }
        }
        self.net.maybeHeal(self.now);
        if (!self.net.partition_active and
            r.uintLessThan(u16, 1000) < self.scenario.partition_permille)
        {
            const duration = r.intRangeAtMost(
                u64,
                self.scenario.partition_min_ms,
                self.scenario.partition_max_ms,
            );
            self.net.startPartition(&self.prng, self.scenario.node_count, self.now + duration);
        }
    }

    // ── Message handling ───────────────────────────────────────────────

    fn deliverAll(self: *Simulator) !void {
        self.delivery.clearRetainingCapacity();
        try self.net.deliverDue(self.now, &self.delivery);
        // Drain from the front so a message is out of the list before it
        // can error — anything still listed on an error path is released
        // exactly once, by deinit.
        while (self.delivery.items.len > 0) {
            var msg = self.delivery.orderedRemove(0);
            defer self.net.release(&msg);
            const node = self.node_(msg.to);
            if (!node.up) continue;
            try self.handleMessage(node, &msg);
        }
    }

    fn handleMessage(self: *Simulator, node: *SimNode, msg: *const Message) !void {
        switch (msg.body) {
            .vote_req => |req| {
                const resp = node.raft.handleVoteRequest(req);
                if (resp.vote_granted and !self.options.no_timer_reset) self.armTimer(node);
                try self.net.send(&self.prng, &self.scenario, self.now, node.id, msg.from, .{ .vote_resp = resp });
            },
            .vote_resp => |resp| {
                if (node.raft.handleVoteResponse(resp)) {
                    self.elections_won += 1;
                    self.checker.onLeader(node, self.now);
                    // Send first heartbeats immediately.
                    node.last_heartbeat = @splat(0);
                    node.sent_at = @splat(0);
                }
            },
            .append_req => |req| {
                var entries: [MAX_BATCH]entry_mod.Entry = undefined;
                for (req.entries, 0..) |e, i| {
                    entries[i] = entry_mod.buildEntry(
                        @enumFromInt(e.entry_type),
                        e.flags,
                        e.term,
                        e.index,
                        e.timestamp_ns,
                        e.payload,
                    );
                    entries[i].header.crc32c = entries[i].computeCrc();
                }
                const result = node.raft.handleAppendEntries(.{
                    .term = req.term,
                    .leader_id = req.leader_id,
                    .prev_log_index = req.prev_log_index,
                    .prev_log_term = req.prev_log_term,
                    .entries = entries[0..req.entries.len],
                    .leader_commit = req.leader_commit,
                }) catch {
                    // Protocol-legal input must never error out of the
                    // public API — this is a finding, not a drop.
                    self.checker.fail(.{
                        .invariant = .api_error,
                        .node = node.id,
                        .index = req.prev_log_index + 1,
                        .tick = self.now,
                        .detail = "handleAppendEntries returned an error on legal input",
                    });
                    return;
                };
                // Any append from a legitimate current-term leader proves
                // the leader is alive — success or not. Re-arming only on
                // success livelocks a log-mismatched follower: it deposes
                // its leader every timeout while the one-step next_index
                // walk restarts from scratch.
                if (result.term == req.term and !self.options.no_timer_reset) self.armTimer(node);
                try self.net.send(&self.prng, &self.scenario, self.now, node.id, msg.from, .{ .append_resp = result });
            },
            .append_resp => |resp| {
                node.raft.handleAppendResponse(resp);
            },
        }
    }

    // ── Node tick: elections + replication pump ────────────────────────

    fn tickNodes(self: *Simulator) !void {
        for (self.nodes) |*node| {
            if (!node.up) continue;
            const result = node.raft.tick(self.now);
            if (result.start_election) {
                const req = node.raft.startElection();
                node.max_term_seen = @max(node.max_term_seen, node.raft.current_term);
                self.armTimer(node);
                for (0..node.raft.peer_count) |i| {
                    try self.net.send(&self.prng, &self.scenario, self.now, node.id, node.raft.peer_ids[i], .{ .vote_req = req });
                }
            }
            if (node.raft.role == .leader) try self.pump(node);
            node.max_term_seen = @max(node.max_term_seen, node.raft.current_term);
        }
    }

    /// The replication pump the production runtime is missing: heartbeat
    /// on interval, batched entries when a peer is behind, inflight
    /// tracking with a resend timeout (a dropped response would otherwise
    /// wedge `PeerState.inflight` forever).
    fn pump(self: *Simulator, node: *SimNode) !void {
        const last = node.raft.log.lastIndex();
        for (0..node.raft.peer_count) |i| {
            const peer_id = node.raft.peer_ids[i];
            // A stale success ack can outlive its leadership stint:
            // handleAppendResponse takes any success response at face
            // value, so an ack delayed across this leader's
            // crash-restart or conflict truncation (which shrank its
            // log) can push next_index
            // past the tip — clamp, or prev_log below is unbuildable.
            if (node.raft.peers[i].next_index > last + 1) {
                node.raft.peers[i].next_index = last + 1;
            }
            const next = node.raft.peers[i].next_index;
            const behind = next <= last;
            const inflight_timeout = self.now -| node.sent_at[i] >= self.scenario.rpc_timeout_ms;
            const want_data = behind and (!node.raft.peers[i].inflight or inflight_timeout);
            const want_heartbeat = self.now -| node.last_heartbeat[i] >= self.scenario.heartbeat_interval_ms;
            if (!want_data and !want_heartbeat) continue;

            const prev_index = next - 1;
            const prev_term = node.raft.log.entryTerm(prev_index) orelse blk: {
                if (prev_index == 0) break :blk @as(u64, 0);
                // The entry a repair needs is gone from the ring and there
                // is no snapshot/catch-up path — the stall the --small-ring
                // scenario exists to expose. Surfaces as convergence failure.
                self.apply_stalls += 1;
                continue;
            };

            var entries: []OwnedEntry = &.{};
            if (want_data) {
                var count = node.raft.log.getRange(next, self.range_entries, self.range_arena);
                if (count == 0) {
                    // The range a repair needs is evicted from the ring —
                    // without a catch-up path the peer wedges silently
                    // (term lookups still succeed via the term cache, so
                    // the prev_term stall above never fires). Count it,
                    // and outside the --small-ring scenario fall back to
                    // the harness's stable storage — the same
                    // reference-implementation role as the pump itself,
                    // standing in for the snapshot/catch-up path the
                    // runtime doesn't have. --small-ring keeps the wedge
                    // observable as the acceptance test for that work.
                    self.eviction_stalls += 1;
                    if (!self.scenario.small_ring) {
                        const disk = node.disk.entries.items;
                        var k: usize = 0;
                        var arena_used: usize = 0;
                        while (k < MAX_BATCH and next - 1 + k < disk.len) : (k += 1) {
                            const de = disk[next - 1 + k];
                            if (arena_used + de.payload.len > self.range_arena.len) break;
                            @memcpy(self.range_arena[arena_used..][0..de.payload.len], de.payload);
                            self.range_entries[k] = entry_mod.buildEntry(
                                @enumFromInt(de.entry_type),
                                de.flags,
                                de.term,
                                next + k,
                                de.timestamp_ns,
                                self.range_arena[arena_used..][0..de.payload.len],
                            );
                            arena_used += de.payload.len;
                        }
                        count = k;
                    }
                }
                if (count > 0) {
                    const owned = try self.allocator.alloc(OwnedEntry, count);
                    var built: usize = 0;
                    errdefer {
                        for (owned[0..built]) |e| self.allocator.free(e.payload);
                        self.allocator.free(owned);
                    }
                    for (self.range_entries[0..count], 0..) |e, k| {
                        owned[k] = .{
                            .entry_type = e.header.entry_type,
                            .flags = e.header.flags,
                            .term = e.header.term,
                            .index = e.header.index,
                            .timestamp_ns = e.header.timestamp_ns,
                            .payload = try self.allocator.dupe(u8, e.payload),
                        };
                        built += 1;
                    }
                    entries = owned;
                    node.raft.peers[i].inflight = true;
                    node.sent_at[i] = self.now;
                }
            }
            node.last_heartbeat[i] = self.now;
            try self.net.send(&self.prng, &self.scenario, self.now, node.id, peer_id, .{ .append_req = .{
                .term = node.raft.current_term,
                .leader_id = node.id,
                .prev_log_index = prev_index,
                .prev_log_term = prev_term,
                .leader_commit = node.raft.commit_index,
                .entries = entries,
            } });
        }
    }

    // ── Workload ───────────────────────────────────────────────────────

    fn submitOps(self: *Simulator) !void {
        const r = self.prng.random();
        const chance: u8 = if (self.phase == .safety)
            self.scenario.request_percent
        else
            // Convergence probes: low-rate traffic keeps commit advancing —
            // a new leader cannot commit prior-term entries without fresh
            // proposals (becomeLeader appends no noop).
            5;
        if (r.uintLessThan(u8, 100) >= chance) return;

        // Submit to whichever node believes it is leader; several may,
        // across a partition — the client picking a stale leader is the
        // dangerous case the oracle must handle, not avoid.
        var leaders: [MAX_NODES]NodeId = undefined;
        var n: usize = 0;
        for (self.nodes) |*node| {
            if (node.up and node.raft.role == .leader and !self.isIsolated(node.id)) {
                leaders[n] = node.id;
                n += 1;
            }
        }
        if (n == 0) return;
        const target = self.node_(leaders[r.uintLessThan(usize, n)]);
        const op = try self.workload.nextOp();
        const res = target.raft.propose(op.entry_type, 0, 0, op.payload) catch return;
        self.workload.recordProposal(op.id, target.id, res.term, res.index);
        try self.pending_ops.append(self.allocator, op.id);
    }

    fn isIsolated(self: *const Simulator, id: NodeId) bool {
        return self.net.isolated[id - 1];
    }

    fn ackSweep(self: *Simulator) !void {
        var i: usize = 0;
        while (i < self.pending_ops.items.len) {
            const op_id = self.pending_ops.items[i];
            const op = self.workload.ops.items[op_id];
            const proposer = self.node_(op.proposer);
            if (!proposer.up) {
                i += 1;
                continue;
            }
            const t = proposer.raft.log.entryTerm(op.index);
            if (t != null and t.? != op.term) {
                // A conflicting term overwrote the slot before commit.
                self.workload.markLost(op_id);
                _ = self.pending_ops.swapRemove(i);
                continue;
            }
            if (t != null and proposer.raft.commit_index >= op.index) {
                self.workload.ack(op_id);
                try self.acked_at.put(self.allocator, op_id, self.now);
                self.max_acked_index = @max(self.max_acked_index, op.index);
                _ = self.pending_ops.swapRemove(i);
                continue;
            }
            i += 1;
        }
    }

    // ── Apply loop ─────────────────────────────────────────────────────

    fn applyLoop(self: *Simulator) void {
        for (self.nodes) |*node| {
            if (!node.up) continue;
            while (node.raft.last_applied < node.raft.commit_index) {
                const idx = node.raft.last_applied + 1;
                // Never bare getEntry (the wrap-null trap), and never
                // ring-only: a restarted node's durable log can exceed its
                // ring, so committed-but-evicted entries fall back to disk.
                var term: u64 = undefined;
                var payload: []const u8 = undefined;
                if (node.raft.log.getEntryCopy(idx, self.apply_buf)) |e| {
                    term = e.header.term;
                    payload = e.payload;
                } else if (idx <= node.disk.entries.items.len) {
                    const de = node.disk.entries.items[idx - 1];
                    term = de.term;
                    payload = de.payload;
                } else {
                    self.apply_stalls += 1;
                    break;
                }
                self.checker.onApply(&self.workload, node.id, idx, term, payload, self.now, self.latest_crash_at);
                node.raft.last_applied = idx;
            }
        }
    }

    // ── Durability + hard-state sync (runs before fault injection) ─────

    fn syncStableStorage(self: *Simulator) void {
        for (self.nodes) |*node| {
            if (!node.up) continue;
            node.disk.term = node.raft.current_term;
            node.disk.voted_for = node.raft.voted_for;
            switch (self.scenario.durability) {
                .sync => node.disk.durable_len = node.disk.entries.items.len,
                .async_flush => {
                    if (self.now - node.disk.last_flush >= self.scenario.flush_interval_ms) {
                        node.disk.durable_len = node.disk.entries.items.len;
                        node.disk.last_flush = self.now;
                    }
                },
            }
        }
    }

    // ── Phases ─────────────────────────────────────────────────────────

    fn transitionToConvergence(self: *Simulator) !void {
        self.phase = .convergence;
        const r = self.prng.random();
        const n = self.scenario.node_count;
        const quorum = n / 2 + 1;
        // Random quorum-sized core.
        var chosen: u8 = 0;
        while (chosen < quorum) {
            const pick = r.uintLessThan(u8, n);
            if (!self.core[pick]) {
                self.core[pick] = true;
                chosen += 1;
            }
        }
        self.net.healAll();
        self.net.core_healed = true;
        self.net.core = self.core;
        for (self.nodes, 0..) |*node, i| {
            if (self.core[i]) {
                if (!node.up) try self.restartNode(node);
            } else {
                // Permanent isolation, not frozen fault rates: a live
                // non-core node with a crash-loop-inflated term would
                // depose the core leader through every leaked vote
                // request (no pre-vote, no check-quorum) and turn
                // scenario noise into false liveness failures.
                self.net.isolate(node.id);
            }
        }
        if (self.options.verbose) {
            std.debug.print("[vopr] convergence: core =", .{});
            for (self.nodes, 0..) |_, i| {
                if (self.core[i]) std.debug.print(" {d}", .{i + 1});
            }
            std.debug.print("\n", .{});
        }
    }

    fn converged(self: *Simulator) bool {
        // Every acked op applied by every core node.
        const target = self.max_acked_index;
        for (self.nodes, 0..) |*node, i| {
            if (!self.core[i]) continue;
            if (!node.up) return false;
            if (node.raft.last_applied < target) return false;
        }
        return true;
    }

    /// Durability, checked at the end: every acked op is in canonical
    /// history at its acked (term, index). Mode-aware — under
    /// async_flush, losing ops acked inside the flush window of a crash
    /// is the mode's documented contract, not a finding.
    fn finalDurabilityCheck(self: *Simulator) void {
        for (self.workload.ops.items) |op| {
            if (op.state != .acked) continue;
            if (self.scenario.durability == .async_flush) {
                // Excuse the op if any crash landed within a flush interval
                // on EITHER side of the ack — that loss is the mode's
                // documented contract. Before the ack matters too: the
                // leader acks on replicas that are still unflushed, so a
                // crash shortly before the ack can already have destroyed
                // a replica the ack relied on.
                const at = self.acked_at.get(op.id) orelse 0;
                const flush = self.scenario.flush_interval_ms;
                var excused = false;
                for (self.crash_ticks.items) |ct| {
                    if (ct + flush >= at and ct <= at + flush) {
                        excused = true;
                        break;
                    }
                }
                if (excused) continue;
            }
            const bad = blk: {
                if (op.index == 0 or op.index > self.checker.canonical.items.len) break :blk true;
                const canon = self.checker.canonical.items[op.index - 1] orelse break :blk true;
                break :blk canon.term != op.term or canon.op_id != op.id;
            };
            if (bad) self.checker.fail(.{
                .invariant = .durability,
                .node = op.proposer,
                .index = op.index,
                .tick = self.now,
                .detail = "acked op is missing from canonical history at its acked (term, index)",
            });
        }
    }

    // ── Main loop ──────────────────────────────────────────────────────

    /// Within-tick order is pinned (it defines ack-vs-crash semantics):
    /// deliver → ticks + pump → submit → apply → ack sweep →
    /// stable-storage sync → faults. An op acked in the tick its acker
    /// crashes was acked before the crash — the client has the response;
    /// the guarantee stands.
    fn tick(self: *Simulator) !void {
        self.now += 1;
        try self.deliverAll();
        try self.tickNodes();
        try self.submitOps();
        self.applyLoop();
        try self.ackSweep();
        self.syncStableStorage();
        try self.injectFaults();
    }

    fn progress(self: *const Simulator) void {
        if (self.options.progress_every > 0 and self.now % self.options.progress_every == 0) {
            // Wall-clock only ever reaches a print — never a decision — so
            // determinism is untouched; the swarm parent uses the elapsed
            // time to tell a hung child (silent) from a slow one (advancing).
            std.debug.print("[vopr] progress tick={d} elapsed_ms={d}\n", .{ self.now, stdx.time.milliTimestamp() - self.started_ms });
        }
    }

    pub fn run(self: *Simulator) !Summary {
        while (self.now < self.scenario.ticks_safety and self.checker.violations.items.len == 0) {
            try self.tick();
            self.progress();
        }
        var converged_ok = false;
        if (self.checker.violations.items.len == 0) {
            try self.transitionToConvergence();
            // Budget scales with repair cost: next_index backs off one
            // step per round trip, so a from-zero follower needs round
            // trips linear in the log length, at message latency.
            var max_log: u64 = 0;
            for (self.nodes) |*node| max_log = @max(max_log, node.raft.log.lastIndex());
            const budget = @max(
                self.scenario.ticks_convergence,
                max_log * (self.scenario.msg_delay_max_ms + 2) * 2,
            );
            const deadline = self.now + budget;
            while (self.now < deadline and self.checker.violations.items.len == 0) {
                try self.tick();
                self.progress();
                if (self.options.verbose and self.now % 5000 == 0) {
                    std.debug.print("[dbg] tick={d}\n", .{self.now});
                    for (self.nodes) |*nd| std.debug.print(
                        "  n{d} up={} core={} role={s} term={d} dl={d} commit={d} applied={d} log={d}\n",
                        .{ nd.id, nd.up, self.core[nd.id - 1], @tagName(nd.raft.role), nd.raft.current_term, nd.raft.election_deadline_ms, nd.raft.commit_index, nd.raft.last_applied, nd.raft.log.lastIndex() },
                    );
                }
                if (self.converged()) {
                    converged_ok = true;
                    break;
                }
            }
            if (!converged_ok and self.checker.violations.items.len == 0) {
                self.checker.fail(.{
                    .invariant = .convergence,
                    .node = 0,
                    .index = 0,
                    .tick = self.now,
                    .detail = "core did not apply all acked ops within the budget",
                });
            }
        }
        self.finalDurabilityCheck();
        return self.summary();
    }

    fn summary(self: *Simulator) Summary {
        var max_committed: u64 = 0;
        for (self.nodes) |*node| max_committed = @max(max_committed, node.raft.commit_index);
        var h = std.hash.Wyhash.init(HASH_SEED);
        for (self.checker.canonical.items) |maybe| {
            if (maybe) |c| {
                h.update(std.mem.asBytes(&c.term));
                h.update(std.mem.asBytes(&c.hash));
            } else {
                h.update("hole");
            }
        }
        return .{
            .seed = self.scenario.seed,
            .ok = self.checker.violations.items.len == 0,
            .ticks = self.now,
            .ops_submitted = self.workload.next_op_id,
            .ops_acked = self.workload.acked_count,
            .ops_lost = self.workload.lost_count,
            .max_committed = max_committed,
            .elections_won = self.elections_won,
            .crashes = self.crashes,
            .restarts = self.restarts,
            .messages_delivered = self.net.delivered,
            .messages_dropped = self.net.dropped,
            .apply_stalls = self.apply_stalls,
            .eviction_stalls = self.eviction_stalls,
            .canonical_hash = h.final(),
            .violation_count = self.checker.violations.items.len,
        };
    }

    pub fn printViolations(self: *const Simulator, writer: anytype) !void {
        for (self.checker.violations.items) |v| {
            try writer.print("  ", .{});
            try v.format(writer);
            try writer.print("\n", .{});
        }
    }

    pub fn printNodeStates(self: *const Simulator, writer: anytype) !void {
        for (self.nodes) |*node| {
            try writer.print(
                "  node {d}: {s} {s} term={d} commit={d} applied={d} log={d} durable={d}\n",
                .{
                    node.id,
                    if (node.up) "up" else "down",
                    @tagName(node.raft.role),
                    node.raft.current_term,
                    node.raft.commit_index,
                    node.raft.last_applied,
                    node.raft.log.lastIndex(),
                    node.disk.durable_len,
                },
            );
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "vopr sim: calm scenario elects, replicates, acks and converges" {
    var sim = try Simulator.init(testing.allocator, Scenario.calm(1), .{});
    defer sim.deinit();
    const s = try sim.run();
    if (!s.ok) {
        std.debug.print("calm(1) failed:\n", .{});
        var buf: [4096]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        sim.printViolations(&w) catch {};
        sim.printNodeStates(&w) catch {};
        std.debug.print("{s}\n", .{w.buffered()});
    }
    try testing.expect(s.ok);
    try testing.expectEqual(@as(usize, 0), s.violation_count);
    try testing.expect(s.elections_won >= 1);
    try testing.expect(s.ops_acked > 0);
    try testing.expect(s.max_committed > 0);
}

test "vopr sim: same seed twice produces identical summaries" {
    var a = try Simulator.init(testing.allocator, Scenario.calm(42), .{});
    defer a.deinit();
    var b = try Simulator.init(testing.allocator, Scenario.calm(42), .{});
    defer b.deinit();
    const sa = try a.run();
    const sb = try b.run();
    try testing.expectEqual(sa, sb);
    // Equal counters can hide divergent checker content — compare the
    // violation lists element-wise too.
    try testing.expectEqual(a.checker.violations.items.len, b.checker.violations.items.len);
    for (a.checker.violations.items, b.checker.violations.items) |va, vb| {
        try testing.expectEqual(va.invariant, vb.invariant);
        try testing.expectEqual(va.node, vb.node);
        try testing.expectEqual(va.index, vb.index);
        try testing.expectEqual(va.tick, vb.tick);
    }
}

test "vopr sim: volatile hard state trips term monotonicity on restart" {
    var scenario = Scenario.calm(5);
    // Crashes must be rarer than election timeouts, or no node lives long
    // enough to leave term 0 and there is nothing for monotonicity to
    // violate.
    scenario.crash_permille = 1;
    scenario.restart_permille = 50;
    scenario.ticks_safety = 10_000;
    var sim = try Simulator.init(testing.allocator, scenario, .{ .volatile_hard_state = true });
    defer sim.deinit();
    const s = try sim.run();
    try testing.expect(!s.ok);
    var found = false;
    for (sim.checker.violations.items) |v| {
        if (v.invariant == .term_monotonicity) found = true;
    }
    try testing.expect(found);
}

test "vopr sim: crashes with sync durability still converge" {
    var scenario = Scenario.calm(77);
    scenario.crash_permille = 5;
    scenario.restart_permille = 100;
    scenario.ticks_safety = 6_000;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    const s = try sim.run();
    if (!s.ok) {
        std.debug.print("crash/sync seed 77 failed:\n", .{});
        var buf: [4096]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        sim.printViolations(&w) catch {};
        sim.printNodeStates(&w) catch {};
        std.debug.print("{s}\n", .{w.buffered()});
    }
    try testing.expect(s.ok);
    try testing.expect(s.crashes > 0);
    try testing.expect(s.restarts > 0);
}
