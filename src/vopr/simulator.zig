//! VOPR Simulator — a deterministic cluster of real replicas under fault
//! injection and invariant checking
//!
//! The unit under test is the production replica (`src/replica`) and the
//! `RaftNode` it drives: frames in, the Raft clock, the leader loop, and
//! every frame serialized through the wire codec in both directions. The
//! simulator is each replica's `Io` — its clock, its links, its disk —
//! and its host: what the shard does for it (applying, the leader's own
//! membership changes, stopping when it diverges) is modelled here, with
//! the invariant checks on the way.
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
const repair = @import("../node/repair.zig");
const scenario_mod = @import("scenario.zig");
const workload_mod = @import("workload.zig");
const network_mod = @import("network.zig");
const membership = @import("../raft/membership.zig");
const transport = @import("../raft/transport.zig");
const RaftFrame = @import("../raft/raft_queue.zig").Frame;
const replica_mod = @import("../replica/replica.zig");
const families = @import("families.zig");
const log = @import("stdx").log;

const Allocator = std.mem.Allocator;
const RaftNode = raft_node.RaftNode;
const Scenario = scenario_mod.Scenario;
const Workload = workload_mod.Workload;
const SimNetwork = network_mod.SimNetwork;
const Message = network_mod.Message;
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
    /// A committed config that is not a legal next step from the one
    /// committed before it: two in flight, or voters changed by more than
    /// one.
    config_safety,
    /// A committed entry stamped no later than the one before it: the
    /// leader's stamps must order the log whatever the clocks do.
    stamp_order,
    /// A node found the group's log contradicting history it had applied
    /// and stopped taking part. Only an async-mode crash can erase applied
    /// history; anywhere else it is a safety bug.
    divergence,
    /// Nodes that halted did so at different entries: a halt must come
    /// from the entry alone, the same on every replica.
    halt_agreement,
    /// Two replicas hold different family state after applying the same
    /// history, or a replica rebuilt by replay holds other state than one
    /// that applied those entries live: apply read something besides the
    /// entry and the state.
    state_checksum,
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
/// `on_append` hook; term and vote arrive through the node's own
/// `HardStateSink`, before the node acts on them — the same contract as
/// production's HARDSTATE file. Nothing else writes hard state here.
const SimDisk = struct {
    allocator: Allocator,
    term: u64 = 0,
    voted_for: u32 = 0,
    lost_log: bool = false,
    entries: std.ArrayListUnmanaged(DiskEntry) = .empty,
    /// entries[0..durable_len] survive a crash.
    durable_len: usize = 0,
    last_flush: u64 = 0,
    /// Replication batches the log read from here, below its ring: a run
    /// that never does has not exercised catch-up. Applies read here too
    /// (a restarted node's log outgrows its ring) and are not counted.
    catch_up_reads: u64 = 0,
    applying: bool = false,

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

    /// UAL fires this on every log append. Truncation reaches the disk
    /// through its own hook first, so an append never lands at or below
    /// the tip; one that does is a log bug, not a disk event.
    fn onAppend(ctx: *anyopaque, entry: *const entry_mod.Entry) void {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        const idx = entry.header.index;
        if (idx != self.entries.items.len + 1) @panic("sim disk: append is not at the tip");
        const payload = self.allocator.dupe(u8, entry.payload) catch @panic("sim disk OOM");
        self.entries.append(self.allocator, .{
            .entry_type = entry.header.entry_type,
            .flags = entry.header.flags,
            .term = entry.header.term,
            .timestamp_ns = entry.header.timestamp_ns,
            .payload = payload,
        }) catch @panic("sim disk OOM");
    }

    /// The log truncated a conflicting suffix; the disk drops the same one.
    /// Durability is clamped too: the suffix is gone from the log a correct
    /// disk would persist.
    fn onTruncate(ctx: *anyopaque, after_index: u64) void {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        var i = self.entries.items.len;
        while (i > after_index) : (i -= 1) {
            self.allocator.free(self.entries.items[i - 1].payload);
        }
        self.entries.shrinkRetainingCapacity(@intCast(@min(after_index, self.entries.items.len)));
        self.durable_len = @min(self.durable_len, self.entries.items.len);
    }

    /// The log reads below its ring from here: the production runtime's
    /// writer buffer plus sealed segments, as one array.
    fn readRange(ctx: *anyopaque, start: u64, buf: []entry_mod.Entry, arena: []u8) usize {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        var n: usize = 0;
        var used: usize = 0;
        while (n < buf.len and start - 1 + n < self.entries.items.len) : (n += 1) {
            const de = self.entries.items[start - 1 + n];
            if (used + de.payload.len > arena.len) break;
            @memcpy(arena[used..][0..de.payload.len], de.payload);
            buf[n] = entry_mod.buildEntry(
                @fromBackingInt(@intCast(de.entry_type)),
                de.flags,
                de.term,
                start + n,
                de.timestamp_ns,
                arena[used..][0..de.payload.len],
            );
            used += de.payload.len;
        }
        if (!self.applying) self.catch_up_reads += 1;
        return n;
    }

    /// RaftNode persists through this before granting a vote, adopting a
    /// term, or starting or ending its lost-log guard. The sim disk never
    /// fails a write.
    fn persistHardState(ctx: *anyopaque, term: u64, voted_for: u32, lost_log: bool) bool {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        self.term = term;
        self.voted_for = voted_for;
        self.lost_log = lost_log;
        return true;
    }

    /// Everything the log holds is on disk: a config entry is made durable
    /// before it takes effect, in either durability mode.
    fn flushLog(ctx: *anyopaque) bool {
        const self: *SimDisk = @ptrCast(@alignCast(ctx));
        self.flush();
        return true;
    }

    fn flush(self: *SimDisk) void {
        self.durable_len = self.entries.items.len;
    }

    /// The disk is gone: log and hard state alike, as a replaced volume.
    fn wipe(self: *SimDisk) void {
        for (self.entries.items) |e| self.allocator.free(e.payload);
        self.entries.clearRetainingCapacity();
        self.durable_len = 0;
        self.last_flush = 0;
        self.term = 0;
        self.voted_for = 0;
        self.lost_log = false;
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
    replica: Replica,
    /// The simulator, for the replica's `Io`. Set at the start of every
    /// tick: `Simulator.init` returns by value, so an address taken there
    /// would not survive the move.
    sim: *Simulator = undefined,
    disk: SimDisk,
    /// Highest term this node has ever held — restarting below it is the
    /// term-monotonicity violation (fires only for a node without a
    /// hard-state sink: volatile mode).
    max_term_seen: u64,
    /// Its disk was wiped while down: it restarts as a lost-log node.
    wiped: bool = false,
    /// Diverged where no config could spare it: left as it is.
    beyond_tolerance: bool = false,
    /// Vouches its earlier lives took.
    vouches_before_restart: u64 = 0,
    /// Only its hard state was lost: it restarts with its log, and must
    /// confirm the term before it votes.
    lost_hard_state: bool = false,
    /// A lost-log node the convergence phase waits on to finish its guard.
    guard_watched: bool = false,
    /// Its clock runs at 1 + this / 1e6 of the simulation's.
    clock_rate_ppm: i32 = 0,
    /// Its wall clock's error, and the wall clock its Raft node reads,
    /// set each tick.
    wall_offset_ms: i64 = 0,
    wall_ns: u64 = 0,
    /// The stamp of the last entry it applied, and that entry's index, for
    /// `stamp_order`.
    applied_stamp: u64 = 0,
    applied_stamp_index: u64 = 0,
    /// The last config this node applied, for `config_safety`.
    applied_config: membership.Config = .{},
    /// The entry it halted at, 0 for none: it applies nothing from it on.
    halted_at: u64 = 0,
    /// It took the committed repair that skips the group's halt, and
    /// passes over that entry.
    skipped: bool = false,
    /// The tick it applied each index at (index i at [i - 1]), for judging
    /// whether a crash could have erased what it applied.
    applied_ticks: std.ArrayListUnmanaged(u64) = .empty,
    /// The toy family it applies, and the index it last applied it at.
    counter: families.Counter = .{},
    families_index: u64 = 0,
    /// Its apply rewound without a restart, so the family holds entries
    /// the log no longer does and cannot roll them back: the checker
    /// leaves it out until it restarts.
    families_stale: bool = false,
    /// A hash over every entry it applied in this life, in order: two
    /// replicas' states are compared only after the same history.
    history: u64 = 0,
    /// The highest index it applied before its last restart; applying one
    /// at or below it rebuilds state, above it is live.
    replay_through: u64 = 0,
};

// ═══════════════════════════════════════════════════════════════════════════════
// SimIo — the replica's clock, links, disk and host
// ═══════════════════════════════════════════════════════════════════════════════

const Replica = replica_mod.Replica(SimIo);

const SimIo = struct {
    fn of(r: *Replica) *SimNode {
        return @alignCast(@fieldParentPtr("replica", r));
    }

    pub fn nowMs(r: *Replica) u64 {
        const node = of(r);
        return node.sim.nodeNow(node);
    }

    pub fn networked(_: *Replica) bool {
        return true;
    }

    pub fn send(r: *Replica, peer: u32, msg_type: transport.MsgType, group: u32, payload: []const u8) bool {
        _ = group;
        const node = of(r);
        const sim = node.sim;
        sim.net.send(&sim.prng, &sim.scenario, sim.now, node.id, @intCast(peer), msg_type, payload) catch @panic("sim OOM");
        return true;
    }

    pub fn durableCommits(r: *Replica) bool {
        return of(r).sim.scenario.durability == .sync;
    }

    /// Asked only when commits are durable: the batch is on disk before the
    /// ack, as the shard's segment flush makes it. Async mode flushes on
    /// its interval alone (`syncStableStorage`), as production's does.
    pub fn flushForAck(r: *Replica) bool {
        of(r).disk.flush();
        return true;
    }

    /// The network takes any member's frames; the replica's own guard
    /// decides whom it hears.
    pub fn setVoters(_: *Replica, _: []const u32) void {}

    pub const Host = struct {
        pub fn raftNode(r: *Replica) *RaftNode {
            return &of(r).raft;
        }

        /// Removal here drops a member without a tombstone, so no node is
        /// ever told it was removed.
        pub fn isRemoved(_: *Replica) bool {
            return false;
        }

        pub fn checkRemoved(_: *Replica, _: u64) bool {
            return false;
        }

        pub fn tickMembership(r: *Replica, _: u64) void {
            const node = of(r);
            if (node.raft.role != .leader) return;
            node.sim.writeConfig(node);
            node.sim.changeMembership(node);
        }

        pub fn applyCommitted(r: *Replica) bool {
            const node = of(r);
            node.sim.applyCommitted(node);
            return true;
        }

        pub fn clearRemovalNotices(_: *Replica) void {}

        pub fn strangerFrame(r: *Replica, _: RaftFrame) void {
            of(r).sim.stranger_frames += 1;
        }

        // The simulator sends none of these: joiners are added by the
        // leader, and clients propose on the leader directly.
        pub fn handleJoinRequest(r: *Replica, _: u32) void {
            of(r).sim.unexpected_frames += 1;
        }

        pub fn takeRemovedNotice(r: *Replica, _: RaftFrame) void {
            of(r).sim.unexpected_frames += 1;
        }

        pub fn runForwardedWrite(r: *Replica, _: RaftFrame) void {
            of(r).sim.unexpected_frames += 1;
        }

        pub fn takeForwardReply(r: *Replica, _: RaftFrame) void {
            of(r).sim.unexpected_frames += 1;
        }

        /// As the shard: lead nothing. A diverged node is a finding unless
        /// an async-mode crash could have erased what it applied; then it
        /// is repaired as its operator would (`repairDiverged`).
        pub fn stopTakingPart(r: *Replica, message: []const u8) void {
            const node = of(r);
            if (node.raft.role == .leader) {
                node.raft.role = .follower;
                node.raft.leader_id = 0;
            }
            stoppedLeading(r, message);
            if (node.replica.diverged) node.sim.onDiverged(node);
        }

        pub fn stoppedLeading(r: *Replica, _: []const u8) void {
            of(r).sim.steps_down += 1;
        }
    };
};

// ═══════════════════════════════════════════════════════════════════════════════
// Checker
// ═══════════════════════════════════════════════════════════════════════════════

const Canon = struct { term: u64, hash: u64, op_id: ?u64, recorded_at: u64 };
const StateCanon = struct { history: u64, checksum: u64, rebuilt: bool };

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
    /// The longest an ack is in flight: the network's largest delay.
    ack_flight_ms: u64 = 0,
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
    /// states[i] is the first family state recorded after index i+1, with
    /// the history that led to it.
    states: std.ArrayListUnmanaged(?StateCanon) = .empty,
    /// States compared after the same history, and how many of those set
    /// a rebuilt replica against a live one.
    state_checks: u64 = 0,
    rebuilt_checks: u64 = 0,

    fn deinit(self: *Checker) void {
        self.canonical.deinit(self.allocator);
        self.states.deinit(self.allocator);
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

    fn onState(self: *Checker, node: NodeId, idx: u64, history: u64, checksum: u64, rebuilt: bool, tick: u64) void {
        while (self.states.items.len < idx) self.states.append(self.allocator, null) catch @panic("checker OOM");
        const slot = &self.states.items[idx - 1];
        const canon = slot.* orelse {
            slot.* = .{ .history = history, .checksum = checksum, .rebuilt = rebuilt };
            return;
        };
        // Another history reached this index: async mode can erase applied
        // history, and the log's own checks judge whether it was legal.
        // States after it are compared from here on.
        if (canon.history != history) {
            slot.* = .{ .history = history, .checksum = checksum, .rebuilt = rebuilt };
            return;
        }
        self.state_checks += 1;
        if (canon.rebuilt != rebuilt) self.rebuilt_checks += 1;
        if (canon.checksum == checksum) return;
        self.fail(.{
            .invariant = .state_checksum,
            .node = node,
            .index = idx,
            .tick = tick,
            .detail = if (canon.rebuilt != rebuilt)
                "a replica rebuilt by replay holds other state than one that applied the same history live"
            else
                "two replicas hold different state after applying the same history",
        });
    }

    fn onApply(
        self: *Checker,
        workload: *const Workload,
        node: NodeId,
        idx: u64,
        term: u64,
        payload: []const u8,
        is_op: bool,
        tick: u64,
        latest_crash: u64,
    ) void {
        const hash = std.hash.Wyhash.hash(HASH_SEED, payload);
        const op_id = if (is_op) Workload.opIdFromPayload(payload) else null;
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
                // An ack can be on the wire when its sender crashes: the
                // leader commits on it, and applies, up to one delivery
                // after the crash that erased it.
                const erasable = self.allow_cross_term_rewrite and latest_crash + self.ack_flight_ms >= canon.recorded_at;
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
    /// Give nodes no hard-state sink: what a node with no data dir does.
    /// Every restart then re-enters term 0 — the double-vote hazard the
    /// sink exists to close, kept here as the demonstrator of its absence.
    volatile_hard_state: bool = false,
    /// Restart wiped nodes without the lost-log guard: the demonstrator
    /// of what the guard prevents.
    no_lost_log_guard: bool = false,
    /// The guard without its boot wait: the demonstrator of the split vote
    /// the wait prevents.
    no_lost_log_wait: bool = false,
    /// A bug planted in the toy family's apply or checksum: the
    /// demonstrator of what `state_checksum` catches.
    family_mutant: families.Mutant = .none,
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
    /// Crashes that also lost the disk, or its hard state.
    wipes: u64,
    /// Guarded nodes the convergence phase waited on to finish.
    guards_watched: u64,
    messages_delivered: u64,
    messages_dropped: u64,
    apply_stalls: u64,
    /// Replication batches read from below the leaders' rings.
    catch_up_reads: u64,
    /// Diverged nodes wiped and rejoined (async mode, after a crash).
    divergences: u64,
    /// Diverged nodes no config could spare, left diverged; a run with one
    /// is excused from converging.
    divergences_beyond_tolerance: u64,
    /// Committed configs taken on a member's word by a node that had
    /// missed them.
    vouches_taken: u64,
    /// Wyhash over the canonical history — two runs of one seed must agree.
    canonical_hash: u64,
    /// Family states compared after the same history; of those, a
    /// replica rebuilt by replay against one that applied live.
    state_checks: u64,
    rebuilt_checks: u64,
    violation_count: usize,
};

pub const Simulator = struct {
    allocator: Allocator,
    scenario: Scenario,
    options: Options,
    prng: PRNG,
    /// Clock skew and steps draw from their own stream, so a scenario
    /// without skew replays as it did before skew existed.
    clock_prng: PRNG,
    /// The outsider's choices draw from their own stream, so every other
    /// slice replays as it did before the outsider existed.
    outsider_prng: PRNG,
    /// Frames the outsider sent, by kind: votes, appends, term checks.
    outsider_sent: [3]u64 = @splat(0),
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
    /// Where the first node halted, which every other must match.
    halt: ?struct { index: u64, term: u64, hash: u64 } = null,
    /// Where the leader put the repair that skips the halt (`repair_halt`),
    /// 0 until it does.
    repair_at: u64 = 0,
    // Scratch, allocated once.
    apply_buf: []u8,
    delivery: std.ArrayListUnmanaged(Message),

    crashes: u64 = 0,
    restarts: u64 = 0,
    wipes: u64 = 0,
    config_written: bool = false,
    /// What was acked when convergence began: every member must apply it.
    /// The probes acked after are the leader's to commit; the final
    /// durability check holds every acked op either way.
    convergence_target: u64 = 0,
    elections_won: u64 = 0,
    apply_stalls: u64 = 0,
    /// Frames from a node outside the receiver's membership, dropped.
    stranger_frames: u64 = 0,
    /// Join, removal and forward frames, which nothing here sends.
    unexpected_frames: u64 = 0,
    steps_down: u64 = 0,
    divergences: u64 = 0,
    divergences_beyond_tolerance: u64 = 0,

    const RAFT_GROUP: u32 = 1;

    pub fn init(allocator: Allocator, scenario: Scenario, options: Options) !Simulator {
        std.debug.assert(scenario.node_count >= 3 and scenario.node_count <= MAX_NODES);
        std.debug.assert(scenario.outsider_permille == 0 or scenario.node_count < MAX_NODES);
        var self = Simulator{
            .allocator = allocator,
            .scenario = scenario,
            .options = options,
            .prng = PRNG.init(scenario.seed ^ 0x51D0_2A7E),
            .clock_prng = PRNG.init(scenario.seed ^ 0xC10C_5EED),
            .outsider_prng = PRNG.init(scenario.seed ^ 0x0075_1DE7),
            .nodes = try allocator.alloc(SimNode, scenario.node_count),
            .net = SimNetwork.init(allocator),
            .workload = try Workload.init(allocator, &scenario),
            .checker = .{
                .allocator = allocator,
                .allow_cross_term_rewrite = scenario.durability == .async_flush,
                .ack_flight_ms = scenario.msg_delay_max_ms,
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
            .apply_buf = try allocator.alloc(u8, @as(usize, scenario.payload_max) + 64),
            .delivery = .empty,
        };
        for (self.nodes, 0..) |*node, i| {
            const id: NodeId = @intCast(i + 1);
            node.* = .{
                .id = id,
                .up = true,
                .raft = try RaftNode.init(allocator, id, RAFT_GROUP, scenario.log_capacity, self.raftConfig()),
                .replica = try newReplica(allocator, &scenario),
                .disk = .{ .allocator = allocator },
                .max_term_seen = 0,
            };
            // Drawn only with drift on, so a run without it replays as before.
            if (scenario.clock_drift_ppm > 0) {
                const d: i32 = @intCast(scenario.clock_drift_ppm);
                node.clock_rate_ppm = self.prng.random().intRangeAtMost(i32, -d, d);
            }
            if (scenario.clock_skew_ms > 0) {
                const k: i64 = scenario.clock_skew_ms;
                node.wall_offset_ms = self.clock_prng.random().intRangeAtMost(i64, -k, k);
            }
            attachHost(node);
            for (self.nodes, 0..) |_, j| {
                if (i != j) node.raft.addPeer(@intCast(j + 1));
            }
            self.commitAllMembers(node);
        }
        // Hooks attach after (empty) replay, mirroring production wiring.
        for (self.nodes) |*node| self.attachDisk(node);
        self.setWallClocks();
        return self;
    }

    pub fn deinit(self: *Simulator) void {
        for (self.nodes) |*node| {
            node.raft.deinit();
            node.disk.deinit();
            node.applied_ticks.deinit(self.allocator);
            node.counter.deinit(self.allocator);
            self.allocator.free(node.replica.rpc_entries);
            self.allocator.free(node.replica.rpc_arena);
            self.allocator.free(node.replica.rpc_out);
        }
        self.allocator.free(self.nodes);
        self.net.deinit();
        self.workload.deinit();
        self.checker.deinit();
        self.pending_ops.deinit(self.allocator);
        self.acked_at.deinit(self.allocator);
        self.crash_ticks.deinit(self.allocator);
        self.allocator.free(self.apply_buf);
        for (self.delivery.items) |*m| self.net.release(m);
        self.delivery.deinit(self.allocator);
    }

    /// The node owns its election timer and its jitter; the seed comes
    /// from the run PRNG so a seed replays exactly and identical-log
    /// candidates do not time out in lockstep.
    fn raftConfig(self: *Simulator) raft_node.Config {
        return .{
            .election_timeout_min_ms = self.scenario.election_timeout_min_ms,
            .election_timeout_max_ms = self.scenario.election_timeout_max_ms,
            .heartbeat_interval_ms = self.scenario.heartbeat_interval_ms,
            .enable_pre_vote = true,
            .durable_commits = self.scenario.durability == .sync,
            // A leader's own copy counts once on disk, as the runtime sets it.
            .self_counts_when_durable = self.scenario.durability == .sync,
            .rng_seed = self.prng.random().int(u64) | 1,
            .rpc_timeout_ms = self.scenario.rpc_timeout_ms,
        };
    }

    /// A replica's scratch, sized for the scenario's largest batch: up to
    /// MAX_BATCH entries of the largest payload, and the frame that holds
    /// them.
    fn newReplica(allocator: Allocator, scenario: *const Scenario) !Replica {
        const arena_len = @as(usize, scenario.payload_max) * MAX_BATCH + 64;
        const rpc_entries = try allocator.alloc(entry_mod.Entry, MAX_BATCH);
        errdefer allocator.free(rpc_entries);
        const rpc_arena = try allocator.alloc(u8, arena_len);
        errdefer allocator.free(rpc_arena);
        const rpc_out = try allocator.alloc(u8, transport.APPEND_REQ_PREFIX + MAX_BATCH * entry_mod.HEADER_SIZE + arena_len);
        return .{ .id = 0, .rpc_entries = rpc_entries, .rpc_arena = rpc_arena, .rpc_out = rpc_out, .diverged = false };
    }

    /// The replica a restarted node gets: its scratch, nothing else.
    fn resetReplica(node: *SimNode) void {
        // What the run counts survives the restart; the replica's state
        // does not.
        node.vouches_before_restart += node.replica.vouches_taken;
        node.replica = .{
            .id = 0,
            .rpc_entries = node.replica.rpc_entries,
            .rpc_arena = node.replica.rpc_arena,
            .rpc_out = node.replica.rpc_out,
            .diverged = false,
        };
    }

    /// Log appends, truncations and hard state all flow into the sim disk
    /// through the node's own hooks, and reads below the ring come back
    /// from it. Volatile mode attaches no hard-state sink.
    fn attachDisk(self: *Simulator, node: *SimNode) void {
        node.raft.log.ual.on_append_ctx = @ptrCast(&node.disk);
        node.raft.log.ual.on_append = SimDisk.onAppend;
        node.raft.log.on_truncate_ctx = @ptrCast(&node.disk);
        node.raft.log.on_truncate = SimDisk.onTruncate;
        node.raft.log.catch_up = .{ .ctx = @ptrCast(&node.disk), .read_range = SimDisk.readRange };
        node.raft.log_flush_sink = .{ .ctx = @ptrCast(&node.disk), .flush = SimDisk.flushLog };
        if (!self.options.volatile_hard_state) {
            node.raft.hard_state_sink = .{ .ctx = @ptrCast(&node.disk), .persist = SimDisk.persistHardState };
        }
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

    /// A crash that loses the disk too: log and hard state. The node comes
    /// back with nothing, so its highest seen term starts over.
    fn wipeNode(self: *Simulator, node: *SimNode) void {
        self.crashNode(node);
        node.disk.wipe();
        node.wiped = true;
        node.max_term_seen = 0;
        self.wipes += 1;
    }

    /// Only the hard state is gone: the term and vote, not the log.
    fn wipeHardState(self: *Simulator, node: *SimNode) void {
        self.crashNode(node);
        node.disk.term = 0;
        node.disk.voted_for = 0;
        node.disk.lost_log = false;
        node.lost_hard_state = true;
        node.max_term_seen = 0;
        self.wipes += 1;
    }

    /// Wiped and down, or back but still guarded.
    fn isLost(node: *const SimNode) bool {
        return node.replica.diverged or node.wiped or node.lost_hard_state or node.disk.lost_log or (node.up and node.raft.lost_log != .none);
    }

    /// No other node may be lost now: at most the scenario's `max_lost_nodes` at once,
    /// since losing a majority's disks loses committed data whatever Raft
    /// does.
    fn anyLostLog(self: *const Simulator) bool {
        var lost: u8 = 0;
        for (self.nodes) |*node| lost += @intFromBool(isLost(node));
        return lost >= self.scenario.max_lost_nodes;
    }

    /// Whether `candidate` may be lost now: within `max_lost_nodes`, and
    /// every config a node holds keeps a majority of voters that are not
    /// lost. A guarded voter votes for no one until a leader has caught it
    /// up, so losing more than a config tolerates has no way back short of
    /// force-members — the operator's, not the protocol's, to take.
    fn mayLose(self: *const Simulator, candidate: NodeId) bool {
        if (self.anyLostLog()) return false;
        for (self.nodes) |*node| {
            if (!self.toleratesLoss(&node.raft.latest_config, candidate)) return false;
        }
        return true;
    }

    fn toleratesLoss(self: *const Simulator, cfg: *const membership.Config, also: NodeId) bool {
        if (cfg.member_count == 0) return true;
        var voters: u8 = 0;
        var lost: u8 = 0;
        for (cfg.memberSlice()) |m| {
            if (!m.voter) continue;
            voters += 1;
            if (m.id == also or isLost(&self.nodes[m.id - 1])) lost += 1;
        }
        return voters - lost >= voters / 2 + 1;
    }

    /// `node` reached the halting op's entry: as production's shard does,
    /// it applies nothing from it on and proposes only configs and no-ops.
    fn haltNode(self: *Simulator, node: *SimNode, e: *const entry_mod.Entry) void {
        const at: @TypeOf(self.halt.?) = .{ .index = e.header.index, .term = e.header.term, .hash = std.hash.Wyhash.hash(HASH_SEED, e.payload) };
        node.halted_at = at.index;
        node.raft.halt_at = .{ .index = e.header.index, .term = e.header.term, .crc = e.header.crc32c };
        if (self.halt) |first| {
            if (first.index != at.index or first.term != at.term or first.hash != at.hash) self.checker.fail(.{
                .invariant = .halt_agreement,
                .node = node.id,
                .index = at.index,
                .tick = self.now,
                .detail = "halted at another entry than the first node to halt",
            });
        } else self.halt = at;
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
        attachHost(node);
        node.applied_stamp = 0;
        node.applied_stamp_index = 0;
        // Its apply starts again from the log, and halts where it did.
        node.halted_at = 0;
        for (self.nodes, 0..) |_, j| {
            if (j + 1 != node.id) node.raft.addPeer(@intCast(j + 1));
        }
        self.commitAllMembers(node);
        // Replay the durable log before attaching the hook, exactly as
        // production wires persistence after segment replay — a hook
        // active during replay would re-feed the disk.
        for (node.disk.entries.items[0..node.disk.durable_len], 1..) |e, idx| {
            var entry = entry_mod.buildEntry(
                @fromBackingInt(@intCast(e.entry_type)),
                e.flags,
                e.term,
                @intCast(idx),
                e.timestamp_ns,
                e.payload,
            );
            entry.header.crc32c = entry.computeCrc();
            _ = node.raft.log.append(&entry) catch @panic("sim replay append");
            // The configs the log holds, as production's boot records them.
            if (e.entry_type == @backingInt(entry_mod.EntryType.raft_config)) {
                if (membership.decode(e.payload)) |cfg| {
                    node.raft.recordConfig(@intCast(idx), e.term, &cfg);
                    // The latest config in the log is the membership, as
                    // production's boot reads it.
                    node.raft.setMembership(&cfg, @intCast(idx));
                    node.raft.membership_term = e.term;
                }
            }
        }
        // What the sink persisted is what comes back — nothing in volatile
        // mode, so such a node restarts at term 0.
        // The log is evidence of the term too, as in production's boot.
        // Volatile mode keeps the demonstrator of a node with no hard state
        // at all: it restarts at term 0.
        node.raft.current_term = if (self.options.volatile_hard_state) node.disk.term else @max(node.disk.term, node.raft.log.lastTerm());
        node.raft.voted_for = node.disk.voted_for;
        if (node.disk.lost_log) node.raft.resumeLostLog();
        self.attachDisk(node);
        if (node.wiped) {
            node.wiped = false;
            if (!self.options.no_lost_log_guard) node.raft.enterLostLog() catch unreachable;
        }
        if (node.lost_hard_state) {
            node.lost_hard_state = false;
            if (!self.options.no_lost_log_guard) node.raft.enterLostVote() catch unreachable;
        }
        if (self.options.no_lost_log_wait) node.raft.hold_pending = false;
        resetReplica(node);
        node.up = true;
        self.restarts += 1;
        // Replay applies from the start again.
        node.applied_config = .{};
        node.replay_through = node.applied_ticks.items.len;
        node.counter.reset(self.allocator, @intCast(@min(node.disk.durable_len, std.math.maxInt(u32))));
        node.families_index = 0;
        node.families_stale = false;
        node.history = 0;
        // A node whose log holds the entry the group halted at comes up
        // halted there, as production's boot replay stops at it.
        if (self.halt) |h| if (node.raft.log.getEntryCopy(h.index, self.apply_buf)) |e| {
            if (e.header.term == h.term and std.hash.Wyhash.hash(HASH_SEED, e.payload) == h.hash) {
                node.halted_at = h.index;
                node.raft.halt_at = .{ .index = e.header.index, .term = e.header.term, .crc = e.header.crc32c };
            }
        };
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
                    if (r.uintLessThan(u16, 1000) < self.scenario.wipe_permille and self.mayLose(node.id)) {
                        if (r.boolean()) self.wipeNode(node) else self.wipeHardState(node);
                    } else {
                        self.crashNode(node);
                    }
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
        // is handled — anything still listed on an error path is released
        // exactly once, by deinit.
        while (self.delivery.items.len > 0) {
            var msg = self.delivery.orderedRemove(0);
            defer self.net.release(&msg);
            // The outsider reads nothing it is sent.
            if (msg.to > self.nodes.len) continue;
            const node = self.node_(msg.to);
            if (!node.up) continue;
            self.drive(node, .{
                .source_node = msg.from,
                .group_id = RAFT_GROUP,
                .msg_type = msg.msg_type,
                .payload = msg.payload,
            });
        }
    }

    // ── The outsider: the secret, no seat ──────────────────────────────

    fn outsiderId(self: *const Simulator) NodeId {
        return @intCast(self.nodes.len + 1);
    }

    /// Now and then the outsider sends a member something only a member
    /// may: a vote request at a raised term, skipping pre-vote; an append
    /// claiming to lead, its prefix a real member's history (as a removed
    /// member's log would be) and forged entries after it with a commit
    /// index covering them; or a term check. Whatever it gets back is
    /// dropped. Taken as genuine, these elect it or commit what it forged:
    /// the checker's election, state-machine and integrity invariants.
    fn outsiderActs(self: *Simulator) !void {
        if (self.scenario.outsider_permille == 0) return;
        const r = self.outsider_prng.random();
        if (r.uintLessThan(u16, 1000) >= self.scenario.outsider_permille) return;
        const to: NodeId = @intCast(1 + r.uintLessThan(usize, self.nodes.len));
        const me = self.outsiderId();
        // The freshest history any member holds: what it claims to lead.
        var src = &self.nodes[0];
        for (self.nodes) |*node| {
            if (node.raft.current_term > src.raft.current_term or (node.raft.current_term == src.raft.current_term and node.raft.log.lastIndex() > src.raft.log.lastIndex())) src = node;
        }
        const term = src.raft.current_term + 1 + r.uintLessThan(u64, 3);
        const kind = r.uintLessThan(u8, 3);
        self.outsider_sent[kind] += 1;
        switch (kind) {
            0 => {
                var buf: [transport.VOTE_REQ_SIZE]u8 = undefined;
                const n = transport.serializeVoteRequest(.{ .term = term, .candidate_id = me, .last_log_index = src.raft.log.lastIndex() + 10, .last_log_term = term, .is_pre_vote = false }, &buf).?;
                try self.net.send(&self.prng, &self.scenario, self.now, me, to, .request_vote, buf[0..n]);
            },
            1 => {
                const last = src.raft.log.lastIndex();
                const prev = if (last == 0) 0 else r.uintAtMost(u64, last);
                const prev_term = src.raft.log.entryTerm(prev) orelse return;
                var entries: [3]entry_mod.Entry = undefined;
                var payloads: [3][32]u8 = undefined;
                const count = 1 + r.uintLessThan(usize, 3);
                for (entries[0..count], 0..) |*e, k| {
                    // An op id the workload made, with bytes it didn't.
                    std.mem.writeInt(u64, payloads[k][0..8], 0, .little);
                    r.bytes(payloads[k][8..]);
                    e.* = entry_mod.buildEntry(.kv_put, 0, term, prev + 1 + k, (WALL_EPOCH_MS + self.now) * std.time.ns_per_ms, &payloads[k]);
                    e.header.crc32c = e.computeCrc();
                }
                var buf: [transport.APPEND_REQ_PREFIX + 3 * (entry_mod.HEADER_SIZE + 32)]u8 = undefined;
                const n = transport.serializeAppendRequest(.{ .term = term, .leader_id = me, .prev_log_index = prev, .prev_log_term = prev_term, .leader_commit = prev + count, .entries = entries[0..count] }, &buf).?;
                try self.net.send(&self.prng, &self.scenario, self.now, me, to, .append_entries, buf[0..n]);
            },
            else => {
                var buf: [transport.TERM_CHECK_SIZE]u8 = undefined;
                const n = transport.serializeTermCheck(.{ .term = term, .from = me }, &buf).?;
                try self.net.send(&self.prng, &self.scenario, self.now, me, to, .term_check, buf[0..n]);
            },
        }
    }

    /// One call into a replica: a frame, or (null) its tick. A leadership
    /// won inside the call is seen when it returns — no call both wins and
    /// loses one.
    fn drive(self: *Simulator, node: *SimNode, frame: ?RaftFrame) void {
        const was_leader = node.raft.role == .leader;
        const was_term = node.raft.current_term;
        if (frame) |f| node.replica.handleRaftFrame(f) else node.replica.tickRaft();
        node.max_term_seen = @max(node.max_term_seen, node.raft.current_term);
        if (node.raft.role == .leader and (!was_leader or node.raft.current_term != was_term)) self.onWon(node);
    }

    fn onWon(self: *Simulator, node: *SimNode) void {
        self.elections_won += 1;
        self.checker.onLeader(node, self.now);
        self.writeConfig(node);
        // A change the moment a leader wins, before its term's noop
        // commits: what the own-term gate is for, when an earlier term
        // left a change uncommitted.
        if (self.config_written and self.scenario.config_change_permille > 0 and self.prng.random().boolean()) self.randomChange(node);
    }

    /// A replica stopped taking part: its log contradicts history it
    /// applied. Sync mode never erases applied history, and async mode
    /// only in a crash.
    /// The history it applied that the leader contradicts begins just
    /// past its rewound commit index. Only a crash at or after an ack for
    /// that entry left its sender, one network delay before this node
    /// applied it, could have erased it.
    fn onDiverged(self: *Simulator, node: *SimNode) void {
        // A leader said it applied the entry this node halted at. Every
        // replica here halts at the same entry on the same build, so none
        // ever does.
        if (node.replica.halted_alone) return self.checker.fail(.{
            .invariant = .halt_agreement,
            .node = node.id,
            .index = node.halted_at,
            .tick = self.now,
            .detail = "a halted node was told a leader applied the entry it halted at",
        });
        const contradicted = node.raft.commit_index + 1;
        const applied_at: ?u64 = if (contradicted <= node.applied_ticks.items.len) node.applied_ticks.items[contradicted - 1] else null;
        const erasable = self.scenario.durability == .async_flush and self.crashes > 0 and
            (applied_at == null or self.latest_crash_at + self.scenario.msg_delay_max_ms >= applied_at.?);
        if (!erasable) self.checker.fail(.{
            .invariant = .divergence,
            .node = node.id,
            .index = node.raft.commit_index,
            .tick = self.now,
            .detail = "a node's applied history contradicts the group's log",
        });
    }

    /// What the replica's message tells its operator: move the data aside
    /// and rejoin. It comes back as a wiped node, under the lost-log guard.
    ///
    /// Only within what the configs tolerate losing, as every other wipe
    /// here. A diverged voter the group can't spare stays diverged: the
    /// async crash that erased its history cost the group its quorum, and
    /// the way back is the operator's force-members, which only a
    /// hand-built test here models. That run is excused from converging.
    fn repairDiverged(self: *Simulator) !void {
        for (self.nodes) |*node| {
            if (!node.up or !node.replica.diverged) continue;
            if (!self.mayRepair(node.id)) {
                if (!node.beyond_tolerance) {
                    node.beyond_tolerance = true;
                    self.divergences_beyond_tolerance += 1;
                }
                continue;
            }
            self.divergences += 1;
            self.wipeNode(node);
            try self.restartNode(node);
        }
    }

    /// `mayLose` for a node that is lost already (diverged): the limits
    /// count the others, and every config must spare it.
    fn mayRepair(self: *const Simulator, id: NodeId) bool {
        var lost: u8 = 0;
        for (self.nodes) |*node| lost += @intFromBool(node.id != id and isLost(node));
        if (lost >= self.scenario.max_lost_nodes) return false;
        for (self.nodes) |*node| {
            if (!self.toleratesLoss(&node.raft.latest_config, id)) return false;
        }
        return true;
    }

    /// Every node is a member from the start: the committed membership a
    /// truncation can fall back to, as production's founding config is.
    fn commitAllMembers(self: *Simulator, node: *SimNode) void {
        var ids: [network_mod.MAX_NODES]NodeId = undefined;
        for (0..self.scenario.node_count) |i| ids[i] = @intCast(i + 1);
        const cfg = membership.Config.ofVoters(ids[0..self.scenario.node_count]);
        node.raft.commitMembership(&cfg, 0);
    }

    /// A leader with no config writes one naming every node, as
    /// production's founder does: membership the logs carry, so a wiped
    /// node's term check reads its committed config from the log it caught
    /// up to. Not only the first leader: one whose founding config was cut
    /// from the log before it committed leaves the next to write it.
    fn writeConfig(self: *Simulator, node: *SimNode) void {
        if (node.raft.latest_config.member_count != 0) return;
        var ids: [network_mod.MAX_NODES]NodeId = undefined;
        for (0..self.scenario.node_count) |i| ids[i] = @intCast(i + 1);
        const cfg = membership.Config.ofVoters(ids[0..self.scenario.node_count]);
        const outcome = self.proposeConfig(node, &cfg) orelse return;
        if (outcome != .proposed) return;
        self.config_written = true;
    }

    // ── Node tick: the replica's clock ─────────────────────────────────

    fn tickNodes(self: *Simulator) void {
        for (self.nodes) |*node| {
            if (node.up) self.drive(node, null);
        }
    }

    /// A membership change through the node's own gate, checked: a leader
    /// that is not a voter starts no change, and only the lone voter of a
    /// config makes two voters at once, the step whose old and new
    /// majorities need not meet. History alone cannot show who proposed.
    fn proposeConfig(self: *Simulator, node: *SimNode, next: *const membership.Config) ?RaftNode.ConfigProposal {
        const prev = node.raft.latest_config;
        const was_voter = node.raft.timer_enabled;
        const outcome = node.raft.proposeConfig(next) catch return null;
        if (outcome != .proposed or prev.member_count == 0) return outcome;
        const why: ?[]const u8 = if (!was_voter)
            "a leader that is not a voter proposed a membership change"
        else if (prev.voterCount() == 1 and next.voterCount() == 3 and !prev.isVoter(node.id))
            "two voters added at once by a node that is not the lone voter"
        else
            null;
        if (why) |detail| self.checker.fail(.{ .invariant = .config_safety, .node = node.id, .index = outcome.proposed.index, .tick = self.now, .detail = detail });
        return outcome;
    }

    /// Where the simulation's wall clock starts: far enough from 0 that no
    /// skew takes a node's below it.
    const WALL_EPOCH_MS: u64 = 1_700_000_000_000;

    /// What the shard gives its Raft node: the wall clock, one build for
    /// every node (a versioned one, so a leader's halt verdict counts), and
    /// whether a repair skipped an entry.
    fn attachHost(node: *SimNode) void {
        node.raft.wall_clock = .{ .ctx = &node.wall_ns, .now_ns = readWall };
        node.raft.build = 1;
        node.raft.repaired = .{ .ctx = node, .check = nodeRepaired };
    }

    fn nodeRepaired(ctx: ?*anyopaque, h: raft_node.HaltAt) bool {
        const node: *SimNode = @ptrCast(@alignCast(ctx.?));
        const halt = node.sim.halt orelse return false;
        return node.skipped and h.index == halt.index and h.term == halt.term;
    }

    /// A halted node takes a committed repair that names its halt, as the
    /// shard's apply does, and goes on past it.
    fn resumeIfSkipped(self: *Simulator, node: *SimNode) bool {
        const h = node.raft.halt_at orelse return false;
        var idx = h.index + 1;
        while (idx <= node.raft.commit_index) : (idx += 1) {
            const e = node.raft.log.getEntryCopy(idx, self.apply_buf) orelse continue;
            const skip = repair.Skip.decode(&e) orelse continue;
            if (skip.index != h.index or skip.term != h.term or skip.crc != h.crc) continue;
            node.skipped = true;
            node.halted_at = 0;
            node.raft.halt_at = null;
            return true;
        }
        return false;
    }

    /// The operator's repair, once every core node halted: the leader
    /// proposes the skip. One a later leader cut from the log is proposed
    /// again.
    fn repairHalt(self: *Simulator) !void {
        if (!self.scenario.repair_halt) return;
        const h = self.halt orelse return;
        for (self.nodes, 0..) |*node, i| {
            if (self.core[i] and (!node.up or node.halted_at != h.index)) return;
        }
        const leader = for (self.nodes) |*node| {
            if (node.up and node.raft.role == .leader) break node;
        } else return;
        if (self.repair_at != 0) {
            if (leader.raft.log.getEntryCopy(self.repair_at, self.apply_buf)) |e| if (repair.Skip.decode(&e)) |sk| if (sk.index == h.index) return;
        }
        const halted = leader.raft.log.getEntryCopy(h.index, self.apply_buf) orelse return;
        var p: [repair.Skip.MAX_PAYLOAD]u8 = undefined;
        const proposed = leader.raft.propose(.repair_skip, entry_mod.Flags.NONE, repair.Skip.of(&halted, "vopr").encode(&p)) catch return;
        self.repair_at = proposed.index;
    }

    fn readWall(ctx: ?*anyopaque) u64 {
        const wall: *const u64 = @ptrCast(@alignCast(ctx.?));
        return wall.*;
    }

    /// Each node's wall clock for this tick: its own drifting time, off by
    /// its skew. Now and then one steps, mostly back, as a host's does when
    /// NTP corrects it.
    fn setWallClocks(self: *Simulator) void {
        const k: i64 = self.scenario.clock_skew_ms;
        if (k > 0) {
            const r = self.clock_prng.random();
            if (r.uintLessThan(u32, 2000) == 0) {
                const node = &self.nodes[r.uintLessThan(usize, self.nodes.len)];
                const step = r.intRangeAtMost(i64, 1, k);
                node.wall_offset_ms += if (r.uintLessThan(u8, 4) == 0) step else -step;
                node.wall_offset_ms = std.math.clamp(node.wall_offset_ms, -2 * k, 2 * k);
            }
        }
        for (self.nodes) |*node| {
            const ms = @as(i64, @intCast(WALL_EPOCH_MS + self.nodeNow(node))) + node.wall_offset_ms;
            node.wall_ns = @as(u64, @intCast(ms)) * std.time.ns_per_ms;
        }
    }

    /// The node's own clock: the simulation's, run fast or slow by its
    /// drift, as a real host's is against another's.
    fn nodeNow(self: *const Simulator, node: *const SimNode) u64 {
        const skew = @divTrunc(@as(i128, self.now) * node.clock_rate_ppm, 1_000_000);
        return @intCast(@max(0, @as(i128, self.now) + skew));
    }

    /// The leader's own changes (promote caught-up replicas, age out
    /// joiners), and now and then one the scenario makes: add a node that
    /// is not a member as a replica, promote a replica, or remove a voter,
    /// the leader itself included. Each goes through `proposeConfig`, so a
    /// change too soon after an election, or with one in flight, is refused
    /// there, and a leader cut off holds a change the others never take.
    fn changeMembership(self: *Simulator, node: *SimNode) void {
        // Convergence asks a fixed core to finish; the membership holds.
        if (!self.config_written or self.phase == .convergence) return;
        const raft = &node.raft;
        var peers_progress: [MAX_PEERS]membership.Progress = undefined;
        if (membership.nextAutomatic(&raft.latest_config, raft.memberProgress(&peers_progress), self.nodeNow(node), raft.voterCountBefore())) |next| {
            _ = self.proposeConfig(node, &next);
            return;
        }
        const r = self.prng.random();
        if (self.scenario.config_change_permille == 0 or r.uintLessThan(u16, 1000) >= self.scenario.config_change_permille) return;
        self.randomChange(node);
    }

    /// One change the scenario makes: add a node that is not a member as a
    /// replica, promote a replica, or remove a voter.
    fn randomChange(self: *Simulator, node: *SimNode) void {
        const raft = &node.raft;
        const r = self.prng.random();
        const cfg = raft.latest_config;
        if (cfg.member_count == 0) return;
        var outside: ?NodeId = null;
        for (self.nodes) |*other| {
            if (!cfg.names(other.id)) outside = other.id;
        }
        const next = if (outside != null and r.boolean())
            cfg.withJoiner(outside.?, true)
        else blk: {
            const pick = cfg.members[r.uintLessThan(u8, cfg.member_count)];
            if (!pick.voter) break :blk cfg.withMember(.{ .id = pick.id, .voter = true, .may_vote = true, .caught_up = true });
            // Without a tombstone, so the node can be added back.
            break :blk cfg.without(pick.id, null);
        };
        // Within what the voters tolerate, as fault injection stays.
        if (!self.toleratesLoss(&next, 0)) return;
        _ = self.proposeConfig(node, &next);
    }

    // ── Workload ───────────────────────────────────────────────────────

    fn submitOps(self: *Simulator) !void {
        const r = self.prng.random();
        const chance: u8 = if (self.phase == .safety)
            self.scenario.request_percent
        else
            // Convergence probes: a trickle, so the checker sees a live
            // cluster, not one that only ever catches up.
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
        const res = target.raft.propose(op.entry_type, 0, op.payload) catch return;
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

    fn applyAll(self: *Simulator) void {
        for (self.nodes) |*node| {
            if (node.up) self.applyCommitted(node);
        }
    }

    /// The host's apply: with sync durability the log is flushed and the
    /// node told so first, as the shard does before every apply — a
    /// leader's own copy counts toward commit only once on disk.
    fn applyCommitted(self: *Simulator, node: *SimNode) void {
        if (self.scenario.durability == .sync) {
            node.disk.flush();
            node.raft.markDurable(node.raft.log.lastIndex());
        }
        node.disk.applying = true;
        defer node.disk.applying = false;
        if (node.halted_at != 0 and !self.resumeIfSkipped(node)) return;
        {
            while (node.raft.last_applied < node.raft.commit_index) {
                const idx = node.raft.last_applied + 1;
                // Apply rewound (async flush lost committed history and the
                // log took the leader's): the entry before is the log's now.
                const stamp_before: ?u64 = if (idx == node.applied_stamp_index + 1)
                    node.applied_stamp
                else if (node.raft.log.getEntryCopy(idx - 1, self.apply_buf)) |prev|
                    prev.header.timestamp_ns
                else
                    null;
                // Never bare getEntry (the wrap-null trap). A restarted
                // node's durable log can exceed its ring; the log reads the
                // rest from the disk itself.
                const e = node.raft.log.getEntryCopy(idx, self.apply_buf) orelse {
                    self.apply_stalls += 1;
                    break;
                };
                // The group's own entries (noops, configs) are history too,
                // but carry no workload op.
                // An entry a repair skips is passed over: it applies nothing.
                const passed_over = node.skipped and self.halt != null and idx == self.halt.?.index;
                const is_op = !passed_over and e.header.entry_type != @backingInt(entry_mod.EntryType.raft_noop) and e.header.entry_type != @backingInt(entry_mod.EntryType.raft_config) and e.header.entry_type != @backingInt(entry_mod.EntryType.repair_skip);
                if (is_op and self.scenario.halt_op != 0 and Workload.opIdFromPayload(e.payload) == self.scenario.halt_op - 1) {
                    self.haltNode(node, &e);
                    break;
                }
                if (stamp_before != null and e.header.timestamp_ns <= stamp_before.?) self.checker.fail(.{
                    .invariant = .stamp_order,
                    .node = node.id,
                    .index = idx,
                    .tick = self.now,
                    .detail = "committed entry stamped no later than the one before it",
                });
                node.applied_stamp = e.header.timestamp_ns;
                node.applied_stamp_index = idx;
                while (node.applied_ticks.items.len < idx) node.applied_ticks.append(self.allocator, 0) catch @panic("sim OOM");
                node.applied_ticks.items[idx - 1] = self.now;
                node.raft.noteApplied(e.header.timestamp_ns);
                self.checker.onApply(&self.workload, node.id, idx, e.header.term, e.payload, is_op, self.now, self.latest_crash_at);
                // A config applied is committed, as production's applier
                // records it.
                if (e.header.entry_type == @backingInt(entry_mod.EntryType.raft_config)) {
                    if (membership.decode(e.payload)) |cfg| {
                        // Each committed config follows from the one before:
                        // what one change at a time, after an own-term
                        // commit, is there to guarantee.
                        if (node.applied_config.member_count > 0) {
                            // Who proposed is not in the entry: a lone voter's
                            // own two-at-once step passes here, and
                            // `proposeConfig` checks the proposer.
                            var lone: [membership.MAX_MEMBERS]u32 = undefined;
                            const prev_voters = node.applied_config.voterIds(&lone);
                            const proposer: u32 = if (prev_voters.len == 1) prev_voters[0] else 0;
                            if (membership.checkChange(&node.applied_config, &cfg, proposer)) |why| self.checker.fail(.{
                                .invariant = .config_safety,
                                .node = node.id,
                                .index = idx,
                                .tick = self.now,
                                .detail = why.message(),
                            });
                        }
                        node.applied_config = cfg;
                        node.raft.commitMembership(&cfg, idx);
                        node.raft.recordConfig(idx, e.header.term, &cfg);
                    }
                }
                self.applyFamilies(node, &e, idx, is_op);
                node.raft.last_applied = idx;
            }
        }
    }

    /// The toy family's apply, then its state for the checker.
    fn applyFamilies(self: *Simulator, node: *SimNode, e: *const entry_mod.Entry, idx: u64, is_op: bool) void {
        if (idx <= node.families_index) node.families_stale = true;
        node.families_index = idx;
        if (node.families_stale) return;
        const step = [2]u64{ e.header.term, std.hash.Wyhash.hash(HASH_SEED, e.payload) };
        node.history = std.hash.Wyhash.hash(node.history, std.mem.asBytes(&step));
        const live = idx > node.replay_through;
        const mutant = self.options.family_mutant;
        if (is_op and families.Counter.owns(e.header.entry_type)) _ = node.counter.apply(self.allocator, e, .{
            .mutant = mutant,
            .node_now = self.nodeNow(node),
            .leader = node.raft.role == .leader,
            .live = live,
        });
        self.checker.onState(node.id, idx, node.history, node.counter.checksum(mutant), !live, self.now);
    }

    // ── Log durability (runs before fault injection) ───────────────────
    // Hard state is not synced here: it reaches the disk only through the
    // node's sink, so a term the node never persisted is a term it loses.
    // Sync mode flushes where the host does (before an ack, before an
    // apply); async mode on its interval alone.

    fn syncStableStorage(self: *Simulator) void {
        if (self.scenario.durability != .async_flush) return;
        for (self.nodes) |*node| {
            if (!node.up) continue;
            if (self.now - node.disk.last_flush >= self.scenario.flush_interval_ms) {
                node.disk.flush();
                node.disk.last_flush = self.now;
            }
        }
    }

    // ── Phases ─────────────────────────────────────────────────────────

    fn transitionToConvergence(self: *Simulator) !void {
        self.phase = .convergence;
        self.convergence_target = self.max_acked_index;
        const r = self.prng.random();
        const n = self.scenario.node_count;
        const quorum = n / 2 + 1;
        // Random quorum-sized core, of nodes with their logs when there are
        // enough: a lost-log node helps elect no one until a leader has
        // caught it up, so a bare majority holding one would wait forever.
        var whole: u8 = 0;
        for (self.nodes) |*node| {
            if (!isLost(node)) whole += 1;
        }
        var chosen: u8 = 0;
        if (self.scenario.config_change_permille > 0) {
            self.chooseMembershipCore();
        } else while (chosen < quorum) {
            const pick = r.uintLessThan(u8, n);
            const p = &self.nodes[pick];
            if (isLost(p) and whole >= quorum) continue;
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
            } else if (isLost(node) and self.namedAnywhere(node.id)) {
                // Connected and watched: the run converges only once every
                // guarded node has finished its guard with the core's
                // leader. Isolated, a guard that never completes would pass
                // every seed.
                node.guard_watched = true;
                if (!node.up) try self.restartNode(node);
            } else {
                // Permanent isolation, not frozen fault rates: a live
                // non-core node with a crash-loop-inflated term would
                // keep the core busy answering it and turn scenario noise
                // into false liveness failures.
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

    /// With the membership changing, which nodes the group ends with is
    /// decided in the run, so every node is healed and restarted, and the
    /// run converges once a leader has committed its latest config and
    /// every node that config names has applied every acked op
    /// (`convergedMembership`).
    fn chooseMembershipCore(self: *Simulator) void {
        for (self.core[0..self.scenario.node_count]) |*c| c.* = true;
    }

    fn namedAnywhere(self: *const Simulator, id: NodeId) bool {
        for (self.nodes) |*node| {
            if (node.raft.latest_config.names(id)) return true;
        }
        return false;
    }

    fn converged(self: *Simulator) bool {
        if (self.scenario.config_change_permille > 0) return self.convergedMembership();
        if (self.halt) |h| {
            // Halted: every core node stopped at the same entry.
            if (!self.scenario.repair_halt) {
                for (self.nodes, 0..) |*node, i| {
                    if (!self.core[i]) continue;
                    if (!node.up or node.halted_at != h.index) return false;
                }
                return true;
            }
            // Repaired: every core node goes on past it, as below.
            if (self.repair_at == 0) return false;
        }
        // Every acked op applied by every core node.
        const target = @max(self.max_acked_index, self.repair_at);
        for (self.nodes) |*node| {
            if (node.guard_watched and (!node.up or node.raft.lost_log != .none)) return false;
        }
        for (self.nodes, 0..) |*node, i| {
            if (!self.core[i]) continue;
            if (!node.up or node.halted_at != 0) return false;
            if (node.raft.last_applied < target) return false;
        }
        return true;
    }

    fn convergedMembership(self: *const Simulator) bool {
        const leader = for (self.nodes) |*node| {
            if (node.up and node.raft.role == .leader) break node;
        } else return false;
        const cfg = &leader.raft.latest_config;
        if (cfg.member_count == 0 or leader.raft.membership_index > leader.raft.commit_index) return false;
        if (self.halt == null and leader.raft.last_applied < self.max_acked_index) return false;
        for (cfg.memberSlice()) |m| {
            const node = &self.nodes[m.id - 1];
            if (!node.up or node.raft.lost_log != .none) return false;
            if (self.halt) |h| if (!self.scenario.repair_halt) {
                if (node.halted_at != h.index) return false;
                continue;
            };
            if (node.halted_at != 0) return false;
            if (node.raft.last_applied < @max(self.convergence_target, self.repair_at)) return false;
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
            // Past a halt nothing applies, so nothing records it; past a
            // repair, all but the skipped entry does.
            if (self.halt) |h| if (op.index == h.index or (!self.scenario.repair_halt and op.index >= h.index)) continue;
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
    /// deliver → replica ticks → submit → apply → ack sweep →
    /// stable-storage sync → diverged repair → faults. An op acked in the
    /// tick its acker crashes was acked before the crash — the client has
    /// the response; the guarantee stands.
    fn tick(self: *Simulator) !void {
        for (self.nodes) |*node| node.sim = self;
        // The replicas say what a node's operator reads; the run's own
        // report is the checker's.
        const level = log.getLevel();
        if (!self.options.verbose) log.setLevel(.fatal);
        defer log.setLevel(level);
        self.now += 1;
        self.setWallClocks();
        try self.deliverAll();
        try self.outsiderActs();
        self.tickNodes();
        try self.submitOps();
        self.applyAll();
        try self.ackSweep();
        try self.repairHalt();
        self.syncStableStorage();
        try self.repairDiverged();
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
            // Only async mode lets a crash erase applied history; sync
            // fails on any divergence (`onDiverged`).
            const excused = self.divergences_beyond_tolerance > 0 and self.scenario.durability == .async_flush;
            if (!converged_ok and self.checker.violations.items.len == 0 and !excused) {
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
            .wipes = self.wipes,
            .guards_watched = blk: {
                var n: u64 = 0;
                for (self.nodes) |*node| n += @intFromBool(node.guard_watched);
                break :blk n;
            },
            .messages_delivered = self.net.delivered,
            .messages_dropped = self.net.dropped,
            .apply_stalls = self.apply_stalls,
            .catch_up_reads = blk: {
                var n: u64 = 0;
                for (self.nodes) |*node| n += node.disk.catch_up_reads;
                break :blk n;
            },
            .divergences = self.divergences,
            .divergences_beyond_tolerance = self.divergences_beyond_tolerance,
            .vouches_taken = blk: {
                var n: u64 = 0;
                for (self.nodes) |*node| n += node.vouches_before_restart + node.replica.vouches_taken;
                break :blk n;
            },
            .canonical_hash = h.final(),
            .state_checks = self.checker.state_checks,
            .rebuilt_checks = self.checker.rebuilt_checks,
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
            // Its view of the group: who it hears from, and who votes.
            try writer.print("    guard={s} diverged={} config@{d} (committed {d}):", .{ @tagName(node.raft.lost_log), node.replica.diverged, node.raft.membership_index, node.raft.commit_index });
            for (node.raft.latest_config.memberSlice()) |m| {
                try writer.print(" {d}{s}", .{ m.id, if (m.voter) "v" else if (m.caught_up) "r+" else "r" });
            }
            try writer.print("; committed config@{d}:", .{node.raft.committed_config_index});
            for (node.raft.committed_config.memberSlice()) |m| {
                try writer.print(" {d}{s}", .{ m.id, if (m.voter) "v" else if (m.caught_up) "r+" else "r" });
            }
            try writer.print("\n", .{});
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

test "vopr sim: a node without a hard-state sink trips term monotonicity on restart" {
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

test "vopr sim: the sink is the only path hard state takes to disk" {
    var scenario = Scenario.calm(5);
    scenario.crash_permille = 1;
    scenario.restart_permille = 50;
    scenario.ticks_safety = 10_000;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    const s = try sim.run();
    try testing.expect(s.ok);
    try testing.expect(s.restarts > 0);
    try testing.expect(s.elections_won > 0);
    // With the harness's per-tick copy gone, disk and node agree only
    // because the node persisted every term and vote before using it.
    for (sim.nodes) |*node| {
        try testing.expectEqual(node.raft.current_term, node.disk.term);
        try testing.expectEqual(node.raft.voted_for, node.disk.voted_for);
        try testing.expect(node.disk.term > 0);
    }
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

/// A run with crashes and restarts, so replicas rebuild by replay.
fn restartingScenario(seed: u64) Scenario {
    var scenario = Scenario.calm(seed);
    scenario.crash_permille = 5;
    scenario.restart_permille = 100;
    scenario.ticks_safety = 6_000;
    return scenario;
}

test "vopr sim: replicas, and replicas rebuilt by replay, hold the same family state after the same history" {
    var checks: u64 = 0;
    var rebuilt: u64 = 0;
    for ([_]u64{ 77, 78, 79 }) |seed| {
        for ([_]scenario_mod.DurabilityMode{ .sync, .async_flush }) |durability| {
            var scenario = restartingScenario(seed);
            scenario.durability = durability;
            var sim = try Simulator.init(testing.allocator, scenario, .{});
            defer sim.deinit();
            const s = try sim.run();
            if (!s.ok) {
                var buf: [4096]u8 = undefined;
                var w: std.Io.Writer = .fixed(&buf);
                sim.printViolations(&w) catch {};
                std.debug.print("seed {d} {s}:\n{s}\n", .{ seed, @tagName(durability), w.buffered() });
            }
            try testing.expect(s.ok);
            checks += s.state_checks;
            rebuilt += s.rebuilt_checks;
        }
    }
    try testing.expect(checks >= 1000);
    try testing.expect(rebuilt >= 500);
}

/// What `state_checksum` found under a mutant: any violation, and one
/// that set a replica rebuilt by replay against one that applied live.
const Caught = struct { any: bool = false, rebuilt_vs_live: bool = false };

fn stateChecksumUnder(mutant: families.Mutant) !Caught {
    var sim = try Simulator.init(testing.allocator, restartingScenario(77), .{ .family_mutant = mutant });
    defer sim.deinit();
    _ = try sim.run();
    var found: Caught = .{};
    for (sim.checker.violations.items) |v| {
        if (v.invariant != .state_checksum) continue;
        found.any = true;
        if (std.mem.startsWith(u8, v.detail, "a replica rebuilt by replay")) found.rebuilt_vs_live = true;
    }
    return found;
}

test "vopr sim: apply that reads the node's clock fails state_checksum" {
    try testing.expect((try stateChecksumUnder(.node_clock)).any);
}

test "vopr sim: a refusal decided by the replica, not the entry, fails state_checksum" {
    try testing.expect((try stateChecksumUnder(.local_refusal)).any);
}

test "vopr sim: a checksum in map order fails state_checksum between a rebuilt replica and a live one" {
    try testing.expect((try stateChecksumUnder(.map_order)).rebuilt_vs_live);
}

test "vopr sim: an effect only live apply runs fails state_checksum between a rebuilt replica and a live one" {
    try testing.expect((try stateChecksumUnder(.live_only_effect)).rebuilt_vs_live);
}

fn runTicks(sim: *Simulator, n: u64) !void {
    for (0..n) |_| try sim.tick();
}

fn leaderOf(sim: *Simulator) ?NodeId {
    for (sim.nodes) |*node| {
        if (node.up and node.raft.role == .leader) return node.id;
    }
    return null;
}

fn hasViolation(sim: *const Simulator, invariant: Invariant) bool {
    for (sim.checker.violations.items) |v| {
        if (v.invariant == invariant) return true;
    }
    return false;
}

/// Three nodes. The leader commits entries while node y is down; then the
/// leader goes down, node x loses its disk, and both come back. Returns the
/// old leader's id.
fn wipeAVoterBesideAStaleNode(sim: *Simulator) !NodeId {
    try runTicks(sim, 2000);
    const l = leaderOf(sim) orelse return error.NoLeader;
    const x: NodeId = if (l == 1) 2 else 1;
    const y: NodeId = 6 - l - x;
    sim.crashNode(sim.node_(y));
    const committed = sim.node_(l).raft.commit_index;
    try runTicks(sim, 2000);
    if (sim.node_(l).raft.commit_index <= committed) return error.NoProgress;
    sim.crashNode(sim.node_(l));
    sim.wipeNode(sim.node_(x));
    try sim.restartNode(sim.node_(x));
    try sim.restartNode(sim.node_(y));
    try runTicks(sim, 4000);
    return l;
}

test "vopr sim: a wiped voter without the guard elects a node missing committed entries" {
    var scenario = Scenario.calm(7);
    scenario.restart_permille = 0;
    var sim = try Simulator.init(testing.allocator, scenario, .{ .no_lost_log_guard = true });
    defer sim.deinit();
    _ = try wipeAVoterBesideAStaleNode(&sim);
    try testing.expect(hasViolation(&sim, .leader_completeness));
}

test "vopr sim: a wiped voter grants no vote until caught up, and rejoins once the leader is back" {
    var scenario = Scenario.calm(7);
    scenario.restart_permille = 0;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    const l = try wipeAVoterBesideAStaleNode(&sim);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
    try testing.expectEqual(@as(?NodeId, null), leaderOf(&sim));

    try sim.restartNode(sim.node_(l));
    try runTicks(&sim, 6000);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
    for (sim.nodes) |*node| try testing.expectEqual(raft_node.LostLog.none, node.raft.lost_log);
    try testing.expect(leaderOf(&sim) != null);
}

test "vopr sim: with two of three wiped no leader is elected, and force-members on the survivor brings the group back" {
    var scenario = Scenario.calm(11);
    scenario.restart_permille = 0;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    try runTicks(&sim, 2000);
    const l = leaderOf(&sim) orelse return error.NoLeader;
    for (sim.nodes) |*node| {
        if (node.id == l) sim.crashNode(node) else sim.wipeNode(node);
    }
    for (sim.nodes) |*node| try sim.restartNode(node);
    try runTicks(&sim, 5000);
    // The survivor needs a vote, and a lost-log node grants none.
    try testing.expectEqual(@as(?NodeId, null), leaderOf(&sim));
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);

    // force-members: the survivor's config names only it, and the nodes
    // left out cannot reach it (in production, the retired secret).
    const s = sim.node_(l);
    s.raft.setVoters(&.{l}, s.raft.log.lastIndex());
    for (sim.nodes) |*node| {
        if (node.id != l) sim.net.isolate(node.id);
    }
    const committed = s.raft.commit_index;
    try runTicks(&sim, 3000);
    try testing.expectEqual(@as(?NodeId, l), leaderOf(&sim));
    try testing.expect(s.raft.commit_index > committed);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

test "vopr sim: swarms with wiped disks keep every invariant" {
    var wipes: u64 = 0;
    var watched: u64 = 0;
    var seed: u64 = 1;
    while (seed <= 24) : (seed += 1) {
        var scenario = Scenario.fromSeed(seed);
        scenario.wipe_permille = 500;
        scenario.crash_permille = @max(scenario.crash_permille, 2);
        scenario.ticks_safety = 8_000;
        var sim = try Simulator.init(testing.allocator, scenario, .{});
        defer sim.deinit();
        const s = try sim.run();
        if (!s.ok) {
            var buf: [8192]u8 = undefined;
            var w: std.Io.Writer = .fixed(&buf);
            sim.printViolations(&w) catch {};
            std.debug.print("seed {d} (wipes={d}):\n{s}\n", .{ seed, s.wipes, w.buffered() });
        }
        try testing.expect(s.ok);
        wipes += s.wipes;
        watched += s.guards_watched;
    }
    try testing.expect(wipes >= 12);
    // Some runs end with a node still guarded, and convergence waited for
    // it to finish.
    try testing.expect(watched >= 1);
}

/// Seven nodes. A and B stand in one term t, cut off from the rest; X votes
/// for A, loses its disk, restarts, catches up from the old leader L and
/// polls. The moment X's guard is done, the votes are split: X, L and P
/// to A (X's from before the wipe), and X, Q and R to B. Two leaders in t
/// unless every candidacy in t has given up by then.
fn splitVoteAcrossAWipe(sim: *Simulator, stall_candidates: bool) !void {
    try runTicks(sim, 10_000);
    // No new writes, so every log is the same and each candidate's is
    // up to date for every voter.
    sim.scenario.request_percent = 0;
    try runTicks(sim, 2_000);
    const l = leaderOf(sim) orelse return error.NoLeader;
    var rest: [6]NodeId = undefined;
    var n: usize = 0;
    for (sim.nodes) |*node| {
        if (node.id == l) continue;
        rest[n] = node.id;
        n += 1;
    }
    const a = sim.node_(rest[0]);
    const b = sim.node_(rest[1]);
    const x = sim.node_(rest[2]);
    const p = sim.node_(rest[3]);
    const q = sim.node_(rest[4]);
    const r = sim.node_(rest[5]);
    sim.net.isolate(a.id);
    sim.net.isolate(b.id);
    const req_a = a.raft.startElectionNow().?;
    const req_b = b.raft.startElectionNow().?;
    try testing.expectEqual(req_a.term, req_b.term);
    // X had lost touch with L too when A asked.
    x.raft.last_leader_contact_ms = 0;
    const x_for_a = x.raft.handleVoteRequest(req_a);
    try testing.expect(x_for_a.vote_granted);
    // Stalled, A and B do not tick: only their deadline, checked against
    // the clock when they handle a vote, ends their candidacies.
    if (stall_candidates) {
        a.up = false;
        b.up = false;
    }

    sim.wipeNode(x);
    try sim.restartNode(x);
    var guard_ticks: u64 = 0;
    while (x.raft.lost_log != .none) : (guard_ticks += 1) {
        if (guard_ticks > 20_000) return error.GuardNeverDone;
        try sim.tick();
    }

    if (stall_candidates) {
        a.up = true;
        b.up = true;
    }
    a.raft.observeTime(sim.now);
    b.raft.observeTime(sim.now);
    // L is gone from here on, as far as X, P, Q and R can tell.
    for ([_]*SimNode{ x, p, q, r }) |node| node.raft.last_leader_contact_ms = 0;
    const x_for_b = x.raft.handleVoteRequest(req_b);
    const votes_a = [_]raft_node.VoteResponse{ x_for_a, sim.node_(l).raft.handleVoteRequest(req_a), p.raft.handleVoteRequest(req_a) };
    const votes_b = [_]raft_node.VoteResponse{ x_for_b, q.raft.handleVoteRequest(req_b), r.raft.handleVoteRequest(req_b) };
    for (votes_a) |v| if (a.raft.handleVoteResponse(v) == .won) sim.checker.onLeader(a, sim.now);
    for (votes_b) |v| if (b.raft.handleVoteResponse(v) == .won) sim.checker.onLeader(b, sim.now);
}

fn splitVoteScenario() Scenario {
    var scenario = Scenario.calm(21);
    scenario.node_count = 7;
    scenario.restart_permille = 0;
    // Long candidacies, so one is still collecting votes when a guard
    // without the wait finishes.
    scenario.election_timeout_min_ms = 2000;
    scenario.election_timeout_max_ms = 3000;
    scenario.heartbeat_interval_ms = 50;
    return scenario;
}

test "vopr sim: a wiped voter's guard without the boot wait lets a split vote elect two leaders in one term" {
    var sim = try Simulator.init(testing.allocator, splitVoteScenario(), .{ .no_lost_log_wait = true });
    defer sim.deinit();
    try splitVoteAcrossAWipe(&sim, false);
    try testing.expect(hasViolation(&sim, .election_safety));
}

test "vopr sim: with the boot wait, a split vote across a wipe elects no second leader" {
    var sim = try Simulator.init(testing.allocator, splitVoteScenario(), .{});
    defer sim.deinit();
    try splitVoteAcrossAWipe(&sim, false);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

test "vopr sim: a wiped voter's acks to a stale leader count toward no commit" {
    var scenario = Scenario.calm(7);
    scenario.restart_permille = 0;
    scenario.election_timeout_min_ms = 1000;
    scenario.election_timeout_max_ms = 2000;
    scenario.heartbeat_interval_ms = 50;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    sim.scenario.request_percent = 2;
    try runTicks(&sim, 6000);
    sim.scenario.request_percent = 0;
    try runTicks(&sim, 500);
    const l = leaderOf(&sim) orelse return error.NoLeader;
    const x: NodeId = if (l == 1) 2 else 1;
    const c: NodeId = 6 - l - x;
    const lead = sim.node_(l);
    const wiped = sim.node_(x);
    const other = sim.node_(c);
    // L is cut off; C wins the next term with X's vote and commits
    // through X.
    sim.net.isolate(l);
    wiped.raft.last_leader_contact_ms = 0;
    other.raft.last_leader_contact_ms = 0;
    const req = other.raft.startElectionNow().?;
    const v = wiped.raft.handleVoteRequest(req);
    try testing.expect(v.vote_granted);
    try testing.expect(other.raft.handleVoteResponse(v) == .won);
    sim.checker.onLeader(other, sim.now);
    try runTicks(&sim, 200);
    try testing.expect(other.raft.commit_index >= other.raft.log.lastIndex());

    // X loses its disk and restarts; C is cut off, and L, still leading
    // the older term inside its check-quorum window, reaches X.
    sim.wipeNode(wiped);
    try sim.restartNode(wiped);
    sim.net.isolate(c);
    sim.net.isolated[l - 1] = false;
    try testing.expect(lead.raft.role == .leader);
    const op = try sim.workload.nextOp();
    const res = try lead.raft.propose(op.entry_type, 0, op.payload);
    var i: usize = 0;
    while (i < 20 and lead.raft.commit_index < res.index) : (i += 1) try runTicks(&sim, 300);
    // X's acks are guarded, so L commits nothing over C's history.
    try testing.expect(lead.raft.commit_index < res.index);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

test "vopr sim: swarms with two nodes lost at once keep every invariant" {
    var wipes: u64 = 0;
    var seed: u64 = 100;
    while (seed < 112) : (seed += 1) {
        var scenario = Scenario.fromSeed(seed);
        scenario.node_count = 7;
        scenario.wipe_permille = 700;
        scenario.crash_permille = @max(scenario.crash_permille, 3);
        scenario.ticks_safety = 8_000;
        scenario.max_lost_nodes = 2;
        var sim = try Simulator.init(testing.allocator, scenario, .{});
        defer sim.deinit();
        const s = try sim.run();
        if (!s.ok) {
            var buf: [8192]u8 = undefined;
            var w: std.Io.Writer = .fixed(&buf);
            sim.printViolations(&w) catch {};
            std.debug.print("seed {d} (wipes={d}):\n{s}\n", .{ seed, s.wipes, w.buffered() });
        }
        try testing.expect(s.ok);
        wipes += s.wipes;
    }
    try testing.expect(wipes >= 12);
}

test "vopr sim: a wiped voter does not catch up to a new leader's commit index that lags what it acked" {
    var scenario = Scenario.calm(7);
    scenario.restart_permille = 0;
    scenario.election_timeout_min_ms = 1000;
    scenario.election_timeout_max_ms = 2000;
    scenario.heartbeat_interval_ms = 50;
    scenario.msg_delay_min_ms = 40;
    scenario.msg_delay_max_ms = 50;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    sim.scenario.request_percent = 60;
    try runTicks(&sim, 4000);
    const p = leaderOf(&sim) orelse return error.NoLeader;
    const x: NodeId = if (p == 1) 2 else 1;
    const d: NodeId = 6 - p - x;
    const lead = sim.node_(p);
    const wiped = sim.node_(x);
    const behind = sim.node_(d);
    // D catches up fully and crashes; P commits more with X alone.
    sim.scenario.request_percent = 0;
    try runTicks(&sim, 1500);
    sim.crashNode(behind);
    sim.scenario.request_percent = 60;
    try runTicks(&sim, 300);
    sim.scenario.request_percent = 0;
    try runTicks(&sim, 300);
    try testing.expect(lead.raft.commit_index > behind.disk.entries.items.len);
    // P crashes and X loses its disk; P and D come back and P wins the
    // next term with D's vote. P's commit index starts at 0.
    sim.crashNode(lead);
    sim.wipeNode(wiped);
    try sim.restartNode(lead);
    try sim.restartNode(behind);
    behind.raft.last_leader_contact_ms = 0;
    const req = lead.raft.startElectionNow().?;
    const v = behind.raft.handleVoteRequest(req);
    try testing.expect(v.vote_granted);
    try testing.expect(lead.raft.handleVoteResponse(v) == .won);
    sim.checker.onLeader(lead, sim.now);
    // A partial partition: P and D cannot talk; X reaches both.
    sim.net.cutPair(p, d);
    try sim.restartNode(wiped);
    var ticks: u64 = 0;
    while (wiped.raft.lost_log != .none and ticks < 20_000) : (ticks += 1) try sim.tick();
    // P goes down; D and X are left. X must not vote D in while D lacks
    // what P committed with X.
    sim.crashNode(lead);
    try runTicks(&sim, 6000);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

test "vopr sim: stalled candidates past their deadline count no late vote, so a wiped voter's split vote elects no one" {
    var sim = try Simulator.init(testing.allocator, splitVoteScenario(), .{});
    defer sim.deinit();
    try splitVoteAcrossAWipe(&sim, true);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

// Five nodes. Y wins term t+1 with X's and B's votes while A, the term-t
// leader, and Z are cut off; then X and Y lose their disks, B is cut off,
// and A, still leading t, keeps X and Y as guarded followers. A guarded
// node's answer is no evidence: X may count only A, Z and B, and B is
// gone, so X must stay guarded and A cannot win t+1 with X.
test "vopr sim: a guarded member's term check answer counts for nothing" {
    var scenario = Scenario.calm(31);
    scenario.node_count = 5;
    scenario.restart_permille = 0;
    scenario.election_timeout_min_ms = 1000;
    scenario.election_timeout_max_ms = 2000;
    scenario.heartbeat_interval_ms = 50;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    sim.scenario.request_percent = 2;
    try runTicks(&sim, 6000);
    sim.scenario.request_percent = 0;
    try runTicks(&sim, 1000);
    const l = leaderOf(&sim) orelse return error.NoLeader;
    var rest: [4]NodeId = undefined;
    var n: usize = 0;
    for (sim.nodes) |*node| {
        if (node.id == l) continue;
        rest[n] = node.id;
        n += 1;
    }
    const a = sim.node_(l);
    const b = sim.node_(rest[0]);
    const x = sim.node_(rest[1]);
    const y = sim.node_(rest[2]);
    const z = sim.node_(rest[3]);
    for ([_]NodeId{ b.id, x.id, y.id }) |id| {
        sim.net.cutPair(a.id, id);
        sim.net.cutPair(z.id, id);
    }
    for ([_]*SimNode{ b, x, y }) |node| node.raft.last_leader_contact_ms = 0;
    const req_y = y.raft.startElectionNow().?;
    for ([_]*SimNode{ x, b }) |voter| {
        if (y.raft.handleVoteResponse(voter.raft.handleVoteRequest(req_y)) == .won) sim.checker.onLeader(y, sim.now);
    }
    try testing.expectEqual(raft_node.Role.leader, y.raft.role);

    sim.wipeNode(x);
    sim.wipeNode(y);
    try sim.restartNode(x);
    try sim.restartNode(y);
    sim.net.isolate(b.id);
    for ([_]NodeId{ x.id, y.id }) |id| {
        for ([_]NodeId{ a.id, z.id }) |other| {
            sim.net.pair_cut[id - 1][other - 1] = false;
            sim.net.pair_cut[other - 1][id - 1] = false;
        }
    }
    try runTicks(&sim, 6000);
    try testing.expect(x.raft.lost_log != .none);

    // A stands for t+1 with Z and X.
    a.raft.observeTime(sim.now);
    for ([_]*SimNode{ x, z }) |node| node.raft.last_leader_contact_ms = 0;
    const req_a = a.raft.startElectionNow().?;
    for ([_]*SimNode{ x, z }) |voter| {
        if (a.raft.handleVoteResponse(voter.raft.handleVoteRequest(req_a)) == .won) sim.checker.onLeader(a, sim.now);
    }
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

// A guarded node refuses a newer candidate's vote but takes its term, so
// the stale leader it was following hears that term from it and steps
// down rather than keep it as a follower.
test "vopr sim: a guarded node's refused vote moves it past a stale leader's term" {
    var scenario = Scenario.calm(41);
    scenario.restart_permille = 0;
    scenario.election_timeout_min_ms = 1000;
    scenario.election_timeout_max_ms = 2000;
    scenario.heartbeat_interval_ms = 50;
    var sim = try Simulator.init(testing.allocator, scenario, .{});
    defer sim.deinit();
    try runTicks(&sim, 5000);
    const l = leaderOf(&sim) orelse return error.NoLeader;
    const x: NodeId = if (l == 1) 2 else 1;
    const c: NodeId = 6 - l - x;
    const lead = sim.node_(l);
    const wiped = sim.node_(x);
    const other = sim.node_(c);
    sim.net.cutPair(l, c);
    sim.wipeNode(wiped);
    try sim.restartNode(wiped);
    try runTicks(&sim, 300);
    try testing.expect(wiped.raft.lost_log != .none);
    try testing.expectEqual(raft_node.Role.leader, lead.raft.role);
    // C stands; the guarded X refuses but takes C's term.
    other.raft.last_leader_contact_ms = 0;
    const req = other.raft.startElectionNow().?;
    try testing.expect(!wiped.raft.handleVoteRequest(req).vote_granted);
    try runTicks(&sim, 300);
    try testing.expect(lead.raft.role != .leader);
    try testing.expectEqual(@as(usize, 0), sim.checker.violations.items.len);
}

test "vopr sim: a truncated uncommitted config leaves the node with the members the log still names" {
    // Default seed 165: seven nodes, no crashes. A deposed leader's config
    // is truncated; without a recorded membership to fall back to, three
    // nodes were left with none and the group stopped electing.
    var sim = try Simulator.init(testing.allocator, Scenario.fromSeed(165), .{});
    defer sim.deinit();
    const s = try sim.run();
    if (!s.ok) {
        var buf: [8192]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        sim.printViolations(&w) catch {};
        std.debug.print("{s}\n", .{w.buffered()});
    }
    try testing.expect(s.ok);
}
