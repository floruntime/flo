//! The boot-replay contract for one durable entry: restore it into the Raft
//! log, and below the watermark apply it, with the watermark gate, repair-skip
//! pass-over and stamp-regression halt that decide what a replay does. The
//! shard plugs `applyEntryCore` on a partition; the deterministic simulator
//! plugs its checker model. Both drive `replayOneEntry`, so the real decisions
//! run in the sim instead of a parallel copy.
//!
//! LEAF MODULE, shared with src/vopr/simulator.zig: it must NOT import shard.zig
//! or anything that pulls in the server stack (networking, protocol handlers,
//! sinks, channels, processing). Its imports stay leaves (raft/log, ual/entry,
//! repair, silent_loss, plant, stdx, durable_log for its read-error class) so
//! the vopr binary, which never links shard.zig, can compile it.

const std = @import("std");
const log = @import("stdx").log;
const entry_mod = @import("../storage/ual/entry.zig");
const silent_loss = @import("silent_loss.zig");
const repair = @import("repair.zig");
const plant = @import("../plant.zig");
const RaftLog = @import("../raft/log.zig").RaftLog;
const DurableLog = @import("../storage/durable_log.zig").DurableLog;

/// An entry every replica fails the same way, and why. A shard halted at
/// it applies nothing from it on until a repair.
pub const Halt = struct {
    index: u64,
    term: u64,
    /// The entry's checksum: with its index and term, what a repair names.
    crc: u32,
    /// The inbound message it halted at, for a `channel_inbound`: what a
    /// repair of only that message names.
    message: ?Applying = null,
    len: usize = 0,
    buf: [192]u8 = undefined,

    pub fn init(e: *const entry_mod.Entry, comptime fmt: []const u8, args: anytype) Halt {
        var h: Halt = .{ .index = e.header.index, .term = e.header.term, .crc = e.header.crc32c };
        // Too long for the buffer: kept cut short.
        h.len = if (std.fmt.bufPrint(&h.buf, fmt, args)) |w| w.len else |_| h.buf.len;
        return h;
    }

    pub fn reason(self: *const Halt) []const u8 {
        return self.buf[0..self.len];
    }
};

/// What an apply came to, for the shard: whether to go on.
pub const Verdict = union(enum) {
    applied,
    /// An index the partition already holds: counted, nothing lost.
    duplicate,
    /// No replica can apply it: why.
    halt: []const u8,
    /// This node couldn't, for want of memory or a disk; another may.
    local: silent_loss.Damage,
};

/// What boot replay did: the last index it applied, and the damage it
/// stopped at, if any.
pub const ReplayOutcome = struct {
    applied_through: u64 = 0,
    damage: ?silent_loss.Damage = null,
    /// The entry replay halted at: the rest of the log is restored, and
    /// none of it applied.
    halt: ?Halt = null,
    /// The stamp of the last entry applied.
    last_stamp: u64 = 0,
    /// Another shard's boot failed, and this one stopped early.
    cancelled: bool = false,
    /// A segment that would not open for want of resources.
    blocked: ?BlockedRead = null,
    /// Entries read back, for the progress lines.
    entries: u64 = 0,
    started_ms: u64 = 0,
    next_line_ms: u64 = 0,
};

pub const BlockedRead = struct { name: []const u8, err: anyerror };

/// Every durable entry is part of the Raft log; restore it there. A later
/// copy of an index the log already holds is ignored; a gap is damage.
/// The outcome of restoring one durable entry into the Raft log.
pub const Restore = union(enum) {
    /// Restored, or a duplicate the log already holds (ignored).
    ok,
    /// This node could not restore it, and no restart will change that: a hole,
    /// or a segment entry the log cannot represent. The boot stops, damaged.
    damage: silent_loss.Damage,
    /// This node ran out of a resource restoring it (memory, file handles).
    /// Another try, once the resource is free, will succeed, so the boot is
    /// refused rather than booted over a lost entry.
    blocked: BlockedRead,
};

pub fn restoreRaftEntry(shard_id: u16, raft_log: *RaftLog, e: *const entry_mod.Entry) Restore {
    _ = raft_log.append(e) catch |err| switch (err) {
        // A non-increasing index (a truncation recorded in a later segment, or an
        // entry re-recorded after a term change) fails `append`'s index check
        // (src/raft/log.zig:126) before `ual.append`, so it lands here, not as
        // the ual's IndexNotIncreasing: a copy the log already holds is ignored,
        // a true gap is damage.
        error.IndexGap => {
            const tip = raft_log.lastIndex();
            if (e.header.index <= tip) {
                log.err("shard {d}: segments hold index {d} twice (log tip {d}); the later copy is ignored", .{ shard_id, e.header.index, tip });
                return .ok;
            }
            return .{ .damage = silent_loss.Damage.init(.index_hole, "no segment holds entries {d}..{d}", .{ tip + 1, e.header.index - 1 }) };
        },
        // Any other append failure: a resource shortage (out of memory) is
        // retryable, so refuse the boot — the same resource-vs-fault split the
        // segment reads use (bootRefuses). A deterministic failure (an entry too
        // large for the ring, or a RaftLog/ual index inconsistency) is damage.
        else => {
            if (DurableLog.readBlockClass(err) == .resource) {
                log.err("shard {d}: out of resources restoring index {d} ({s}); refusing the boot to retry", .{ shard_id, e.header.index, @errorName(err) });
                return .{ .blocked = .{ .name = "raft-log restore", .err = err } };
            }
            log.err("shard {d}: cannot restore index {d}: {s}", .{ shard_id, e.header.index, @errorName(err) });
            return .{ .damage = silent_loss.Damage.init(.segment_unreadable, "index {d} will not restore ({s})", .{ e.header.index, @errorName(err) }) };
        },
    };
    return .ok;
}

/// Whether a committed repair skips `e` on this shard. A skip entry itself
/// is never skipped: it changes nothing a pass-over would leave out.
pub fn skippedHere(skips: *const repair.Skips, e: *const entry_mod.Entry) bool {
    return !isSkip(e) and skips.skips(e);
}

pub fn isSkip(e: *const entry_mod.Entry) bool {
    return e.header.entry_type == @backingInt(entry_mod.EntryType.repair_skip);
}

/// How a replay turns an entry into state, plugged in by the source: the
/// applier below the watermark, and the pass-over a repair skip takes.
/// Production plugs `applyEntryCore` on a `Partition` (wrapped in the crash
/// marker and `verdictOf`); the simulator plugs its checker model. The restore,
/// the watermark gate, the repair-skip detection and the stamp-regression halt
/// are `replayOneEntry`'s decisions, shared by both so the sim drives the real
/// ones instead of a parallel copy.
pub const ReplayApply = struct {
    ctx: *anyopaque,
    /// Apply `entry` and report the verdict.
    apply: *const fn (ctx: *anyopaque, entry: *const entry_mod.Entry) Verdict,
    /// A repair skips `entry`: take it out of the applied set without running
    /// it. Non-null is damage this node could not even do that on.
    pass_over: *const fn (ctx: *anyopaque, entry: *const entry_mod.Entry) ?silent_loss.Damage,
    /// Set (or clear, null) the skip a repair names in the inbound about to
    /// apply: a skipped `channel_inbound` applies its skip, not nothing
    /// (`channels.zig`). Null for a source that holds no skips.
    inbound_skip: ?*const fn (ctx: *anyopaque, skip: ?InboundSkip) void = null,
    /// The inbound message an apply that halted was running, if any.
    halting_message: ?*const fn (ctx: *anyopaque) ?Applying = null,
};

pub const InboundSkip = @import("../contract/store.zig").InboundSkip;
pub const Applying = @import("../contract/store.zig").Applying;

/// Everything a replay needs that isn't the entry. One value per the two call
/// sites, so same-typed inputs can't be transposed.
pub const ReplayCtx = struct {
    raft_log: *RaftLog,
    watermark: u64,
    skips: *const repair.Skips,
    shard_id: u16,
    apply: ReplayApply,
};

/// Restore one durable entry, and if it is at or below the watermark with no
/// halt open, apply it — the production boot-replay decision for one entry,
/// shared so the simulator drives the same watermark gate, repair-skip
/// pass-over, stamp-regression halt and verdict handling. `out.damage` stops
/// the replay; `out.halt` restores the rest without applying it; neither is set
/// on a clean apply. The file (or the sim's disk) reading stays with the caller.
pub fn replayOneEntry(ctx: ReplayCtx, entry: *const entry_mod.Entry, out: *ReplayOutcome) void {
    // Every durable entry is part of the Raft log.
    switch (restoreRaftEntry(ctx.shard_id, ctx.raft_log, entry)) {
        .ok => {},
        .damage => |d| {
            out.damage = d;
            return;
        },
        .blocked => |b| {
            out.blocked = b;
            return;
        },
    }
    // Above the watermark: in the log, not yet in the projections. Past a halt:
    // in the log, and nothing of it applies until a repair.
    const mark = if (comptime plant.active == .replay_watermark_off_by_one) ctx.watermark + 1 else ctx.watermark;
    if (entry.header.index > mark or out.halt != null) return;
    // A repair skips an inbound: its skip applies, as live (`channels.zig`).
    const skipped_inbound = skippedHere(ctx.skips, entry) and entry.header.entry_type == @backingInt(entry_mod.EntryType.channel_inbound) and ctx.apply.inbound_skip != null;
    if (skipped_inbound) ctx.apply.inbound_skip.?(ctx.apply.ctx, ctx.skips.inboundOf(entry).?);
    defer if (skipped_inbound) ctx.apply.inbound_skip.?(ctx.apply.ctx, null);
    // A repair skips anything else: taken as a halt on it left a replica that
    // halted live, and its applier never runs.
    if (!skipped_inbound and skippedHere(ctx.skips, entry)) {
        if (ctx.apply.pass_over(ctx.apply.ctx, entry)) |damage| {
            out.damage = damage;
            return;
        }
        log.info("shard {d}: entry {d} is skipped by a repair; passing over it", .{ ctx.shard_id, entry.header.index });
        out.applied_through = entry.header.index;
        return;
    }
    if (out.last_stamp != 0 and entry.header.timestamp_ns <= out.last_stamp and !isSkip(entry)) {
        out.halt = Halt.init(entry, "entry {d}: its stamp {d} is not after the last applied entry's, {d} (a Flo bug)", .{ entry.header.index, entry.header.timestamp_ns, out.last_stamp });
        return;
    }
    switch (ctx.apply.apply(ctx.apply.ctx, entry)) {
        .applied => {},
        // At boot, segments replay in index order, so every entry should apply
        // exactly once; a duplicate means overlapping segments or a wrong
        // watermark.
        .duplicate => {
            const type_name = std.enums.tagName(entry_mod.EntryType, @fromBackingInt(@intCast(entry.header.entry_type))) orelse "unknown";
            log.warn("shard {d}: replayed entry {d} ({s}) was skipped at apply (the partition rejected it as not increasing); its effect is absent after this boot", .{ ctx.shard_id, entry.header.index, type_name });
        },
        .halt => |why| {
            const type_name = std.enums.tagName(entry_mod.EntryType, @fromBackingInt(@intCast(entry.header.entry_type))) orelse "unknown";
            const msg = if (ctx.apply.halting_message) |f| f(ctx.apply.ctx) else null;
            var mb: [64]u8 = undefined;
            const at = if (msg) |m| std.fmt.bufPrint(&mb, ", message {d} from shard {d}", .{ m.seq, m.from }) catch "" else "";
            out.halt = Halt.init(entry, "entry {d} ({s}){s}: {s} (a Flo bug)", .{ entry.header.index, type_name, at, why });
            out.halt.?.message = msg;
            return;
        },
        .local => |damage| {
            out.damage = damage;
            return;
        },
    }
    // A skip is exempt from the stamp check, so it may be stamped before the
    // entry ahead of it; the applied stamp never goes back, as live.
    out.last_stamp = @max(out.last_stamp, entry.header.timestamp_ns);
    out.applied_through = entry.header.index;
}

test "replay: a boot replay halts at an entry stamped no later than the one before it" {
    const allocator = std.testing.allocator;
    var raft_log = try RaftLog.init(allocator, 4096);
    defer raft_log.deinit();
    var skips: repair.Skips = .{};
    defer skips.deinit(allocator);

    // An applier that takes every entry, so only the stamp halt can stop replay.
    const TakeAll = struct {
        fn apply(_: *anyopaque, _: *const entry_mod.Entry) Verdict {
            return .applied;
        }
        fn passOver(_: *anyopaque, _: *const entry_mod.Entry) ?silent_loss.Damage {
            return null;
        }
    };
    var unused: u8 = 0;
    const ctx: ReplayCtx = .{
        .raft_log = &raft_log,
        .watermark = 100,
        .skips = &skips,
        .shard_id = 0,
        .apply = .{ .ctx = &unused, .apply = TakeAll.apply, .pass_over = TakeAll.passOver },
    };
    var out: ReplayOutcome = .{};

    const stamp = struct {
        fn at(index: u64, ts: u64) entry_mod.Entry {
            var e = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, index, ts, "");
            e.header.crc32c = e.computeCrc();
            return e;
        }
    }.at;

    // Rising stamps apply cleanly.
    var e1 = stamp(1, 100);
    replayOneEntry(ctx, &e1, &out);
    var e2 = stamp(2, 200);
    replayOneEntry(ctx, &e2, &out);
    try std.testing.expect(out.halt == null);
    try std.testing.expectEqual(@as(u64, 2), out.applied_through);
    try std.testing.expectEqual(@as(u64, 200), out.last_stamp);

    // An entry stamped at or before the last applied one halts replay there,
    // and nothing from it on applies. The durable log is monotonic by design
    // (a cut is made durable before any entry above it is), so this guards a
    // Flo bug, not a crash this reaches by itself.
    var e3 = stamp(3, 200);
    replayOneEntry(ctx, &e3, &out);
    try std.testing.expect(out.halt != null);
    try std.testing.expect(std.mem.indexOf(u8, out.halt.?.reason(), "is not after the last applied entry") != null);
    try std.testing.expectEqual(@as(u64, 2), out.applied_through);
}

test "replay: an out-of-memory restore refuses the boot instead of dropping the entry" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var raft_log = try RaftLog.init(failing.allocator(), 4096);
    defer raft_log.deinit();
    // Let deinit free cleanly after the induced failure (runs before deinit).
    defer failing.fail_index = std.math.maxInt(usize);

    // Fail the next allocation — the append's term-run growth — so the restore
    // hits OutOfMemory, a resource shortage.
    failing.fail_index = failing.alloc_index;
    var e = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 100, "");
    e.header.crc32c = e.computeCrc();

    // It must refuse the boot (blocked), not drop the entry: a restart with
    // memory free retries. The log still holds nothing — the entry is not lost.
    const result = restoreRaftEntry(0, &raft_log, &e);
    try std.testing.expect(result == .blocked);
    try std.testing.expectEqual(@as(anyerror, error.OutOfMemory), result.blocked.err);
    try std.testing.expectEqual(@as(u64, 0), raft_log.lastIndex());

    // The retry (memory now free) restores it.
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect(restoreRaftEntry(0, &raft_log, &e) == .ok);
    try std.testing.expectEqual(@as(u64, 1), raft_log.lastIndex());
}

test "replay: an entry too large for the ring is damage, not a silent skip" {
    var raft_log = try RaftLog.init(std.testing.allocator, 512);
    defer raft_log.deinit();

    // Payload past the ring capacity → ual.append returns EntryTooLarge, a
    // deterministic fault no restart fixes: damage, so the boot stops rather
    // than continuing over a dropped entry.
    var payload: [1024]u8 = undefined;
    var e = entry_mod.buildEntry(.raft_noop, entry_mod.Flags.NONE, 1, 1, 100, &payload);
    e.header.crc32c = e.computeCrc();
    try std.testing.expect(restoreRaftEntry(0, &raft_log, &e) == .damage);
}

test "replay: a skipped inbound replays with its skip set, and the skip is cleared after" {
    const allocator = std.testing.allocator;
    var raft_log = try RaftLog.init(allocator, 4096);
    defer raft_log.deinit();
    var skips: repair.Skips = .{};
    defer skips.deinit(allocator);
    var e = entry_mod.buildEntry(.channel_inbound, entry_mod.Flags.NONE, 1, 1, 10, "x");
    e.header.crc32c = e.computeCrc();
    var skip = repair.Skip.of(&e, "");
    skip.seq = 4;
    try skips.add(allocator, &skip);
    const Seen = struct {
        var during: ?InboundSkip = null;
        var now: ?InboundSkip = null;
        var applied: bool = false;
        fn apply(_: *anyopaque, _: *const entry_mod.Entry) Verdict {
            during = now;
            applied = true;
            return .applied;
        }
        fn passOver(_: *anyopaque, _: *const entry_mod.Entry) ?silent_loss.Damage {
            return null;
        }
        fn set(_: *anyopaque, s: ?InboundSkip) void {
            now = s;
        }
    };
    var unused: u8 = 0;
    var out: ReplayOutcome = .{};
    replayOneEntry(.{ .raft_log = &raft_log, .watermark = 1, .skips = &skips, .shard_id = 0, .apply = .{ .ctx = &unused, .apply = Seen.apply, .pass_over = Seen.passOver, .inbound_skip = Seen.set } }, &e, &out);
    try std.testing.expect(Seen.applied);
    try std.testing.expect(Seen.during.?.skips(4) and !Seen.during.?.skips(3));
    try std.testing.expectEqual(@as(u64, 1), Seen.during.?.index);
    try std.testing.expect(Seen.now == null);
    try std.testing.expectEqual(@as(u64, 1), out.applied_through);
}
