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
//! repair, silent_loss, plant, stdx) so the vopr binary, which never links
//! shard.zig, can compile it.

const std = @import("std");
const log = @import("stdx").log;
const entry_mod = @import("../storage/ual/entry.zig");
const silent_loss = @import("silent_loss.zig");
const repair = @import("repair.zig");
const plant = @import("../plant.zig");
const RaftLog = @import("../raft/log.zig").RaftLog;

/// An entry every replica fails the same way, and why. A shard halted at
/// it applies nothing from it on until a repair.
pub const Halt = struct {
    index: u64,
    term: u64,
    /// The entry's checksum: with its index and term, what a repair names.
    crc: u32,
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
pub fn restoreRaftEntry(shard_id: u16, raft_log: *RaftLog, e: *const entry_mod.Entry) ?silent_loss.Damage {
    _ = raft_log.append(e) catch |err| switch (err) {
        error.IndexGap => {
            const tip = raft_log.lastIndex();
            if (e.header.index <= tip) {
                log.err("shard {d}: segments hold index {d} twice (log tip {d}); the later copy is ignored", .{ shard_id, e.header.index, tip });
                return null;
            }
            return silent_loss.Damage.init(.index_hole, "no segment holds entries {d}..{d}", .{ tip + 1, e.header.index - 1 });
        },
        else => log.err("shard {d}: cannot restore index {d}: {s}", .{ shard_id, e.header.index, @errorName(err) }),
    };
    return null;
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
};

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
    if (restoreRaftEntry(ctx.shard_id, ctx.raft_log, entry)) |damage| {
        out.damage = damage;
        return;
    }
    // Above the watermark: in the log, not yet in the projections. Past a halt:
    // in the log, and nothing of it applies until a repair.
    const mark = if (comptime plant.active == .replay_watermark_off_by_one) ctx.watermark + 1 else ctx.watermark;
    if (entry.header.index > mark or out.halt != null) return;
    // A repair skips it: taken as a halt on it left a replica that halted live,
    // and its applier never runs.
    if (skippedHere(ctx.skips, entry)) {
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
            out.halt = Halt.init(entry, "entry {d} ({s}): {s} (a Flo bug)", .{ entry.header.index, type_name, why });
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
