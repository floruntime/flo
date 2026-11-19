//! Processing jobs as the server keeps them: the processing machine
//! (`processing/sinks_machine.zig`) on a store of each replica's own, the
//! checker comparing its families' checksums, and each entry checked
//! against a ledger of the jobs the entries made, kept apart from the
//! machine:
//!
//! - a submit stores its job with the status it names, or failed when its
//!   definition doesn't parse; past the shard's limit it is refused;
//! - a stop or cancel changes only a running job; one naming the status
//!   the job has is a no-op; any other is refused;
//! - a remove drops only a job that isn't running;
//! - the jobs the store holds are the ledger's, with the ledger's statuses,
//!   never more than the limit.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const store_mod = @import("../contract/store.zig");
const machine_mod = @import("../contract/machine.zig");
const view_mod = @import("../contract/view.zig");
const pm = @import("../processing/sinks_machine.zig");
const families = @import("families.zig");

const Allocator = std.mem.Allocator;
const EntryType = entry_mod.EntryType;
const Replica = families.Replica;
const JobStatus = pm.JobStatus;
const NS: @import("../namespace/table.zig").Id = .default;

/// The jobs the workload names, and the most a shard holds here: small,
/// so the limit is reached.
pub const JOBS = [_][]const u8{ "j0", "j1", "j2", "j3", "j4", "j5" };
pub const LIMIT: usize = 4;

const GOOD_YAML = "kind: Processing\nname: p\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n";
/// A definition this build refuses (a range of partitions): stored as
/// failed.
const BAD_YAML = "kind: Processing\nname: p\nsources: [{stream: {name: in, partitions: \"0-1\"}}]\nsinks: [{stream: {name: out}}]\n";

pub const Jobs = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    /// Each job the entries stored, by index into `JOBS`, and its status.
    ledger: [JOBS.len]?JobStatus = @splat(null),

    pub const FAMILIES = [_]u16{ pm.JobFam.ID, pm.CursorFam.ID };
    pub const NAMES = [_][]const u8{ "processing.job", "processing.cursor" };

    pub const Kind = enum { submit, stop, cancel, remove };

    pub const Effect = struct {
        kind: Kind,
        refused: bool,
        broke: ?[]const u8 = null,
    };

    pub const Counts = struct {
        applied: [std.enums.values(Kind).len]u64 = @splat(0),
        refused: [std.enums.values(Kind).len]u64 = @splat(0),

        pub fn count(self: *Counts, o: Effect) void {
            const k = @backingInt(o.kind);
            if (o.refused) self.refused[k] += 1 else self.applied[k] += 1;
        }

        pub fn add(self: *Counts, other: Counts) void {
            for (&self.applied, other.applied) |*a, b| a.* += b;
            for (&self.refused, other.refused) |*a, b| a.* += b;
        }
    };

    pub fn init(allocator: Allocator) Jobs {
        var j: Jobs = .{ .store = store_mod.Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
        j.store.strict_keys = true;
        // Every replica of the run alike: the machine's limit is a constant
        // of the build, lowered here so the workload reaches it.
        pm.max_jobs = LIMIT;
        return j;
    }

    pub fn deinit(self: *Jobs) void {
        self.store.deinit();
        self.scratch.deinit();
    }

    /// Replay re-applies from the start: the ledger goes with the store.
    pub fn reset(self: *Jobs) void {
        self.store.clear();
        self.ledger = @splat(null);
    }

    pub fn owns(entry_type: u8) bool {
        return switch (@as(EntryType, @fromBackingInt(entry_type))) {
            .processing_submit, .processing_stop, .processing_cancel, .processing_remove => true,
            else => false,
        };
    }

    fn kindOf(t: u8) Kind {
        return switch (@as(EntryType, @fromBackingInt(t))) {
            .processing_submit => .submit,
            .processing_stop => .stop,
            .processing_cancel => .cancel,
            else => .remove,
        };
    }

    fn jobIndex(key: []const u8) ?usize {
        for (JOBS, 0..) |name, i| if (std.mem.eql(u8, name, key)) return i;
        return null;
    }

    fn held(self: *const Jobs) usize {
        var n: usize = 0;
        for (self.ledger) |s| n += @intFromBool(s != null);
        return n;
    }

    /// What the ledger says entry `kind` on job `j` does: its new status
    /// (null for gone), or that it is refused.
    const Want = union(enum) { refused, status: ?JobStatus };

    fn predict(self: *const Jobs, kind: Kind, j: usize, value: []const u8) Want {
        const now = self.ledger[j];
        switch (kind) {
            .submit => {
                if (now == null and self.held() >= LIMIT) return .refused;
                const yaml = value[7 + std.mem.readInt(u16, value[5..7], .little) ..];
                if (std.mem.eql(u8, yaml, BAD_YAML)) return .{ .status = .failed };
                return .{ .status = std.enums.fromInt(JobStatus, value[0]).? };
            },
            .stop, .cancel => {
                const to: JobStatus = if (kind == .stop) .stopped else .cancelled;
                const s = now orelse return .refused;
                if (s == to) return .{ .status = s };
                if (s != .running) return .refused;
                return .{ .status = to };
            },
            .remove => {
                const s = now orelse return .refused;
                if (s == .running) return .refused;
                return .{ .status = null };
            },
        }
    }

    /// Apply `e` as the shard does, then check it against the ledger. A
    /// refusal must leave every family as it was.
    pub fn apply(self: *Jobs, e: *const entry_mod.Entry, r: Replica) (persistence.ApplyError || error{RefusalChangedState})!Effect {
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
        const kind = kindOf(e.header.entry_type);
        const j = jobIndex(cmd.key) orelse return error.Halt;
        const want = self.predict(kind, j, cmd.value);
        const before = self.commits();
        // Held across the apply, which may rewrite or free its record, so
        // copied out of the store.
        var prev_arena = std.heap.ArenaAllocator.init(self.store.allocator);
        defer prev_arena.deinit();
        const prev = if (pm.jobOf(.{ .store = &self.store }, NS, cmd.key)) |job| try copyJob(prev_arena.allocator(), job) else null;
        var out: persistence.Result = .{};
        out.reset(e);
        try machine_mod.applyEntry(&pm.machine, &self.store, &self.scratch, e, &out);
        var effect: Effect = .{ .kind = kind, .refused = out.refused() != null };
        if (effect.refused) {
            if (!std.mem.eql(u64, &before, &self.commits())) return error.RefusalChangedState;
            if (want != .refused) effect.broke = "the processing machine refused a job entry the ledger applies";
            if (r.mutant == .job_limit_unchecked and kind == .submit) try self.storeAnyway(e);
        } else {
            if (want == .refused) effect.broke = "the processing machine applied a job entry the ledger refuses";
            if (r.mutant == .job_stop_keeps_running and (kind == .stop or kind == .cancel)) if (prev) |p| try self.putBack(cmd.key, p);
            if (r.mutant == .job_remove_keeps_job and kind == .remove) if (prev) |p| try self.putBack(cmd.key, p);
        }
        switch (want) {
            .refused => {},
            .status => |s| self.ledger[j] = s,
        }
        if (effect.broke == null) effect.broke = self.rule();
        return effect;
    }

    /// The jobs the store holds against the ledger's.
    fn rule(self: *Jobs) ?[]const u8 {
        const rv: view_mod.ReadView = .{ .store = &self.store };
        for (JOBS, self.ledger) |name, want| {
            const got = pm.jobOf(rv, NS, name);
            const s = want orelse {
                if (got != null) return "a job the ledger doesn't hold is stored";
                continue;
            };
            const g = got orelse return "a job the ledger holds is gone";
            if (pm.statusOf(g) != s) return "a job's status is not the ledger's";
        }
        if (self.store.family(pm.JobFam.ID).count > LIMIT) return "the shard holds more jobs than its limit";
        return null;
    }

    fn copyJob(a: Allocator, j: pm.Job.Value) Allocator.Error!pm.Job.Value {
        var c = j;
        c.name = try a.dupe(u8, j.name);
        c.home = try a.dupe(u8, j.home);
        c.yaml = try a.dupe(u8, j.yaml);
        c.failure = try a.dupe(u8, j.failure);
        return c;
    }

    /// `job_stop_keeps_running` and `job_remove_keeps_job`: the job as it
    /// was before the entry, put back.
    fn putBack(self: *Jobs, key: []const u8, job: pm.Job.Value) persistence.ApplyError!void {
        var kb: [64]u8 = undefined;
        const k = view_mod.idKey(&kb, NS, key) catch return error.Halt;
        const bytes = try self.store.allocator.alloc(u8, pm.Job.encodedSize(job) catch return error.Halt);
        _ = pm.Job.encodeInto(bytes, job) catch return error.Halt;
        if (self.store.find(pm.JobFam.ID, k)) |node| return self.store.replace(pm.JobFam.ID, node, bytes);
        const rec = self.store.allocRecord(k) catch |err| {
            self.store.allocator.free(bytes);
            return err;
        };
        self.store.link(pm.JobFam.ID, rec, bytes);
    }

    /// `job_limit_unchecked`: a submit refused at the limit, stored anyway.
    fn storeAnyway(self: *Jobs, e: *const entry_mod.Entry) persistence.ApplyError!void {
        const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
        const v = cmd.value;
        const job: pm.Job.Value = .{ .name = "p", .status = v[0], .batch_size = 0, .home = "default", .created_at_ms = e.header.timestamp_ns / std.time.ns_per_ms, .yaml = GOOD_YAML, .failure = "" };
        try self.putBack(cmd.key, job);
    }

    fn commits(self: *const Jobs) [FAMILIES.len]u64 {
        var out: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &out) |fam, *c| c.* = self.store.families[fam].commits;
        return out;
    }

    pub fn checksums(self: *Jobs) [FAMILIES.len]u64 {
        var sums: [FAMILIES.len]u64 = undefined;
        for (FAMILIES, &sums) |fam, *sum| sum.* = store_mod.checksum(&self.store, fam);
        return sums;
    }
};

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

/// A job entry for one of `JOBS`, as its handler proposes it: mostly
/// submits of running jobs and stops, now and then a failing definition,
/// a cancel or a remove.
pub fn make(r: std.Random, buf: []u8) ?Made {
    const key = JOBS[r.uintLessThan(usize, JOBS.len)];
    var vb: [256]u8 = undefined;
    const roll = r.uintLessThan(u8, 100);
    const etype: EntryType, const value: []const u8 = if (roll < 40) blk: {
        const yaml = if (r.uintLessThan(u8, 8) == 0) BAD_YAML else GOOD_YAML;
        const status: JobStatus = if (r.uintLessThan(u8, 4) == 0) .stopped else .running;
        break :blk .{ .processing_submit, pm.encodeSubmit(&vb, status, 0, "default", yaml).? };
    } else if (roll < 65) .{ .processing_stop, &.{@backingInt(JobStatus.stopped)} } else if (roll < 80) .{ .processing_cancel, &.{@backingInt(JobStatus.cancelled)} } else .{ .processing_remove, "" };
    const cmd: entry_mod.CommandPayload = .{ .namespace_id = NS, .key_length = @intCast(key.len), .value_length = @intCast(value.len), .key = key, .value = value };
    const n = cmd.serialize(buf) orelse return null;
    return .{ .entry_type = etype, .payload = buf[0..n] };
}
