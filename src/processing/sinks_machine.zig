//! The processing machine: each job, as its submit made it and its stop,
//! cancel or remove changed it, on its home shard; and each pipeline's
//! source cursor, with the sink writes that move it.
//!
//! - `processing.job`, keyed (ns id, job id): the job's name, status, batch
//!   size, home namespace, created-at stamp and definition, and why it
//!   failed, if it did. A job lives on its first source's shard, the slot
//!   its id carries.
//! - `processing.cursor`, keyed (ns id, pipeline key): where the pipeline
//!   has read to, and how many records its batches have covered.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const schema = @import("../protocol/schema.zig");
const pe = @import("../protocol/processing_entries.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const delivery = @import("../contract/delivery.zig");
const NamespaceId = @import("../namespace/table.zig").Id;
const parser = @import("parser.zig");
const MAX_PERSIST_PAYLOAD = persistence.MAX_PERSIST_PAYLOAD;

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const ReadView = view_mod.ReadView;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

pub const JobStatus = enum(u8) {
    running = 0,
    stopped = 1,
    cancelled = 2,
    failed = 3,
    completed = 4,

    pub fn toString(self: JobStatus) []const u8 {
        return switch (self) {
            .running => "RUNNING",
            .stopped => "STOPPED",
            .cancelled => "CANCELLED",
            .failed => "FAILED",
            .completed => "COMPLETED",
        };
    }
};

/// A processing_submit entry's value: `[status:u8][batch_size:u32]`
/// `[home_len:u16][home][definition]`, in `buf`; null if it doesn't fit.
pub fn encodeSubmit(buf: []u8, status: JobStatus, batch_size: u32, home: []const u8, yaml: []const u8) ?[]const u8 {
    const n = 7 + home.len + yaml.len;
    if (n > buf.len or home.len > std.math.maxInt(u16)) return null;
    buf[0] = @backingInt(status);
    std.mem.writeInt(u32, buf[1..5], batch_size, .little);
    std.mem.writeInt(u16, buf[5..7], @intCast(home.len), .little);
    @memcpy(buf[7..][0..home.len], home);
    @memcpy(buf[7 + home.len ..][0..yaml.len], yaml);
    return buf[0..n];
}

/// The most of a failure reason a job keeps.
pub const MAX_FAILURE_REASON = 1024;

/// A job, as its entries made it.
pub const Job = Record(&.{
    .{ .name = "name", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "status", .T = Int(u8) },
    .{ .name = "batch_size", .T = Int(u32) },
    // The namespace its submit was parsed in: what its definition is
    // parsed in again.
    .{ .name = "home", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    .{ .name = "created_at_ms", .T = Int(u64) },
    .{ .name = "yaml", .T = Bytes(MAX_PERSIST_PAYLOAD) },
    // Why it failed when its submit applied; empty unless it did.
    .{ .name = "failure", .T = Bytes(MAX_FAILURE_REASON) },
});

pub const JobFam = view_mod.Family(9, "processing.job", .id, delivery.Stored(Job));

/// Most jobs one shard holds. Every held job counts, stopped or not: each
/// keeps its definition, up to 64 KiB, until `processing.remove` drops it.
/// A submit past it is refused, in log order. A var only so tests can
/// lower it; every replica of a build has the same.
pub var max_jobs: usize = 10_000;

/// Job `id` in namespace `ns` as applied, or null.
pub fn jobOf(read: ReadView, ns: NamespaceId, id: []const u8) ?Job.Value {
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, id) catch return null;
    const v = (read.get(JobFam, k) catch null) orelse return null;
    return v.v;
}

pub fn statusOf(j: Job.Value) JobStatus {
    return std.enums.fromInt(JobStatus, j.status) orelse .failed;
}

/// A pipeline's source cursor, keyed by its job's namespace and its
/// pipeline key (`pe.SinkBatch`).
pub const Cursor = Record(&.{
    .{ .name = "a", .T = Int(u64) },
    .{ .name = "b", .T = Int(u64) },
    // The source records its batches covered, ever.
    .{ .name = "records", .T = Int(u64) },
});

pub const CursorFam = view_mod.Family(63, "processing.cursor", .id, delivery.Stored(Cursor));

pub const At = struct { a: u64 = 0, b: u64 = 0 };

/// The source records pipeline `key`'s batches have covered.
pub fn recordsOf(read: ReadView, ns: NamespaceId, key: []const u8) u64 {
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, key) catch return 0;
    const v = (read.get(CursorFam, k) catch null) orelse return 0;
    return v.v.records;
}

/// Pipeline `key`'s cursor in namespace `ns` as applied, or (0, 0) for none.
pub fn cursorOf(read: ReadView, ns: NamespaceId, key: []const u8) At {
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, key) catch return .{};
    const v = (read.get(CursorFam, k) catch null) orelse return .{};
    return .{ .a = v.v.a, .b = v.v.b };
}

fn typeOf(e: *const Entry) ?EntryType {
    return std.enums.fromInt(EntryType, e.header.entry_type);
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    if (typeOf(e) != .processing_sink_batch) return jobKeys(e, out);
    var diag: schema.Diagnostic = .{};
    const p = pe.SinkBatch.decodeAll(e.payload, &diag) catch return;
    try out.addIn(CursorFam.ID, .of(p.ns), p.pipeline);
}

/// A job entry's job; a remove's also its pipelines' cursors: its key, or
/// its key then `\0` and a source's index.
fn jobKeys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return;
    try out.addIn(JobFam.ID, cmd.namespace_id, cmd.key);
    if (typeOf(e) != .processing_remove) return;
    try out.addIn(CursorFam.ID, cmd.namespace_id, cmd.key);
    var pb: [pe.MAX_PIPELINE_KEY + 1]u8 = undefined;
    if (cmd.key.len + 1 > pb.len) return;
    @memcpy(pb[0..cmd.key.len], cmd.key);
    pb[cmd.key.len] = 0;
    out.addRangeIn(CursorFam.ID, cmd.namespace_id, pb[0 .. cmd.key.len + 1]) catch |err| switch (err) {
        error.NoNamespace => unreachable,
        else => |o| return o,
    };
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    switch (typeOf(e) orelse return error.Halt) {
        .processing_sink_batch => return batchApply(view, e),
        .processing_submit => return submitApply(view, e, out),
        .processing_stop => return statusApply(view, e, .stopped, out),
        .processing_cancel => return statusApply(view, e, .cancelled, out),
        .processing_remove => return removeApply(view, e, out),
        else => return error.Halt,
    }
}

const NOT_FOUND = "not found: no such processing job";

fn jobKey(view: *View, ns: NamespaceId, id: []const u8) ApplyError![]const u8 {
    const buf = try view.arena.alloc(u8, 4 + id.len);
    return view_mod.idKey(buf, ns, id) catch error.Halt;
}

/// A submit: the job made at the entry's stamp, its definition parsed in
/// its namespace as the submit did, so every replica of a build stores the
/// same. One that doesn't parse here, or that this build refuses, is kept
/// as failed with its definition, and its answer says why; past the
/// shard's limit it is refused. One already held is a replay of itself.
fn submitApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const value = cmd.value;
    if (value.len < 7) return error.Halt;
    const status = std.enums.fromInt(JobStatus, value[0]) orelse return error.Halt;
    const batch_size = std.mem.readInt(u32, value[1..5], .little);
    const home_len = std.mem.readInt(u16, value[5..7], .little);
    if (7 + home_len > value.len) return error.Halt;
    const home = value[7 .. 7 + home_len];
    const yaml = value[7 + home_len ..];

    const key = try jobKey(view, cmd.namespace_id, cmd.key);
    const held = try view.get(JobFam, key);
    if (held == null and view.store.family(JobFam.ID).count >= max_jobs)
        return out.refuseFmt(.overloaded, .unclassified, "job not submitted: this shard holds at most {d} processing jobs; remove a stopped, cancelled or failed one with `flo processing remove <job_id>`", .{max_jobs});

    var diag: parser.Diagnostic = .{};
    var why_buf: [256]u8 = undefined;
    var failure: []const u8 = "";
    var name: []const u8 = cmd.key;
    if (parser.parseJobDefinitionWithNamespace(view.arena, yaml, home, &diag)) |def| {
        name = try view.arena.dupe(u8, def.name);
        failure = def.namespaceRefusal() orelse def.homeRefusal(home, &why_buf) orelse "";
        if (failure.len > 0) failure = try view.arena.dupe(u8, failure);
    } else |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        failure = try view.arena.dupe(u8, diag.message());
    }
    const stored_failure = try boundedReason(view.arena, failure);
    const job: Job.Value = .{
        .name = name,
        .status = @backingInt(if (failure.len > 0) JobStatus.failed else status),
        .batch_size = batch_size,
        .home = home,
        .created_at_ms = e.header.timestamp_ns / std.time.ns_per_ms,
        .yaml = yaml,
        .failure = stored_failure,
    };
    try view.put(JobFam, key, .{ .v = job });
    // Answered with its id, and why it failed if it did: stored either way,
    // so it ran, and its edge refuses a failed one as such (`answerSubmit`).
    out.putBytes(cmd.key);
    if (failure.len > 0) out.putBytes(stored_failure[0..@min(stored_failure.len, FAILURE_ANSWER_MAX)]);
}

/// The most of a failure reason a submit's answer carries.
const FAILURE_ANSWER_MAX = 256;

/// Why a submit's job applied as failed, from its answer; null if it didn't.
pub fn submitFailure(out: *const Result) ?[]const u8 {
    var r = out.reader();
    _ = r.slice() orelse return null;
    const why = r.slice() orelse return null;
    return if (why.len > 0) why else null;
}

/// Cut to `MAX_FAILURE_REASON`, marked, never splitting a UTF-8 sequence.
pub fn boundedReason(arena: std.mem.Allocator, reason: []const u8) ![]const u8 {
    if (reason.len <= MAX_FAILURE_REASON) return reason;
    const mark = "... (cut)";
    var end: usize = MAX_FAILURE_REASON - mark.len;
    while (end > 0 and reason[end] & 0xC0 == 0x80) end -= 1;
    return std.mem.concat(arena, u8, &.{ reason[0..end], mark });
}

/// A stop or cancel. Only a running job changes: asking again for the
/// status it has is a no-op that succeeds, and any other is refused, so a
/// failed job keeps its status and its reason. Checked here, not when
/// proposed, since a stop and a cancel may both be in flight; one taken
/// before a remove applied lands after it.
fn statusApply(view: *View, e: *const Entry, status: JobStatus, out: *Result) ApplyError!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const key = try jobKey(view, cmd.namespace_id, cmd.key);
    var job = ((try view.get(JobFam, key)) orelse return out.refuse(.not_found, .not_found, NOT_FOUND)).v;
    const now = statusOf(job);
    if (now == status) return;
    if (now != .running) return out.refuseFmt(.conflict, .unclassified, "job isn't running (it is {s})", .{@tagName(now)});
    job.status = @backingInt(status);
    try view.put(JobFam, key, .{ .v = job });
}

/// A remove of a job that no longer runs: it and its pipelines' cursors go.
/// A running job stays, refused in log order on every replica; a job
/// already gone was removed by an earlier entry.
fn removeApply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
    if (cmd.namespace_id == .invalid or cmd.key.len == 0) return error.Halt;
    const key = try jobKey(view, cmd.namespace_id, cmd.key);
    const job = ((try view.get(JobFam, key)) orelse return out.refuse(.not_found, .not_found, NOT_FOUND)).v;
    if (statusOf(job) == .running) return out.refuse(.conflict, .job_running, "job is running: stop or cancel it, then remove it");
    try view.delete(JobFam, key);
    if (cmd.key.len + 1 > pe.MAX_PIPELINE_KEY) return;
    try view.delete(CursorFam, key);
    const prefix = try view.arena.alloc(u8, key.len + 1);
    @memcpy(prefix[0..key.len], key);
    prefix[key.len] = 0;
    try view.deleteRange(CursorFam, prefix, null);
}

fn batchApply(view: *View, e: *const Entry) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    // Only a leader makes these, from its own pipelines: one that doesn't
    // decode is a Flo bug.
    const p = pe.SinkBatch.decodeAll(e.payload, &diag) catch return error.Halt;
    const ns: NamespaceId = .of(p.ns);
    if (ns == .invalid) return error.Halt;
    var kb: [4 + pe.MAX_PIPELINE_KEY]u8 = undefined;
    const k = view_mod.idKey(&kb, ns, p.pipeline) catch return error.Halt;
    const held: ?Cursor.Value = if (try view.get(CursorFam, k)) |v| v.v else null;
    const at: At = if (held) |v| .{ .a = v.a, .b = v.b } else .{};
    // Its cursor moved on: applied already, or left behind by a leader change.
    if (at.a != p.from_a or at.b != p.from_b) return;
    const records = (if (held) |v| v.records else 0) +| p.records;
    try view.put(CursorFam, k, .{ .v = .{ .a = p.to_a, .b = p.to_b, .records = records } });
    var it = p.messages.iterator();
    while (it.next() catch return error.Halt) |m| {
        if (m.flags != entry_mod.Flags.SINK) return error.Halt;
        try view.sendFlagged(.user(.of(m.ns)), m.slot, m.entry_type, m.flags, m.payload);
    }
}

pub const machine: machine_mod.Machine = .{
    .name = "processing",
    .entry_types = &.{ .processing_submit, .processing_stop, .processing_cancel, .processing_remove, .processing_sink_batch },
    .families = &.{ machine_mod.info(JobFam), machine_mod.info(CursorFam) },
    .keys = keys,
    .apply = apply,
};

const testing = std.testing;
const store_mod = @import("../contract/store.zig");
const channels = @import("../contract/channels.zig");
const schema_w = schema.Writer;

const T = struct {
    store: store_mod.Store,
    scratch: std.heap.ArenaAllocator,
    index: u64 = 0,

    fn init() T {
        var t: T = .{ .store = store_mod.Store.init(testing.allocator), .scratch = std.heap.ArenaAllocator.init(testing.allocator) };
        t.store.place = .{ .group = 0, .count = 2 };
        t.store.strict_keys = true;
        return t;
    }

    fn deinit(t: *T) void {
        t.store.deinit();
        t.scratch.deinit();
    }

    fn entry(t: *T, etype: EntryType, payload: []const u8) Entry {
        t.index += 1;
        var h = std.mem.zeroes(entry_mod.Header);
        h.entry_type = @backingInt(etype);
        h.index = t.index;
        h.payload_len = @intCast(payload.len);
        return .{ .header = h, .payload = payload };
    }

    /// A batch for pipeline "job" moving its cursor `from` to `to`, sending
    /// one write per body to slot 1 (group 1's).
    fn batch(t: *T, from: At, to: At, bodies: []const []const u8) !void {
        var mb: [1024]u8 = undefined;
        var w: schema_w = .{ .buf = &mb };
        for (bodies) |b| try pe.SinkMessage.encode(&w, .{ .slot = 1, .entry_type = @backingInt(EntryType.kv_put), .flags = entry_mod.Flags.SINK, .ns = 1, .payload = b });
        var buf: [2048]u8 = undefined;
        const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = "job", .from_a = from.a, .from_b = from.b, .to_a = to.a, .to_b = to.b, .records = 0, .messages = .{ .bytes = w.written(), .count = @intCast(bodies.len) } });
        const e = t.entry(.processing_sink_batch, payload);
        var r = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r);
        try testing.expect(r.refused() == null);
    }

    /// A job entry for `key` in namespace 21, applied; refused fails.
    fn command(t: *T, etype: EntryType, key: []const u8, value: []const u8, buf: []u8) !void {
        t.index += 1;
        const e = entry_mod.buildCommandEntry(etype, 0, 1, t.index, 0, .of(21), key, value, buf).?;
        var r = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r);
        try testing.expect(r.refused() == null);
    }

    fn read(t: *T) ReadView {
        return .{ .store = &t.store };
    }

    fn sent(t: *T) u64 {
        return channels.metaOf(t.read(), 1, .sink).next_seq - 1;
    }
};

test "sinks: a batch moves its pipeline's cursor and sends its writes once; one whose cursor moved on changes nothing" {
    var t = T.init();
    defer t.deinit();
    try t.batch(.{}, .{ .a = 5, .b = 1 }, &.{ "w1", "w2" });
    try testing.expectEqual(At{ .a = 5, .b = 1 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 2), t.sent());
    // The same batch again (a leader change proposed it twice): nothing.
    try t.batch(.{}, .{ .a = 5, .b = 1 }, &.{ "w1", "w2" });
    try testing.expectEqual(@as(u64, 2), t.sent());
    // From where the first left it: applies.
    try t.batch(.{ .a = 5, .b = 1 }, .{ .a = 9, .b = 0 }, &.{"w3"});
    try testing.expectEqual(At{ .a = 9, .b = 0 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 3), t.sent());
    // An older one, applied late: nothing.
    try t.batch(.{ .a = 5, .b = 1 }, .{ .a = 7, .b = 0 }, &.{"stale"});
    try testing.expectEqual(At{ .a = 9, .b = 0 }, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(@as(u64, 3), t.sent());
}

test "processing: a logged submit the parser now refuses applies as a failed job, saying why" {
    var t = T.init();
    defer t.deinit();
    var cb: [512]u8 = undefined;
    var vb: [256]u8 = undefined;
    // `value_field` beside `fields`, which it would leave unread.
    const yaml = "kind: Processing\nsources: [{stream: {name: in}}]\nsinks: [{ts: {measurement: m, value_field: v, fields: {load: l}}}]\n";
    try t.command(.processing_submit, "job", encodeSubmit(&vb, .running, 0, "default", yaml).?, &cb);
    const job = jobOf(t.read(), .of(21), "job").?;
    try testing.expectEqual(@backingInt(JobStatus.failed), job.status);
    try testing.expect(std.mem.startsWith(u8, job.failure, "TS sink 'default-sink-0' can't give both \"value_field\" and \"fields\""));
}

test "processing: removing a job drops it and its pipelines' cursors, and no other job's" {
    var t = T.init();
    defer t.deinit();
    var cb: [512]u8 = undefined;
    // Job "job", stopped, in namespace 21.
    var vb: [256]u8 = undefined;
    const yaml = "kind: Processing\nsources: [{stream: {name: in}}]\nsinks: [{stream: {name: out}}]\n";
    try t.command(.processing_submit, "job", encodeSubmit(&vb, .stopped, 0, "default", yaml).?, &cb);
    try testing.expect(jobOf(t.read(), .of(21), "job") != null);
    try t.batch(.{}, .{ .a = 1, .b = 1 }, &.{});
    // Two more pipelines: the job's second source, and another job.
    const keys_ = [_][]const u8{ "job\x001", "jobber" };
    for (keys_) |k| {
        var buf: [256]u8 = undefined;
        const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = k, .from_a = 0, .from_b = 0, .to_a = 2, .to_b = 2, .records = 3, .messages = .{ .bytes = "", .count = 0 } });
        const e = t.entry(.processing_sink_batch, payload);
        var r = Result.start(&e);
        try machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r);
    }
    try testing.expectEqual(@as(u64, 3), recordsOf(t.read(), .of(21), "jobber"));
    try t.command(.processing_remove, "job", "", &cb);
    try testing.expect(jobOf(t.read(), .of(21), "job") == null);
    try testing.expectEqual(At{}, cursorOf(t.read(), .of(21), "job"));
    try testing.expectEqual(At{}, cursorOf(t.read(), .of(21), "job\x001"));
    try testing.expectEqual(At{ .a = 2, .b = 2 }, cursorOf(t.read(), .of(21), "jobber"));
}

test "sinks: a batch whose write carries a flag other than the sink's halts: only a leader's own pipelines make these" {
    var t = T.init();
    defer t.deinit();
    var mb: [256]u8 = undefined;
    var w: schema_w = .{ .buf = &mb };
    try pe.SinkMessage.encode(&w, .{ .slot = 1, .entry_type = @backingInt(EntryType.kv_put), .flags = 0, .ns = 1, .payload = "x" });
    var buf: [512]u8 = undefined;
    const payload = try pe.SinkBatch.encodeInto(&buf, .{ .ns = 21, .pipeline = "job", .from_a = 0, .from_b = 0, .to_a = 1, .to_b = 0, .records = 0, .messages = .{ .bytes = w.written(), .count = 1 } });
    const e = t.entry(.processing_sink_batch, payload);
    var r = Result.start(&e);
    try testing.expectError(error.Halt, machine_mod.applyEntry(&machine, &t.store, &t.scratch, &e, &r));
}
