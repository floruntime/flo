//! How much run history a live run may still add, as `history.usage`
//! counts it: what a run reserves of its namespace's history quota at
//! admission, so runs admitted under the quota can't take their namespace
//! far past it.
//!
//! Each figure is a pure function of the run's own owner and run id
//! lengths, which its row keeps for its life: what it reserves at
//! admission is what it releases at its end.
//!
//! - A workflow run reserves `workflow(...)`: what it can add once its
//!   namespace's usage has reached its part, when its steps are held. That
//!   is one step in flight, its end, and one signal that wakes it, with the
//!   chunk headers and folds those make. However long the run, it adds no
//!   more past the part.
//! - An action run reserves `action(...)`: what its history can still add,
//!   from its own attempts. Every event is a bounded preview but its end.
//!
//! Each figure counts what a row's `total` does: each chunk's whole message
//! and each fold's own. Records go out in chunks (`history_producer`): one
//! is sent when the next record wouldn't fit `HIST_CHUNK`, and a record over
//! half a chunk goes at once, so two chunks in a row hold more than a
//! chunk's records between them.

const std = @import("std");
const hp = @import("../stream/history_producer.zig");
const he = @import("../protocol/history_entries.zig");
const persistence = @import("../storage/persistence.zig");
const runs_machine = @import("../workflow/runs_machine.zig");
const actions_machine = @import("../actions/machine.zig");
const action_ops = @import("../protocol/action_ops.zig");
const ae = @import("../protocol/action_entries.zig");

const rb = hp.recordBound;

/// The most chunks `records` bytes of events go out in: by the producer's
/// flush rules, two chunks in a row carry more than `HIST_CHUNK` between
/// them, and the last two may carry less.
pub fn chunks(records: u64) u64 {
    return 2 * ((std.math.divCeil(u64, records, hp.HIST_CHUNK) catch unreachable)) + 2;
}

/// A chunk message's bytes besides its records, for a run whose owner and
/// id are `owner_len` and `run_len` bytes: the header at its largest, less
/// what shorter names save.
pub fn chunkHeader(owner_len: usize, run_len: usize) u64 {
    return hp.chunkBound(0) - (he.MAX_OWNER - owner_len) - (he.MAX_RUN - run_len);
}

/// A fold's message for such a run, its locators at their most.
pub fn foldBytes(owner_len: usize, run_len: usize) u64 {
    return he.Index.max_size - (he.MAX_OWNER - owner_len) - (he.MAX_RUN - run_len);
}

/// The most folds `c` chunks make, landing on a run that had up to 15 live
/// rows already: each fold takes 16 rows to one, and its end folds what is
/// left to one more.
pub fn folds(c: u64) u64 {
    return (std.math.divCeil(u64, 15 + c, he.INDEX_FANOUT - 1) catch unreachable) + 1;
}

/// What `records` bytes of a run's events come to as rows count them:
/// their chunks with headers, and the folds those chunks make.
pub fn landed(owner_len: usize, run_len: usize, records: u64) u64 {
    std.debug.assert(owner_len <= he.MAX_OWNER and run_len <= he.MAX_RUN);
    const c = chunks(records);
    return records + c * chunkHeader(owner_len, run_len) + folds(c) * foldBytes(owner_len, run_len);
}

// ─── A workflow run ──────────────────────────────────────────────────────

/// What a record of a step's event takes over the event's own encoding in
/// the step entry: its seq and its wider lengths.
const STEP_RECORD_OVERHEAD: u64 = hp.HistRec.min_size - runs_machine.Event.min_size;

/// One step in flight: its events are at most the step entry's list, each
/// recorded with its overhead.
const STEP_RECORDS: u64 = persistence.MAX_PERSIST_PAYLOAD + runs_machine.MAX_STEP_EVENTS * STEP_RECORD_OVERHEAD;

/// Its end: the events still pending, a cancel's event (its detail a
/// reason, held at apply), and the output kept whole when its preview was
/// cut (at most an entry's payload).
const END_RECORDS: u64 = hp.HIST_CHUNK + rb(runs_machine.MAX_NAME, runs_machine.MAX_COMPLETE_DETAIL, 0) + rb(runs_machine.OUTPUT_EVENT.len, persistence.MAX_PERSIST_PAYLOAD, 0);

/// The one signal a held run's wait lets in: its receipt and its match.
const WAKE_RECORDS: u64 = rb("signal_received".len, runs_machine.MAX_NAME, 0) + rb("signal_matched".len, runs_machine.MAX_NAME, 0);

/// What a workflow run reserves, its workflow's name and its id
/// `owner_len` and `run_len` bytes.
pub fn workflow(owner_len: usize, run_len: usize) u64 {
    return landed(owner_len, run_len, STEP_RECORDS + END_RECORDS + WAKE_RECORDS);
}

/// At the longest names, and at typical ones (a 16-byte workflow, a
/// 24-byte run id).
pub const WORKFLOW_MOST: u64 = workflow(he.MAX_OWNER, he.MAX_RUN);
pub const WORKFLOW_TYPICAL: u64 = workflow(16, 24);

// ─── An action run ───────────────────────────────────────────────────────

const PREVIEW = actions_machine.PREVIEW;
const TRUNCATED = actions_machine.TRUNCATED;

/// Each attempt's events: a delivery, a hand-back, and a failure (a release
/// or an expiry), each a preview.
const DELIVERED: u64 = rb(actions_machine.EV_DELIVERED.len, PREVIEW, TRUNCATED.len);
const ABANDONED: u64 = rb(actions_machine.EV_ABANDONED.len, PREVIEW, TRUNCATED.len);
const FAILED: u64 = @max(rb(actions_machine.EV_RELEASED.len, PREVIEW, TRUNCATED.len), rb(actions_machine.EV_EXPIRED.len, actions_machine.LEASE_EXPIRED.len, 0));

/// Its end, in the one chunk the apply that ends it sends: what is pending,
/// the end's own event (a settle's result and outcome, or a last failure's
/// error), and `action_ended`.
const ENDED: u64 = rb(actions_machine.EV_ENDED.len, actions_machine.ENDED_DETAIL_MAX, 0);
const END_RECORDS_ACT: u64 = hp.HIST_CHUNK + ENDED + @max(rb(actions_machine.EV_SETTLED.len, action_ops.MAX_OUTPUT, ae.MAX_OUTCOME), rb(actions_machine.EV_RELEASED.len, action_ops.MAX_ERROR, 0));

/// What an action run can still add, its action's name and its id
/// `owner_len` and `run_len` bytes and its own `max_attempts`, having
/// recorded `deliveries`, `abandons` and `failures`: each counter up to the
/// most `record` keeps, and its end. 0 once it has ended.
pub fn action(owner_len: usize, run_len: usize, max_attempts: u32, deliveries: u64, abandons: u64, failures: u64, ended: bool) u64 {
    if (ended) return 0;
    const m: u64 = max_attempts;
    const records = END_RECORDS_ACT +
        (actions_machine.recordedDeliveries(max_attempts) -| deliveries) * DELIVERED +
        (m -| abandons) * ABANDONED +
        (m -| failures) * FAILED;
    return landed(owner_len, run_len, records);
}

/// An action run at the default attempts, as invoked, at the longest names
/// (an action run's id is the server's, at most `ae.MAX_RUN_ID`).
pub const ACTION_DEFAULT_MOST: u64 = action(ae.MAX_ACTION_NAME, ae.MAX_RUN_ID, @import("../actions/handler.zig").DEFAULT_MAX_ATTEMPTS, 0, 0, 0, false);

/// The smallest part of a namespace's quota on one shard: one workflow run
/// or one default action run, at the longest names, must fit, or every
/// start there is refused for good.
pub const PART_FLOOR: u64 = @max(WORKFLOW_MOST, ACTION_DEFAULT_MOST);

comptime {
    // A step's events are bounded by the entry they come in.
    std.debug.assert(runs_machine.MAX_BODY <= persistence.MAX_PERSIST_PAYLOAD);
    std.debug.assert(hp.HistRec.min_size > runs_machine.Event.min_size);
    // An end's chunk holds what is pending and the end, as the actions
    // machine asserts.
    std.debug.assert(END_RECORDS_ACT <= he.MAX_CHUNK_RECORDS);
    // The figures the plan gives, near enough to say the bound is the one
    // argued: a workflow run under 1 MiB, a default action run under 256 KiB.
    std.debug.assert(WORKFLOW_MOST < 1 << 20);
    std.debug.assert(ACTION_DEFAULT_MOST < 256 << 10);
    std.debug.assert(ae.MAX_ACTION_NAME <= he.MAX_OWNER and ae.MAX_RUN_ID <= he.MAX_RUN);
}

// ─── Tests ───────────────────────────────────────────────────────────────

const testing = std.testing;
const view_mod = @import("../contract/view.zig");
const store_mod = @import("../contract/store.zig");
const entry_mod = @import("../storage/ual/entry.zig");

/// What `events` come to through the producer, as rows count them: each
/// chunk message sent, then a last one, and the folds those chunks make.
fn throughProducer(owner_len: usize, run_len: usize, events: []const hp.Event, pending: usize) !u64 {
    var store = store_mod.Store.init(testing.allocator);
    defer store.deinit();
    store.place = .{ .group = 0, .count = 1 };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ks = view_mod.KeySet.init(arena.allocator());
    var view = view_mod.View.init(&store, arena.allocator(), &ks);
    const owners: [he.MAX_OWNER]u8 = @splat('o');
    const runs: [he.MAX_RUN]u8 = @splat('r');
    const o: hp.Owner = .{ .family = .workflow, .ns = .default, .owner = owners[0..owner_len], .run = runs[0..run_len], .created_ns = std.math.maxInt(u64), .slot = 0 };
    // What was pending when the hold began: one record short of a chunk.
    var h: hp.Hist = .{ .next = 1, .flushed = 1, .pending = "" };
    const filler: [hp.HIST_CHUNK]u8 = @splat('p');
    if (pending > 0) try hp.append(&view, o, &h, .{ .event = "p", .detail = filler[0 .. pending - rb(1, 0, 0)], .at_ms = 0 }, .hold);
    for (events) |ev| try hp.append(&view, o, &h, ev, .flush_on_size);
    try hp.flush(&view, o, &h, 1);
    var total: u64 = 0;
    for (view.sends.items) |snd| total += snd.payload.len;
    return total + folds(view.sends.items.len) * foldBytes(owner_len, run_len);
}

test "history reserve: a workflow run's step, end and wake, at every bound, land within its reserve" {
    const a = testing.allocator;
    const name_max: [runs_machine.MAX_NAME]u8 = @splat('n');
    const body: []u8 = try a.alloc(u8, persistence.MAX_PERSIST_PAYLOAD);
    defer a.free(body);
    @memset(body, 'b');
    for ([_][2]usize{ .{ 1, 1 }, .{ 16, 24 }, .{ he.MAX_OWNER, he.MAX_RUN } }) |names| for ([_]usize{ 1, 2, 7, 33, 64 }) |n| for ([_]usize{ 0, hp.HIST_CHUNK - 1 }) |pending| {
        var events: std.ArrayListUnmanaged(hp.Event) = .empty;
        defer events.deinit(a);
        // A step's n events sharing its list's bytes, each as long as it
        // may be.
        const per = (persistence.MAX_PERSIST_PAYLOAD - n * runs_machine.Event.min_size) / n;
        for (0..n) |_| try events.append(a, .{ .event = "", .detail = body[0..@min(per, runs_machine.MAX_BODY)], .at_ms = 0 });
        // Its wake, its cancel, and its output kept whole.
        try events.append(a, .{ .event = "signal_received", .detail = &name_max, .at_ms = 0 });
        try events.append(a, .{ .event = "signal_matched", .detail = &name_max, .at_ms = 0 });
        try events.append(a, .{ .event = &name_max, .detail = body[0..runs_machine.MAX_COMPLETE_DETAIL], .at_ms = 0 });
        try events.append(a, .{ .event = runs_machine.OUTPUT_EVENT, .detail = body, .at_ms = 0 });
        const got = try throughProducer(names[0], names[1], events.items, pending);
        try testing.expect(got <= workflow(names[0], names[1]));
    };
}

test "history reserve: an action run's attempts and end land within its reserve, which falls as they're made" {
    const a = testing.allocator;
    const big: []u8 = try a.alloc(u8, action_ops.MAX_OUTPUT);
    defer a.free(big);
    @memset(big, 'r');
    const preview: [PREVIEW]u8 = @splat('e');
    for ([_][2]usize{ .{ 1, 1 }, .{ 16, 24 }, .{ ae.MAX_ACTION_NAME, ae.MAX_RUN_ID } }) |names| for ([_]u32{ 1, 3, 40 }) |m| {
        const o = names[0];
        const r = names[1];
        var events: std.ArrayListUnmanaged(hp.Event) = .empty;
        defer events.deinit(a);
        for (0..m) |_| {
            for (0..2) |_| try events.append(a, .{ .event = actions_machine.EV_DELIVERED, .detail = &preview, .extra = TRUNCATED, .at_ms = 0 });
            try events.append(a, .{ .event = actions_machine.EV_ABANDONED, .detail = &preview, .extra = TRUNCATED, .at_ms = 0 });
            try events.append(a, .{ .event = actions_machine.EV_RELEASED, .detail = &preview, .extra = TRUNCATED, .at_ms = 0 });
        }
        try events.append(a, .{ .event = actions_machine.EV_SETTLED, .detail = big, .extra = &(@as([ae.MAX_OUTCOME]u8, @splat('o'))), .at_ms = 0 });
        try testing.expect(try throughProducer(o, r, events.items, 0) <= action(o, r, m, 0, 0, 0, false));
        // Each counter made lowers what is left; the end takes it to none.
        try testing.expect(action(o, r, m, 2, 0, 0, false) < action(o, r, m, 0, 0, 0, false));
        try testing.expect(action(o, r, m, 0, 1, 0, false) < action(o, r, m, 0, 0, 0, false));
        try testing.expect(action(o, r, m, 0, 0, 1, false) < action(o, r, m, 0, 0, 0, false));
        try testing.expectEqual(action(o, r, m, 2 * m, m, m, false), action(o, r, m, 4 * m, 2 * m, 2 * m, false));
        try testing.expectEqual(@as(u64, 0), action(o, r, m, 0, 0, 0, true));
    };
    try testing.expect(PART_FLOOR >= WORKFLOW_MOST and PART_FLOOR >= ACTION_DEFAULT_MOST);
}

test "history reserve: what a run reserves at admission is what it releases, a function of its own names alone, shortest to longest" {
    // The same names in, the same figure out: what the row keeps for its
    // life gives the same reserve at its start and its end.
    for ([_][2]usize{ .{ 1, 1 }, .{ he.MAX_OWNER, he.MAX_RUN } }) |names| {
        try testing.expectEqual(workflow(names[0], names[1]), workflow(names[0], names[1]));
        try testing.expect(workflow(1, 1) <= workflow(names[0], names[1]));
    }
    try testing.expect(WORKFLOW_TYPICAL < WORKFLOW_MOST);
    try testing.expectEqual(chunkHeader(he.MAX_OWNER, he.MAX_RUN), hp.chunkBound(0));
}
