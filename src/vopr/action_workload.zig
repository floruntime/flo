//! Action entries for the VOPR workload, made as the leader's handler
//! makes them: from the proposing leader's own applied action state, so a
//! deliver names a ready key and a report names a live token. Every kind
//! is also spoiled now and then (an early deliver, a stale token, a
//! resend, a lease not yet due) so the machine's refusals and no-ops are
//! exercised too. The leader's state can lag the log, so a valid entry
//! may still be refused when it applies, as on a real leader.
//!
//! Unlike the rest of the workload these payloads aren't a function of
//! (seed, op id): the oracle keeps each one's hash, and finds its op by
//! where it was proposed.

const std = @import("std");
const entry_mod = @import("../storage/ual/entry.zig");
const ae = @import("../protocol/action_entries.zig");
const action_machine = @import("../actions/machine.zig");
const families = @import("families.zig");

const EntryType = entry_mod.EntryType;

const NS = "default";
/// Each action is always registered with the same lease and attempts, so
/// a deliver's lease is its action's whichever register it read. One
/// attempt for the first: its runs end at their first failure or expiry.
const ACTIONS = [_]struct { name: []const u8, lease_ms: u64, max_attempts: u32 }{
    .{ .name = "a0", .lease_ms = 5_000, .max_attempts = 1 },
    .{ .name = "a1", .lease_ms = 7_000, .max_attempts = 3 },
};
/// Holders whose name starts so are workers that die holding the run:
/// nothing reports for them, and their leases expire.
const DEAD = "x";
/// And workers that hold a run longer than its lease, extending it, so
/// its old deadline keys go stale behind it.
const SLOW = "s";
/// What the expire duty waits after a term begins, as the handler does.
const MAX_GRACE_MS: u64 = 30_000;

/// The proposing leader: its clock, and when its term began.
pub const Leader = struct {
    now_ns: u64,
    term_since_ns: u64,
};

pub const Made = struct { entry_type: EntryType, payload: []const u8 };

const Kind = enum { register, delete, invoke, deliver, settle, release, extend, expire };
/// Out of 100. Reports need leases, so delivers come often; an action is
/// deleted now and then, and registered again.
const WEIGHTS = [_]u8{ 3, 1, 25, 27, 12, 12, 8, 12 };
/// Percent of entries spoiled, by kind: enough that each kind the machine
/// can refuse is refused a fifth to a third of the time, counting those a
/// lagging leader's state spoils on its own.
const SPOIL = [_]u8{ 0, 0, 15, 15, 15, 15, 10, 15 };

/// An action entry for op `op_id`, in `buf`, at most `max_len` bytes; null
/// when the leader has nothing of the kind drawn to propose.
pub fn make(a: *families.Actions, l: Leader, r: std.Random, op_id: u64, buf: []u8, max_len: usize) ?Made {
    const out = buf[0..@min(buf.len, max_len)];
    var have_def = false;
    for (ACTIONS) |act| {
        if (def(a, act.name) != null) have_def = true;
    }
    var roll = r.uintLessThan(u8, 100);
    // A leader that hasn't applied its actions yet registers one now and
    // then, as an operator would.
    if (!have_def and r.uintLessThan(u8, 8) != 0) return null;
    const kind: Kind = if (!have_def) .register else for (WEIGHTS, 0..) |w, i| {
        if (roll < w) break @fromBackingInt(@intCast(i));
        roll -= w;
    } else .invoke;
    const spoil = r.uintLessThan(u8, 100) < SPOIL[@backingInt(kind)];
    var hex: [17]u8 = undefined;
    const tag = std.fmt.bufPrint(&hex, "{x}", .{op_id}) catch unreachable;
    return switch (kind) {
        .register => register(out, r),
        .delete => delete(out, r),
        .invoke => invoke(a, out, r, spoil, tag),
        .deliver => deliver(a, out, l, r, spoil, tag),
        .settle, .release, .extend => report(a, out, kind, l, r, spoil, tag),
        .expire => expire(a, out, l, spoil),
    } catch null;
}

fn register(out: []u8, r: std.Random) !?Made {
    const act = ACTIONS[r.uintLessThan(usize, ACTIONS.len)];
    const p = try ae.Register.encodeInto(out, .{ .ns = NS, .name = act.name, .owner = "", .lease_ms = act.lease_ms, .max_attempts = act.max_attempts });
    return .{ .entry_type = .action_register, .payload = p };
}

/// An action deleted: its runs stay, but none is delivered until it is
/// registered again.
fn delete(out: []u8, r: std.Random) !?Made {
    const p = try ae.Delete.encodeInto(out, .{ .ns = NS, .name = ACTIONS[r.uintLessThan(usize, ACTIONS.len)].name });
    return .{ .entry_type = .action_delete, .payload = p };
}

/// A new run; spoiled, one with a taken id.
fn invoke(a: *families.Actions, out: []u8, r: std.Random, spoil: bool, tag: []const u8) !?Made {
    var rb: [24]u8 = undefined;
    var ib: [ae.MAX_RUN_ID]u8 = undefined;
    const taken = if (spoil) pick(a, r, any, &ib) else null;
    const p = try ae.Invoke.encodeInto(out, .{
        .ns = NS,
        .action = ACTIONS[r.uintLessThan(usize, ACTIONS.len)].name,
        .run = if (taken) |t| t.id else try std.fmt.bufPrint(&rb, "r{s}", .{tag}),
        .input = tag,
        .labels = null,
        .source = .direct,
        .caller_run = null,
        .caller_workflow = null,
        .caller_shard = null,
    });
    return .{ .entry_type = .action_invoke, .payload = p };
}

/// A run the leader holds in `status`, picked at random, with its id.
const Picked = struct { id: []const u8, run: action_machine.Run.Value };

fn pick(a: *families.Actions, r: std.Random, want: *const fn (action_machine.Run.Value) bool, id_buf: []u8) ?Picked {
    var n: u32 = 0;
    var chosen: ?Picked = null;
    var it = a.read().range(action_machine.Runs, "");
    while (it.next() catch null) |item| {
        if (!want(item.value.v)) continue;
        n += 1;
        // Reservoir: each match equally likely.
        if (r.uintLessThan(u32, n) != 0) continue;
        const id = item.key[1 + @as(usize, item.key[0]) ..];
        @memcpy(id_buf[0..id.len], id);
        chosen = .{ .id = id_buf[0..id.len], .run = item.value.v };
    }
    return chosen;
}

fn any(_: action_machine.Run.Value) bool {
    return true;
}

fn isReady(v: action_machine.Run.Value) bool {
    return v.status == .ready;
}

fn isLeased(v: action_machine.Run.Value) bool {
    return v.status == .leased;
}

/// Leased to a worker that is still alive to report.
fn isReported(v: action_machine.Run.Value) bool {
    return v.status == .leased and !std.mem.startsWith(u8, v.holder, DEAD);
}

fn isSettled(v: action_machine.Run.Value) bool {
    return v.status == .completed and v.ended == .settled;
}

fn deliver(a: *families.Actions, out: []u8, l: Leader, r: std.Random, spoil: bool, tag: []const u8) !?Made {
    var ib: [ae.MAX_RUN_ID]u8 = undefined;
    // Spoiled: a leased run (a repeat), or a ready one not yet due, or due
    // at another time than its key's.
    const p = pick(a, r, if (spoil and r.boolean()) isLeased else isReady, &ib) orelse return null;
    if (!spoil and p.run.due_ns > l.now_ns) return null;
    const d = def(a, p.run.action) orelse return null;
    var hb: [24]u8 = undefined;
    var kb: [8]u8 = undefined;
    const payload = try ae.Deliver.encodeInto(out, .{
        .ns = NS,
        .action = p.run.action,
        .run = p.id,
        .due_ns = if (spoil and p.run.status == .ready and r.boolean()) p.run.due_ns + 1 else p.run.due_ns,
        .holder = try std.fmt.bufPrint(&hb, "{s}{s}", .{ switch (r.uintLessThan(u8, 5)) {
            0, 1 => DEAD,
            2 => SLOW,
            else => "w",
        }, tag }),
        .request_key = if (r.uintLessThan(u8, 3) == 0) try std.fmt.bufPrint(&kb, "k{d}", .{r.uintLessThan(u8, 4)}) else null,
        .lease_ms = d.lease_ms,
    });
    return .{ .entry_type = .action_deliver, .payload = payload };
}

/// A settle, release or extend of a live delivery, as its holder sends it;
/// spoiled, of a delivery that moved on, by another worker, or resent.
fn report(a: *families.Actions, out: []u8, kind: Kind, l: Leader, r: std.Random, spoil: bool, tag: []const u8) !?Made {
    var ib: [ae.MAX_RUN_ID]u8 = undefined;
    // A settle resent after it applied answers as it did.
    const resend = spoil and kind == .settle and r.boolean();
    const p = pick(a, r, if (resend) isSettled else isReported, &ib) orelse return null;
    const lease_ns = p.run.lease_ms * std.time.ns_per_ms;
    // A worker touches its lease once half of it has gone, as an SDK does;
    // a slow one reports only once it has held longer than a lease.
    if (!spoil and !resend) {
        if (kind == .extend and p.run.expiry_ns -| l.now_ns > lease_ns / 2) return null;
        if (kind != .extend and std.mem.startsWith(u8, p.run.holder, SLOW) and l.now_ns -| p.run.delivered_ns < lease_ns) return null;
    }
    const v = p.run;
    var holder = v.holder;
    var deliveries = v.deliveries;
    var token = v.token;
    // An extend names no delivery count, so it is spoiled another way:
    // not its delivery's own start.
    var held_since = v.delivered_ns;
    if (spoil and !resend) switch (r.uintLessThan(u8, 3)) {
        0 => token.index -|= 1,
        1 => if (kind == .extend) {
            held_since += 1;
        } else {
            deliveries -|= 1;
        },
        else => holder = "w-other",
    };
    const stale_seen = kind == .extend and r.uintLessThan(u8, 4) == 0;
    var rb: [24]u8 = undefined;
    const payload = switch (kind) {
        .settle => try ae.Settle.encodeInto(out, .{
            .ns = NS,
            .action = v.action,
            .run = p.id,
            .holder = holder,
            .request_key = v.request_key,
            .deliveries = deliveries,
            .token = token,
            .expiry_ns = v.expiry_ns,
            .outcome = if (resend) v.outcome else "",
            .result = if (resend) v.result else try std.fmt.bufPrint(&rb, "o{s}", .{tag}),
        }),
        .release => try ae.Release.encodeInto(out, .{
            .ns = NS,
            .action = v.action,
            .run = p.id,
            .holder = holder,
            .request_key = v.request_key,
            .deliveries = deliveries,
            .token = token,
            .expiry_ns = v.expiry_ns,
            .abandoned = r.uintLessThan(u8, 4) == 0,
            .retry = r.uintLessThan(u8, 4) != 0,
            .delay_ms = if (r.boolean()) 0 else r.uintLessThan(u64, 3_000),
            .@"error" = try std.fmt.bufPrint(&rb, "e{s}", .{tag}),
        }),
        else => try ae.Extend.encodeInto(out, .{
            .ns = NS,
            .action = v.action,
            .run = p.id,
            .holder = holder,
            .token = token,
            // A leader that saw an older expiry: the extend applies, and
            // leaves the lease's real key behind, stale once it moves.
            .expiry_ns = if (stale_seen) v.expiry_ns - 1 else v.expiry_ns,
            .held_since_ns = held_since,
            .lease_ms = v.lease_ms,
        }),
    };
    return .{ .entry_type = switch (kind) {
        .settle => .action_settle,
        .release => .action_release,
        else => .action_extend,
    }, .payload = payload };
}

/// The duty's expire: deadline keys past due, a live lease's only after
/// the term's grace. Spoiled, it also names a live lease not yet due.
fn expire(a: *families.Actions, out: []u8, l: Leader, spoil: bool) !?Made {
    var items: [ae.MAX_EXPIRE_ITEMS * 128]u8 = undefined;
    var w: usize = 0;
    var n: u32 = 0;
    // Room for the list's own count.
    const room = out.len -| 4;
    var it = a.read().range(action_machine.Deadlines, "");
    while (it.next() catch null) |item| {
        if (n == ae.MAX_EXPIRE_ITEMS) break;
        const k = action_machine.parseDeadlineKey(item.key[1..]) orelse continue;
        var holder: []const u8 = "";
        var request_key: ?[]const u8 = null;
        const run = a.run(k.ns, k.run);
        const live = if (run) |v| v.status == .leased and v.expiry_ns == k.expiry_ns else false;
        if (k.expiry_ns > l.now_ns) {
            // Not yet due: the duty stops here; a spoiled entry names one.
            if (!(spoil and live and n == 0)) break;
        } else if (live) {
            const grace = @as(u64, @min(run.?.lease_ms, MAX_GRACE_MS)) * std.time.ns_per_ms;
            if (l.now_ns < l.term_since_ns +| grace) continue;
        }
        if (live) {
            holder = run.?.holder;
            request_key = run.?.request_key;
        }
        const bytes = try ae.ExpireItem.encodeInto(items[w..], .{ .ns = k.ns, .action = item.value.v.action, .run = k.run, .expiry_ns = k.expiry_ns, .holder = holder, .request_key = request_key });
        if (w + bytes.len > room) break;
        w += bytes.len;
        n += 1;
        if (k.expiry_ns > l.now_ns) break;
    }
    if (n == 0) return null;
    const payload = try ae.Expire.encodeInto(out, .{ .items = .{ .bytes = items[0..w], .count = n } });
    return .{ .entry_type = .action_expire, .payload = payload };
}

fn def(a: *families.Actions, name: []const u8) ?action_machine.Def.Value {
    var b: [1 + NS.len + ae.MAX_ACTION_NAME]u8 = undefined;
    const k = @import("../contract/view.zig").key(&b, NS, name) catch return null;
    const got = a.read().get(action_machine.Defs, k) catch return null;
    return if (got) |d| d.v else null;
}
