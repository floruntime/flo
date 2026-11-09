//! The delivery model: items a scope's holders take one at a time, each
//! under a lease until it is reported or the lease runs out. Actions use
//! it (an action's runs); so will queues. A family of items is the user's;
//! this keeps the three it leans on, and the rules every lease follows.
//!
//! - ready: each ready item, keyed (ns, scope, due, id), so a leader picks
//!   the next of a scope without a scan.
//! - deadline: each lease's deadline, keyed (expiry, ns, id) in no
//!   namespace, so the expire duty reads the earliest without a scan.
//! - request: the delivery a holder's request key got, so a retried take
//!   gets it again from whichever leader it reaches.
//!
//! A lease's identity is its deadline key: an item leased until exactly
//! that instant is the lease the key names. Deliver and extend each write
//! one. Settle, release and extend name the expiry the leader saw and
//! delete that key when it is still the item's; one that misses leaves its
//! key to the expire duty, which drops every past-due key it reads.
//!
//! An item is a record with the fields `Lease` names, read and written
//! here by name, and a scope field the caller names (`Config.scope`).

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const persistence = @import("../storage/persistence.zig");
const ae = @import("../protocol/action_entries.zig");
const view_mod = @import("view.zig");

const View = view_mod.View;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;
const Record = schema.Record;

/// A family value stored as a schema record, so its codec is the schema's.
pub fn Stored(comptime R: type) type {
    return struct {
        v: R.Value,

        pub fn encode(self: @This(), w: *std.Io.Writer) std.Io.Writer.Error!void {
            const n = R.encodedSize(self.v) catch return error.WriteFailed;
            _ = R.encodeInto(try w.writableSlice(n), self.v) catch return error.WriteFailed;
        }

        pub fn decode(bytes: []const u8) error{Malformed}!@This() {
            var diag: schema.Diagnostic = .{};
            return .{ .v = R.decodeAll(bytes, &diag) catch return error.Malformed };
        }
    };
}

pub const Empty = struct {
    pub fn encode(_: Empty, _: *std.Io.Writer) std.Io.Writer.Error!void {}
    pub fn decode(bytes: []const u8) error{Malformed}!Empty {
        if (bytes.len != 0) return error.Malformed;
        return .{};
    }
};

/// How an item's latest delivery ended, so a resent report is answered by
/// what happened to it.
pub const Ended = enum(u8) { none = 0, settled = 1, released = 2, abandoned = 3, expired = 4 };

/// The scope a deadline's item belongs to, so the duty can name its ready
/// key; the namespace and item are in the deadline key.
pub const Deadline = Record(&.{
    .{ .name = "scope", .T = ae.ActionName },
});

/// The delivery a holder's request key got.
pub const Request = Record(&.{
    .{ .name = "run", .T = ae.RunId },
    .{ .name = "term", .T = Int(u64) },
    .{ .name = "index", .T = Int(u64) },
});

pub const Config = struct {
    ready_id: u16,
    ready_name: []const u8,
    deadline_id: u16,
    deadline_name: []const u8,
    request_id: u16,
    request_name: []const u8,
    /// The item field naming its scope (an action, a queue).
    scope: []const u8,
    /// The longest scope name and item id.
    max_scope: usize,
    max_id: usize,
    max_namespace: usize,
};

pub fn Model(comptime cfg: Config) type {
    return struct {
        pub const Ready = view_mod.Family(cfg.ready_id, cfg.ready_name, Empty);
        pub const Deadlines = view_mod.Family(cfg.deadline_id, cfg.deadline_name, Stored(Deadline));
        pub const Requests = view_mod.Family(cfg.request_id, cfg.request_name, Stored(Request));

        /// The longest key these families hold, before a namespace prefix.
        pub const MAX_KEY = cfg.max_scope + 1 + 8 + cfg.max_id;
        pub const KeyBuf = [MAX_KEY]u8;
        pub const ReqBuf = [ae.Holder.max_size + 1 + ae.MAX_REQUEST_KEY]u8;

        comptime {
            std.debug.assert(8 + 1 + cfg.max_namespace + cfg.max_id <= MAX_KEY);
        }

        /// `[scope][0][due:u64 BE][id]`: a scope's ready items, earliest
        /// first. A scope name holds no NUL, so one scope's keys never run
        /// into another's.
        pub fn readyKey(buf: []u8, scope: []const u8, due_ns: u64, id: []const u8) []const u8 {
            @memcpy(buf[0..scope.len], scope);
            buf[scope.len] = 0;
            std.mem.writeInt(u64, buf[scope.len + 1 ..][0..8], due_ns, .big);
            @memcpy(buf[scope.len + 9 ..][0..id.len], id);
            return buf[0 .. scope.len + 9 + id.len];
        }

        /// The prefix of a scope's ready keys: `[scope][0]`.
        pub fn readyPrefix(buf: []u8, scope: []const u8) []const u8 {
            @memcpy(buf[0..scope.len], scope);
            buf[scope.len] = 0;
            return buf[0 .. scope.len + 1];
        }

        /// `[expiry:u64 BE][ns len:u8][ns][id]`, in no namespace: every
        /// lease, earliest first. The namespace is in it, so the same id in
        /// two namespaces is two keys.
        pub fn deadlineKey(buf: []u8, expiry_ns: u64, ns: []const u8, id: []const u8) []const u8 {
            std.mem.writeInt(u64, buf[0..8], expiry_ns, .big);
            buf[8] = @intCast(ns.len);
            @memcpy(buf[9..][0..ns.len], ns);
            @memcpy(buf[9 + ns.len ..][0..id.len], id);
            return buf[0 .. 9 + ns.len + id.len];
        }

        pub const DeadlineKey = struct { expiry_ns: u64, ns: []const u8, run: []const u8 };

        /// A deadline key read back, without its family's namespace prefix.
        pub fn parseDeadlineKey(k: []const u8) ?DeadlineKey {
            if (k.len < 9 or k.len < 9 + @as(usize, k[8])) return null;
            const ns_len = k[8];
            return .{ .expiry_ns = std.mem.readInt(u64, k[0..8], .big), .ns = k[9..][0..ns_len], .run = k[9 + ns_len ..] };
        }

        /// `[holder][0][request_key]`, in the namespace: a holder's request
        /// keys. A holder holds no NUL, so one's keys never run into
        /// another's.
        pub fn requestKey(buf: []u8, holder: []const u8, request_key: []const u8) []const u8 {
            @memcpy(buf[0..holder.len], holder);
            buf[holder.len] = 0;
            @memcpy(buf[holder.len + 1 ..][0..request_key.len], request_key);
            return buf[0 .. holder.len + 1 + request_key.len];
        }

        /// Lease `item` to `holder` at `stamp`: its ready key goes, its
        /// deadline and request record are written. Its delivery count
        /// moves on, and its token is the entry's.
        pub fn deliver(view: *View, ns: []const u8, id: []const u8, item: anytype, holder: []const u8, request_key: ?[]const u8, token: ae.Token.Value, lease_ms: u64, stamp: u64) ApplyError!void {
            var b: KeyBuf = undefined;
            try view.delete(Ready, try nsKey(view, ns, readyKey(&b, @field(item, cfg.scope), item.due_ns, id)));
            item.status = .leased;
            item.deliveries +|= 1;
            item.holder = holder;
            item.request_key = request_key;
            item.token = token;
            item.lease_ms = lease_ms;
            item.delivered_ns = stamp;
            item.expiry_ns = after(stamp, lease_ms);
            item.ended = .none;
            if (item.started_ns == 0) item.started_ns = stamp;
            try putDeadline(view, ns, id, item.*);
            if (request_key) |k| {
                var rb: ReqBuf = undefined;
                try view.put(Requests, try nsKey(view, ns, requestKey(&rb, holder, k)), .{ .v = .{ .run = id, .term = token.term, .index = token.index } });
            }
        }

        /// A report's lease ends: its deadline, if still the item's, and its
        /// request record go.
        pub fn endLease(view: *View, ns: []const u8, id: []const u8, item: anytype, holder: []const u8, request_key: ?[]const u8, seen_expiry: u64) ApplyError!void {
            try dropDeadline(view, ns, id, item, seen_expiry);
            try dropRequest(view, ns, holder, request_key, id);
        }

        /// Ready again, due at `due_ns`.
        pub fn readyAgain(view: *View, ns: []const u8, id: []const u8, item: anytype, due_ns: u64) ApplyError!void {
            item.status = .ready;
            item.due_ns = due_ns;
            var b: KeyBuf = undefined;
            try view.put(Ready, try nsKey(view, ns, readyKey(&b, @field(item, cfg.scope), due_ns, id)), .{});
        }

        /// An extend at `stamp` of the live lease: a no-op while more than
        /// half of it remains or at the hold ceiling; otherwise the lease
        /// moves on. Returns the expiry it leaves.
        pub fn extend(view: *View, ns: []const u8, id: []const u8, item: anytype, seen_expiry: u64, stamp: u64) ApplyError!?u64 {
            if (item.expiry_ns -| stamp > (item.lease_ms *| std.time.ns_per_ms) / 2) return null;
            const to = extendedTo(stamp, item.lease_ms, item.delivered_ns);
            if (to <= item.expiry_ns) return null;
            try dropDeadline(view, ns, id, item.*, seen_expiry);
            item.expiry_ns = to;
            try putDeadline(view, ns, id, item.*);
            return to;
        }

        /// One expire item: its key goes if past due at `stamp`, unless it
        /// is a live lease of another scope's item; whether it is `item`'s
        /// live lease, which the caller then ends.
        pub fn expireKey(view: *View, ns: []const u8, id: []const u8, scope: []const u8, expiry_ns: u64, found: anytype, stamp: u64) ApplyError!bool {
            if (expiry_ns > stamp) return false;
            const live = if (found) |r| r.status == .leased and r.expiry_ns == expiry_ns else false;
            const ours = if (found) |r| std.mem.eql(u8, @field(r, cfg.scope), scope) else true;
            if (!ours and live) return false;
            var b: KeyBuf = undefined;
            try view.delete(Deadlines, try nsKey(view, "", deadlineKey(&b, expiry_ns, ns, id)));
            return live;
        }

        pub fn putDeadline(view: *View, ns: []const u8, id: []const u8, item: anytype) ApplyError!void {
            // An extend or a redeliver landing on the same instant rewrites
            // the same key: still one deadline for the one live lease.
            var b: KeyBuf = undefined;
            try view.put(Deadlines, try nsKey(view, "", deadlineKey(&b, item.expiry_ns, ns, id)), .{ .v = .{ .scope = @field(item, cfg.scope) } });
        }

        /// Delete the key at `seen`, the expiry the leader saw, if it is
        /// still the item's; if an extend moved it since, the duty drops it
        /// once past due.
        pub fn dropDeadline(view: *View, ns: []const u8, id: []const u8, item: anytype, seen: u64) ApplyError!void {
            if (seen != item.expiry_ns) return;
            var b: KeyBuf = undefined;
            try view.delete(Deadlines, try nsKey(view, "", deadlineKey(&b, seen, ns, id)));
        }

        /// A delivery's request record goes with it, if it still names the
        /// item.
        pub fn dropRequest(view: *View, ns: []const u8, holder: []const u8, request_key: ?[]const u8, id: []const u8) ApplyError!void {
            const k = request_key orelse return;
            var rb: ReqBuf = undefined;
            const key = try nsKey(view, ns, requestKey(&rb, holder, k));
            const got = (try view.get(Requests, key)) orelse return;
            if (std.mem.eql(u8, got.v.run, id)) try view.delete(Requests, key);
        }
    };
}

pub fn after(stamp_ns: u64, millis: u64) u64 {
    return stamp_ns +| (millis *| std.time.ns_per_ms);
}

/// Where an extend at `stamp` moves a lease to: a lease on from now, but
/// never past the hold ceiling from its deliver.
pub fn extendedTo(stamp: u64, lease_ms: u64, held_since_ns: u64) u64 {
    return @min(after(stamp, lease_ms), after(held_since_ns, ae.MAX_HOLD_MS));
}

/// Whether the report's delivery is the item's live one, reported by its
/// holder.
pub fn holds(item: anytype, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
    return item.status == .leased and sameDelivery(item, holder, deliveries, token);
}

pub fn sameDelivery(item: anytype, holder: []const u8, deliveries: u32, token: ae.Token.Value) bool {
    return item.deliveries == deliveries and item.token.term == token.term and item.token.index == token.index and std.mem.eql(u8, item.holder, holder);
}

/// A report for a delivery that has moved on, or from a holder that
/// doesn't hold it, said by what happened.
pub fn stale(out: *Result, item: anytype, holder: []const u8, deliveries: u32, token: ae.Token.Value) void {
    out.refuse(.conflict, .stale_report, staleWhy(item, holder, deliveries, token));
}

pub fn staleWhy(item: anytype, holder: []const u8, deliveries: u32, token: ae.Token.Value) []const u8 {
    if (item.deliveries != deliveries or item.token.term != token.term or item.token.index != token.index) return "stale report: the run was delivered again since";
    if (!std.mem.eql(u8, item.holder, holder)) return "stale report: another worker holds this delivery";
    return switch (item.ended) {
        .expired => "stale report: the lease expired and the run moved on",
        .settled => "stale report: the run was completed",
        .released, .abandoned => "stale report: the run was released",
        .none => "stale report: the run isn't held",
    };
}

/// `k` in `ns`, as a family stores it, in the entry's scratch.
pub fn nsKey(view: *View, ns: []const u8, k: []const u8) ApplyError![]const u8 {
    if (ns.len > view_mod.MAX_NAMESPACE) return error.Halt;
    const buf = try view.arena.alloc(u8, 1 + ns.len + k.len);
    return view_mod.key(buf, ns, k) catch unreachable;
}
