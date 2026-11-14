//! Consumer groups: the ops a client sends, on the group machine's families
//! (`stream/group_machine.zig`). Every change is an entry a leader proposes
//! and every replica applies alike; reads are of applied state.
//!
//! - A group read leases the group's ready records, then the stream's past
//!   its cursor, until an ack, a nack or the lease running out. It is
//!   answered when its deliver applies, with the records and their
//!   delivery's token, which an ack, nack or touch names back. Delivery is
//!   at least once: a lease that runs out readies its records again.
//! - A record nacked or expired `max_attempts` times is dead, kept until its
//!   group is deleted.
//!
//! Wire (the stream is the request's key; a group or consumer is
//! `[len:u16][name]`; numbers little-endian):
//! - create `[group]`; options ack_timeout_ms (the lease), max_deliver (the
//!   attempts). Answer `[created:u8]`.
//! - delete `[group]`. Answer `[records dropped:u64]`.
//! - read `[group][consumer]`; options count (0, or past 100, is read
//!   as 100, a deliver's most), block_ms. Answer
//!   `[term:u64][index:u64][expires_at_ms:i64][count:u32]` then each record
//!   as a stream read gives it, followed by `[deliveries:u32]`.
//! - ack, touch `[group][consumer][term:u64][index:u64][count:u32]`
//!   `([ms:u64][seq:u64][deliveries:u32])*`; nack the same, then
//!   `[error_len:u16][error]` (at most 1024 bytes, 0 for none), with options
//!   delay_ms, dead. Answer `[applied:u32][refused:u32]`
//!   `([ms:u64][seq:u64][why:u8])*`; a touch's leads with
//!   `[expires_at_ms:i64]`. A group that isn't there is refused not_found.
//! - info `[group]`. Answer `[pending:u64][holders:u32][created_at_ns:u64]`
//!   `[ready:u64][leased:u64][dead:u64][trimmed_pending:u64]`: the records
//!   out and waiting, then by state; those at or below a trim, cleanup not
//!   yet having taken them, are `trimmed_pending` alone, so the five add up
//!   to what the group holds.
//! - pending `[group]` or `[group][consumer]`: up to a read's worth of the
//!   group's records out with a holder, as `[count:u32]` then each
//!   `[ts:u64][seq:u64][delivery_count:u32][consumer_len:u16][consumer]`.

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
const proto = @import("../protocol/proto.zig");
const schema = @import("../protocol/schema.zig");
const ae = @import("../protocol/action_entries.zig");
const ge = @import("../protocol/group_entries.zig");
const shard_mod = @import("../node/shard.zig");
const connection_mod = @import("../node/connection.zig");
const waiter_pool_mod = @import("../node/waiter_pool.zig");
const entry_mod = @import("../storage/ual/entry.zig");
const persistence_mod = @import("../storage/persistence.zig");
const view_mod = @import("../contract/view.zig");
const delivery = @import("../contract/delivery.zig");
const wire = @import("../util/wire.zig");
const stream_mod = @import("../projection/stream.zig");
const machine = @import("group_machine.zig");
const StreamHandler = @import("handler.zig").StreamHandler;

const Shard = shard_mod.Shard;
const Connection = connection_mod.Connection;
const Waiter = waiter_pool_mod.Waiter;
const ReplyTo = @import("../node/reply_to.zig").ReplyTo;
const Request = proto.Request;
const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const Result = persistence_mod.Result;
const NamespaceId = machine.NamespaceId;
const StreamID = machine.StreamID;
const Scope = machine.Scope;
const WireReader = wire.WireReader;
const Item = machine.Item.Value;
const Meta = machine.Meta.Value;

/// The longest a group read waits for a record.
pub const MAX_WAIT_MS: u64 = 300_000;
/// A new leader ends a lease only this long past its first duty tick in its
/// term, at most, so a holder that touched the old leader isn't cut off.
const MAX_GRACE_MS: u64 = 30_000;
/// A read answer's fixed part: term, index, expiry, count.
pub const READ_FIXED = 8 + 8 + 8 + 4;
/// What a record adds to a read answer past its payload and headers: its
/// framing and its deliveries.
const RECORD_FRAMING = 8 + 8 + 1 + 4 + 1 + 4 + 4 + 4;

pub const Groups = struct {
    allocator: Allocator,
    /// Reads answered when their deliver applies.
    pending: std.ArrayListUnmanaged(Pending) = .empty,
    /// Per group (namespace id, scope), the records delivers in flight will
    /// take: a read proposes one only for records beyond these.
    claimed: std.AutoHashMapUnmanaged([4 + machine.SCOPE_LEN]u8, u32) = .empty,
    /// The expire proposed and not yet applied: one at a time.
    expiring: ?u64 = null,
    expiring_full: bool = false,
    /// The cleanup proposed and not yet applied: one at a time.
    cleaning: ?u64 = null,
    duty_term: u64 = 0,
    duty_since_ns: u64 = 0,
    /// The stream whose waiting reads are being woken: its namespace's id,
    /// then its name hash.
    waking: [12]u8 = undefined,

    const Pending = struct {
        index: u64,
        term: u64,
        reply_to: ReplyTo,
        request_id: u64,
        group: [4 + machine.SCOPE_LEN]u8,
        claims: u32,
    };

    pub fn init(allocator: Allocator) Groups {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Groups) void {
        self.pending.deinit(self.allocator);
        self.claimed.deinit(self.allocator);
    }

    fn unclaim(self: *Groups, group: [4 + machine.SCOPE_LEN]u8, n: u32) void {
        const got = self.claimed.getPtr(group) orelse return;
        got.* -|= n;
        if (got.* == 0) _ = self.claimed.remove(group);
    }
};

fn groupKey(ns: NamespaceId, scope: Scope) [4 + machine.SCOPE_LEN]u8 {
    return ns.bytes() ++ scope;
}

fn groupsOf(shard: *Shard) *Groups {
    return &shard.stream_handler.groups;
}

fn readOf(shard: *Shard) view_mod.ReadView {
    return .{ .store = &shard.defaultPartition().store };
}

/// A stream's floor and generation, as its groups read them.
pub fn streamRecOf(shard: *Shard, sk: machine.StreamKey) machine.StreamRec.Value {
    var b: [13]u8 = undefined;
    const got = (readOf(shard).get(machine.Streams_, machine.recKey(&b, sk)) catch return machine.NO_STREAM_REC) orelse return machine.NO_STREAM_REC;
    return got.v;
}

/// The group named, if its meta is this one's (another may hash the same)
/// and it isn't gone.
pub fn metaOf(shard: *Shard, ns: NamespaceId, stream: []const u8, group: []const u8) ?Meta {
    var b: [4 + machine.SCOPE_LEN]u8 = undefined;
    const scope = machine.scopeOf(ns, stream, group);
    const got = (readOf(shard).get(machine.Metas, machine.storeKey(&b, ns, &scope)) catch return null) orelse return null;
    if (!std.mem.eql(u8, got.v.stream, stream) or !std.mem.eql(u8, got.v.group, group)) return null;
    if (!machine.live(got.v, streamRecOf(shard, machine.streamOf(ns, stream)))) return null;
    return got.v;
}

/// A group's records at or below its stream's floor that cleanup hasn't
/// taken yet: counted in ready, leased or dead until it does.
pub const Trimmed = struct { ready: u64 = 0, leased: u64 = 0, dead: u64 = 0, total: u64 = 0 };

/// A group's records at or below its stream's floor that cleanup hasn't
/// taken yet, by state: its counts hold them until it does.
pub fn trimmedPending(shard: *Shard, ns: NamespaceId, scope: Scope) Trimmed {
    const floor = machine.floorOf(streamRecOf(shard, machine.streamOfScope(ns, &scope)));
    var t: Trimmed = .{};
    var kb: [4 + machine.SCOPE_LEN]u8 = undefined;
    var it = readOf(shard).range(machine.Items, machine.storeKey(&kb, ns, &scope));
    while (it.next() catch null) |item| {
        if (machine.recordOf(item.key[4..]).greaterThan(floor)) break;
        switch (item.value.v.status) {
            .ready => t.ready += 1,
            .leased => t.leased += 1,
            .dead => t.dead += 1,
        }
        t.total += 1;
    }
    return t;
}

/// Whether a group holds any record at or below its stream's floor: its
/// first record, as records sort by id.
fn hasTrimmed(shard: *Shard, ns: NamespaceId, scope: Scope) bool {
    const floor = machine.floorOf(streamRecOf(shard, machine.streamOfScope(ns, &scope)));
    var kb: [4 + machine.SCOPE_LEN]u8 = undefined;
    var it = readOf(shard).range(machine.Items, machine.storeKey(&kb, ns, &scope));
    const first = (it.next() catch null) orelse return false;
    return !machine.recordOf(first.key[4..]).greaterThan(floor);
}

fn metaByScope(shard: *Shard, ns: NamespaceId, scope: Scope) ?Meta {
    var b: [4 + machine.SCOPE_LEN]u8 = undefined;
    const got = (readOf(shard).get(machine.Metas, machine.storeKey(&b, ns, &scope)) catch return null) orelse return null;
    if (!machine.live(got.v, streamRecOf(shard, machine.streamOfScope(ns, &scope)))) return null;
    return got.v;
}

pub fn itemOf(shard: *Shard, ns: NamespaceId, scope: Scope, record: StreamID) ?Item {
    var b: [4 + machine.ID_LEN]u8 = undefined;
    const id = machine.idOf(scope, record);
    const got = (readOf(shard).get(machine.Items, machine.storeKey(&b, ns, &id)) catch return null) orelse return null;
    return got.v;
}

fn propose(shard: *Shard, comptime etype: EntryType, body: anytype) !persistence_mod.ProposeResult {
    const P = ge.Payload(@backingInt(etype));
    const size = try P.encodedSize(body);
    if (size <= shard.entry_buf.len) return shard.raft_node.propose(etype, entry_mod.Flags.NONE, try P.encodeInto(shard.entry_buf[0..size], body));
    const buf = try shard.allocator.alloc(u8, size);
    defer shard.allocator.free(buf);
    return shard.raft_node.propose(etype, entry_mod.Flags.NONE, try P.encodeInto(buf, body));
}

fn nsToMs(ns: u64) i64 {
    return @intCast(ns / std.time.ns_per_ms);
}

const NOT_LEADING: proto.Refusal = .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: this node's leadership isn't settled yet; retry" };

fn refuse(shard: *Shard, conn: *Connection, request_id: u64, status: proto.StatusCode, message: []const u8) void {
    shard.sendRefusal(conn, request_id, status, proto.unclassified(status, message));
}

/// A request's group and, if it names one, consumer.
const Named = struct { group: []const u8, consumer: []const u8, rest: WireReader };

fn named(req: Request, with_consumer: bool) ?Named {
    var r = WireReader.init(req.value);
    const group = r.readLengthPrefixed(u16) orelse return null;
    if (group.len == 0 or group.len > ge.MAX_GROUP_NAME) return null;
    const consumer = if (with_consumer) (r.readLengthPrefixed(u16) orelse "") else "";
    return .{ .group = group, .consumer = consumer, .rest = r };
}

/// What a request's stream and group must be to name a group, or null.
fn check(shard: *Shard, conn: *Connection, req: Request) bool {
    if (req.key.len == 0) {
        refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a stream name is required");
        return false;
    }
    if (req.key.len > ge.StreamName.max_size) {
        refuse(shard, conn, req.header.request_id, .bad_request, "bad request: stream name too long");
        return false;
    }
    // Names hold no NUL, as streams' and every other resource's don't.
    var r = WireReader.init(req.value);
    const group = r.readLengthPrefixed(u16) orelse "";
    if (std.mem.indexOfScalar(u8, req.key, 0) != null or std.mem.indexOfScalar(u8, group, 0) != null) {
        refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a stream or group name must not contain NUL");
        return false;
    }
    if (group.len > ge.MAX_GROUP_NAME) {
        refuse(shard, conn, req.header.request_id, .bad_request, std.fmt.comptimePrint("bad request: group name too long (at most {d} bytes)", .{ge.MAX_GROUP_NAME}));
        return false;
    }
    if (!shard.raft_node.leaderReady()) {
        shard.sendRefusal(conn, req.header.request_id, .unavailable, NOT_LEADING);
        return false;
    }
    // A namespace this node can't name, answered as a write to it is.
    if (req.ns_id == .invalid) {
        shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(error.NamespaceUnknown), persistence_mod.failureRefusal(error.NamespaceUnknown, "group op not run"));
        return false;
    }
    return true;
}

// ── Removed ops ─────────────────────────────────────────────────────────

/// Ops groups no longer have. Each is refused naming what replaces it,
/// whatever namespace it names: no namespace need exist to say so.
pub const REMOVED = [_]proto.OpCode{ .stream_group_join, .stream_group_leave, .stream_group_claim, .stream_group_configure_sweeper };

/// An op groups no longer have: refused, naming what replaces it.
pub fn dispatchRemoved(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    const op: proto.OpCode = @fromBackingInt(@intCast(req.header.op_code));
    refuse(shard, conn, req.header.request_id, .bad_request, switch (op) {
        .stream_group_join, .stream_group_leave => "bad request: groups have no join or leave: a group's members are the consumers holding its records; read to take some, ack or nack to let them go",
        .stream_group_claim => "bad request: groups have no claim: a read takes expired and nacked records before new ones; ack, nack and touch name a read's deliveries",
        else => "bad request: a group's lease (ack timeout) and attempts (max deliver) are set when it is created",
    });
}

// ── Create, delete ──────────────────────────────────────────────────────

pub fn dispatchCreate(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    shard.namespace_handler.proposeImplicitCreate(req.namespace, shard);
    if (!check(shard, conn, req)) return;
    const n = named(req, false) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    const lease_ms: u64 = if (req.findOption(.ack_timeout_ms)) |o| o.asU32() orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: ack_timeout_ms is a u32") else ge.DEFAULT_LEASE_MS;
    if (lease_ms < ae.MIN_LEASE_MS or lease_ms > ae.MAX_LEASE_MS)
        return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group's lease (ack timeout) is 5 s to 12 h; a record is redelivered once it runs out");
    const attempts: u32 = if (req.findOption(.max_deliver)) |o| o.asU8() orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: max_deliver is a u8") else ge.DEFAULT_MAX_ATTEMPTS;
    if (attempts == 0) return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: max_deliver is 1 to 255: a record is dead once nacked or expired that many times");
    const proposed = propose(shard, .group_create, ge.Create.Value{ .ns = req.ns_id.int(), .stream = req.key, .group = n.group, .lease_ms = lease_ms, .max_attempts = attempts }) catch |err|
        return shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "group create not persisted"));
    shard.park(conn, req, proposed, answerFirstInt(u8));
}

pub fn dispatchDelete(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    if (!check(shard, conn, req)) return;
    const n = named(req, false) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    const proposed = propose(shard, .group_delete, ge.Delete.Value{ .ns = req.ns_id.int(), .stream = req.key, .group = n.group }) catch |err|
        return shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "group delete not persisted"));
    shard.park(conn, req, proposed, answerFirstInt(u64));
}

fn answerFirstInt(comptime T: type) Shard.AnswerFn {
    return struct {
        fn answer(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
            var r = result.reader();
            var buf: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &buf, r.int(T) orelse 0, .little);
            shard.sendOkResponse(conn, request_id, &buf);
        }
    }.answer;
}

// ── Read ────────────────────────────────────────────────────────────────

/// A read's asks, as it was sent and as its waiter keeps them.
const Params = struct {
    ns: NamespaceId,
    scope: Scope,
    count: u32,
    consumer: []const u8,

    /// `[ns id][scope][count:u16][stream_len:u16][stream][group_len:u16]`
    /// `[group][consumer]`: the stream first, so an append wakes only its
    /// own stream's readers; the names, so a read of a group not yet made
    /// can make it.
    fn waiterKey(p: Params, buf: []u8, stream: []const u8, group: []const u8) ?[]const u8 {
        const n = 4 + machine.SCOPE_LEN + 2 + 2 + stream.len + 2 + group.len + p.consumer.len;
        if (n > buf.len) return null;
        buf[0..4].* = p.ns.bytes();
        buf[4..20].* = p.scope;
        std.mem.writeInt(u16, buf[20..22], @intCast(p.count), .little);
        var w: usize = 22;
        for ([_][]const u8{ stream, group }) |name| {
            std.mem.writeInt(u16, buf[w..][0..2], @intCast(name.len), .little);
            @memcpy(buf[w + 2 ..][0..name.len], name);
            w += 2 + name.len;
        }
        @memcpy(buf[w..n], p.consumer);
        return buf[0..n];
    }

    const Waiting = struct { params: Params, stream: []const u8, group: []const u8 };

    fn ofWaiterKey(k: []const u8) ?Waiting {
        if (k.len < 22) return null;
        var r = WireReader.init(k[22..]);
        const stream = r.readLengthPrefixed(u16) orelse return null;
        const group = r.readLengthPrefixed(u16) orelse return null;
        const consumer = k[22 + 2 + stream.len + 2 + group.len ..];
        return .{ .params = .{ .ns = .of(std.mem.readInt(u32, k[0..4], .big)), .scope = k[4..20].*, .count = std.mem.readInt(u16, k[20..22], .little), .consumer = consumer }, .stream = stream, .group = group };
    }
};

pub fn dispatchRead(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    if (req.key.len > 0 and req.key[req.key.len - 1] == '*')
        return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group reads a single stream, not a pattern");
    if (!check(shard, conn, req)) return;
    const n = named(req, true) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    const consumer = if (n.consumer.len > 0) n.consumer else "default";
    if (consumer.len > ae.Holder.max_size) return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: consumer name too long");
    const asked = req.getCount() orelse 0;
    const count: u32 = if (asked == 0) 100 else @min(asked, ge.MAX_BATCH);
    const block_ms: u64 = req.getBlockMs() orelse 0;
    if (block_ms > MAX_WAIT_MS) return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: block_ms is at most 300000 (5 min)");
    const p: Params = .{ .ns = req.ns_id, .scope = machine.scopeOf(req.ns_id, req.key, n.group), .count = count, .consumer = consumer };
    const self = groupsOf(shard);
    // The first read of a group creates it, at its defaults.
    const proposed = deliverHere(self, shard, conn.replyTo(), req.header.request_id, req.key, n.group, p) catch |err| switch (err) {
        // Records being read back from disk: retried, it finds them.
        error.DiskPending => return shard.sendRefusal(conn, req.header.request_id, .unavailable, proto.notRun(shard.stream_handler.diskRefusal())),
        else => false,
    };
    if (proposed) {
        conn.response_deferred = true;
        return;
    }
    if (block_ms == 0) return shard.sendOkResponse(conn, req.header.request_id, &EMPTY);
    var kb: [waiter_pool_mod.MAX_WAITER_KEY]u8 = undefined;
    const key = p.waiterKey(&kb, req.key, n.group) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: consumer name too long to wait on");
    const registered = shard.waiter_pool.register(.{
        .kind = .stream_group_read,
        .reply_to = conn.replyTo(),
        .request_id = req.header.request_id,
        .key = key,
        .min_version = 0,
        .timeout_ms = @intCast(block_ms),
    });
    if (!registered) return shard.refuseWaitersFull(conn, req.header.request_id);
    conn.response_deferred = true;
}

/// A read that took nothing.
pub const EMPTY: [READ_FIXED]u8 = @splat(0);

/// The most trimmed ready records a leader passes over looking for ones to
/// deliver, until cleanup takes them: a read never walks a large trimmed
/// backlog.
const MAX_PASSED_OVER: usize = 256;

/// What a group could deliver now, by this leader's state, in the order a
/// deliver takes them, into `ids_out`: its due ready records not trimmed,
/// then the stream's past its cursor (and its stream's floor), up to `want`.
const Available = struct { total: u32, ready: u32 };

fn available(shard: *Shard, ns: NamespaceId, stream: []const u8, scope: Scope, meta: ?Meta, want: u32, ids_out: []StreamID) Available {
    var n: u32 = 0;
    const sk = machine.streamOf(ns, stream);
    const floor = machine.floorOf(streamRecOf(shard, sk));
    if (meta != null) {
        var pb: [machine.SCOPE_LEN + 1]u8 = undefined;
        var kb: [4 + machine.SCOPE_LEN + 1]u8 = undefined;
        var it = readOf(shard).range(machine.Ready, machine.storeKey(&kb, ns, machine.readyPrefix(&pb, &scope)));
        const now = shard.raft_node.now();
        var passed: usize = 0;
        while (it.next() catch null) |item| {
            if (n >= want or n >= ids_out.len) break;
            const k = machine.parseReadyKey(item.key[4..], machine.SCOPE_LEN) orelse break;
            if (k.due_ns > now) break;
            if (k.id.len != machine.ID_LEN) continue;
            const record = machine.recordOf(k.id);
            if (!record.greaterThan(floor)) {
                passed += 1;
                if (passed == MAX_PASSED_OVER) break;
                continue;
            }
            ids_out[n] = record;
            n += 1;
        }
    }
    const ready = n;
    const cursor: StreamID = if (meta) |m| .{ .timestamp_ms = m.cursor_ms, .sequence = m.cursor_seq } else StreamID.MIN;
    const from = if (floor.greaterThan(cursor)) floor else cursor;
    const room = ids_out[@min(n, ids_out.len)..];
    const got = shard.stream_handler.stream.streamIdsAfter(sk, from, room[0..@min(room.len, want -| n)]);
    return .{ .total = n + @as(u32, @intCast(got)), .ready = ready };
}

/// Propose a deliver of what the group has for `p`, answered to `reply_to`
/// when it applies. False when it has nothing, or none could be proposed.
fn deliverHere(self: *Groups, shard: *Shard, reply_to: ReplyTo, request_id: u64, stream: []const u8, group: []const u8, p: Params) !bool {
    const meta = metaOf(shard, p.ns, stream, group);
    if (meta == null and metaByScope(shard, p.ns, p.scope) != null) return false;
    const gk = groupKey(p.ns, p.scope);
    const held = self.claimed.get(gk) orelse 0;
    var ids: [2 * ge.MAX_BATCH]StreamID = undefined;
    const av = available(shard, p.ns, stream, p.scope, meta, held +| p.count, &ids);
    const can = av.total;
    if (can <= held) return false;
    // A deliver's answer is built as it applies, from what the shard holds:
    // it takes only the records whose payloads are in hand, in the order it
    // takes them, the rest waiting on their disk reads. In flight first.
    const in_hand = shard.stream_handler.prefetch(p.ns, stream, ids[0..@min(can, ids.len)]);
    if (in_hand <= held) return if (shard.stream_handler.held != .no) error.DiskPending else false;
    const take = @min(p.count, @min(can, in_hand) - held);
    try self.pending.ensureUnusedCapacity(self.allocator, 1);
    try self.claimed.ensureUnusedCapacity(self.allocator, 1);
    const lease_ms = if (meta) |m| m.lease_ms else ge.DEFAULT_LEASE_MS;
    // A group is held to the node's queue caps (`[queues]`), as a queue is.
    const caps = shard.queue_handler.caps;
    const max_bytes: u32 = @intCast(@import("../protocol/ops.zig").MAX_RESPONSE_BODY - READ_FIXED - take * RECORD_FRAMING);
    // The leader's choice: the ready records by id, and the bound of the
    // stream's it holds (the last fresh one in hand), past which the
    // deliver takes none.
    const mine = ids[held .. held + take];
    var rb: [ge.RecordIds.max_size]u8 = undefined;
    var rw: usize = 0;
    var nr: u32 = 0;
    var through = StreamID.MIN;
    for (mine, held..) |id, i| {
        if (i < av.ready) {
            rw += (ge.RecordId.encodeInto(rb[rw..], .{ .ms = id.timestamp_ms, .seq = id.sequence }) catch break).len;
            nr += 1;
        } else through = id;
    }
    const proposed = propose(shard, .group_deliver, ge.Deliver.Value{ .ns = p.ns.int(), .stream = stream, .group = group, .count = take, .max_bytes = max_bytes, .holder = p.consumer, .lease_ms = lease_ms, .create = true, .ready = .{ .bytes = rb[0..rw], .count = nr }, .through_ms = through.timestamp_ms, .through_seq = through.sequence, .max_held = caps.max_messages, .max_held_bytes = caps.max_bytes, .shard_held_bytes = caps.shard_held_bytes }) catch |err| {
        if (!shard.quietHalted(err)) log.warn("shard {d}: read of group {s} on stream {s} not proposed: {s}", .{ shard.id, group, stream, @errorName(err) });
        return false;
    };
    self.claimed.putAssumeCapacity(gk, held + take);
    self.pending.appendAssumeCapacity(.{ .index = proposed.index, .term = proposed.term, .reply_to = reply_to, .request_id = request_id, .group = gk, .claims = take });
    return true;
}

/// A deliver applied on the leader: answer the read waiting for it with
/// the records it leased, read back from the stream.
fn deliverApplied(self: *Groups, shard: *Shard, e: *const Entry, result: *const Result) void {
    const at = for (self.pending.items, 0..) |pd, i| {
        if (pd.index == e.header.index and pd.term == e.header.term) break i;
    } else return;
    const pd = self.pending.swapRemove(at);
    self.unclaim(pd.group, pd.claims);
    if (result.refused()) |r| return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, r.status, r.refusal);
    var diag: schema.Diagnostic = .{};
    const p = ge.Deliver.decodeAll(e.payload, &diag) catch return;
    var r = result.reader();
    const n = r.int(u32) orelse 0;
    defer if (n < pd.claims) wakeStream(self, shard, machine.streamOf(.of(p.ns), p.stream));
    var records: [ge.MAX_BATCH]StreamID = undefined;
    var deliveries: [ge.MAX_BATCH]u32 = undefined;
    for (0..n) |i| {
        records[i] = .{ .timestamp_ms = r.int(u64) orelse 0, .sequence = r.int(u64) orelse 0 };
        deliveries[i] = r.int(u32) orelse 0;
    }
    const expiry = delivery.after(e.header.timestamp_ns, p.lease_ms);
    var unread: [ge.MAX_BATCH]StreamHandler.Unread = undefined;
    const body = shard.stream_handler.serializeGroupRecords(machine.streamOf(.of(p.ns), p.stream), e.header.term, e.header.index, if (n == 0) 0 else nsToMs(expiry), records[0..n], deliveries[0..n], unread[0..n]) catch
        return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .internal_error, .{ .reason = .answer_lost, .ran = .yes, .message = "internal error: out of memory for the records; they come back once their lease runs out" });
    defer shard.allocator.free(body);
    shard.deliverDeferredOk(pd.reply_to, pd.request_id, body);
    freeUnread(shard, &p, e, expiry, records[0..n], deliveries[0..n], unread[0..n]);
}

const LOST = "the record's payload can't be read: it is gone from the stream's storage, or malformed";

/// Hand back the records a deliver leased that its answer couldn't carry,
/// ready again now rather than at their lease's end: those whose payload
/// was on its way back from disk with no failure counted, those whose
/// payload is lost as a failure, so their attempts run out and they die
/// rather than go round forever. Best effort; were a proposal lost, the
/// lease's expiry still returns them, counting a failure.
fn freeUnread(shard: *Shard, p: *const ge.Deliver.Value, e: *const Entry, expiry: u64, records: []const StreamID, deliveries: []const u32, unread: []const StreamHandler.Unread) void {
    for ([_]StreamHandler.Unread{ .fetching, .lost }) |kind| {
        var buf: [ge.Items.max_size]u8 = undefined;
        var w: usize = 0;
        var count: u32 = 0;
        for (records, deliveries, unread) |id, d, u| if (u == kind) {
            w += (ge.Item.encodeInto(buf[w..], .{ .ms = id.timestamp_ms, .seq = id.sequence, .deliveries = d }) catch break).len;
            count += 1;
        };
        if (count == 0) continue;
        const free = kind == .fetching;
        _ = propose(shard, .group_release, ge.Release.Value{ .ns = p.ns, .stream = p.stream, .group = p.group, .holder = p.holder, .token = .{ .term = e.header.term, .index = e.header.index }, .expiry_ns = expiry, .items = .{ .bytes = buf[0..w], .count = count }, .delay_ms = 0, .dead = false, .@"error" = if (free) "" else LOST, .free = free }) catch {};
    }
}

// ── Waking ──────────────────────────────────────────────────────────────

/// Records may be ready on the stream: wake its waiting reads, each
/// proposing a deliver if its group has any.
pub fn wakeStream(self: *Groups, shard: *Shard, sk: machine.StreamKey) void {
    // A group read's waiter key leads with its namespace and its scope,
    // whose first part is the stream's name hash: the stream's key.
    self.waking = sk;
    shard.waiter_pool.notifyAny(.stream_group_read, resolveWaiter, @ptrCast(shard));
}

fn resolveWaiter(waiter: *Waiter, ctx: *anyopaque) bool {
    const shard: *Shard = @ptrCast(@alignCast(ctx));
    const k = waiter.key();
    if (k.len < 12 or !std.mem.eql(u8, k[0..12], &groupsOf(shard).waking)) return false;
    return resolve(shard, waiter);
}

fn resolveAny(waiter: *Waiter, ctx: *anyopaque) bool {
    return resolve(@ptrCast(@alignCast(ctx)), waiter);
}

fn resolve(shard: *Shard, waiter: *Waiter) bool {
    if (!shard.raft_node.leaderReady()) return false;
    const w = Params.ofWaiterKey(waiter.key()) orelse return false;
    return deliverHere(groupsOf(shard), shard, waiter.reply_to, waiter.request_id, w.stream, w.group, w.params) catch false;
}

// ── Reports ─────────────────────────────────────────────────────────────

const Report = enum { ack, nack, touch };

/// A report's records as its entry names them, and the first still leased
/// under its token: the expiry the leader saw.
const Parsed = struct {
    term: u64,
    index: u64,
    items: ge.Items.Value,
    lease: ?Item,
};

fn parseReport(shard: *Shard, ns: NamespaceId, scope: Scope, consumer: []const u8, r: *WireReader, buf: []u8) ?Parsed {
    const term = r.readU64() orelse return null;
    const index = r.readU64() orelse return null;
    const count = r.readU32() orelse return null;
    if (count == 0 or count > ge.MAX_BATCH) return null;
    var w: usize = 0;
    var lease: ?Item = null;
    const token: ae.Token.Value = .{ .term = term, .index = index };
    for (0..count) |_| {
        const ms = r.readU64() orelse return null;
        const seq = r.readU64() orelse return null;
        const deliveries = r.readU32() orelse return null;
        w += (ge.Item.encodeInto(buf[w..], .{ .ms = ms, .seq = seq, .deliveries = deliveries }) catch return null).len;
        if (lease == null) if (itemOf(shard, ns, scope, .{ .timestamp_ms = ms, .sequence = seq })) |it| {
            if (delivery.holds(it, consumer, deliveries, token)) lease = it;
        };
    }
    return .{ .term = term, .index = index, .items = .{ .bytes = buf[0..w], .count = count }, .lease = lease };
}

pub fn dispatchAck(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    report(@ptrCast(@alignCast(shard_ptr)), @ptrCast(@alignCast(conn_ptr)), req, .ack);
}

pub fn dispatchNack(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    report(@ptrCast(@alignCast(shard_ptr)), @ptrCast(@alignCast(conn_ptr)), req, .nack);
}

pub fn dispatchTouch(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    report(@ptrCast(@alignCast(shard_ptr)), @ptrCast(@alignCast(conn_ptr)), req, .touch);
}

fn report(shard: *Shard, conn: *Connection, req: Request, kind: Report) void {
    if (!check(shard, conn, req)) return;
    var n = named(req, true) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    const consumer = if (n.consumer.len > 0) n.consumer else "default";
    const scope = machine.scopeOf(req.ns_id, req.key, n.group);
    var buf: [ge.Items.max_size]u8 = undefined;
    const parsed = parseReport(shard, req.ns_id, scope, consumer, &n.rest, &buf) orelse
        return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a report names its delivery (term, index) and 1 to 100 records, each by id and deliveries");
    const token: ae.Token.Value = .{ .term = parsed.term, .index = parsed.index };
    const expiry = if (parsed.lease) |l| l.expiry_ns else 0;
    const proposed = switch (kind) {
        .ack => propose(shard, .group_settle, ge.Settle.Value{ .ns = req.ns_id.int(), .stream = req.key, .group = n.group, .holder = consumer, .token = token, .expiry_ns = expiry, .items = parsed.items }),
        .nack => blk: {
            const delay: u64 = if (req.findOption(.delay_ms)) |o| o.asU64() orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: delay_ms is a u64") else 0;
            if (delay > ge.MAX_DELAY_MS) return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: delay_ms is at most 86400000 (a day)");
            // Its error in the body: an option's length is one byte.
            const text = n.rest.readLengthPrefixed(u16) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a nack ends with its error, [error_len:u16][error], 0 for none");
            if (text.len > ge.MAX_ERROR) return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: an error is at most 1024 bytes");
            break :blk propose(shard, .group_release, ge.Release.Value{ .ns = req.ns_id.int(), .stream = req.key, .group = n.group, .holder = consumer, .token = token, .expiry_ns = expiry, .items = parsed.items, .delay_ms = delay, .dead = req.findOption(.dead) != null, .@"error" = text, .free = false });
        },
        .touch => blk: {
            const l = parsed.lease orelse Item{ .scope = "", .status = .ready, .due_ns = 0, .deliveries = 0, .failures = 0, .holder = "", .request_key = null, .token = token, .lease_ms = ae.MIN_LEASE_MS, .delivered_ns = 0, .expiry_ns = 0, .ended = .none, .started_ns = 0, .@"error" = "", .dead_ns = 0 };
            break :blk propose(shard, .group_extend, ge.Extend.Value{ .ns = req.ns_id.int(), .stream = req.key, .group = n.group, .holder = consumer, .token = token, .expiry_ns = l.expiry_ns, .items = parsed.items, .held_since_ns = l.delivered_ns, .lease_ms = std.math.clamp(l.lease_ms, ae.MIN_LEASE_MS, ae.MAX_LEASE_MS) });
        },
    } catch |err| return shard.sendRefusal(conn, req.header.request_id, persistence_mod.failureStatus(err), persistence_mod.failureRefusal(err, "report not persisted"));
    shard.park(conn, req, proposed, if (kind == .touch) answerTouch else answerReport);
}

/// A report's answer: `[applied:u32][refused:u32]` then each refused.
fn reportAnswer(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result, touch: bool) void {
    var r = result.reader();
    const applied = r.int(u32) orelse 0;
    const to = if (touch) r.int(u64) orelse 0 else 0;
    var buf: [8 + 4 + 4 + ge.MAX_BATCH * 17]u8 = undefined;
    var w: usize = 0;
    if (touch) {
        std.mem.writeInt(i64, buf[0..8], nsToMs(to), .little);
        w = 8;
    }
    std.mem.writeInt(u32, buf[w..][0..4], applied, .little);
    const count_at = w + 4;
    w += 8;
    var refused: u32 = 0;
    while (r.int(u64)) |ms| {
        const seq = r.int(u64) orelse break;
        const why = r.int(u8) orelse break;
        std.mem.writeInt(u64, buf[w..][0..8], ms, .little);
        std.mem.writeInt(u64, buf[w + 8 ..][0..8], seq, .little);
        buf[w + 16] = why;
        w += 17;
        refused += 1;
    }
    std.mem.writeInt(u32, buf[count_at..][0..4], refused, .little);
    shard.sendOkResponse(conn, request_id, buf[0..w]);
}

fn answerReport(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
    reportAnswer(shard, conn, request_id, result, false);
}

fn answerTouch(shard: *Shard, conn: *Connection, request_id: u64, result: *const Result) void {
    reportAnswer(shard, conn, request_id, result, true);
}

// ── Info, pending ───────────────────────────────────────────────────────

/// The holders of a group's records out now, counted once each.
fn holders(shard: *Shard, ns: NamespaceId, scope: Scope, out: [][]const u8) usize {
    var n: usize = 0;
    var kb: [4 + machine.SCOPE_LEN]u8 = undefined;
    var it = readOf(shard).range(machine.Items, machine.storeKey(&kb, ns, &scope));
    while (it.next() catch null) |item| {
        const v = item.value.v;
        if (v.status != .leased) continue;
        for (out[0..n]) |h| {
            if (std.mem.eql(u8, h, v.holder)) break;
        } else {
            if (n == out.len) return n;
            out[n] = v.holder;
            n += 1;
        }
    }
    return n;
}

pub fn dispatchInfo(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    if (!check(shard, conn, req)) return;
    const n = named(req, false) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    const meta = metaOf(shard, req.ns_id, req.key, n.group) orelse return refuse(shard, conn, req.header.request_id, .not_found, machine.NO_GROUP);
    var hb: [1024][]const u8 = undefined;
    const h = holders(shard, req.ns_id, machine.scopeOf(req.ns_id, req.key, n.group), &hb);
    // Each state's count leaves out its trimmed records: they are
    // `trimmed_pending`, until cleanup takes them.
    const t = trimmedPending(shard, req.ns_id, machine.scopeOf(req.ns_id, req.key, n.group));
    const ready = meta.ready - t.ready;
    const leased = meta.leased - t.leased;
    var buf: [52]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], ready + leased, .little);
    std.mem.writeInt(u32, buf[8..12], @intCast(h), .little);
    std.mem.writeInt(u64, buf[12..20], meta.created_ns, .little);
    std.mem.writeInt(u64, buf[20..28], ready, .little);
    std.mem.writeInt(u64, buf[28..36], leased, .little);
    std.mem.writeInt(u64, buf[36..44], meta.dead - t.dead, .little);
    std.mem.writeInt(u64, buf[44..52], t.total, .little);
    shard.sendOkResponse(conn, req.header.request_id, &buf);
}

pub fn dispatchPending(shard_ptr: *anyopaque, conn_ptr: *anyopaque, req: Request) void {
    const shard: *Shard = @ptrCast(@alignCast(shard_ptr));
    const conn: *Connection = @ptrCast(@alignCast(conn_ptr));
    if (!check(shard, conn, req)) return;
    const n = named(req, true) orelse return refuse(shard, conn, req.header.request_id, .bad_request, "bad request: a group name is required");
    if (metaOf(shard, req.ns_id, req.key, n.group) == null) return refuse(shard, conn, req.header.request_id, .not_found, machine.NO_GROUP);
    const scope = machine.scopeOf(req.ns_id, req.key, n.group);
    var w: std.Io.Writer.Allocating = .init(shard.allocator);
    defer w.deinit();
    w.writer.writeInt(u32, 0, .little) catch return refuse(shard, conn, req.header.request_id, .overloaded, "overloaded: no memory for this answer; retry");
    var count: u32 = 0;
    var kb: [4 + machine.SCOPE_LEN]u8 = undefined;
    var it = readOf(shard).range(machine.Items, machine.storeKey(&kb, req.ns_id, &scope));
    while (it.next() catch null) |item| {
        if (count == ge.MAX_BATCH) break;
        const v = item.value.v;
        if (v.status != .leased) continue;
        if (n.consumer.len > 0 and !std.mem.eql(u8, v.holder, n.consumer)) continue;
        const record = machine.recordOf(item.key[4..]);
        const ok = blk: {
            w.writer.writeInt(u64, record.timestamp_ms, .little) catch break :blk false;
            w.writer.writeInt(u64, record.sequence, .little) catch break :blk false;
            w.writer.writeInt(u32, v.deliveries, .little) catch break :blk false;
            w.writer.writeInt(u16, @intCast(v.holder.len), .little) catch break :blk false;
            w.writer.writeAll(v.holder) catch break :blk false;
            break :blk true;
        };
        if (!ok) return refuse(shard, conn, req.header.request_id, .overloaded, "overloaded: no memory for this answer; retry");
        count += 1;
    }
    const out = w.written();
    std.mem.writeInt(u32, out[0..4], count, .little);
    shard.sendOkResponse(conn, req.header.request_id, out);
}

// ── After apply ─────────────────────────────────────────────────────────

/// What the leader does once a group entry applies: answer the read a
/// deliver was for, wake reads for records newly ready, and chain a full
/// expire into the next. Every replica runs it; only a leader holds
/// anything it acts on.
pub fn afterApply(shard: *Shard, e: *const Entry, result: *const Result) void {
    const self = groupsOf(shard);
    const etype: EntryType = @fromBackingInt(@intCast(e.header.entry_type));
    const leads = shard.raft_node.role == .leader;
    var diag: schema.Diagnostic = .{};
    switch (etype) {
        .group_deliver => if (leads) deliverApplied(self, shard, e, result),
        .group_release => if (leads and result.refused() == null) {
            const p = ge.Release.decodeAll(e.payload, &diag) catch return;
            if (p.delay_ms == 0) wakeStream(self, shard, machine.streamOf(.of(p.ns), p.stream));
        },
        .group_clean => if (self.cleaning) |i| if (i == e.header.index) {
            self.cleaning = null;
        },
        .group_expire => {
            if (self.expiring) |i| if (i == e.header.index) {
                self.expiring = null;
                if (self.expiring_full and shard.raft_node.leaderReady()) _ = expireNext(self, shard, shard.raft_node.now());
            };
            if (leads) shard.waiter_pool.notifyAny(.stream_group_read, resolveAny, @ptrCast(shard));
        },
        else => {},
    }
}

/// A stream append applied: its stream's waiting reads may have records.
pub fn appended(shard: *Shard, sk: machine.StreamKey) void {
    if (shard.raft_node.role != .leader) return;
    wakeStream(groupsOf(shard), shard, sk);
}

// ── The expire duty, and leadership ─────────────────────────────────────

/// The leader's tick: end leases past their deadline, as one expire of
/// the earliest keys past due. A new leader ends a lease only past a grace
/// of min(lease, 30 s) after it first ran in its term. Also wakes reads
/// waiting on a group whose delayed records came due. Returns how many keys
/// it named.
pub fn expireDue(shard: *Shard) u64 {
    const self = groupsOf(shard);
    if (shard.raft_node.role == .leader and shard.heldIfHalted(.group_expire, shard.defaultPartition().store.family(machine.Deadlines.ID).count > 0)) {
        self.duty_term = 0;
        return 0;
    }
    if (!shard.raft_node.leaderReady()) return 0;
    shard.duties.note(.group_expire, null, @import("stdx").time.monotonicMs());
    const now = shard.raft_node.now();
    if (self.duty_term != shard.raft_node.current_term) {
        self.duty_term = shard.raft_node.current_term;
        self.duty_since_ns = now;
    }
    shard.waiter_pool.notifyAny(.stream_group_read, resolveAny, @ptrCast(shard));
    return expireNext(self, shard, now) + cleanNext(self, shard);
}

/// Propose a cleanup of groups holding records they no longer should: a
/// gone group's, or those at or below its stream's floor. One at a time.
fn cleanNext(self: *Groups, shard: *Shard) u64 {
    if (self.cleaning != null) return 0;
    var items: [ge.MAX_CLEAN_GROUPS * ge.CleanItem.max_size]u8 = undefined;
    var w: usize = 0;
    var n: u32 = 0;
    var it = readOf(shard).range(machine.Metas, "");
    while (it.next() catch null) |m| {
        if (n == ge.MAX_CLEAN_GROUPS) break;
        if (m.key.len != 4 + machine.SCOPE_LEN) continue;
        const ns: NamespaceId = .of(std.mem.readInt(u32, m.key[0..4], .big));
        const scope: Scope = m.key[4..][0..machine.SCOPE_LEN].*;
        const rec = streamRecOf(shard, machine.streamOfScope(ns, &scope));
        const due = !machine.live(m.value.v, rec) or hasTrimmed(shard, ns, scope);
        if (!due) continue;
        w += (ge.CleanItem.encodeInto(items[w..], .{ .ns = ns.int(), .scope = &scope }) catch break).len;
        n += 1;
    }
    if (n == 0) return 0;
    const proposed = propose(shard, .group_clean, ge.Clean.Value{ .items = .{ .bytes = items[0..w], .count = n } }) catch |err| {
        if (!shard.quietHalted(err)) log.warn("shard {d}: group records to remove not cleaned: {s}", .{ shard.id, @errorName(err) });
        return 0;
    };
    self.cleaning = proposed.index;
    return n;
}

fn expireNext(self: *Groups, shard: *Shard, now: u64) u64 {
    if (self.expiring != null) return 0;
    var items: [ae.MAX_EXPIRE_ITEMS * ge.ExpireItem.max_size]u8 = undefined;
    var w: usize = 0;
    var n: u32 = 0;
    var it = machine.pastDue(readOf(shard), now);
    var visited: usize = 0;
    while (it.next()) |due| {
        if (n == ae.MAX_EXPIRE_ITEMS or visited == 4 * ae.MAX_EXPIRE_ITEMS) break;
        visited += 1;
        const k = due.key;
        if (k.id.len != machine.ID_LEN) continue;
        if (itemOf(shard, k.ns, k.id[0..machine.SCOPE_LEN].*, machine.recordOf(k.id))) |m| if (m.status == .leased and m.expiry_ns == k.expiry_ns) {
            const grace = @as(u64, @min(m.lease_ms, MAX_GRACE_MS)) *| std.time.ns_per_ms;
            if (now < self.duty_since_ns +| grace) continue;
        };
        w += (ge.ExpireItem.encodeInto(items[w..], .{ .ns = k.ns.int(), .id = k.id, .expiry_ns = k.expiry_ns }) catch break).len;
        n += 1;
    }
    if (n == 0) return 0;
    const proposed = propose(shard, .group_expire, ge.Expire.Value{ .items = .{ .bytes = items[0..w], .count = n } }) catch |err| {
        if (!shard.quietHalted(err)) log.warn("shard {d}: group leases past their deadline not ended: {s}", .{ shard.id, @errorName(err) });
        return 0;
    };
    self.expiring = proposed.index;
    self.expiring_full = n == ae.MAX_EXPIRE_ITEMS;
    return n;
}

/// A group entry a repair skipped: its read is told it didn't run.
pub fn skipped(shard: *Shard, e: *const Entry) void {
    const self = groupsOf(shard);
    // The duty's own entry skipped: free to propose the next.
    if (self.expiring == e.header.index) self.expiring = null;
    if (self.cleaning == e.header.index) self.cleaning = null;
    for (self.pending.items, 0..) |pd, i| if (pd.index == e.header.index and pd.term == e.header.term) {
        _ = self.pending.swapRemove(i);
        self.unclaim(pd.group, pd.claims);
        return shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .no, .message = "unavailable: a repair skipped this read's entry, so it took nothing — retry" });
    };
}

/// This node stopped leading: reads waiting on a deliver are told, and the
/// duty starts its grace again under the next term it leads.
pub fn stoppedLeading(shard: *Shard) void {
    const self = groupsOf(shard);
    for (self.pending.items) |pd| {
        shard.deliverDeferredRefusal(pd.reply_to, pd.request_id, .unavailable, .{ .reason = .leader_unknown, .ran = .unknown, .message = "unavailable: this node stopped leading before the records were given; any it leased come back once their lease runs out — retry" });
    }
    self.pending.clearRetainingCapacity();
    self.claimed.clearRetainingCapacity();
    self.expiring = null;
    self.cleaning = null;
    self.duty_term = 0;
}
