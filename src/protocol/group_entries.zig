//! A consumer group's log entries. Every change to which of a stream's
//! records a group has delivered, to whom, until when, and which are dead,
//! is one of these, so every replica, a restarted node and a new leader
//! hold the same.
//!
//! A deliver names no records: it leases a group's ready records first,
//! then the stream's records past the group's cursor, as every replica
//! holds them at that index. A report names its records and their
//! delivery's token, the (term, index) of the deliver that leased them, so
//! a stale one applies nothing. A namespace is its id; a group is its
//! stream's name and its own.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const ops = @import("ops.zig");
const proto = @import("proto.zig");
const ae = @import("action_entries.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;
const BoundedList = schema.BoundedList;

/// A namespace's id (`namespace/table.zig`); 0 names none.
pub const NamespaceId = Int(u32);
pub const StreamName = Bytes(@intCast(ops.MAX_KEY));
pub const MAX_GROUP_NAME: u32 = 256;
pub const GroupName = Bytes(MAX_GROUP_NAME);
/// The most records one deliver leases, and one report names: as many as a
/// deliver's answer in its result holds (`group_machine` checks it).
pub const MAX_BATCH: u32 = 100;
/// A group's default lease, and the most failures before a record is dead.
pub const DEFAULT_LEASE_MS: u64 = 30 * 1000;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;
pub const MAX_ATTEMPTS: u32 = 255;
pub const MAX_DELAY_MS: u64 = ae.MAX_DELAY_MS;
pub const MAX_ERROR: u32 = 1024;
pub const Holder = ae.Holder;
pub const Token = ae.Token;
pub const LeaseMs = ae.LeaseMs;

/// A group of a stream, with its lease and attempts. A create of a group
/// that exists with these settings changes nothing; with others it is
/// refused, naming the group's own.
pub const Create = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "stream", .T = StreamName },
    .{ .name = "group", .T = GroupName },
    .{ .name = "lease_ms", .T = LeaseMs },
    .{ .name = "max_attempts", .T = schema.Range(u32, 1, MAX_ATTEMPTS) },
});

/// A group gone, and every record it held.
pub const Delete = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "stream", .T = StreamName },
    .{ .name = "group", .T = GroupName },
});

/// A record a deliver or a report names: its id in the stream.
pub const RecordId = Record(&.{
    .{ .name = "ms", .T = Int(u64) },
    .{ .name = "seq", .T = Int(u64) },
});
pub const RecordIds = schema.List(RecordId, MAX_BATCH);

/// Up to `count` of the group's records leased to `holder`: the ready ones
/// the leader chose (`ready`), each still ready and due, then the stream's
/// past the cursor up to `through`, the bound the leader held. The ready
/// ones are a subset of those named; the fresh ones may include records an
/// earlier deliver left, but none past the bound, all in the prefix whose
/// payloads the leader holds, so the answer carries what the deliver takes.
/// `max_bytes` bounds that answer. A group not there is created first, at
/// its defaults, when `create` is set: a read creates the group it names.
pub const Deliver = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "stream", .T = StreamName },
    .{ .name = "group", .T = GroupName },
    .{ .name = "count", .T = schema.Range(u32, 1, MAX_BATCH) },
    .{ .name = "max_bytes", .T = Int(u32) },
    .{ .name = "holder", .T = Holder },
    .{ .name = "lease_ms", .T = LeaseMs },
    .{ .name = "create", .T = schema.Bool },
    .{ .name = "ready", .T = RecordIds },
    .{ .name = "through_ms", .T = Int(u64) },
    .{ .name = "through_seq", .T = Int(u64) },
    // The proposing leader's caps (`[queues]`): new records are taken only
    // while the group, and the shard's queues and groups, hold no more.
    .{ .name = "max_held", .T = Int(u32) },
    .{ .name = "max_held_bytes", .T = Int(u64) },
    .{ .name = "shard_held_bytes", .T = Int(u64) },
});

/// A record a report names: its id, and the delivery count its holder was
/// given with it.
pub const Item = Record(&.{
    .{ .name = "ms", .T = Int(u64) },
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "deliveries", .T = Int(u32) },
});
pub const Items = schema.List(Item, MAX_BATCH);

/// What a report names: its group, its holder and delivery, and the
/// lease's expiry as the leader saw it, whose deadline keys go when they
/// are still its.
const report = [_]schema.Field{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "stream", .T = StreamName },
    .{ .name = "group", .T = GroupName },
    .{ .name = "holder", .T = Holder },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "items", .T = Items },
};

/// Records done with by their holder: acked.
pub const Settle = Record(&report);

/// How records were handed back.
pub const HandBack = enum(u8) {
    /// They failed: a nack. Ready again after `delay_ms`, or dead at once
    /// (`dead`) or once their attempts are spent.
    failed = 0,
    /// Their holder never started them (a worker stopping): ready again at
    /// once, the delivery counted, no failure.
    abandoned = 1,
    /// The leader's own, for records a read leased but its answer couldn't
    /// carry: ready again at once, neither delivery nor failure counted.
    unread = 2,
};

/// Records handed back, by their holder or, `unread`, by the leader.
pub const Release = Record(&(report ++ [_]schema.Field{
    .{ .name = "delay_ms", .T = schema.DurationMs(MAX_DELAY_MS) },
    .{ .name = "dead", .T = schema.Bool },
    .{ .name = "error", .T = Bytes(MAX_ERROR) },
    .{ .name = "ended", .T = schema.Enum(HandBack) },
}));

/// A delivery's lease moved out. It names the delivery's own lease and when
/// it began, so the hold ceiling follows from the entry.
pub const Extend = Record(&(report ++ [_]schema.Field{
    .{ .name = "held_since_ns", .T = Int(u64) },
    .{ .name = "lease_ms", .T = LeaseMs },
}));

/// An item's id in its namespace: its group's scope (its stream's name
/// hash, then its group's), then the record's id, all big-endian.
pub const SCOPE_LEN = 16;
pub const ID_LEN = SCOPE_LEN + 16;
pub const ItemId = Bytes(ID_LEN);

/// A deadline key the expire duty found past due.
pub const ExpireItem = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "id", .T = ItemId },
    .{ .name = "expiry_ns", .T = Int(u64) },
});

pub const Expire = Record(&.{
    .{ .name = "items", .T = schema.List(ExpireItem, ae.MAX_EXPIRE_ITEMS) },
});

/// A group the cleanup duty names: its namespace and scope.
pub const CleanItem = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "scope", .T = Bytes(SCOPE_LEN) },
});

/// The most groups one cleanup names, and records it removes from each.
pub const MAX_CLEAN_GROUPS: u32 = 8;
pub const CLEAN_RECORDS: u32 = 256;

/// Records a group no longer holds removed, at most `CLEAN_RECORDS` from each
/// group named: all of a deleted group's, or those a trim of its stream cut.
pub const Clean = Record(&.{
    .{ .name = "items", .T = schema.List(CleanItem, MAX_CLEAN_GROUPS) },
});

pub const table = [_]entries.Entry{
    .{ .name = "group.create", .type_code = 0x43, .payload = Create, .max_payload = Create.max_size, .doc = "A consumer group of a stream, with its lease and attempts." },
    .{ .name = "group.delete", .type_code = 0x44, .payload = Delete, .max_payload = Delete.max_size, .doc = "A consumer group removed, with every record it held." },
    .{ .name = "group.deliver", .type_code = 0x45, .payload = Deliver, .max_payload = Deliver.max_size, .doc = "A group's ready records, then the stream's past its cursor, leased to a consumer under a new token." },
    .{ .name = "group.settle", .type_code = 0x46, .payload = Settle, .max_payload = Settle.max_size, .doc = "Leased records acked by their holder." },
    .{ .name = "group.release", .type_code = 0x47, .payload = Release, .max_payload = Release.max_size, .doc = "Leased records nacked by their holder: ready again, or dead." },
    .{ .name = "group.extend", .type_code = 0x48, .payload = Extend, .max_payload = Extend.max_size, .doc = "A delivery's lease moved out by its holder." },
    .{ .name = "group.expire", .type_code = 0x49, .payload = Expire, .max_payload = Expire.max_size, .doc = "Leases past their deadline ended, as failures." },
    .{ .name = "group.clean", .type_code = 0x4A, .payload = Clean, .max_payload = Clean.max_size, .doc = "Records a group no longer holds removed, a bounded number at a time." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no group entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
