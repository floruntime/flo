//! The queue family's log entries. Every change to a queue's messages is
//! one of these, so every replica, a restarted node and a new leader hold
//! the same messages: which are ready, which are leased to whom, until when,
//! and which are dead.
//!
//! A deliver doesn't name its messages: it applies to the first ready ones
//! of its queue in (priority, due, seq) order, as every replica holds them.
//! A report names its messages and their delivery's token, the (term,
//! index) of the deliver that leased them, so a stale one applies nothing.
//! Times are the entry's stamp plus a duration, so every replica computes
//! the same deadline. A namespace is its id; a queue is its name.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const ops = @import("ops.zig");
const ae = @import("action_entries.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;
const BoundedList = schema.BoundedList;

/// A namespace's id (`namespace/table.zig`); 0 names none.
pub const NamespaceId = Int(u32);
pub const QueueName = Bytes(@intCast(ops.MAX_KEY));
/// The most one message holds.
pub const MAX_MESSAGE: u32 = 64 * 1024;
/// The most messages one deliver leases, and one report names.
pub const MAX_BATCH: u32 = 100;
/// A queue's default lease, and the most delivery attempts before a
/// message is dead.
pub const DEFAULT_LEASE_MS: u64 = 30 * 1000;
pub const MAX_ATTEMPTS: u32 = 5;
/// The longest an enqueue or a release may hold a message back.
pub const MAX_DELAY_MS: u64 = ae.MAX_DELAY_MS;
/// What a queue may hold, ready, leased and dead together, unless the
/// node's settings say otherwise (`[queues]` in flo.toml), and the most
/// they may say. Past it, an enqueue is refused; nothing held is dropped.
pub const DEFAULT_MAX_MESSAGES: u32 = 100_000;
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_MAX_MESSAGES: u32 = 16 * 1024 * 1024;
pub const MAX_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// The payload bytes all of a shard's queues may hold together.
pub const DEFAULT_SHARD_HELD_BYTES: u64 = 1024 * 1024 * 1024;
/// What a message holds besides its payload and its error text: its
/// record and its keys, measured at 336 bytes in RAM (macOS, ReleaseSafe,
/// 100,000 messages; see the queue machine's per-message test), and a dead
/// one's dead key, rounded up. Charged against the caps with its payload, so
/// empty messages fill a queue too.
pub const PER_MESSAGE: u64 = 512;
pub const MAX_SHARD_HELD_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const MAX_ERROR: u32 = 1024;
/// A delivery's holder: the consumer the dequeue named, or none.
pub const Holder = ae.Holder;
pub const Token = ae.Token;
pub const LeaseMs = ae.LeaseMs;

pub const Enqueue = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
    // Lower first; 0 is the highest.
    .{ .name = "priority", .T = Int(u32) },
    .{ .name = "delay_ms", .T = schema.DurationMs(MAX_DELAY_MS) },
    .{ .name = "payload", .T = Bytes(MAX_MESSAGE) },
    // The proposing leader's caps: apply refuses the message when its
    // queue or its shard would hold more. Carried in the entry, so every
    // replica decides alike whatever its own settings.
    .{ .name = "max_messages", .T = Int(u32) },
    .{ .name = "max_bytes", .T = Int(u64) },
    .{ .name = "shard_held_bytes", .T = Int(u64) },
});

/// Up to `count` of the queue's due ready messages leased to `holder`.
/// `max_bytes` is the most their payloads may take, so the answer that
/// carries them fits.
pub const Deliver = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
    .{ .name = "count", .T = schema.Range(u32, 1, MAX_BATCH) },
    .{ .name = "max_bytes", .T = Int(u32) },
    .{ .name = "holder", .T = Holder },
    .{ .name = "lease_ms", .T = LeaseMs },
});

/// A message a report names: its seq, and the delivery count its holder
/// was given with it.
pub const Item = Record(&.{
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "deliveries", .T = Int(u32) },
});
pub const Items = schema.List(Item, MAX_BATCH);

/// Messages done with by their holder. `expiry_ns` is their lease's expiry
/// as the leader saw it: its deadline keys go when it is still theirs.
pub const Settle = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
    .{ .name = "holder", .T = Holder },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "items", .T = Items },
});

/// Messages handed back by their holder: ready again after `delay_ms`, or
/// dead at once (`dead`) or once their attempts are spent.
pub const Release = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
    .{ .name = "holder", .T = Holder },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "items", .T = Items },
    .{ .name = "delay_ms", .T = schema.DurationMs(MAX_DELAY_MS) },
    .{ .name = "dead", .T = schema.Bool },
    .{ .name = "error", .T = Bytes(MAX_ERROR) },
});

/// A delivery's lease moved out. It names the delivery's own lease, when
/// the delivery began (so the hold ceiling follows from the entry) and the
/// expiry the leader saw, whose keys it replaces.
pub const Extend = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
    .{ .name = "holder", .T = Holder },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "held_since_ns", .T = Int(u64) },
    .{ .name = "lease_ms", .T = LeaseMs },
    .{ .name = "items", .T = Items },
});

/// The id a queue's message has in its namespace: the queue's name hash,
/// then its seq, both big-endian.
pub const ID_LEN = 16;
pub const MessageId = Bytes(ID_LEN);

/// A deadline key the expire duty found past due. The key is the lease's
/// identity: if the message is leased until exactly then, that lease has
/// expired; otherwise the lease moved on and only the key goes.
pub const ExpireItem = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "id", .T = MessageId },
    .{ .name = "expiry_ns", .T = Int(u64) },
});

pub const Expire = Record(&.{
    .{ .name = "items", .T = schema.List(ExpireItem, ae.MAX_EXPIRE_ITEMS) },
});

/// Every message of a queue gone, ready, leased and dead.
pub const Purge = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "queue", .T = QueueName },
});

pub const table = [_]entries.Entry{
    .{ .name = "queue.enqueue", .type_code = 0x20, .payload = Enqueue, .max_payload = Enqueue.max_size, .doc = "A message added to a queue, ready once its delay passes." },
    .{ .name = "queue.deliver", .type_code = 0x21, .payload = Deliver, .max_payload = Deliver.max_size, .doc = "A queue's next ready messages leased to a consumer under a new token." },
    .{ .name = "queue.settle", .type_code = 0x22, .payload = Settle, .max_payload = Settle.max_size, .doc = "Leased messages completed by their holder." },
    .{ .name = "queue.release", .type_code = 0x23, .payload = Release, .max_payload = Release.max_size, .doc = "Leased messages failed by their holder: ready again, or dead." },
    .{ .name = "queue.extend", .type_code = 0x24, .payload = Extend, .max_payload = Extend.max_size, .doc = "A delivery's lease moved out by its holder." },
    .{ .name = "queue.expire", .type_code = 0x25, .payload = Expire, .max_payload = Expire.max_size, .doc = "Leases past their deadline ended, as failures." },
    .{ .name = "queue.purge", .type_code = 0x26, .payload = Purge, .max_payload = Purge.max_size, .doc = "Every message of a queue removed." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no queue entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
