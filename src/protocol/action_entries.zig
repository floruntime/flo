//! The action family's log entries. Every change to an action or a run is
//! one of these, so every replica, a restarted node and a new leader hold
//! the same runs: who holds each, at which delivery, until when.
//!
//! A delivery is fenced by its token, the (term, index) of the deliver
//! entry that made it, which no other delivery can share: a report or a
//! touch naming another token applies nothing. Times are the entry's
//! stamp plus a duration, so every replica computes the same deadline.
//! The namespace is its id, leading each entry (or each expire item), so
//! the same action or run id in two namespaces is two keys.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const action_ops = @import("action_ops.zig");
const kv_entries = @import("kv_entries.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;
const BoundedList = schema.BoundedList;

/// A namespace's id (`namespace/table.zig`); 0 names none.
pub const NamespaceId = kv_entries.NamespaceId;
pub const MAX_ACTION_NAME: u32 = 256;
pub const ActionName = Bytes(MAX_ACTION_NAME);
/// Run ids are made by the server, 32 bytes.
pub const MAX_RUN_ID: u32 = 32;
pub const RunId = Bytes(MAX_RUN_ID);
pub const Holder = Bytes(action_ops.MAX_WORKER_ID);
pub const MAX_INPUT: u32 = 64 * 1024;
pub const MAX_LABELS: u32 = 4096;
pub const MAX_OWNER: u32 = action_ops.MAX_OWNER;
/// A lease's floor, above three election timeouts, so a new leader's
/// grace never outlasts it; and its ceiling, which also bounds how long
/// one delivery may be held, however often it's touched.
pub const MIN_LEASE_MS: u64 = 5 * 1000;
pub const MAX_LEASE_MS: u64 = 12 * 60 * 60 * 1000;
pub const MAX_HOLD_MS: u64 = MAX_LEASE_MS;
/// The longest a release may hold a run back before it is ready again.
pub const MAX_DELAY_MS: u64 = 24 * 60 * 60 * 1000;
/// The most leases one expire entry ends: at about 2.4 µs a lease, 24
/// apply in under a quarter of a single-node commit, and a backlog drains
/// as one expire after another.
pub const MAX_EXPIRE_ITEMS: u32 = 24;
/// A retried await's key, so it gets the delivery the first one made.
pub const MAX_REQUEST_KEY: u32 = 64;

pub const LeaseMs = schema.Range(u64, MIN_LEASE_MS, MAX_LEASE_MS);
/// A named outcome a workflow step follows ("success" when none is named).
pub const MAX_OUTCOME: u32 = 256;
pub const Outcome = Bytes(MAX_OUTCOME);

/// Where a run came from.
pub const Source = enum(u8) { direct = 0, workflow = 1, trigger = 2 };

/// The delivery a report or touch names: the deliver entry's (term, index).
pub const Token = Record(&.{
    .{ .name = "term", .T = Int(u64) },
    .{ .name = "index", .T = Int(u64) },
});

pub const Register = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "name", .T = ActionName },
    .{ .name = "owner", .T = Bytes(MAX_OWNER) },
    .{ .name = "lease_ms", .T = LeaseMs },
    .{ .name = "max_attempts", .T = schema.Range(u32, 1, 1000) },
});

pub const Delete = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "name", .T = ActionName },
});

pub const Invoke = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "input", .T = Bytes(MAX_INPUT) },
    .{ .name = "labels", .T = Optional(Bytes(MAX_LABELS)) },
    .{ .name = "source", .T = schema.Enum(Source) },
    .{ .name = "caller_run", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
    .{ .name = "caller_workflow", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
    // The shard the calling workflow's run is on, which its run id may
    // not say (a client may choose it): the run's end is told there.
    .{ .name = "caller_shard", .T = Optional(Int(u16)) },
});

/// A worker given a ready run. `due` is the ready key the leader chose it
/// by, so the entry names that key; a run that has moved since isn't
/// delivered.
pub const Deliver = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "due_ns", .T = Int(u64) },
    .{ .name = "holder", .T = Holder },
    .{ .name = "request_key", .T = Optional(Bytes(MAX_REQUEST_KEY)) },
    // The leader fills in the action's lease when the await names none,
    // so the deadline follows from the entry alone.
    .{ .name = "lease_ms", .T = LeaseMs },
});

/// A delivery completed by its holder. `expiry_ns` is the run's expiry as
/// the leader saw it: its deadline key goes with the lease when it is
/// still the run's.
pub const RequestKey = Optional(Bytes(MAX_REQUEST_KEY));

pub const Settle = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "holder", .T = Holder },
    // The delivery's request key, if it had one: its record goes too.
    .{ .name = "request_key", .T = RequestKey },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "outcome", .T = Outcome },
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
});

/// A delivery handed back: failed (retried unless `retry` is false or its
/// attempts are spent), or abandoned, which counts the delivery but not a
/// failure.
pub const Release = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "holder", .T = Holder },
    .{ .name = "request_key", .T = RequestKey },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "abandoned", .T = schema.Bool },
    .{ .name = "retry", .T = schema.Bool },
    .{ .name = "delay_ms", .T = schema.DurationMs(MAX_DELAY_MS) },
    .{ .name = "error", .T = Bytes(action_ops.MAX_ERROR) },
});

/// A delivery's lease moved out. It names the delivery's own lease, when
/// the delivery began (`held_since_ns`, so the hold ceiling follows from
/// the entry) and the expiry the leader saw, whose key it replaces.
pub const Extend = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "holder", .T = Holder },
    .{ .name = "token", .T = Token },
    .{ .name = "expiry_ns", .T = Int(u64) },
    .{ .name = "held_since_ns", .T = Int(u64) },
    .{ .name = "lease_ms", .T = LeaseMs },
});

/// A deadline key the expire duty found past due: (`expiry_ns`, run). The
/// key is the lease's identity: if the run is leased until exactly then,
/// that lease has expired, whichever delivery or extend set it; otherwise
/// the lease moved on and only the key goes.
pub const ExpireItem = Record(&.{
    .{ .name = "ns", .T = NamespaceId },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "expiry_ns", .T = Int(u64) },
    // The live delivery's holder and request key, as the duty read them:
    // its request record goes with the lease.
    .{ .name = "holder", .T = Holder },
    .{ .name = "request_key", .T = RequestKey },
});

/// The most bytes one expire's items take: a holder can be long, so the
/// duty stops adding items short of it.
pub const MAX_EXPIRE_BYTES: u32 = 512 * 1024;

pub const Expire = Record(&.{
    .{ .name = "items", .T = BoundedList(ExpireItem, MAX_EXPIRE_ITEMS, MAX_EXPIRE_BYTES) },
});

pub const table = [_]entries.Entry{
    .{ .name = "action.register", .type_code = 0x70, .payload = Register, .max_payload = Register.max_size, .doc = "An action registered, or its definition replaced." },
    .{ .name = "action.delete", .type_code = 0x71, .payload = Delete, .max_payload = Delete.max_size, .doc = "An action deleted." },
    .{ .name = "action.invoke", .type_code = 0x72, .payload = Invoke, .max_payload = Invoke.max_size, .doc = "A run created, ready at once." },
    .{ .name = "action.deliver", .type_code = 0x74, .payload = Deliver, .max_payload = Deliver.max_size, .doc = "A ready run given to a worker under a new token." },
    .{ .name = "action.settle", .type_code = 0x75, .payload = Settle, .max_payload = Settle.max_size, .doc = "A delivery completed by its holder." },
    .{ .name = "action.release", .type_code = 0x76, .payload = Release, .max_payload = Release.max_size, .doc = "A delivery failed or abandoned by its holder." },
    .{ .name = "action.extend", .type_code = 0x77, .payload = Extend, .max_payload = Extend.max_size, .doc = "A delivery's lease moved out by its holder." },
    .{ .name = "action.expire", .type_code = 0x78, .payload = Expire, .max_payload = Expire.max_size, .doc = "Leases past their deadline ended, as failures." },
};

/// The payload schema of entry type `code`.
pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no action entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
