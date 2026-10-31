//! The action family's log entries. Every change to an action or a run is
//! one of these, so every replica, a restarted node and a new leader hold
//! the same runs: who holds each, at which delivery, until when.
//!
//! A delivery is fenced by its token, the (term, index) of the deliver
//! entry that made it, which no other delivery can share: a report or a
//! touch naming another token applies nothing. Times are the entry's
//! stamp plus a duration, so every replica computes the same deadline.
//! The namespace is a field, never empty: the default one is "default".

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const action_ops = @import("action_ops.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;
const BoundedList = schema.BoundedList;

pub const Ns = Bytes(limits.MAX_NAMESPACE_NAME);
pub const MAX_ACTION_NAME: u32 = 256;
pub const ActionName = Bytes(MAX_ACTION_NAME);
/// Run ids are made by the server, 32 bytes.
pub const MAX_RUN_ID: u32 = 32;
pub const RunId = Bytes(MAX_RUN_ID);
pub const Holder = Bytes(action_ops.MAX_WORKER_ID);
pub const MAX_INPUT: u32 = 64 * 1024;
pub const MAX_LABELS: u32 = 4096;
pub const MAX_OWNER: u32 = 256;
/// The longest lease a deliver or an extend may ask for.
pub const MAX_LEASE_MS: u64 = 60 * 60 * 1000;
/// The longest a release may hold a run back before it is ready again.
pub const MAX_DELAY_MS: u64 = 24 * 60 * 60 * 1000;
/// The most leases one expire entry ends.
pub const MAX_EXPIRE_ITEMS: u32 = 256;
/// A retried await's key, so it gets the delivery the first one made.
pub const MAX_REQUEST_KEY: u32 = 64;

pub const LeaseMs = schema.DurationMs(MAX_LEASE_MS);

/// Where a run came from.
pub const Source = enum(u8) { direct = 0, workflow = 1, trigger = 2 };

/// What a worker settled a run with.
pub const Outcome = enum(u8) { success = 0, failure = 1, pending = 2 };

/// The delivery a report or touch names: the run's delivery count when it
/// was made, and the deliver entry's (term, index).
pub const Token = Record(&.{
    .{ .name = "term", .T = Int(u64) },
    .{ .name = "index", .T = Int(u64) },
});

pub const Register = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "name", .T = ActionName },
    .{ .name = "owner", .T = Bytes(MAX_OWNER) },
    .{ .name = "version", .T = Int(u32) },
    .{ .name = "lease_ms", .T = LeaseMs },
    .{ .name = "max_attempts", .T = schema.Range(u32, 1, 1000) },
});

pub const Delete = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "name", .T = ActionName },
});

pub const Invoke = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "input", .T = Bytes(MAX_INPUT) },
    .{ .name = "labels", .T = Optional(Bytes(MAX_LABELS)) },
    .{ .name = "source", .T = schema.Enum(Source) },
    .{ .name = "caller_run", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
    .{ .name = "caller_workflow", .T = Optional(Bytes(limits.MAX_QUALIFIED_KEY)) },
});

/// A worker given a ready run. `due` is the ready key the leader chose it
/// by, so the entry names that key; a run that has moved since isn't
/// delivered.
pub const Deliver = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "due_ns", .T = Int(u64) },
    .{ .name = "holder", .T = Holder },
    .{ .name = "request_key", .T = Optional(Bytes(MAX_REQUEST_KEY)) },
    // The leader fills in the action's lease when the await names none,
    // so the deadline follows from the entry alone.
    .{ .name = "lease_ms", .T = LeaseMs },
});

pub const Settle = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "run", .T = RunId },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "token", .T = Token },
    .{ .name = "outcome", .T = schema.Enum(Outcome) },
    .{ .name = "result", .T = Bytes(action_ops.MAX_OUTPUT) },
});

/// A delivery handed back: failed (retried unless `retry` is false or its
/// attempts are spent), or abandoned, which counts the delivery but not a
/// failure.
pub const Release = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "deliveries", .T = Int(u32) },
    .{ .name = "token", .T = Token },
    .{ .name = "abandoned", .T = schema.Bool },
    .{ .name = "retry", .T = schema.Bool },
    .{ .name = "delay_ms", .T = schema.DurationMs(MAX_DELAY_MS) },
    .{ .name = "error", .T = Bytes(action_ops.MAX_ERROR) },
});

pub const Extend = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "run", .T = RunId },
    .{ .name = "token", .T = Token },
    .{ .name = "lease_ms", .T = LeaseMs },
});

/// A deadline key the expire duty found past due: (`expiry_ns`, run). The
/// key is the lease's identity: if the run is leased until exactly then,
/// that lease has expired, whichever delivery or extend set it; otherwise
/// the lease moved on and only the key goes.
pub const ExpireItem = Record(&.{
    .{ .name = "ns", .T = Ns },
    .{ .name = "action", .T = ActionName },
    .{ .name = "run", .T = RunId },
    .{ .name = "expiry_ns", .T = Int(u64) },
});

pub const Expire = Record(&.{
    .{ .name = "items", .T = BoundedList(ExpireItem, MAX_EXPIRE_ITEMS, MAX_EXPIRE_ITEMS * ExpireItem.max_size) },
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
