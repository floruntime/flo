//! The KV family's ops. The namespace and key travel in the envelope, so
//! they aren't fields; for `kv.scan` the key is the prefix, and for the
//! transaction ops it is the routing key the transaction is pinned by.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bool = schema.Bool;
const Bytes = schema.Bytes;
const String = schema.String;
const Optional = schema.Optional;
const Defaulted = schema.Defaulted;
const Record = schema.Record;

/// The largest value a write takes: what one request holds once the
/// longest key, routing key and every other `kv.set` field are in. KV
/// builds its own log entries, sized to the request frame, so the frame is
/// the bound.
pub const MAX_VALUE: u32 = @intCast(ops.MAX_REQUEST_BODY - SetOthers.max_size - 4);

/// The largest document `kv.json_set` takes, the same way: its path is the
/// longer field, so its document is a little smaller than a value.
pub const MAX_DOCUMENT: u32 = @intCast(ops.MAX_REQUEST_BODY - JsonSetOthers.max_size - 4);

const JsonSetOthers = Record(&.{
    .{ .name = "path", .T = Defaulted(String(MAX_PATH), "$") },
    .{ .name = "routing_key", .T = RoutingKey },
});

/// `kv.set`'s fields but its value (whose u32 length prefix is the 4).
const SetOthers = Record(&.{
    .{ .name = "ttl_ms", .T = Optional(Ttl) },
    .{ .name = "condition", .T = Defaulted(schema.Enum(Condition), .always) },
    .{ .name = "cas_version", .T = Optional(Version) },
    .{ .name = "routing_key", .T = RoutingKey },
    .{ .name = "txn", .T = Txn },
});
/// The longest TTL: one that still fits in nanoseconds.
pub const MAX_TTL_MS: u64 = std.math.maxInt(u64) / std.time.ns_per_ms;
/// The longest a read may wait for a key.
pub const MAX_WAIT_MS: u64 = 300_000;
pub const MAX_PATH: u32 = 1024;
pub const MAX_SCAN: u32 = 1024;
pub const MAX_MGET: u32 = 256;
/// Versions a key keeps: the current one and its chain.
pub const MAX_HISTORY: u32 = 65;
pub const MAX_CURSOR: u32 = 64;

/// Room an answer's list has once its other fields are in.
fn listRoom(comptime others: usize) u32 {
    return @intCast(ops.MAX_RESPONSE_BODY - others);
}

pub const RoutingKey = Optional(Bytes(ops.MAX_KEY));
pub const Txn = Optional(Int(u64));
pub const Version = Int(u64);
pub const Ttl = DurationMs(MAX_TTL_MS);

fn DurationMs(comptime max: u64) type {
    return schema.DurationMs(max);
}

/// What `kv.get` waits for when it waits.
pub const WaitFor = enum(u8) {
    /// The key to exist; one that exists answers at once.
    exists = 0,
    /// The key's next version after the one current when the read arrived.
    change = 1,
};

/// When `kv.set` may write.
pub const Condition = enum(u8) {
    always = 0,
    if_absent = 1,
    if_present = 2,
};

pub const HistoryEntry = Record(&.{
    .{ .name = "version", .T = Version },
    .{ .name = "at_ms", .T = Int(i64) },
    .{ .name = "value", .T = Bytes(MAX_VALUE) },
});

pub const MgetEntry = Record(&.{
    .{ .name = "found", .T = Bool },
    .{ .name = "version", .T = Version },
    .{ .name = "value", .T = Bytes(MAX_VALUE) },
});

/// The op in `table` named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no KV op named '" ++ name ++ "'");
}

pub const table = [_]ops.Op{
    .{
        .name = "kv.get",
        .code = 0x101,
        .class = .read,
        .wait = .{ .field = "wait_ms", .default_ms = 0, .max_ms = MAX_WAIT_MS },
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "wait_ms", .T = Defaulted(DurationMs(MAX_WAIT_MS), 0) },
            .{ .name = "wait_for", .T = Defaulted(schema.Enum(WaitFor), .exists) },
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        .response = Record(&.{
            .{ .name = "version", .T = Version },
            .{ .name = "value", .T = Bytes(MAX_VALUE) },
        }),
        .doc = "Read a key's value, waiting up to wait_ms for it to exist or change.",
    },
    .{
        .name = "kv.set",
        .code = 0x100,
        .class = .write,
        .creates = true,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "value", .T = Bytes(MAX_VALUE) },
            .{ .name = "ttl_ms", .T = Optional(Ttl) },
            .{ .name = "condition", .T = Defaulted(schema.Enum(Condition), .always) },
            .{ .name = "cas_version", .T = Optional(Version) },
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        // Null inside a transaction: the write has no version until commit.
        .response = Record(&.{.{ .name = "version", .T = Optional(Version) }}),
        .doc = "Set a key's value.",
    },
    .{
        .name = "kv.delete",
        .code = 0x103,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "cas_version", .T = Optional(Version) },
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        // Null inside a transaction, where it isn't known until commit.
        .response = Record(&.{.{ .name = "existed", .T = Optional(Bool) }}),
        .doc = "Delete a key; deleting one that doesn't exist succeeds.",
    },
    .{
        .name = "kv.scan",
        .code = 0x104,
        .class = .read,
        .fanout = .walk,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_SCAN), MAX_SCAN) },
            .{ .name = "cursor", .T = Bytes(MAX_CURSOR) },
        }),
        .response = Record(&.{
            .{ .name = "names", .T = schema.BoundedList(String(ops.MAX_KEY), MAX_SCAN, listRoom(4 + 1 + 1 + MAX_CURSOR)) },
            // Null on the last page.
            .{ .name = "cursor", .T = Optional(Bytes(MAX_CURSOR)) },
        }),
        .doc = "List the names of keys starting with a prefix, a page at a time.",
    },
    .{
        .name = "kv.history",
        .code = 0x105,
        .class = .read,
        .request = Record(&.{.{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_HISTORY), 10) }}),
        .response = Record(&.{.{ .name = "versions", .T = schema.BoundedList(HistoryEntry, MAX_HISTORY, listRoom(1)) }}),
        .doc = "A key's versions, newest first, as many as fit one answer.",
    },
    .{
        .name = "kv.mget",
        .code = 0x102,
        .class = .read,
        .route = .local,
        .request = Record(&.{.{ .name = "keys", .T = schema.BoundedList(Bytes(ops.MAX_KEY), MAX_MGET, ops.MAX_REQUEST_BODY - 2) }}),
        .response = Record(&.{
            // In the order asked, as many as fit one answer.
            .{ .name = "entries", .T = schema.BoundedList(MgetEntry, MAX_MGET, listRoom(2 + 1 + 2)) },
            // The index of the first key not answered; ask again from it.
            .{ .name = "next", .T = Optional(Int(u16)) },
        }),
        .doc = "Read many keys at once.",
    },
    .{
        .name = "kv.incr",
        .code = 0x10B,
        .class = .write,
        .creates = true,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "delta", .T = Defaulted(Int(i64), 1) },
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        // Both null inside a transaction: the sum isn't known until commit.
        .response = Record(&.{
            .{ .name = "version", .T = Optional(Version) },
            .{ .name = "value", .T = Optional(Int(i64)) },
        }),
        .doc = "Add delta to a key's 8-byte counter, starting from 0.",
    },
    .{
        .name = "kv.expire",
        .code = 0x113,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{
            // 0 clears the key's TTL.
            .{ .name = "ttl_ms", .T = Ttl },
            .{ .name = "cas_version", .T = Optional(Version) },
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        .response = Record(&.{}),
        .doc = "Set or clear (ttl_ms 0) a key's TTL.",
    },
    .{
        .name = "kv.exists",
        .code = 0x115,
        .class = .read,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "routing_key", .T = RoutingKey },
            .{ .name = "txn", .T = Txn },
        }),
        .response = Record(&.{
            .{ .name = "exists", .T = Bool },
            .{ .name = "version", .T = Version },
        }),
        .doc = "Whether a key exists, and its version.",
    },
    .{
        .name = "kv.json_get",
        .code = 0x10C,
        .class = .read,
        .request = Record(&.{
            .{ .name = "path", .T = Defaulted(String(MAX_PATH), "$") },
            .{ .name = "routing_key", .T = RoutingKey },
        }),
        .response = Record(&.{
            .{ .name = "version", .T = Version },
            .{ .name = "document", .T = Bytes(MAX_VALUE) },
        }),
        .doc = "Read the JSON at a path in a key's document.",
    },
    .{
        .name = "kv.json_set",
        .code = 0x10D,
        .class = .write,
        .creates = true,
        .request = Record(&.{
            .{ .name = "path", .T = Defaulted(String(MAX_PATH), "$") },
            .{ .name = "document", .T = schema.Parsed("json", MAX_DOCUMENT) },
            .{ .name = "routing_key", .T = RoutingKey },
        }),
        .response = Record(&.{.{ .name = "version", .T = Version }}),
        .doc = "Set the JSON at a path in a key's document.",
    },
    .{
        .name = "kv.json_del",
        .code = 0x10E,
        .class = .write,
        .request = Record(&.{
            .{ .name = "path", .T = Defaulted(String(MAX_PATH), "$") },
            .{ .name = "routing_key", .T = RoutingKey },
        }),
        // Null when the path is the whole document, which deletes the key.
        .response = Record(&.{.{ .name = "version", .T = Optional(Version) }}),
        .doc = "Delete the JSON at a path; the path $ deletes the key.",
    },
    .{
        .name = "kv.txn_begin",
        .code = 0x110,
        .class = .write,
        .creates = true,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "txn", .T = Int(u64) },
            .{ .name = "pinned", .T = Int(u64) },
        }),
        .doc = "Begin a transaction on the shard that owns the key.",
    },
    .{
        .name = "kv.txn_commit",
        .code = 0x111,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{.{ .name = "txn", .T = Int(u64) }}),
        .response = Record(&.{
            .{ .name = "commit_index", .T = Int(u64) },
            .{ .name = "ops", .T = Int(u16) },
        }),
        .doc = "Commit a transaction's writes as one.",
    },
    .{
        .name = "kv.txn_rollback",
        .code = 0x112,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{.{ .name = "txn", .T = Int(u64) }}),
        .response = Record(&.{}),
        .doc = "Drop a transaction and its writes.",
    },
};

comptime {
    ops.check(&table);
}

test "kv ops: the table passes its rules, and a value is as large as the frame allows" {
    try std.testing.expectEqual(@as(usize, 15), table.len);
    // The frame less its header, the longest namespace and key, and the
    // other kv.set fields at their largest.
    try std.testing.expectEqual(@as(u32, 253817), MAX_VALUE);
    try std.testing.expectEqual(@as(u32, 252819), MAX_DOCUMENT);
    // A value that large still comes back in history and mget, one a page.
    inline for (.{ HistoryEntry, MgetEntry }) |E| try std.testing.expect(E.max_size < ops.MAX_RESPONSE_BODY);
}
