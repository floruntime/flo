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

/// The largest document `kv.json.set` takes, the same way: its path is the
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
/// A key's history keeps at most this many older versions, and at most
/// `HISTORY_MAX_BYTES` of them, each charged its value's bytes plus
/// `HISTORY_ENTRY_COST`; the oldest go first, and the newest older version
/// is always kept. Every replica evicts by these, so changing one changes
/// what every replica keeps: a format change, not a tuning knob.
pub const HISTORY_MAX_VERSIONS: u32 = 64;
pub const HISTORY_MAX_BYTES: u32 = 64 * 1024;
/// A literal, not a struct's size, so what replicas keep doesn't depend
/// on a layout or the pointer width.
pub const HISTORY_ENTRY_COST: u32 = 64;
/// Versions one history answer can list: the current one and the chain.
pub const MAX_HISTORY: u32 = HISTORY_MAX_VERSIONS + 1;
/// A scan's cursor: the shard it stopped on (u16), then the last key it
/// returned.
pub const MAX_CURSOR: u32 = 2 + @as(u32, @intCast(ops.MAX_KEY));

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
    .{ .name = "deleted", .T = Bool },
    .{ .name = "value", .T = Bytes(MAX_VALUE) },
});

pub const MgetEntry = Record(&.{
    .{ .name = "found", .T = Bool },
    .{ .name = "version", .T = Version },
    .{ .name = "value", .T = Bytes(MAX_VALUE) },
});

/// A kv.set body carrying only `value`: every other field absent or its
/// default.
pub fn plainSetBody(buf: []u8, value: []const u8) schema.EncodeError![]const u8 {
    return op("kv.set").request.encodeInto(buf, .{ .value = value, .ttl_ms = null, .condition = .always, .cas_version = null, .routing_key = null, .txn = null });
}

/// A kv.get body that doesn't wait, route or join a transaction.
pub fn plainGetBody(buf: []u8) schema.EncodeError![]const u8 {
    return op("kv.get").request.encodeInto(buf, .{ .wait_ms = 0, .wait_for = .exists, .routing_key = null, .txn = null });
}

/// The op in `table` named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no KV op named '" ++ name ++ "'");
}

// Fields several ops share, with the help text their flags carry.
const routing_key_field: schema.Field = .{ .name = "routing_key", .T = RoutingKey, .doc = "Routing key for shard co-location (same as {tag} in the key)" };
const txn_field: schema.Field = .{ .name = "txn", .T = Txn, .doc = "Transaction id from `kv txn begin`; omit outside a transaction" };
const cas_field: schema.Field = .{ .name = "cas_version", .T = Optional(Version), .doc = "Only if the key's version is this; 0 means the key doesn't exist" };
const path_field: schema.Field = .{ .name = "path", .T = Defaulted(String(MAX_PATH), "$"), .doc = "JSONPath expression" };
const txn_id_field: schema.Field = .{ .name = "txn", .T = Int(u64), .doc = "Transaction id from `kv txn begin`" };

/// Short flags most KV commands share.
const rk_short: ops.Short = .{ .field = "routing_key", .letter = 'r' };
const txn_short: ops.Short = .{ .field = "txn", .letter = 't' };

pub const table = [_]ops.Op{
    .{
        .name = "kv.get",
        .code = 0x101,
        .class = .read,
        .wait = .{ .field = "wait_ms", .default_ms = 0, .max_ms = MAX_WAIT_MS },
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "wait_ms", .T = Defaulted(DurationMs(MAX_WAIT_MS), 0), .doc = "How long to wait: milliseconds, or a number and a unit (500ms, 30s, 5m); at most 5m; 0 doesn't wait" },
            .{ .name = "wait_for", .T = Defaulted(schema.Enum(WaitFor), .exists), .doc = "What to wait for: exists (answers at once if the key exists) or change (the next version); on timeout, a change wait prints the unchanged value" },
            routing_key_field,
            txn_field,
        }),
        .response = Record(&.{
            .{ .name = "version", .T = Version, .doc = "The key's version" },
            .{ .name = "value", .T = Bytes(MAX_VALUE), .doc = "The key's value" },
            .{ .name = "expires_at_ms", .T = Optional(Int(i64)), .doc = "When the key's TTL runs out, in milliseconds since the epoch; absent with no TTL" },
        }),
        .cli = .{
            .render = .raw,
            .show = "value",
            .short = &.{ .{ .field = "wait_ms", .letter = 'w' }, rk_short, txn_short },
            .examples = &.{
                "flo kv get mykey",
                "flo kv get mykey -o json",
                "flo kv get mykey --wait-ms 5s",
                "flo kv get mykey --wait-ms 5s --wait-for change",
                "flo kv get balance:alice --routing-key user:123",
            },
        },
        .doc = "Read a key's value, waiting up to wait_ms for it to exist or change.",
    },
    .{
        .name = "kv.set",
        .code = 0x100,
        .class = .write,
        .creates = true,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "value", .T = Bytes(MAX_VALUE), .doc = "The value to store" },
            .{ .name = "ttl_ms", .T = Optional(Ttl), .doc = "Time to live: milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d); omitted for none" },
            .{ .name = "condition", .T = Defaulted(schema.Enum(Condition), .always), .doc = "always, if_absent (only if the key doesn't exist) or if_present (only if it does)" },
            cas_field,
            routing_key_field,
            txn_field,
        }),
        // Null inside a transaction: the write has no version until commit.
        .response = Record(&.{.{ .name = "version", .T = Optional(Version), .doc = "The key's new version" }}),
        .cli = .{
            .args = &.{"value"},
            .short = &.{ rk_short, txn_short },
            .examples = &.{
                "flo kv set mykey myvalue",
                "flo kv set mykey myvalue --ttl-ms 1h",
                "flo kv set mykey myvalue --condition if_absent",
                "flo kv set mykey newvalue --cas-version 42",
                "flo kv set balance:alice 500 --routing-key user:123",
            },
        },
        .doc = "Set a key's value.",
    },
    .{
        .name = "kv.delete",
        .code = 0x103,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{ cas_field, routing_key_field, txn_field }),
        // Null inside a transaction, where it isn't known until commit.
        .response = Record(&.{.{ .name = "existed", .T = Optional(Bool), .doc = "Whether the key existed" }}),
        .cli = .{ .short = &.{ rk_short, txn_short } },
        .doc = "Delete a key; deleting one that doesn't exist succeeds.",
    },
    .{
        .name = "kv.scan",
        .code = 0x104,
        .class = .read,
        .fanout = .walk,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_SCAN), MAX_SCAN), .doc = "Most names on the page" },
            .{ .name = "cursor", .T = Bytes(MAX_CURSOR), .doc = "Where the last page stopped: the hex cursor it printed; omit to start" },
        }),
        .response = Record(&.{
            .{ .name = "names", .T = schema.BoundedList(String(ops.MAX_KEY), MAX_SCAN, listRoom(4 + 1 + 1 + MAX_CURSOR)), .doc = "Key names: in order within a shard, shard by shard across them" },
            // Null on the last page.
            .{ .name = "cursor", .T = Optional(Bytes(MAX_CURSOR)), .doc = "Where the next page starts; none on the last page" },
        }),
        .cli = .{
            .key = .{ .name = "prefix", .optional = true, .doc = "Only keys starting with this" },
            .render = .table,
            .hex = &.{"cursor"},
            .short = &.{.{ .field = "limit", .letter = 'l' }},
            .examples = &.{
                "flo kv scan",
                "flo kv scan user: --limit 100",
                "flo kv scan user: --cursor 0000757365723a3432",
            },
        },
        .doc = "List the names of keys starting with a prefix, a page at a time.",
    },
    .{
        .name = "kv.history",
        .code = 0x105,
        .class = .read,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_HISTORY), 10), .doc = "Most versions to show" },
            .{ .name = "before_version", .T = Optional(Version), .doc = "Only versions older than this one; omitted for from the newest" },
            routing_key_field,
        }),
        .response = Record(&.{
            .{ .name = "versions", .T = schema.BoundedList(HistoryEntry, MAX_HISTORY, listRoom(1 + Bool.max_size)), .doc = "The key's versions, newest first" },
            .{ .name = "more", .T = Bool, .doc = "Whether older kept versions follow: ask again with before_version set to the last one here" },
        }),
        .cli = .{ .expose = .hand, .render = .table, .short = &.{ .{ .field = "limit", .letter = 'l' }, rk_short } },
        .doc = "A key's versions, newest first, a page at a time.",
    },
    .{
        .name = "kv.mget",
        .code = 0x102,
        .class = .read,
        // With a routing key the batch is answered on its shard; without
        // one it runs where it lands and looks each key up on its own.
        .route = .{ .field = "routing_key" },
        .request = Record(&.{
            .{ .name = "keys", .T = schema.BoundedList(Bytes(ops.MAX_KEY), MAX_MGET, ops.MAX_REQUEST_BODY - 2 - RoutingKey.max_size), .doc = "The keys to read" },
            // Every key was written with this routing key: all are looked
            // up on its shard. Without it, each key is on its own.
            .{ .name = "routing_key", .T = RoutingKey, .doc = "The routing key every key was written with; all are read from its shard" },
        }),
        .response = Record(&.{
            // In the order asked, as many as fit one answer.
            .{ .name = "entries", .T = schema.BoundedList(MgetEntry, MAX_MGET, listRoom(2 + 1 + 2)), .doc = "One per key answered, in the order asked" },
            // The index of the first key not answered; ask again from it.
            .{ .name = "next", .T = Optional(Int(u16)), .doc = "The first key not answered; ask again from it" },
        }),
        // It sends as many requests as its keys need, so it stays written
        // by hand.
        .cli = .{ .expose = .hand, .key = null, .args = &.{"keys"} },
        .doc = "Read many keys at once.",
    },
    .{
        .name = "kv.incr",
        .code = 0x10B,
        .class = .write,
        .creates = true,
        .txn = "txn",
        .request = Record(&.{
            .{ .name = "delta", .T = Defaulted(Int(i64), 1), .doc = "What to add (may be negative)" },
            routing_key_field,
            txn_field,
        }),
        // Both null inside a transaction: the sum isn't known until commit.
        .response = Record(&.{
            .{ .name = "version", .T = Optional(Version), .doc = "The key's new version" },
            .{ .name = "value", .T = Optional(Int(i64)), .doc = "The counter's new value" },
        }),
        .cli = .{
            .render = .raw,
            .show = "value",
            .short = &.{ .{ .field = "delta", .letter = 'd' }, rk_short, txn_short },
            .examples = &.{ "flo kv incr counter", "flo kv incr counter --delta -1" },
        },
        .doc = "Add delta to a key's 8-byte counter, starting from 0.",
    },
    .{
        .name = "kv.expire",
        .code = 0x113,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{
            // 0 clears the key's TTL.
            .{ .name = "ttl_ms", .T = Ttl, .doc = "TTL from now: milliseconds, or a number and a unit (500ms, 30s, 5m, 1h, 1d); 0 clears it" },
            cas_field,
            routing_key_field,
            txn_field,
        }),
        .response = Record(&.{}),
        .cli = .{
            .short = &.{ rk_short, txn_short },
            .examples = &.{ "flo kv expire session:abc --ttl-ms 1h", "flo kv expire session:abc --ttl-ms 0" },
        },
        .doc = "Set or clear (ttl_ms 0) a key's TTL.",
    },
    .{
        .name = "kv.exists",
        .code = 0x115,
        .class = .read,
        .txn = "txn",
        .request = Record(&.{ routing_key_field, txn_field }),
        .response = Record(&.{
            .{ .name = "exists", .T = Bool, .doc = "Whether the key exists" },
            .{ .name = "version", .T = Version, .doc = "Its version; 0 for a write buffered in a transaction" },
        }),
        .cli = .{ .absent = "exists", .short = &.{ rk_short, txn_short } },
        .doc = "Whether a key exists, and its version (exit 0 if it does, 1 if not).",
    },
    .{
        .name = "kv.json.get",
        .code = 0x10C,
        .class = .read,
        .request = Record(&.{ path_field, routing_key_field }),
        .response = Record(&.{
            .{ .name = "version", .T = Version, .doc = "The key's version" },
            .{ .name = "document", .T = Bytes(MAX_VALUE), .doc = "The JSON at the path" },
        }),
        .cli = .{
            .render = .raw,
            .show = "document",
            .short = &.{rk_short},
            .examples = &.{ "flo kv json get user:123", "flo kv json get user:123 --path '$.addresses[0].city'" },
        },
        .doc = "Read the JSON at a path in a key's document.",
    },
    .{
        .name = "kv.json.set",
        .code = 0x10D,
        .class = .write,
        .creates = true,
        .request = Record(&.{
            path_field,
            .{ .name = "document", .T = schema.Parsed("json", MAX_DOCUMENT), .doc = "The JSON to put at the path" },
            routing_key_field,
        }),
        .response = Record(&.{.{ .name = "version", .T = Version, .doc = "The key's new version" }}),
        .cli = .{
            .args = &.{"document"},
            .short = &.{rk_short},
            .examples = &.{ "flo kv json set user:123 '{\"name\":\"alice\"}'", "flo kv json set user:123 '\"bob\"' --path '$.name'" },
        },
        .doc = "Set the JSON at a path in a key's document; $ on a missing key writes the whole document.",
    },
    .{
        .name = "kv.json.del",
        .code = 0x10E,
        .class = .write,
        .request = Record(&.{ path_field, routing_key_field }),
        // Null when the path is the whole document, which deletes the key.
        .response = Record(&.{.{ .name = "version", .T = Optional(Version), .doc = "The key's new version; none when the key was deleted" }}),
        .cli = .{ .short = &.{rk_short} },
        .doc = "Delete the JSON at a path; the path $ deletes the key.",
    },
    .{
        .name = "kv.txn.begin",
        .code = 0x110,
        .class = .write,
        .creates = true,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "txn", .T = Int(u64), .doc = "The transaction's id; pass it as --txn" },
            .{ .name = "pinned", .T = Int(u64), .doc = "The routing hash it is pinned to" },
        }),
        .cli = .{
            .key = .{ .name = "routing-key", .doc = "The routing key every op in the transaction gives" },
            .examples = &.{"flo kv txn begin user:123"},
        },
        .doc = "Begin a transaction on the shard that owns the key.",
    },
    .{
        .name = "kv.txn.commit",
        .code = 0x111,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{txn_id_field}),
        .response = Record(&.{
            .{ .name = "commit_index", .T = Int(u64), .doc = "The log index its writes committed at" },
            .{ .name = "ops", .T = Int(u16), .doc = "How many writes it committed" },
        }),
        .cli = .{ .key = null, .args = &.{"txn"} },
        .doc = "Commit a transaction's writes as one.",
    },
    .{
        .name = "kv.txn.rollback",
        .code = 0x112,
        .class = .write,
        .txn = "txn",
        .request = Record(&.{txn_id_field}),
        .response = Record(&.{}),
        .cli = .{ .key = null, .args = &.{"txn"} },
        .doc = "Drop a transaction and its writes.",
    },
};

/// The KV ops grouped under a subcommand.
pub const groups = [_]ops.Group{
    .{ .name = "kv.json", .doc = "Read and change JSON documents at a path" },
    .{ .name = "kv.txn", .doc = "Transactions: writes on one shard, committed as one" },
};

comptime {
    ops.check(&table);
    ops.checkGroups(&table, &groups);
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
