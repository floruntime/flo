//! The namespace family's ops on the table. Namespace metadata has one
//! owner, shard 0, so every op routes there. The namespace an op acts on
//! is its key, not the envelope's namespace: these ops act on the server's
//! set of namespaces, not inside one.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");
const limits = @import("limits.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Optional = schema.Optional;
const Record = schema.Record;
const Defaulted = schema.Defaulted;

/// A namespace's name.
pub const Name = Bytes(limits.MAX_NAMESPACE_NAME);

/// The most names one page lists.
pub const MAX_LIST: u32 = 1024;

const names_bytes = MAX_LIST * Name.max_size;

const name_key: ops.KeyArg = .{ .name = "name", .doc = "The namespace's name" };

/// A run-history quota in bytes over every shard; omitted for none.
pub const Quota = schema.Range(u64, 1, std.math.maxInt(u64) - 1);

/// The most shards a node has.
pub const MAX_SHARDS: u32 = @import("../config/server.zig").MAX_SHARDS;

/// One shard's part of a namespace's run-history quota and what it holds.
pub const ShardQuota = Record(&.{
    .{ .name = "shard", .T = Int(u16), .doc = "The shard" },
    .{ .name = "bound", .T = schema.Bool, .doc = "Whether its leader has bound a part here yet; none is enforced until it has" },
    .{ .name = "part_bytes", .T = Optional(Int(u64)), .doc = "Its part of the quota, bytes; none when the quota is off or not yet bound" },
    .{ .name = "usage_bytes", .T = Int(u64), .doc = "The run history its rows reference here, bytes" },
    .{ .name = "reserved_workflow_bytes", .T = Int(u64), .doc = "What its live workflow runs reserve here, bytes" },
    .{ .name = "reserved_action_bytes", .T = Int(u64), .doc = "What its live action runs reserve here, bytes" },
    .{ .name = "orphans", .T = Int(u32), .doc = "Deleted runs whose history couldn't be dropped, still counted in usage" },
    .{ .name = "orphan_bytes", .T = Int(u64), .doc = "Their bytes" },
    .{ .name = "held_runs", .T = Int(u32), .doc = "Workflow runs this shard's leader holds for the quota" },
});
pub const ShardQuotas = schema.List(ShardQuota, MAX_SHARDS);

pub const table = [_]ops.Op{
    .{
        .name = "namespace.create",
        .code = 0x010,
        .class = .ns_create,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{}),
        .response = Record(&.{}),
        // Says what it created, which the generated command wouldn't.
        .cli = .{ .expose = .hand, .key = name_key, .examples = &.{"flo namespace create myapp"} },
        .doc = "Create a namespace.",
    },
    .{
        .name = "namespace.delete",
        .code = 0x011,
        // Refused until a namespace's data can be removed on every shard
        // and its id retired.
        .class = .write,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{}),
        .response = Record(&.{}),
        .cli = .{ .expose = .hand, .key = name_key, .examples = &.{"flo namespace delete myapp"} },
        .doc = "Delete a namespace (not supported yet: the server refuses it).",
    },
    .{
        .name = "namespace.list",
        .code = 0x012,
        .class = .read,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{
            .{ .name = "limit", .T = Defaulted(schema.Range(u32, 1, MAX_LIST), MAX_LIST), .doc = "Most names on the page" },
            .{ .name = "cursor", .T = Name, .doc = "The last name of the page before; omit to start" },
        }),
        .response = Record(&.{
            .{ .name = "names", .T = schema.BoundedList(Name, MAX_LIST, names_bytes), .doc = "The namespaces, in name order" },
            .{ .name = "cursor", .T = Optional(Name), .doc = "Where the next page starts; none on the last page" },
        }),
        // It follows the cursor to the last page, so it stays written by hand.
        .cli = .{ .expose = .hand, .key = null, .render = .table, .examples = &.{"flo namespace list"} },
        .doc = "List the namespaces in name order, a page at a time.",
    },
    .{
        .name = "namespace.info",
        .code = 0x013,
        .class = .read,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "created_at_ms", .T = Int(i64), .doc = "When it was created, in ms since the epoch; 0 for default" },
            .{ .name = "history_quota", .T = Optional(Int(u64)), .doc = "Its run-history quota over every shard, bytes; none when off" },
            .{ .name = "history_quota_own", .T = schema.Bool, .doc = "Whether it set its own quota, not the default" },
            .{ .name = "shards", .T = ShardQuotas, .doc = "Each shard's part, usage and reserves" },
        }),
        .cli = .{ .expose = .hand, .key = name_key, .examples = &.{"flo namespace info myapp"} },
        .doc = "A namespace's record and its run-history quota on every shard; not_found if there is none.",
    },
    .{
        .name = "namespace.alter",
        .code = 0x014,
        .class = .write,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{
            .{ .name = "history_quota", .T = Optional(Quota), .doc = "Its run-history quota over every shard, bytes; omit to turn it off" },
        }),
        .response = Record(&.{}),
        .cli = .{ .expose = .hand, .key = name_key, .examples = &.{ "flo namespace alter myapp --history-quota 20GiB", "flo namespace alter myapp --history-quota none" } },
        .doc = "Set a namespace's run-history quota. Not a privileged action yet: any client can change any namespace's quota.",
    },
    .{
        .name = "namespace.ensure",
        .code = 0x016,
        .class = .ns_create,
        .origin = .internal,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "id", .T = Int(u32), .doc = "The namespace's id" },
            .{ .name = "published_index", .T = Int(u64), .doc = "Shard 0's log index the namespace is known at" },
        }),
        .cli = .{ .expose = .none, .key = name_key },
        .doc = "A shard asking shard 0 for a namespace's id, creating the namespace if it is new.",
    },
    .{
        .name = "namespace.published",
        .code = 0x017,
        .class = .read,
        .origin = .internal,
        .scope = .server,
        .route = .shard0,
        .request = Record(&.{}),
        .response = Record(&.{
            .{ .name = "published_index", .T = Int(u64), .doc = "Shard 0's log index its namespace table reflects" },
        }),
        .cli = .{ .expose = .none },
        .doc = "A follower asking the leader how far its namespace table is published, before it resolves a name it doesn't know.",
    },
};

/// The namespace op in the table named `name`; a name not there fails the build.
pub fn op(comptime name: []const u8) ops.Op {
    for (table) |o| if (std.mem.eql(u8, o.name, name)) return o;
    @compileError("no namespace op named '" ++ name ++ "'");
}

comptime {
    ops.check(&table);
}

test "namespace.list's largest page fits one response body" {
    try std.testing.expect(op("namespace.list").response.max_size <= ops.MAX_RESPONSE_BODY);
}
