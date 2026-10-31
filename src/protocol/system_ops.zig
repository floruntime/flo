//! System ops. Here, ones a shard sends another, never a client: a list or
//! scan asks each shard that holds part of it for its page
//! (`system.walk_page`, and `system.runs_page` for a workflow's runs),
//! answered on that shard's own thread from its own state, and the shard
//! that took the request merges the pages.

const std = @import("std");
const ops = @import("ops.zig");
const schema = @import("schema.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

/// The most items one page holds: a walk's largest limit.
pub const MAX_PAGE: u32 = 1024;

/// One entry, as the list's answer encodes it for a client: names and a
/// few small fields, a failed job's reason the longest.
pub const MAX_ENTRY: u32 = 16 * 1024;

/// Where a scan that pages itself resumes on its shard.
pub const MAX_LOCAL_CURSOR: u32 = @intCast(ops.MAX_KEY);

pub const Entry = Record(&.{
    .{ .name = "name", .T = Bytes(@intCast(ops.MAX_KEY)), .doc = "The item's name: what the walk orders and resumes by" },
    .{ .name = "entry", .T = Bytes(MAX_ENTRY), .doc = "The item as the list's answer encodes it" },
});

const more_size = schema.Bool.max_size;
pub const Entries = schema.BoundedList(Entry, MAX_PAGE, @intCast(ops.MAX_RESPONSE_BODY - more_size - @sizeOf(u32)));

/// A run id's longest: the start request's u16 length.
pub const MAX_RUN_ID: u32 = std.math.maxInt(u16);
/// A run's status, as the list names it.
pub const MAX_RUN_STATUS: u32 = 32;
/// The longest search a run list takes.
pub const MAX_RUN_SEARCH: u32 = @intCast(ops.MAX_KEY);
/// One run as the list's answer encodes it: its id, workflow name, status
/// and start time.
pub const MAX_RUN_ENTRY: u32 = 2 + MAX_RUN_ID + 2 + 256 + 2 + MAX_RUN_STATUS + 8;

/// Where a run list stands: newest first, and by id among runs started in
/// the same millisecond.
pub const RunAt = Record(&.{
    .{ .name = "created_at", .T = Int(i64), .doc = "When the run started, in ms" },
    .{ .name = "run_id", .T = Bytes(MAX_RUN_ID), .doc = "The run's id" },
});

pub const Run = Record(&.{
    .{ .name = "at", .T = RunAt, .doc = "The run's place in the list's order" },
    .{ .name = "entry", .T = Bytes(MAX_RUN_ENTRY), .doc = "The run as the list's answer encodes it" },
});

pub const Runs = schema.BoundedList(Run, MAX_PAGE, @intCast(ops.MAX_RESPONSE_BODY - more_size - @sizeOf(u32)));

pub const table = [_]ops.Op{
    .{
        .name = "system.walk_page",
        .code = 0x001,
        .class = .read,
        .origin = .internal,
        .route = .local,
        .request = Record(&.{
            .{ .name = "op", .T = Int(u16), .doc = "The list or scan op whose page this is" },
            .{ .name = "cursor", .T = Bytes(MAX_LOCAL_CURSOR), .doc = "Where a scan that pages itself resumes; empty at the shard's start" },
            .{ .name = "skip", .T = Int(u32), .doc = "Items already sent, for a scan paged by offset" },
            .{ .name = "limit", .T = Int(u32), .doc = "The most items the page holds" },
            .{ .name = "room", .T = Int(u32), .doc = "The most bytes its entries take" },
        }),
        .response = Record(&.{
            .{ .name = "entries", .T = Entries, .doc = "The page's items, in the shard's order" },
            .{ .name = "more", .T = schema.Bool, .doc = "Whether the shard holds more past the page" },
        }),
        .cli = .{ .expose = .none },
        .doc = "A shard's page of a list or scan, read from its own state.",
    },
    .{
        .name = "system.runs_page",
        .code = 0x002,
        .class = .read,
        .origin = .internal,
        .route = .local,
        .request = Record(&.{
            .{ .name = "status", .T = Bytes(MAX_RUN_STATUS), .doc = "Only runs with this status; empty for any" },
            .{ .name = "search", .T = Bytes(MAX_RUN_SEARCH), .doc = "Only runs whose id, workflow, step, input or tags hold this, lowercased; empty for any" },
            .{ .name = "after", .T = schema.Optional(RunAt), .doc = "Only runs after this one in the list's order; none from the newest" },
            .{ .name = "limit", .T = Int(u32), .doc = "The most runs the page holds" },
            .{ .name = "room", .T = Int(u32), .doc = "The most bytes its entries take" },
        }),
        .response = Record(&.{
            .{ .name = "runs", .T = Runs, .doc = "The page's runs, in the list's order" },
            .{ .name = "more", .T = schema.Bool, .doc = "Whether the shard holds more past the page" },
        }),
        .cli = .{ .expose = .none },
        .doc = "A shard's page of a workflow's runs, read from its own state.",
    },
};
