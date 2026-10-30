//! System ops. Here, one a shard sends another, never a client: a list or
//! scan asks each shard that holds part of it for its page
//! (`system.walk_page`), answered on that shard's own thread from its own
//! state, and the shard that took the request merges the pages.

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
};
