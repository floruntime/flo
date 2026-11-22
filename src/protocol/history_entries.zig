//! A run's history entries: what a run's apply sends its own group on the
//! history lane (`Flags.HISTORY`), applied by the stream machine into the
//! history families. Each carries a fixed header naming its run's
//! incarnation and the events it covers, so every key it touches comes
//! from the header alone, never from the records. These are durable,
//! replayed records, so their layouts are pinned here against the data
//! format.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

/// The longest owner (a workflow's or an action's name) and run id a
/// header names: the longest either may be.
pub const MAX_OWNER: u32 = 256;
pub const MAX_RUN: u32 = 600;

/// The most record bytes one chunk carries: a full pending chunk and the
/// largest event that ends it.
pub const MAX_CHUNK_RECORDS: u32 = 96 << 10;

/// How many locators one index lists: the live rows it folds into one.
pub const INDEX_FANOUT = 16;

/// Which history family a run's rows are in, by its producer.
pub const Family = enum(u8) { workflow = 0, action = 1 };

/// What a locator points at.
pub const Kind = enum(u8) {
    /// A chunk of the run's events.
    chunk = 0,
    /// An index: a list of locators, the run's earlier rows.
    index = 1,
    /// Events that never landed: a chunk a repair skipped or refused.
    lost = 2,
};

/// The header every history entry starts with. `first_seq..last_seq` are
/// the run's events it covers (none for an index or a drop); `final` says
/// the run has ended with it, at `completed_ms`.
const header = [_]schema.Field{
    .{ .name = "ns", .T = Int(u32) },
    .{ .name = "family", .T = Int(u8) },
    .{ .name = "owner", .T = Bytes(MAX_OWNER) },
    .{ .name = "run", .T = Bytes(MAX_RUN) },
    .{ .name = "created_ns", .T = Int(u64) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "last_seq", .T = Int(u64) },
    .{ .name = "final", .T = schema.Bool },
    .{ .name = "completed_ms", .T = Int(u64) },
};

pub const Header = Record(&header);

/// Where one of a run's rows' payload is in the log: the entry and the
/// message within it (`part` = ordinal + 1), and what it holds.
pub const Locator = Record(&.{
    .{ .name = "ual_index", .T = Int(u64) },
    .{ .name = "part", .T = Int(u8) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "last_seq", .T = Int(u64) },
    .{ .name = "byte_len", .T = Int(u32) },
    .{ .name = "kind", .T = Int(u8) },
});

pub const Locators = schema.List(Locator, INDEX_FANOUT);

/// `history_chunk`: some of a run's events, encoded by its producer.
pub const Chunk = Record(&(header ++ [_]schema.Field{
    .{ .name = "records", .T = Bytes(MAX_CHUNK_RECORDS) },
}));

/// `history_index`: rows of a run folded into one, the oldest first.
pub const Index = Record(&(header ++ [_]schema.Field{
    .{ .name = "locators", .T = Locators },
}));

/// `history_drop`: every row of a run's incarnation, gone.
pub const Drop = Header;

pub const table = [_]entries.Entry{
    .{ .name = "history.chunk", .type_code = 0x16, .payload = Chunk, .max_payload = Chunk.max_size, .doc = "Some of a run's events, kept as a row that locates them in the log." },
    .{ .name = "history.index", .type_code = 0x17, .payload = Index, .max_payload = Index.max_size, .doc = "A run's history rows folded into one, so its live rows stay few." },
    .{ .name = "history.drop", .type_code = 0x18, .payload = Drop, .max_payload = Drop.max_size, .doc = "A deleted run's history rows, all of them, gone." },
};

pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no history entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    const ce = @import("channel_entries.zig");
    for (table) |e| std.debug.assert(e.max_payload <= ce.MAX_MESSAGE_PAYLOAD);
}
