//! The processing log entries with a schema: a pipeline's sink batch, its
//! writes sent with the source cursor they move it to. A durable, replayed
//! record, so its layout is pinned here against the data format.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");
const kv_entries = @import("kv_entries.zig");
const ce = @import("channel_entries.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

/// The longest pipeline key: a job's id, then `\0` and its source's index
/// for a job with several.
pub const MAX_PIPELINE_KEY = 64;

/// The largest sink write: a channel message's, less room for the batch's
/// own fields.
pub const MAX_SINK_PAYLOAD: u32 = @intCast(limits.MAX_LOG_ENTRY_PAYLOAD - 512);

comptime {
    std.debug.assert(MAX_SINK_PAYLOAD <= ce.MAX_MESSAGE_PAYLOAD);
}

/// One sink write: the message its batch sends with `flags`, to the
/// owner of `slot`, for the machine that applies `entry_type`.
pub const SinkMessage = Record(&.{
    .{ .name = "slot", .T = Int(u16) },
    .{ .name = "entry_type", .T = Int(u8) },
    .{ .name = "flags", .T = Int(u16) },
    // The sink's namespace: the message is its user's, and touches only
    // its keys.
    .{ .name = "ns", .T = Int(u32) },
    .{ .name = "payload", .T = Bytes(MAX_SINK_PAYLOAD) },
});

/// A batch's messages: as many as one apply may send.
pub const Messages = schema.BoundedList(SinkMessage, ce.MAX_BATCH, @intCast(limits.MAX_LOG_ENTRY_PAYLOAD - 256));

/// `processing_sink_batch`: pipeline `pipeline` of a job in namespace `ns`
/// moves its source cursor from `from` to `to`, and sends `messages`, each a
/// sink's write, as one. It applies only if its cursor is still at `from`:
/// one a leader change left behind, or applied already, changes nothing.
/// A cursor is a stream source's (timestamp, sequence) or a time-series
/// source's last point's (time, position); (0, 0) is none yet.
pub const SinkBatch = Record(&.{
    .{ .name = "ns", .T = kv_entries.NamespaceId },
    .{ .name = "pipeline", .T = Bytes(MAX_PIPELINE_KEY) },
    .{ .name = "from_a", .T = Int(u64) },
    .{ .name = "from_b", .T = Int(u64) },
    .{ .name = "to_a", .T = Int(u64) },
    .{ .name = "to_b", .T = Int(u64) },
    // The source records the batch covers.
    .{ .name = "records", .T = Int(u32) },
    .{ .name = "messages", .T = Messages },
});

pub const table = [_]entries.Entry{
    .{ .name = "processing.sink_batch", .type_code = 0x87, .payload = SinkBatch, .max_payload = limits.MAX_LOG_ENTRY_PAYLOAD, .doc = "A pipeline's sink writes, sent with the source cursor they move it to." },
};

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
