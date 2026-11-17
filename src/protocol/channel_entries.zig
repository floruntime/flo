//! The channel log entries: messages taken in by their receiver, outbox
//! trims, and the probe a test machine sends with. A message is an entry in
//! waiting, its target's entry type and payload, applied by the receiver
//! through the machine that owns that type. These are durable, replayed
//! records, so their layouts are pinned here against the data format.

const std = @import("std");
const schema = @import("schema.zig");
const entries = @import("entries.zig");
const limits = @import("limits.zig");

const Int = schema.Int;
const Bytes = schema.Bytes;
const Record = schema.Record;

/// Most messages one `inbound` entry takes. A message's place in its entry
/// is a u8 (`View.Message.ordinal`), which time-series points pack beside
/// their index, so this stays at most 256.
pub const MAX_BATCH = 64;

comptime {
    std.debug.assert(MAX_BATCH <= 256);
}

/// What the header fields of `Inbound` and of one `Message` take, so a
/// single message of the largest payload still fits one entry.
const INBOUND_OVERHEAD = 64;

/// The largest message payload: one message alone fits one entry.
pub const MAX_MESSAGE_PAYLOAD: u32 = @intCast(limits.MAX_LOG_ENTRY_PAYLOAD - INBOUND_OVERHEAD);

/// One message as the receiver logs it. Its seq is the batch's first seq
/// plus its place in the batch.
pub const Message = Record(&.{
    // The slot the message is for: its target's key's slot.
    .{ .name = "slot", .T = Int(u16) },
    // The target's entry type, which picks the machine that applies it.
    .{ .name = "entry_type", .T = Int(u8) },
    // Reserved for the slot-move fence: the slot layout the sender saw.
    .{ .name = "layout_epoch", .T = Int(u32) },
    // The entry flags it applies with (a sink's write is `Flags.SINK`), as
    // the sending apply set them: never the inbound's.
    .{ .name = "flags", .T = Int(u16) },
    // Who it was sent for (`View.Origin`): a user's or the system's, and
    // the namespace whose keys alone it may touch.
    .{ .name = "origin_kind", .T = Int(u8) },
    .{ .name = "origin_ns", .T = Int(u32) },
    .{ .name = "payload", .T = Bytes(MAX_MESSAGE_PAYLOAD) },
});

pub const Messages = schema.BoundedList(Message, MAX_BATCH, @intCast(limits.MAX_LOG_ENTRY_PAYLOAD - 16));

/// `channel_inbound`: messages from one sender, consecutive from `first_seq`.
pub const Inbound = Record(&.{
    .{ .name = "from", .T = Int(u16) },
    .{ .name = "first_seq", .T = Int(u64) },
    .{ .name = "messages", .T = Messages },
});

/// Most seqs one trim drops, so its keys are few enough to declare.
pub const MAX_TRIM_SPAN = 1024;

/// `channel_trim`: the sender drops its outbox to `to` from `first` through
/// `through`, which the receiver has durably applied. `first` is the
/// proposer's `acked + 1`; the span is at most `MAX_TRIM_SPAN`.
pub const Trim = Record(&.{
    .{ .name = "to", .T = Int(u16) },
    .{ .name = "first", .T = Int(u64) },
    .{ .name = "through", .T = Int(u64) },
});

/// A parked message's target: its machine's tag, then the target as that
/// machine names it (`MACHINE_TARGET_LEN`). Fixed width, so a target's
/// parked records sort together under it; tagged, so two machines' names
/// never collide.
pub const MACHINE_TARGET_LEN = 12;
pub const TARGET_LEN = 1 + MACHINE_TARGET_LEN;

/// One parked message an unpark applies: as it arrived, with its sender and
/// seq, which it is parked under.
pub const Unparked = Record(&.{
    .{ .name = "from", .T = Int(u16) },
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "entry_type", .T = Int(u8) },
    .{ .name = "flags", .T = Int(u16) },
    .{ .name = "payload", .T = Bytes(MAX_MESSAGE_PAYLOAD) },
});

pub const UnparkedList = schema.BoundedList(Unparked, MAX_BATCH, @intCast(limits.MAX_LOG_ENTRY_PAYLOAD - 32));

/// `channel_unpark`: a target's first parked messages, in their parked
/// order, which its leader saw room for. Each one names itself, so the
/// entry declares every key it may touch; apply checks each is still the
/// target's next from its sender, and applies the longest prefix that fits.
pub const Unpark = Record(&.{
    .{ .name = "target", .T = Bytes(TARGET_LEN) },
    .{ .name = "messages", .T = UnparkedList },
});

/// The largest probe body.
pub const MAX_PROBE_BODY = 1024;

/// `channel_probe`: a test machine's send of `body` to `slot`'s owner. No
/// server machine applies it: a server that reads one halts.
pub const Probe = Record(&.{
    .{ .name = "slot", .T = Int(u16) },
    .{ .name = "body", .T = Bytes(MAX_PROBE_BODY) },
});

pub const table = [_]entries.Entry{
    .{ .name = "channel.inbound", .type_code = 0xA0, .payload = Inbound, .max_payload = limits.MAX_LOG_ENTRY_PAYLOAD, .doc = "Messages from another group, applied in order through their targets' machines." },
    .{ .name = "channel.trim", .type_code = 0xA1, .payload = Trim, .max_payload = Trim.max_size, .doc = "An outbox trimmed through what its receiver durably applied." },
    .{ .name = "channel.probe", .type_code = 0xA2, .payload = Probe, .max_payload = Probe.max_size, .doc = "A test machine's send; no server machine applies it." },
    .{ .name = "channel.unpark", .type_code = 0xA3, .payload = Unpark, .max_payload = limits.MAX_LOG_ENTRY_PAYLOAD, .doc = "A target's parked messages, applied in order as far as it has room." },
};

pub fn Payload(comptime code: u8) type {
    for (table) |e| if (e.type_code == code) return e.payload;
    @compileError(std.fmt.comptimePrint("no channel entry with type 0x{x}", .{code}));
}

comptime {
    entries.check(&table);
    for (table) |e| std.debug.assert(e.max_payload <= limits.MAX_LOG_ENTRY_PAYLOAD);
}
