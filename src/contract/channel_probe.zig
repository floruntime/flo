//! A test machine for channels: a `channel_probe` entry sends its body to a
//! slot's owner, and the owner records what it took in. No server machine
//! list holds it; VOPR and the unit tests drive the channel through it.
//!
//! A body is at least 16 bytes, its first 16 naming it, so the receiver's
//! record is keyed by the entry alone. A body starting "refuse" is refused
//! at the receiver after writing its record, which the refusal must undo.
//! One starting "relay" is sent on again by its receiver, to the same slot;
//! one starting "flood" is sent on more times than an apply may send.

const std = @import("std");
const schema = @import("../protocol/schema.zig");
const ce = @import("../protocol/channel_entries.zig");
const view_mod = @import("view.zig");
const machine_mod = @import("machine.zig");
const delivery = @import("delivery.zig");
const persistence = @import("../storage/persistence.zig");
const entry_mod = @import("../storage/ual/entry.zig");

const Entry = entry_mod.Entry;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
const Int = schema.Int;

pub const NAME_LEN = 16;

pub const Got = schema.Record(&.{
    .{ .name = "from", .T = Int(u16) },
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "hash", .T = Int(u64) },
});

pub const GotFam = view_mod.Family(58, "channel.probe_got", delivery.Stored(Got));

/// A received probe's record key: its name, under no namespace.
pub fn gotKey(buf: *[1 + NAME_LEN]u8, name: []const u8) []const u8 {
    return view_mod.key(buf, "", name) catch unreachable;
}

pub fn hashOf(body: []const u8) u64 {
    return std.hash.Wyhash.hash(0x9B0B, body);
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return;
    if (p.body.len < NAME_LEN) return;
    try out.add(GotFam.ID, "", p.body[0..NAME_LEN]);
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, "bad request: malformed probe");
    if (p.body.len < NAME_LEN) return out.refuse(.bad_request, .malformed, "bad request: a probe body names itself in 16 bytes");
    const m = view.message orelse return view.send(p.slot, @backingInt(entry_mod.EntryType.channel_probe), e.payload);
    var kb: [1 + NAME_LEN]u8 = undefined;
    try view.put(GotFam, gotKey(&kb, p.body[0..NAME_LEN]), .{ .v = .{ .from = m.from, .seq = m.seq, .hash = hashOf(p.body) } });
    if (std.mem.startsWith(u8, p.body, "refuse")) out.refuse(.bad_request, .malformed, "refused, as asked");
    const again: usize = if (std.mem.startsWith(u8, p.body, "relay")) 1 else if (std.mem.startsWith(u8, p.body, "flood")) @import("channels.zig").MAX_SENDS + 1 else 0;
    for (0..again) |_| try view.send(p.slot, @backingInt(entry_mod.EntryType.channel_probe), e.payload);
}

pub const machine: machine_mod.Machine = .{
    .name = "channel_probe",
    .entry_types = &.{.channel_probe},
    .families = &.{machine_mod.info(GotFam)},
    .keys = keys,
    .apply = apply,
};
