//! A test machine for channels: a `channel_probe` entry sends its body to a
//! slot's owner, and the owner records what it took in. No server machine
//! list holds it; VOPR and the unit tests drive the channel through it.
//!
//! A body is at least 16 bytes, its first 16 naming it, so the receiver's
//! record is keyed by the entry alone. A body starting "refuse" is refused
//! at the receiver after writing its record, which the refusal must undo.
//! One starting "relay" is sent on again by its receiver, to the same slot;
//! one starting "flood" is sent on more times than an apply may send.
//!
//! A probe is a sink's write (`Flags.SINK`), and its target is its slot,
//! which can be full: one starting "fill" makes its slot full at its
//! receiver, one starting "drai" (drain) makes it take probes again, and neither
//! has a target, so neither parks. A drain proposed at the slot's owner applies
//! there, unsent. While its slot is full, a probe is
//! refused `overloaded`, so a channel parks it.

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

/// A probe is the system's: the test machine's record of what it took is
/// the shard's (`Scope.shard`), which only the system may touch.
pub const PROBE_ORIGIN: view_mod.Origin = .{ .kind = .system, .ns = .default };

pub const Got = schema.Record(&.{
    .{ .name = "from", .T = Int(u16) },
    .{ .name = "seq", .T = Int(u64) },
    .{ .name = "hash", .T = Int(u64) },
});

pub const GotFam = view_mod.Family(58, "channel.probe_got", .shard, delivery.Stored(Got));

/// A received probe's record key: its name, under no namespace.
pub fn gotKey(buf: *[1 + NAME_LEN]u8, name: []const u8) []const u8 {
    return view_mod.key(buf, "", name) catch unreachable;
}

pub fn hashOf(body: []const u8) u64 {
    return std.hash.Wyhash.hash(0x9B0B, body);
}

/// A slot's full mark at its receiver: under the got family, apart from
/// any name (a name is 16 bytes).
fn fullRaw(slot: u16) [5]u8 {
    var raw: [5]u8 = undefined;
    raw[0..3].* = "\x00fl".*;
    std.mem.writeInt(u16, raw[3..5], slot, .big);
    return raw;
}

fn fullKey(buf: *[1 + 5]u8, slot: u16) []const u8 {
    const raw = fullRaw(slot);
    return view_mod.key(buf, "", &raw) catch unreachable;
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return;
    if (p.body.len < NAME_LEN) return;
    try out.add(GotFam.ID, "", p.body[0..NAME_LEN]);
    const raw = fullRaw(p.slot);
    try out.add(GotFam.ID, "", &raw);
}

/// Whether `slot` is full at the group whose store `read` reads.
pub fn slotFull(read: view_mod.ReadView, slot: u16) bool {
    var fb: [6]u8 = undefined;
    return (read.get(GotFam, fullKey(&fb, slot)) catch null) != null;
}

fn isMark(body: []const u8) bool {
    return std.mem.startsWith(u8, body, "fill") or std.mem.startsWith(u8, body, "drai");
}

/// A probe's target: its slot, unless it is a fill or drain mark.
fn targetOf(e: *const Entry) ?[machine_mod.TARGET_LEN]u8 {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return null;
    if (isMark(p.body)) return null;
    var t: [machine_mod.TARGET_LEN]u8 = @splat(0);
    t[0..5].* = "probe".*;
    std.mem.writeInt(u16, t[5..7], p.slot, .big);
    return t;
}

fn roomFor(read: view_mod.ReadView, e: *const Entry) bool {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return true;
    var fb: [6]u8 = undefined;
    return (read.get(GotFam, fullKey(&fb, p.slot)) catch null) == null;
}

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    var diag: schema.Diagnostic = .{};
    const p = ce.Probe.decodeAll(e.payload, &diag) catch return out.refuse(.bad_request, .malformed, "bad request: malformed probe");
    if (p.body.len < NAME_LEN) return out.refuse(.bad_request, .malformed, "bad request: a probe body names itself in 16 bytes");
    // A body starting "halt-now" halts its applier where it arrives as a
    // message: one every replica fails alike, for the repair tests.
    if (view.message != null and std.mem.startsWith(u8, p.body, "halt-now")) return error.Halt;
    var fb: [6]u8 = undefined;
    const full_key = fullKey(&fb, p.slot);
    // A drain of a slot this group owns applies here, as a consumer's
    // dequeue would: sent, it could wait behind messages to that same slot
    // its owner refuses until it drains.
    if (view.message == null and std.mem.startsWith(u8, p.body, "drai")) if (view.place) |pl| if (@import("channels.zig").ownerOf(pl, p.slot) == pl.group) {
        return view.delete(GotFam, full_key);
    };
    const m = view.message orelse return view.sendFlagged(PROBE_ORIGIN, p.slot, @backingInt(entry_mod.EntryType.channel_probe), entry_mod.Flags.SINK, e.payload);
    if (std.mem.startsWith(u8, p.body, "fill")) {
        try view.put(GotFam, full_key, .{ .v = .{ .from = 0, .seq = 0, .hash = 0 } });
    } else if (std.mem.startsWith(u8, p.body, "drai")) {
        try view.delete(GotFam, full_key);
    } else if (try view.get(GotFam, full_key) != null) {
        return out.refuse(.overloaded, .queue_full, "overloaded: the probe's slot is full");
    }
    var kb: [1 + NAME_LEN]u8 = undefined;
    try view.put(GotFam, gotKey(&kb, p.body[0..NAME_LEN]), .{ .v = .{ .from = m.from, .seq = m.seq, .hash = hashOf(p.body) } });
    if (std.mem.startsWith(u8, p.body, "refuse")) out.refuse(.bad_request, .malformed, "refused, as asked");
    const again: usize = if (std.mem.startsWith(u8, p.body, "relay")) 1 else if (std.mem.startsWith(u8, p.body, "flood")) @import("channels.zig").MAX_SENDS + 1 else 0;
    for (0..again) |_| try view.sendFlagged(PROBE_ORIGIN, p.slot, @backingInt(entry_mod.EntryType.channel_probe), entry_mod.Flags.SINK, e.payload);
}

pub const machine: machine_mod.Machine = .{
    .name = "channel_probe",
    .entry_types = &.{.channel_probe},
    .families = &.{machine_mod.info(GotFam)},
    .keys = keys,
    .apply = apply,
    .target = targetOf,
    .room = roomFor,
};
