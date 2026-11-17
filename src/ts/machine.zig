//! Time series on the contract: every point is a record of a family,
//! changed only through a `View` in `apply`, so every replica holds the
//! same points, and the family state a snapshot carries holds them all.
//!
//! - `ts.series`: each series, keyed (ns id, measurement, field, tag hash):
//!   its canonical tag string, its point count, and its oldest and newest
//!   times. A series goes with its last point, to retention or a delete.
//! - `ts.point`: each point, keyed (series key, time, position), both
//!   big-endian: its value. A point's position is the log index of the
//!   entry it was applied from, then its place among the writes that entry
//!   carries (`positionOf`), so a series' points sort by time and then by
//!   apply order, and no two share a key.
//! - `ts.field`: each measurement field, keyed (ns id, measurement, field):
//!   the newest time any of its series applied, how many series it has,
//!   and how many points arrived behind that newest time, bucketed by how
//!   far (`LATE_EDGES_NS`). A field goes with its last series. Its own
//!   family, so a range over series or points never reaches it.
//!
//! Names end at a NUL, so a measurement's keys are a prefix no other
//! measurement's start with; a name holding one is refused.

const std = @import("std");
const Allocator = std.mem.Allocator;
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const router = @import("../node/router.zig");
const tsp = @import("../projection/ts.zig");

const Entry = entry_mod.Entry;
const EntryType = entry_mod.EntryType;
const View = view_mod.View;
const KeySet = view_mod.KeySet;
const Result = persistence.Result;
const ApplyError = persistence.ApplyError;
pub const NamespaceId = router.NamespaceId;

/// A series: its tags as written (canonical), and what it holds.
/// `[points u64][oldest_ns u64][newest_ns u64][tags]`, little-endian.
pub const SeriesValue = struct {
    points: u64,
    oldest_ns: u64,
    newest_ns: u64,
    tags: []const u8,

    pub fn encode(self: SeriesValue, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u64, self.points, .little);
        try w.writeInt(u64, self.oldest_ns, .little);
        try w.writeInt(u64, self.newest_ns, .little);
        try w.writeAll(self.tags);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!SeriesValue {
        if (bytes.len < 24) return error.Malformed;
        return .{
            .points = std.mem.readInt(u64, bytes[0..8], .little),
            .oldest_ns = std.mem.readInt(u64, bytes[8..16], .little),
            .newest_ns = std.mem.readInt(u64, bytes[16..24], .little),
            .tags = bytes[24..],
        };
    }
};

/// A point's value, `[f64 bits u64]` little-endian.
pub const PointValue = struct {
    value: f64,

    pub fn encode(self: PointValue, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u64, @bitCast(self.value), .little);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!PointValue {
        if (bytes.len != 8) return error.Malformed;
        return .{ .value = @bitCast(std.mem.readInt(u64, bytes[0..8], .little)) };
    }
};

/// How far behind its field's newest time a late point is counted: past
/// each of these. A processing source's lateness window reports against
/// the smallest edge at or above it.
pub const LATE_EDGES_NS = [_]u64{ 0, 1 * NS_PER_S, 5 * NS_PER_S, 15 * NS_PER_S, 30 * NS_PER_S, 60 * NS_PER_S, 300 * NS_PER_S, 3600 * NS_PER_S };
const NS_PER_S = std.time.ns_per_s;
/// The edges as the metric labels them.
pub const LATE_EDGE_NAMES = [_][]const u8{ "0s", "1s", "5s", "15s", "30s", "1m", "5m", "1h" };

/// The smallest edge at or above a lateness window of `window_ns`, so a
/// point counted past it was later than the window; none past the last.
pub fn lateEdgeFor(window_ns: u64) ?usize {
    for (LATE_EDGES_NS, 0..) |edge, i| if (edge >= window_ns) return i;
    return null;
}

/// A measurement field: `[newest_ns u64][series u64][writes u64][late u64
/// × edges]`, little-endian.
pub const FieldValue = struct {
    newest_ns: u64 = 0,
    series: u64 = 0,
    /// Points applied to it, ever: a source sees from it whether the field
    /// is still being written, late or not.
    writes: u64 = 0,
    /// Points that arrived more than each edge behind `newest_ns`.
    late: [LATE_EDGES_NS.len]u64 = @splat(0),

    pub fn encode(self: FieldValue, w: *std.Io.Writer) std.Io.Writer.Error!void {
        try w.writeInt(u64, self.newest_ns, .little);
        try w.writeInt(u64, self.series, .little);
        try w.writeInt(u64, self.writes, .little);
        for (self.late) |n| try w.writeInt(u64, n, .little);
    }

    pub fn decode(bytes: []const u8) error{Malformed}!FieldValue {
        if (bytes.len != 24 + 8 * LATE_EDGES_NS.len) return error.Malformed;
        var v: FieldValue = .{ .newest_ns = std.mem.readInt(u64, bytes[0..8], .little), .series = std.mem.readInt(u64, bytes[8..16], .little), .writes = std.mem.readInt(u64, bytes[16..24], .little) };
        for (&v.late, 0..) |*n, i| n.* = std.mem.readInt(u64, bytes[24 + 8 * i ..][0..8], .little);
        return v;
    }

    /// The point at `ts_ns` applied: counted late past each edge it trails
    /// the newest by, into the record and into `applied` (the node's count
    /// of late applies), then the newest moves up to it.
    pub fn note(self: *FieldValue, ts_ns: u64, had_points: bool, applied: *[LATE_EDGES_NS.len]u64) void {
        self.writes += 1;
        if (had_points and ts_ns < self.newest_ns) {
            const behind = self.newest_ns - ts_ns;
            for (LATE_EDGES_NS, &self.late, applied) |edge, *n, *m| if (behind > edge) {
                n.* += 1;
                m.* += 1;
            };
        }
        self.newest_ns = if (had_points) @max(self.newest_ns, ts_ns) else ts_ns;
    }
};

pub const Series = view_mod.Family(60, "ts.series", .id, SeriesValue);
pub const Points = view_mod.Family(61, "ts.point", .id, PointValue);
pub const Fields = view_mod.Family(62, "ts.field", .id, FieldValue);

comptime {
    std.debug.assert(@typeInfo(@FieldType(@import("../contract/store.zig").Store, "ts_late_points")).array.len == LATE_EDGES_NS.len);
    std.debug.assert(@typeInfo(@FieldType(View, "ts_late_points")).array.len == LATE_EDGES_NS.len);
}

// ─── Keys ────────────────────────────────────────────────────────────────

const NS_LEN = @sizeOf(u32);
const TAG_LEN = @sizeOf(u64);
/// What a point's key adds to its series': time and position.
pub const POINT_SUFFIX = 16;

/// Bits of a position below its log index: a write's place among those
/// one entry carries (a channel's inbound batch), 0 for an entry that
/// carries one.
pub const ORDINAL_BITS = 8;

/// A point's position: its entry's log index, then its place in it. Log
/// indexes stay far below 2^56, so the pair packs into one u64 that sorts
/// as the pair does.
pub fn positionOf(index: u64, ordinal: u8) u64 {
    std.debug.assert(index >> (64 - ORDINAL_BITS) == 0);
    return index << ORDINAL_BITS | ordinal;
}

/// `[ns id][measurement]\0`: every key of a measurement's series and points.
pub fn measurementPrefix(a: Allocator, ns: NamespaceId, measurement: []const u8) Allocator.Error![]u8 {
    return std.mem.concat(a, u8, &.{ &ns.bytes(), measurement, "\x00" });
}

/// `[ns id][measurement]\0[field]\0`: every key of one field's series.
pub fn fieldPrefix(a: Allocator, ns: NamespaceId, measurement: []const u8, field: []const u8) Allocator.Error![]u8 {
    return std.mem.concat(a, u8, &.{ &ns.bytes(), measurement, "\x00", field, "\x00" });
}

/// A series' key: its field prefix and tag hash.
pub fn seriesKey(a: Allocator, ns: NamespaceId, measurement: []const u8, field: []const u8, tag_hash: u64) Allocator.Error![]u8 {
    var tag: [TAG_LEN]u8 = undefined;
    std.mem.writeInt(u64, &tag, tag_hash, .big);
    return std.mem.concat(a, u8, &.{ &ns.bytes(), measurement, "\x00", field, "\x00", &tag });
}

/// A point's key: its series' key, then (time, position).
pub fn pointKey(a: Allocator, series_key: []const u8, ts_ns: u64, position: u64) Allocator.Error![]u8 {
    var suffix: [POINT_SUFFIX]u8 = undefined;
    std.mem.writeInt(u64, suffix[0..8], ts_ns, .big);
    std.mem.writeInt(u64, suffix[8..16], position, .big);
    return std.mem.concat(a, u8, &.{ series_key, &suffix });
}

/// Where a point's key places it: its time, then its position.
pub const At = struct {
    ns: u64,
    /// The point's position (`positionOf`).
    ual: u64,

    pub fn before(a: At, b: At) bool {
        return a.ns < b.ns or (a.ns == b.ns and a.ual < b.ual);
    }
};

/// A point key's (time, position).
pub fn pointAt(key: []const u8) At {
    const s = key[key.len - POINT_SUFFIX ..];
    return .{ .ns = std.mem.readInt(u64, s[0..8], .big), .ual = std.mem.readInt(u64, s[8..16], .big) };
}

/// The parts of a series key.
pub const SeriesName = struct { measurement: []const u8, field: []const u8, tag_hash: u64 };

pub fn seriesName(key: []const u8) SeriesName {
    const body = key[NS_LEN .. key.len - TAG_LEN];
    const m_end = std.mem.indexOfScalar(u8, body, 0) orelse body.len;
    const after = body[@min(m_end + 1, body.len)..];
    const f_end = std.mem.indexOfScalar(u8, after, 0) orelse after.len;
    return .{ .measurement = body[0..m_end], .field = after[0..f_end], .tag_hash = std.mem.readInt(u64, key[key.len - TAG_LEN ..][0..TAG_LEN], .big) };
}

/// A key's bytes past its namespace id, as `KeySet.addIn` takes them.
fn rest(key: []const u8) []const u8 {
    return key[NS_LEN..];
}

// ─── Entries ─────────────────────────────────────────────────────────────

const Write = struct { cmd: entry_mod.CommandPayload, rec: tsp.TsWriteValue, tag_hash: u64, canonical: []const u8 };

/// A write's point as every replica reads it from the entry.
fn writeOf(e: *const Entry, canon_buf: *[tsp.MAX_TAG_STRING]u8) ?Write {
    const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return null;
    const rec = tsp.TsWriteValue.decode(cmd.value, e.header.timestamp_ns) orelse return null;
    const canonical = tsp.canonicalTagString(rec.tags, canon_buf);
    const tag_hash: u64 = if (canonical.len == 0) 0 else std.hash.Wyhash.hash(0, canonical);
    return .{ .cmd = cmd, .rec = rec, .tag_hash = tag_hash, .canonical = canonical };
}

fn typeOf(e: *const Entry) EntryType {
    return @fromBackingInt(@intCast(e.header.entry_type));
}

fn keys(e: *const Entry, out: *KeySet) error{OutOfMemory}!void {
    const a = out.allocator;
    switch (typeOf(e)) {
        .ts_write, .ts_write_batch => {
            var canon_buf: [tsp.MAX_TAG_STRING]u8 = undefined;
            const w = writeOf(e, &canon_buf) orelse return;
            const ns = w.cmd.namespace_id;
            const sk = try seriesKey(a, ns, w.cmd.key, w.rec.field_name, w.tag_hash);
            try out.addIn(Series.ID, ns, rest(sk));
            try out.addIn(Fields.ID, ns, rest(fieldOf(sk)));
            // Every place in the entry: which one a write takes is the
            // batch's, not the entry's own to say.
            const first = try pointKey(a, sk, w.rec.timestamp_ns, positionOf(e.header.index, 0));
            out.addRangeIn(Points.ID, ns, rest(first[0 .. first.len - 1])) catch |err| switch (err) {
                error.NoNamespace => return,
                else => |o| return o,
            };
        },
        // Every series of the measurement, and its points.
        .ts_delete, .ts_retention => {
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return;
            const prefix = try measurementPrefix(a, cmd.namespace_id, cmd.key);
            inline for (.{ Series.ID, Points.ID, Fields.ID }) |fam| out.addRangeIn(fam, cmd.namespace_id, rest(prefix)) catch |err| switch (err) {
                error.NoNamespace => return,
                else => |o| return o,
            };
        },
        else => {},
    }
}

const NUL_IN_NAME = "bad request: a measurement or field name must not contain NUL";
/// Reads answer a point's time as i64 nanoseconds.
const TIME_TOO_LATE = std.fmt.comptimePrint("bad request: a point's time is at most {d} ns", .{std.math.maxInt(i64)});

fn apply(view: *View, e: *const Entry, out: *Result) ApplyError!void {
    const a = view.arena;
    switch (typeOf(e)) {
        .ts_write, .ts_write_batch => {
            // The measurement is the command key; the value carries the
            // reading, field, client timestamp and tags (`TsWriteValue`).
            var canon_buf: [tsp.MAX_TAG_STRING]u8 = undefined;
            const w = writeOf(e, &canon_buf) orelse return error.Halt;
            if (std.mem.indexOfScalar(u8, w.cmd.key, 0) != null or std.mem.indexOfScalar(u8, w.rec.field_name, 0) != null)
                return out.refuse(.bad_request, .unclassified, NUL_IN_NAME);
            const ts = w.rec.timestamp_ns;
            if (ts > std.math.maxInt(i64)) return out.refuse(.bad_request, .unclassified, TIME_TOO_LATE);
            const sk = try seriesKey(a, w.cmd.namespace_id, w.cmd.key, w.rec.field_name, w.tag_hash);
            const existing = try view.get(Series, sk);
            var field: FieldValue = (try view.get(Fields, fieldOf(sk))) orelse .{};
            if (existing == null) field.series += 1;
            field.note(ts, field.series > 1 or existing != null, &view.ts_late_points);
            try view.put(Fields, fieldOf(sk), field);
            var series: SeriesValue = existing orelse .{ .points = 0, .oldest_ns = std.math.maxInt(u64), .newest_ns = 0, .tags = w.canonical };
            series.points += 1;
            series.oldest_ns = @min(series.oldest_ns, ts);
            series.newest_ns = @max(series.newest_ns, ts);
            try view.put(Series, sk, series);
            // Every message in a channel's inbound applies at its index: its
            // place in the batch keeps two at one timestamp apart.
            const ordinal: u8 = if (view.message) |m| m.ordinal else 0;
            try view.put(Points, try pointKey(a, sk, ts, positionOf(e.header.index, ordinal)), .{ .value = w.rec.value });
            out.putBytes(w.cmd.key);
            out.putInt(u64, ts);
        },
        // The value is the canonical tag filter; empty deletes the whole
        // measurement. A filter that matches no series is refused here,
        // not when proposed: a write committed ahead of it may add the
        // series it names.
        .ts_delete => {
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
            // `c\0pu` would reach measurement `c`, field `pu`.
            if (std.mem.indexOfScalar(u8, cmd.key, 0) != null) return out.refuse(.bad_request, .unclassified, NUL_IN_NAME);
            var canon_buf: [tsp.MAX_TAG_STRING]u8 = undefined;
            var preds_buf: [tsp.MAX_TAGS]tsp.TagPredicate = undefined;
            const f = switch (tsp.parseTagFilter(cmd.value, &canon_buf, &preds_buf)) {
                .ok => |f| f,
                else => return error.Halt,
            };
            const removed = try deleteSeries(view, cmd.namespace_id, cmd.key, f.preds);
            if (removed == 0 and f.preds.len > 0)
                out.refuseFmt(.not_found, .not_found, "no series of '{s}' has tags {s}; nothing deleted", .{ cmd.key, f.canonical });
        },
        // The entry carries the duration; the cutoff is its stamp less
        // that, so every replica trims the same points.
        .ts_retention => {
            const cmd = entry_mod.CommandPayload.deserialize(e.payload) orelse return error.Halt;
            if (cmd.value.len != 8) return error.Halt;
            if (std.mem.indexOfScalar(u8, cmd.key, 0) != null) return out.refuse(.bad_request, .unclassified, NUL_IN_NAME);
            const age_ms = std.mem.readInt(u64, cmd.value[0..8], .little);
            _ = try applyRetention(view, cmd.namespace_id, cmd.key, tsp.retentionCutoffNs(e.header.timestamp_ns, age_ms));
        },
        else => return error.Halt,
    }
}

// Each pass reads what it needs before it stages a write: a range rescans
// the entry's staged writes. Points go by range delete, so an entry stages
// a few writes per series however many points it removes.

/// Deletes the series of `measurement` whose tags satisfy every predicate
/// (all of them, when there are none), and every point under them. Returns
/// how many series went.
fn deleteSeries(view: *View, ns: NamespaceId, measurement: []const u8, preds: []const tsp.TagPredicate) ApplyError!usize {
    const a = view.arena;
    var series: std.ArrayListUnmanaged([]const u8) = .empty;
    var it = try view.range(Series, try measurementPrefix(a, ns, measurement));
    while (try it.next()) |s| if (tsp.tagsMatch(s.value.tags, preds)) try series.append(a, try a.dupe(u8, s.key));
    var gone: Gone = .{};
    for (series.items) |sk| {
        try view.deleteRange(Points, sk, null);
        try view.delete(Series, sk);
        try gone.add(a, sk);
    }
    try gone.apply(view);
    return series.items.len;
}

/// Removes `measurement`'s points older than `cutoff_ns`, and each series
/// that leaves empty. Returns how many points went.
fn applyRetention(view: *View, ns: NamespaceId, measurement: []const u8, cutoff_ns: u64) ApplyError!usize {
    const a = view.arena;
    const Trim = struct { key: []const u8, value: SeriesValue, gone: u64 = 0, kept: ?u64 = null };
    var trims: std.ArrayListUnmanaged(Trim) = .empty;
    var it = try view.range(Series, try measurementPrefix(a, ns, measurement));
    while (try it.next()) |s| {
        if (s.value.oldest_ns >= cutoff_ns) continue;
        try trims.append(a, .{ .key = try a.dupe(u8, s.key), .value = .{ .points = s.value.points, .oldest_ns = s.value.oldest_ns, .newest_ns = s.value.newest_ns, .tags = try a.dupe(u8, s.value.tags) } });
    }
    // How many go, and the oldest that stays: the series' new count and
    // start.
    for (trims.items) |*t| {
        var pit = try view.range(Points, t.key);
        while (try pit.next()) |p| {
            const at = pointAt(p.key).ns;
            if (at >= cutoff_ns) {
                t.kept = at;
                break;
            }
            t.gone += 1;
        }
    }
    var evicted: usize = 0;
    var gone: Gone = .{};
    for (trims.items) |t| {
        evicted += t.gone;
        const oldest = t.kept orelse {
            try view.deleteRange(Points, t.key, null);
            try view.delete(Series, t.key);
            try gone.add(a, t.key);
            continue;
        };
        try view.deleteRange(Points, t.key, try pointKey(a, t.key, cutoff_ns, 0));
        var v = t.value;
        v.points -= t.gone;
        v.oldest_ns = oldest;
        try view.put(Series, t.key, v);
    }
    try gone.apply(view);
    return evicted;
}

/// The field a series key belongs to: its key less the tag hash.
fn fieldOf(series_key: []const u8) []const u8 {
    return series_key[0 .. series_key.len - TAG_LEN];
}

/// Series an entry removed, by field: each field's count goes down by its
/// share, and a field left with none goes.
const Gone = struct {
    fields: std.StringArrayHashMapUnmanaged(u64) = .empty,

    fn add(g: *Gone, a: Allocator, series_key: []const u8) Allocator.Error!void {
        const gop = try g.fields.getOrPut(a, fieldOf(series_key));
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    fn apply(g: *Gone, view: *View) ApplyError!void {
        for (g.fields.keys(), g.fields.values()) |fk, n| {
            var f = (try view.get(Fields, fk)) orelse return error.Halt;
            if (f.series <= n) {
                try view.delete(Fields, fk);
                continue;
            }
            f.series -= n;
            // The newest of the series left: one that went may have held it.
            var newest: u64 = 0;
            var it = try view.range(Series, fk);
            while (try it.next()) |s| newest = @max(newest, s.value.newest_ns);
            f.newest_ns = newest;
            try view.put(Fields, fk, f);
        }
    }
};

pub const machine: machine_mod.Machine = .{
    .name = "time series",
    .entry_types = &.{ .ts_write, .ts_write_batch, .ts_delete, .ts_retention },
    .families = &.{ machine_mod.info(Series), machine_mod.info(Points), machine_mod.info(Fields) },
    .keys = keys,
    .apply = apply,
};

// ─── Tests ───────────────────────────────────────────────────────────────

const testing = std.testing;
const store_mod = @import("../contract/store.zig");

/// A log of TS entries, applied through the machine as a shard applies them.
const TestLog = struct {
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    bufs: std.ArrayListUnmanaged(*[1024]u8) = .empty,

    fn deinit(self: *TestLog) void {
        for (self.bufs.items) |b| testing.allocator.destroy(b);
        self.bufs.deinit(testing.allocator);
        self.entries.deinit(testing.allocator);
    }

    fn add(self: *TestLog, comptime etype: EntryType, ns: NamespaceId, key: []const u8, value: []const u8, stamp: u64) !void {
        const b = try testing.allocator.create([1024]u8);
        try self.bufs.append(testing.allocator, b);
        const index = self.entries.items.len + 1;
        try self.entries.append(testing.allocator, entry_mod.buildCommandEntry(etype, 0, 1, index, stamp, ns, key, value, b).?);
    }

    fn write(self: *TestLog, ns: NamespaceId, m: []const u8, f: []const u8, v: f64, ts: u64, tags: []const u8) !void {
        var vb: [256]u8 = undefined;
        try self.add(.ts_write, ns, m, tsp.TsWriteValue.encode(&vb, .{ .value = v, .timestamp_ns = ts, .field_name = f, .tags = tags }).?, ts);
    }

    fn applyAll(self: *const TestLog, store: *store_mod.Store) !void {
        var scratch = std.heap.ArenaAllocator.init(testing.allocator);
        defer scratch.deinit();
        for (self.entries.items) |*e| {
            var out = Result.start(e);
            try machine_mod.applyEntry(&machine, store, &scratch, e, &out);
        }
    }
};

/// A family's canonical bytes (`store.encode`) loaded into `store`, as a
/// snapshot's piece would be.
fn loadFamily(store: *store_mod.Store, bytes: []const u8) !void {
    var at: usize = 10;
    const fam = std.mem.readInt(u16, bytes[0..2], .little);
    const n = std.mem.readInt(u64, bytes[2..10], .little);
    for (0..n) |_| {
        const klen = std.mem.readInt(u32, bytes[at..][0..4], .little);
        const key = bytes[at + 4 ..][0..klen];
        at += 4 + klen;
        const vlen = std.mem.readInt(u32, bytes[at..][0..4], .little);
        const value = try testing.allocator.dupe(u8, bytes[at + 4 ..][0..vlen]);
        at += 4 + vlen;
        store.link(fam, try store.allocRecord(key), value);
    }
    try testing.expectEqual(bytes.len, at);
}

fn familyBytes(store: *store_mod.Store, fam: u16) ![]u8 {
    var w: std.Io.Writer.Allocating = .init(testing.allocator);
    defer w.deinit();
    try store_mod.encode(store, fam, &w.writer);
    return testing.allocator.dupe(u8, w.written());
}

fn buildLog(log: *TestLog) !void {
    // Three hosts, written out of time order, over two fields and two
    // namespaces; then a tag-filtered delete and a retention.
    for (0..40) |i| {
        const t: u64 = 1000 * (40 - i);
        try log.write(.of(7), "cpu", "usage", @floatFromInt(i), t, "host=a");
        try log.write(.of(7), "cpu", "usage", @floatFromInt(i), t, "host=b");
        try log.write(.of(7), "cpu", "idle", @floatFromInt(i), t, "host=c,dc=x");
        try log.write(.of(8), "cpu", "usage", @floatFromInt(i), t, "host=a");
    }
    try log.add(.ts_delete, .of(7), "cpu", "host=b", 50_000);
    var age: [8]u8 = undefined;
    std.mem.writeInt(u64, &age, 0, .little);
    // The stamp is the cutoff: points before 10 µs go.
    try log.add(.ts_retention, .of(7), "cpu", &age, 10_000);
}

test "time series: replaying the log gives byte-identical families" {
    var log: TestLog = .{};
    defer log.deinit();
    try buildLog(&log);
    var a = store_mod.Store.init(testing.allocator);
    defer a.deinit();
    var b = store_mod.Store.init(testing.allocator);
    defer b.deinit();
    try log.applyAll(&a);
    try log.applyAll(&b);
    inline for (.{ Series.ID, Points.ID, Fields.ID }) |fam| try testing.expectEqual(store_mod.checksum(&a, fam), store_mod.checksum(&b, fam));
    // host=b went whole; the others kept their 31 points at or after 10 µs.
    try testing.expectEqual(@as(usize, 3), a.family(Series.ID).count);
    // Each field left one series: (7, usage), (7, idle), (8, usage).
    try testing.expectEqual(@as(usize, 3), a.family(Fields.ID).count);
    try testing.expectEqual(@as(usize, 2 * 31 + 40), a.family(Points.ID).count);
}

test "time series: the families' snapshot bytes load into a store that reads the same points" {
    var log: TestLog = .{};
    defer log.deinit();
    try buildLog(&log);
    var a = store_mod.Store.init(testing.allocator);
    defer a.deinit();
    try log.applyAll(&a);
    var c = store_mod.Store.init(testing.allocator);
    defer c.deinit();
    inline for (.{ Series.ID, Points.ID, Fields.ID }) |fam| {
        const bytes = try familyBytes(&a, fam);
        defer testing.allocator.free(bytes);
        try loadFamily(&c, bytes);
        try testing.expectEqual(store_mod.checksum(&a, fam), store_mod.checksum(&c, fam));
    }
    var ra = tsp.TSProjection.initOn(testing.allocator, &a);
    var rc = tsp.TSProjection.initOn(testing.allocator, &c);
    var ba: [64]tsp.StoredPoint = undefined;
    var bc: [64]tsp.StoredPoint = undefined;
    for ([_][]const u8{ "usage", "idle" }) |f| {
        const na = (try ra.queryRange(.of(7), "cpu", f, null, 0, std.math.maxInt(u64), &ba)).points_in_buffer;
        const nc = (try rc.queryRange(.of(7), "cpu", f, null, 0, std.math.maxInt(u64), &bc)).points_in_buffer;
        try testing.expectEqual(na, nc);
        try testing.expect(na > 0);
        for (ba[0..na], bc[0..nc]) |x, y| {
            try testing.expectEqual(x.timestamp_ns, y.timestamp_ns);
            try testing.expectEqual(x.field_value, y.field_value);
            try testing.expectEqualStrings(x.tags, y.tags);
        }
    }
    var fa: [tsp.TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    var fc: [tsp.TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    try testing.expectEqual(ra.summarize(.of(7), "cpu", &fa), rc.summarize(.of(7), "cpu", &fc));
}

test "time series: a name holding NUL is refused, and changes nothing" {
    var log: TestLog = .{};
    defer log.deinit();
    try log.write(.of(7), "c\x00pu", "usage", 1, 1000, "");
    var s = store_mod.Store.init(testing.allocator);
    defer s.deinit();
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    var out = Result.start(&log.entries.items[0]);
    try machine_mod.applyEntry(&machine, &s, &scratch, &log.entries.items[0], &out);
    try testing.expect(out.refused() != null);
    try testing.expectEqual(@as(usize, 0), s.family(Points.ID).count);

    // A delete or retention naming `c\0pu` leaves measurement `c`'s field
    // `pu` alone.
    try log.write(.of(7), "c", "pu", 1, 1000, "");
    var age: [8]u8 = undefined;
    std.mem.writeInt(u64, &age, 0, .little);
    try log.add(.ts_delete, .of(7), "c\x00pu", "", 2000);
    try log.add(.ts_retention, .of(7), "c\x00pu", &age, 2000);
    for (log.entries.items[1..], 0..) |*e, i| {
        var o = Result.start(e);
        try machine_mod.applyEntry(&machine, &s, &scratch, e, &o);
        if (i == 0) try testing.expect(o.refused() == null) else try testing.expectEqualStrings(NUL_IN_NAME, o.refused().?.refusal.message);
    }
    try testing.expectEqual(@as(usize, 1), s.family(Points.ID).count);
    try testing.expectEqual(@as(usize, 1), s.family(Series.ID).count);
}

test "time series: a point later than an i64 of nanoseconds is refused, and changes nothing" {
    var log: TestLog = .{};
    defer log.deinit();
    try log.write(.of(7), "cpu", "v", 1, std.math.maxInt(i64), "");
    try log.write(.of(7), "cpu", "v", 2, @as(u64, std.math.maxInt(i64)) + 1, "");
    try log.write(.of(7), "cpu", "v", 3, 10_000_000_000_000_000_000, "");
    var s = store_mod.Store.init(testing.allocator);
    defer s.deinit();
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    for (log.entries.items, 0..) |*e, i| {
        var o = Result.start(e);
        try machine_mod.applyEntry(&machine, &s, &scratch, e, &o);
        if (i == 0) try testing.expect(o.refused() == null) else try testing.expectEqualStrings(TIME_TOO_LATE, o.refused().?.refusal.message);
    }
    try testing.expectEqual(@as(usize, 1), s.family(Points.ID).count);
    try testing.expectEqual(@as(u64, std.math.maxInt(i64)), (try fieldIn(&s, .of(7), "cpu", "v")).?.newest_ns);
}

test "time series: two writes at one timestamp in one channel inbound are two points" {
    var log: TestLog = .{};
    defer log.deinit();
    try log.write(.of(7), "cpu", "v", 1, 1000, "");
    try log.write(.of(7), "cpu", "v", 2, 1000, "");
    const ce = @import("../protocol/channel_entries.zig");
    const schema = @import("../protocol/schema.zig");
    var mb: [4096]u8 = undefined;
    var w: schema.Writer = .{ .buf = &mb };
    for (log.entries.items) |e| try ce.Message.encode(&w, .{ .slot = 0, .entry_type = e.header.entry_type, .layout_epoch = 0, .flags = 0, .origin_kind = 1, .origin_ns = 7, .payload = e.payload });
    var ib: [8192]u8 = undefined;
    const inbound = try ce.Inbound.encodeInto(&ib, .{ .from = 0, .first_seq = 1, .messages = .{ .bytes = w.written(), .count = 2 } });
    var h = std.mem.zeroes(entry_mod.Header);
    h.entry_type = @backingInt(EntryType.channel_inbound);
    h.index = 9;
    h.payload_len = @intCast(inbound.len);
    const e: Entry = .{ .header = h, .payload = inbound };
    var s = store_mod.Store.init(testing.allocator);
    defer s.deinit();
    s.place = .{ .group = 0, .count = 1 };
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    var out = Result.start(&e);
    try machine_mod.applyEntry(&@import("../contract/channels.zig").Channels(&.{machine}).machine, &s, &scratch, &e, &out);
    try testing.expectEqual(@as(usize, 2), s.family(Points.ID).count);
}

test "time series: a position orders by log index, then by place in the entry, and two places never share a key" {
    const a = testing.allocator;
    try testing.expect(positionOf(7, 0) < positionOf(7, 1));
    try testing.expect(positionOf(7, 255) < positionOf(8, 0));
    const sk = try seriesKey(a, .of(7), "cpu", "v", 0);
    defer a.free(sk);
    const k0 = try pointKey(a, sk, 100, positionOf(7, 0));
    defer a.free(k0);
    const k1 = try pointKey(a, sk, 100, positionOf(7, 1));
    defer a.free(k1);
    try testing.expect(!std.mem.eql(u8, k0, k1));
    try testing.expectEqual(std.math.Order.lt, std.mem.order(u8, k0, k1));
    // An entry declares the range its places share.
    try testing.expect(std.mem.startsWith(u8, k1, k0[0 .. k0.len - 1]));
    try testing.expectEqual(positionOf(7, 1), pointAt(k1).ual);
}

fn fieldIn(store: *store_mod.Store, ns: NamespaceId, m: []const u8, f: []const u8) !?FieldValue {
    const k = try fieldPrefix(testing.allocator, ns, m, f);
    defer testing.allocator.free(k);
    const rv = view_mod.ReadView{ .store = store };
    return rv.get(Fields, k);
}

test "time series: a lateness window reports against the smallest edge at or above it" {
    const s_ns = std.time.ns_per_s;
    try testing.expectEqual(@as(?usize, 0), lateEdgeFor(0));
    try testing.expectEqual(@as(?usize, 1), lateEdgeFor(1));
    try testing.expectEqual(@as(?usize, 2), lateEdgeFor(3 * s_ns));
    try testing.expectEqual(@as(?usize, 4), lateEdgeFor(30 * s_ns));
    try testing.expectEqual(@as(?usize, 5), lateEdgeFor(30 * s_ns + 1));
    try testing.expectEqual(@as(?usize, 7), lateEdgeFor(3600 * s_ns));
    try testing.expectEqual(@as(?usize, null), lateEdgeFor(3600 * s_ns + 1));
}

test "time series: a late point counts past each edge it trails its field's newest time by, not at the edge" {
    var log: TestLog = .{};
    defer log.deinit();
    const newest: u64 = 10_000 * NS_PER_S;
    // Two series of one field: lateness is the field's, not a series'.
    try log.write(.of(7), "cpu", "usage", 1, newest, "host=a");
    for (LATE_EDGES_NS) |edge| {
        try log.write(.of(7), "cpu", "usage", 1, newest - edge, "host=b");
        try log.write(.of(7), "cpu", "usage", 1, newest - edge - 1, "host=b");
    }
    var s = store_mod.Store.init(testing.allocator);
    defer s.deinit();
    try log.applyAll(&s);
    const f = (try fieldIn(&s, .of(7), "cpu", "usage")).?;
    try testing.expectEqual(newest, f.newest_ns);
    try testing.expectEqual(@as(u64, 2), f.series);
    // Past edge i: the points one past each edge at or above it, and those
    // at each edge above it; at edge 0 the one at the newest itself isn't.
    for (LATE_EDGES_NS, 0..) |_, i| {
        const past_or_at_higher = 2 * (LATE_EDGES_NS.len - 1 - i) + 1;
        try testing.expectEqual(@as(u64, past_or_at_higher), f.late[i]);
    }
    // Every write counts, late or not; the node's count of late applies
    // matches the field's.
    try testing.expectEqual(@as(u64, 1 + 2 * LATE_EDGES_NS.len), f.writes);
    try testing.expectEqualSlices(u64, &f.late, &s.ts_late_points);
    // A point ahead of the newest is never late, and moves it.
    try log.write(.of(7), "cpu", "usage", 1, newest + 5, "host=b");
    var t = store_mod.Store.init(testing.allocator);
    defer t.deinit();
    try log.applyAll(&t);
    const g = (try fieldIn(&t, .of(7), "cpu", "usage")).?;
    try testing.expectEqual(newest + 5, g.newest_ns);
    try testing.expectEqualSlices(u64, &f.late, &g.late);
}

test "time series: a field goes with its last series, to a delete or retention, and no series or point range reaches it" {
    var log: TestLog = .{};
    defer log.deinit();
    try log.write(.of(7), "cpu", "usage", 1, 1000, "host=a");
    try log.write(.of(7), "cpu", "usage", 1, 2000, "host=b");
    try log.write(.of(7), "cpu", "idle", 1, 5000, "host=a");
    // Deleting one of usage's series keeps the field, one series fewer.
    try log.add(.ts_delete, .of(7), "cpu", "host=b", 10_000);
    var s = store_mod.Store.init(testing.allocator);
    defer s.deinit();
    try log.applyAll(&s);
    try testing.expectEqual(@as(u64, 1), (try fieldIn(&s, .of(7), "cpu", "usage")).?.series);
    // host=b held the newest time; the newest is now host=a's.
    try testing.expectEqual(@as(u64, 1000), (try fieldIn(&s, .of(7), "cpu", "usage")).?.newest_ns);
    try testing.expectEqual(@as(usize, 2), s.family(Fields.ID).count);
    // Retention empties usage (its point is at 1000) but not idle's.
    var age: [8]u8 = undefined;
    std.mem.writeInt(u64, &age, 0, .little);
    try log.add(.ts_retention, .of(7), "cpu", &age, 3000);
    var t = store_mod.Store.init(testing.allocator);
    defer t.deinit();
    try log.applyAll(&t);
    try testing.expect((try fieldIn(&t, .of(7), "cpu", "usage")) == null);
    try testing.expectEqual(@as(u64, 1), (try fieldIn(&t, .of(7), "cpu", "idle")).?.series);
    // A whole-measurement delete takes the rest.
    try log.add(.ts_delete, .of(7), "cpu", "", 20_000);
    var u = store_mod.Store.init(testing.allocator);
    defer u.deinit();
    try log.applyAll(&u);
    inline for (.{ Series.ID, Points.ID, Fields.ID }) |fam| try testing.expectEqual(@as(usize, 0), u.family(fam).count);
    // The node's count of late applies never falls with what's deleted.
    var late = store_mod.Store.init(testing.allocator);
    defer late.deinit();
    var l2: TestLog = .{};
    defer l2.deinit();
    try l2.write(.of(7), "m", "f", 1, 10 * NS_PER_S, "host=a");
    try l2.write(.of(7), "m", "f", 1, 1 * NS_PER_S, "host=b");
    try l2.add(.ts_delete, .of(7), "m", "", 20 * NS_PER_S);
    try l2.applyAll(&late);
    try testing.expectEqual(@as(usize, 0), late.family(Fields.ID).count);
    // 9 s behind: past 0 s, 1 s and 5 s, not 15 s.
    try testing.expectEqualSlices(u64, &.{ 1, 1, 1, 0, 0, 0, 0, 0 }, &late.ts_late_points);
}
