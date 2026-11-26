//! Time series reads, and the shapes and rules the time-series machine
//! (`ts/machine.zig`) and its callers share.
//!
//! A point is a record of the contract's `ts.point` family, a series one of
//! `ts.series`: the machine changes them in apply, and `TSProjection`
//! reads them. Data model:
//!   - Measurement: top-level name (e.g., "cpu", "memory")
//!   - Tags: key-value pairs for filtering (e.g., host=web-01)
//!   - Fields: numeric values (e.g., usage=82.5, idle=17.5)
//!   - Timestamp: nanosecond precision

const std = @import("std");
const NamespaceId = @import("../namespace/table.zig").Id;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.ts_projection);
const entry_mod = @import("../storage/ual/entry.zig");
const persistence = @import("../storage/persistence.zig");
const time_units = @import("../util/time_units.zig");
const store_mod = @import("../contract/store.zig");
const view_mod = @import("../contract/view.zig");
const machine_mod = @import("../contract/machine.zig");
const ts_machine = @import("../ts/machine.zig");

const Result = persistence.Result;
const EntryType = entry_mod.EntryType;
const Store = store_mod.Store;
const ReadView = view_mod.ReadView;
const ReadRange = view_mod.ReadRange;

// ═══════════════════════════════════════════════════════════════════════════════
// Data Point
// ═══════════════════════════════════════════════════════════════════════════════

pub const FieldValue = union(enum) {
    float: f64,
    int: i64,
    boolean: bool,
};

pub const Tag = struct {
    key: []const u8,
    value: []const u8,
};

pub const DataPoint = struct {
    measurement: []const u8,
    tags: []const Tag,
    field_name: []const u8,
    field_value: FieldValue,
    timestamp_ns: u64,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Stored Point — a point as a read returns it
// ═══════════════════════════════════════════════════════════════════════════════

pub const StoredPoint = struct {
    timestamp_ns: u64,
    field_value: f64, // normalized to f64
    /// Its position: the log index of the entry it was applied from, then
    /// its place in that entry (`ts_machine.positionOf`).
    ual_index: u64,
    /// Its series' tag hash (0 = untagged).
    tag_hash: u64,
    /// Its series' canonical tags, borrowed from the store a read took it
    /// from: good until that store next changes.
    tags: []const u8 = "",
};

// ═══════════════════════════════════════════════════════════════════════════════
// Series Key — measurement + field name
// ═══════════════════════════════════════════════════════════════════════════════

/// Points older than this go under a retention of `age_ms` applied at
/// `stamp_ns`. Saturates: a duration past the stamp trims nothing.
pub fn retentionCutoffNs(stamp_ns: u64, age_ms: u64) u64 {
    return stamp_ns -| time_units.msToNsSat(age_ms);
}

/// UAL payload layout for a `ts_write` command value (the measurement travels
/// in the command key):
///
///   [value: f64 LE 8][timestamp_ns: u64 LE 8][field_len: u16 LE 2][tags_len: u16 LE 2][field][tags]
///
/// Before #24 the value was just the 8 value bytes, so the field name, the
/// client-supplied timestamp AND the tags were all dropped on replay: a point
/// written as `cpu --field user --tags host=a` came back after a restart as
/// `cpu/value` with no tags. With tags now part of the series key, persisting
/// them is what keeps a replayed series identical to the one that was written.
/// The tag SET is stored, not its hash: the hash is derivable, but the tag
/// dictionary needs the spelling to survive a restart.
pub const TsWriteValue = struct {
    value: f64,
    timestamp_ns: u64,
    field_name: []const u8,
    tags: []const u8,

    pub const MIN_SIZE = 8 + 8 + 2 + 2;

    pub fn encode(buf: []u8, v: TsWriteValue) ?[]const u8 {
        if (v.field_name.len > std.math.maxInt(u16) or v.tags.len > std.math.maxInt(u16)) return null;
        const total = MIN_SIZE + v.field_name.len + v.tags.len;
        if (buf.len < total) return null;
        std.mem.writeInt(u64, buf[0..8], @bitCast(v.value), .little);
        std.mem.writeInt(u64, buf[8..16], v.timestamp_ns, .little);
        std.mem.writeInt(u16, buf[16..18], @intCast(v.field_name.len), .little);
        std.mem.writeInt(u16, buf[18..20], @intCast(v.tags.len), .little);
        var off: usize = 20;
        @memcpy(buf[off..][0..v.field_name.len], v.field_name);
        off += v.field_name.len;
        @memcpy(buf[off..][0..v.tags.len], v.tags);
        return buf[0..total];
    }

    /// Decode a persisted value. A payload too short to carry the full record
    /// is read as a bare f64 (defensive against truncation) and falls back to
    /// the entry header's timestamp.
    pub fn decode(data: []const u8, fallback_ts_ns: u64) ?TsWriteValue {
        if (data.len < 8) return null;
        const value: f64 = @bitCast(std.mem.readInt(u64, data[0..8], .little));
        if (data.len < MIN_SIZE) {
            return .{ .value = value, .timestamp_ns = fallback_ts_ns, .field_name = "value", .tags = "" };
        }
        const ts_ns = std.mem.readInt(u64, data[8..16], .little);
        const flen: usize = std.mem.readInt(u16, data[16..18], .little);
        const tlen: usize = std.mem.readInt(u16, data[18..20], .little);
        var off: usize = 20;
        const field = if (off + flen <= data.len and flen > 0) data[off..][0..flen] else "value";
        off += flen;
        const tags = if (off + tlen <= data.len and tlen > 0) data[off..][0..tlen] else "";
        return .{
            .value = value,
            .timestamp_ns = if (ts_ns > 0) ts_ns else fallback_ts_ns,
            .field_name = field,
            .tags = tags,
        };
    }
};

/// Maximum `key=value` pairs considered when canonicalizing a tag set.
pub const MAX_TAGS = 32;

/// Upper bound on a canonical tag string.
pub const MAX_TAG_STRING = 1024;

/// Normalize a tag set into its canonical form: pairs trimmed, empty pairs
/// dropped, and **sorted**, joined with `,`. This is the string stored in the
/// tag dictionary and the exact bytes `canonicalTagHash` hashes, so a tag set
/// has one identity no matter what order the writer supplied it in.
pub fn canonicalTagString(tags: []const u8, buf: []u8) []const u8 {
    if (tags.len == 0) return "";
    var parts: [MAX_TAGS][]const u8 = undefined;
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, tags, ',');
    while (it.next()) |raw| {
        const part = std.mem.trim(u8, raw, " \t");
        if (part.len == 0) continue;
        if (n >= MAX_TAGS) break;
        parts[n] = part;
        n += 1;
    }
    if (n == 0) return "";
    std.mem.sort([]const u8, parts[0..n], {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    var pos: usize = 0;
    for (parts[0..n], 0..) |part, i| {
        if (i > 0) {
            if (pos + 1 > buf.len) break;
            buf[pos] = ',';
            pos += 1;
        }
        if (pos + part.len > buf.len) break;
        @memcpy(buf[pos..][0..part.len], part);
        pos += part.len;
    }
    return buf[0..pos];
}

/// What's wrong with a write's tag set, if anything: more than
/// `MAX_TAG_STRING` bytes or `MAX_TAGS` pairs (past them
/// `canonicalTagString` would cut it unseen), a byte that isn't UTF-8, a
/// part that isn't `key=value` with both non-empty, or a key given twice.
/// A write is refused for any of them, so every stored series has a tag set
/// a read's or a delete's tag filter can name, one key once.
pub const TagProblem = union(enum) {
    too_long,
    too_many,
    not_utf8,
    /// The part with no key.
    empty_key: []const u8,
    /// The part with no `=` or nothing after it.
    no_value: []const u8,
    /// The key given twice.
    repeated: []const u8,

    /// The refusal a client is answered, naming the part or key where it
    /// has one.
    pub fn text(p: TagProblem, buf: []u8) []const u8 {
        return p.write(buf, "bad request: ") catch p.fixed();
    }

    /// The same, as a sink's dropped record says it: no request to refuse.
    pub fn sinkText(p: TagProblem, buf: []u8) []const u8 {
        return p.write(buf, "") catch p.fixed()["bad request: ".len..];
    }

    fn write(p: TagProblem, buf: []u8, prefix: []const u8) error{NoSpaceLeft}![]const u8 {
        var w: std.Io.Writer = .fixed(buf);
        w.writeAll(prefix) catch return error.NoSpaceLeft;
        switch (p) {
            .empty_key => |part| try echo(&w, "tag \"", part, "\" has no key"),
            .no_value => |part| try echo(&w, "tag \"", part, "\" has no value: a tag is key=value"),
            .repeated => |key| try echo(&w, "tag key \"", key, "\" is given twice; a series has each key once"),
            else => w.writeAll(p.fixed()["bad request: ".len..]) catch return error.NoSpaceLeft,
        }
        return w.buffered();
    }

    /// `before`, the first 64 bytes of `s` cut where a character ends, with
    /// control bytes written as `\xNN`, then `after`: a refusal any client
    /// decodes, whatever the tag held.
    fn echo(w: *std.Io.Writer, before: []const u8, s: []const u8, after: []const u8) error{NoSpaceLeft}!void {
        var end = @min(s.len, 64);
        while (end > 0 and end < s.len and (s[end] & 0xC0) == 0x80) end -= 1;
        w.writeAll(before) catch return error.NoSpaceLeft;
        for (s[0..end]) |c| (if (c < 0x20 or c == 0x7f) w.print("\\x{x:0>2}", .{c}) else w.writeByte(c)) catch return error.NoSpaceLeft;
        w.writeAll(after) catch return error.NoSpaceLeft;
    }

    /// The refusal without the part or key, for where no buffer outlives it.
    pub fn fixed(p: TagProblem) []const u8 {
        return switch (p) {
            .too_long => TAG_TOO_LONG,
            .too_many => TAG_TOO_MANY,
            .not_utf8 => TAG_NOT_UTF8,
            .empty_key => TAG_EMPTY_KEY,
            .no_value => TAG_NO_VALUE,
            .repeated => TAG_REPEATED,
        };
    }
};

pub const TAG_TOO_LONG = std.fmt.comptimePrint("bad request: tags are at most {d} bytes", .{MAX_TAG_STRING});
pub const TAG_TOO_MANY = std.fmt.comptimePrint("bad request: a point has at most {d} tags", .{MAX_TAGS});
pub const TAG_NOT_UTF8 = "bad request: tags must be UTF-8";
pub const TAG_EMPTY_KEY = "bad request: a tag has no key";
pub const TAG_NO_VALUE = "bad request: a tag has no value: a tag is key=value";
pub const TAG_REPEATED = "bad request: a tag key is given twice; a series has each key once";

/// The first problem with `tags`, split and trimmed as
/// `canonicalTagString` reads it; null when it has none.
pub fn tagSetProblem(tags: []const u8) ?TagProblem {
    if (tags.len > MAX_TAG_STRING) return .too_long;
    if (!std.unicode.utf8ValidateSlice(tags)) return .not_utf8;
    var keys: [MAX_TAGS][]const u8 = undefined;
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, tags, ',');
    while (it.next()) |raw| {
        const part = std.mem.trim(u8, raw, " \t");
        if (part.len == 0) continue;
        if (n == MAX_TAGS) return .too_many;
        const eq = std.mem.indexOfScalar(u8, part, '=') orelse return .{ .no_value = part };
        const key = part[0..eq];
        if (key.len == 0) return .{ .empty_key = part };
        if (eq + 1 == part.len) return .{ .no_value = part };
        for (keys[0..n]) |k| if (std.mem.eql(u8, k, key)) return .{ .repeated = key };
        keys[n] = key;
        n += 1;
    }
    return null;
}

test "tagSetProblem: a repeated or empty key, bytes that aren't UTF-8, and a set past the limits" {
    try std.testing.expectEqual(@as(?TagProblem, null), tagSetProblem(""));
    try std.testing.expectEqual(@as(?TagProblem, null), tagSetProblem("host=a, env=prod,,"));
    // A tag is key=value, both non-empty: what a tag filter can name.
    try std.testing.expectEqualStrings("flag", tagSetProblem("host=a, env=prod,,flag").?.no_value);
    try std.testing.expectEqualStrings("host=", tagSetProblem("host=").?.no_value);
    try std.testing.expectEqualStrings("host=", tagSetProblem("a=1, host= ").?.no_value);
    try std.testing.expectEqualStrings("a", tagSetProblem("a=1,b=2,a=3").?.repeated);
    try std.testing.expectEqualStrings("a", tagSetProblem(" a=1 ,a=2").?.repeated);
    try std.testing.expectEqualStrings("=x", tagSetProblem("host=a,=x").?.empty_key);
    try std.testing.expectEqual(TagProblem.not_utf8, tagSetProblem("\xff=1").?);
    try std.testing.expectEqual(TagProblem.not_utf8, tagSetProblem("h=caf\xc3").?);
    try std.testing.expectEqual(@as(?TagProblem, null), tagSetProblem("h=caf\xc3\xa9"));
    try std.testing.expectEqual(TagProblem.too_long, tagSetProblem("k=" ++ comptime @import("stdx").repeat("v", MAX_TAG_STRING - 1)).?);
    var many: [MAX_TAGS + 1][4]u8 = undefined;
    var buf: [256]u8 = undefined;
    var pos: usize = 0;
    for (&many, 0..) |*m, i| {
        m.* = .{ 'k', @intCast('A' + i), '=', '1' };
        @memcpy(buf[pos..][0..4], m);
        pos += 4;
        buf[pos] = ',';
        pos += 1;
    }
    try std.testing.expectEqual(TagProblem.too_many, tagSetProblem(buf[0..pos]).?);
    try std.testing.expectEqual(@as(?TagProblem, null), tagSetProblem(buf[0 .. pos - 6]));
    var tb: [128]u8 = undefined;
    try std.testing.expectEqualStrings("bad request: tag key \"a\" is given twice; a series has each key once", tagSetProblem("a=1,a=2").?.text(&tb));
    try std.testing.expectEqualStrings("tag \"flag\" has no value: a tag is key=value", tagSetProblem("flag").?.sinkText(&tb));
    // The echoed key is cut where a character ends, its control bytes escaped.
    const long = comptime @import("stdx").repeat("x", 63) ++ "\xc3\xa9";
    const msg = tagSetProblem(long ++ "=1," ++ long ++ "=2").?.text(&tb);
    try std.testing.expect(std.unicode.utf8ValidateSlice(msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "\xc3") == null);
    try std.testing.expectEqualStrings("bad request: tag key \"a\\x1b[31m\" is given twice; a series has each key once", tagSetProblem("a\x1b[31m=1,a\x1b[31m=2").?.text(&tb));
}

test "tagSetProblem: every random tag set it accepts canonicalises to itself, has no repeated key, and a tag filter can name it" {
    var prng = std.Random.DefaultPrng.init(0x7a65);
    const r = prng.random();
    const alphabet = "ab=, \t=x\xc3\xa9\x01=";
    var buf: [48]u8 = undefined;
    var canon_a: [MAX_TAG_STRING]u8 = undefined;
    var canon_b: [MAX_TAG_STRING]u8 = undefined;
    var preds: [MAX_TAGS]TagPredicate = undefined;
    var accepted: usize = 0;
    for (0..200_000) |_| {
        const n = r.uintLessThan(usize, buf.len);
        for (buf[0..n]) |*c| c.* = alphabet[r.uintLessThan(usize, alphabet.len)];
        const tags = buf[0..n];
        if (tagSetProblem(tags) != null) continue;
        accepted += 1;
        const once = canonicalTagString(tags, &canon_a);
        try std.testing.expectEqualStrings(once, canonicalTagString(once, &canon_b));
        try std.testing.expect(tagSetProblem(once) == null);
        if (once.len == 0) continue;
        switch (parseTagFilter(once, &canon_b, &preds)) {
            .ok => {},
            else => |bad| {
                std.debug.print("\nfilter refused '{s}': {any}\n", .{ once, bad });
                return error.TestUnexpectedResult;
            },
        }
    }
    try std.testing.expect(accepted > 1000);
}

/// Canonical tag-set hash — `canonicalTagString` then Wyhash. Returns 0 for an
/// empty tag set (the untagged series).
pub fn canonicalTagHash(tags: []const u8) u64 {
    if (tags.len == 0) return 0;
    var buf: [MAX_TAG_STRING]u8 = undefined;
    const canon = canonicalTagString(tags, &buf);
    if (canon.len == 0) return 0;
    return std.hash.Wyhash.hash(0, canon);
}

/// Glob matcher supporting `*` (any run) and `?` (any single byte). FloQL's
/// `=~` / `!~` are glob, not regex — see the `{host=~web-*}` / `{region!~*-west}`
/// e2e cases.
pub fn globMatch(pattern: []const u8, text: []const u8) bool {
    var p: usize = 0;
    var t: usize = 0;
    var star: ?usize = null;
    var star_t: usize = 0;
    while (t < text.len) {
        if (p < pattern.len and (pattern[p] == '?' or pattern[p] == text[t])) {
            p += 1;
            t += 1;
        } else if (p < pattern.len and pattern[p] == '*') {
            star = p;
            star_t = t;
            p += 1;
        } else if (star) |sp| {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else return false;
    }
    while (p < pattern.len and pattern[p] == '*') p += 1;
    return p == pattern.len;
}

pub const TagOp = enum { eq, neq, glob, nglob };

/// One tag predicate, e.g. `host=web-01` or `region!~*-west`.
pub const TagPredicate = struct {
    key: []const u8,
    op: TagOp,
    value: []const u8,
};

/// Value of one tag within a canonical tag string, or null if absent.
pub fn tagValue(canonical_tags: []const u8, key: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, canonical_tags, ',');
    while (it.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        if (std.mem.eql(u8, pair[0..eq], key)) return pair[eq + 1 ..];
    }
    return null;
}

/// Do a series' tags satisfy every predicate?
///
/// Predicates constrain only the tags they name, so `{host=web-01}` matches a
/// point tagged `host=web-01,env=prod` — the partial matching a bare tag hash
/// could never express. A negated predicate is satisfied by an absent tag,
/// matching the usual time-series convention.
pub fn tagsMatch(canonical_tags: []const u8, preds: []const TagPredicate) bool {
    for (preds) |pred| {
        const actual = tagValue(canonical_tags, pred.key);
        const ok = switch (pred.op) {
            .eq => actual != null and std.mem.eql(u8, actual.?, pred.value),
            .neq => actual == null or !std.mem.eql(u8, actual.?, pred.value),
            .glob => actual != null and globMatch(pred.value, actual.?),
            .nglob => actual == null or !globMatch(pred.value, actual.?),
        };
        if (!ok) return false;
    }
    return true;
}

/// A delete's tag filter: every `key=value` in it must be among a series'
/// tags for the series to match. Empty matches every series.
pub const TagFilter = union(enum) {
    ok: struct { canonical: []const u8, preds: []const TagPredicate },
    /// A part that isn't `key=value` with both sides non-empty.
    not_pair: []const u8,
    too_many,
    too_long,
};

/// Parse a tag filter strictly: a part that isn't a pair is refused rather
/// than dropped, since dropping it would widen what the filter matches.
/// The canonical form and the predicates point into `canon_buf`.
pub fn parseTagFilter(tags: []const u8, canon_buf: *[MAX_TAG_STRING]u8, preds_buf: *[MAX_TAGS]TagPredicate) TagFilter {
    if (tags.len > MAX_TAG_STRING) return .too_long;
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, tags, ',');
    while (it.next()) |raw| {
        const part = std.mem.trim(u8, raw, " \t");
        if (part.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, part, '=') orelse return .{ .not_pair = part };
        if (eq == 0 or eq == part.len - 1) return .{ .not_pair = part };
        if (n == MAX_TAGS) return .too_many;
        n += 1;
    }
    const canonical = canonicalTagString(tags, canon_buf);
    var m: usize = 0;
    var cit = std.mem.splitScalar(u8, canonical, ',');
    while (cit.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=').?;
        preds_buf[m] = .{ .key = pair[0..eq], .op = .eq, .value = pair[eq + 1 ..] };
        m += 1;
    }
    return .{ .ok = .{ .canonical = canonical, .preds = preds_buf[0..m] } };
}

/// A single tag as key/value, for callers that hold structured tags rather than
/// a raw `k=v,k=v` string (e.g. FloQL source filters).
pub const TagPair = struct {
    key: []const u8,
    value: []const u8,
};

/// Canonical tag hash from structured pairs. Formats them into `scratch` and
/// defers to `canonicalTagHash`, so a tag set hashes identically no matter
/// whether it arrived as a string or as pairs. Returns 0 if it does not fit.
pub fn canonicalTagHashPairs(pairs: []const TagPair, scratch: []u8) u64 {
    var pos: usize = 0;
    for (pairs, 0..) |kv, i| {
        const written = std.fmt.bufPrint(scratch[pos..], "{s}{s}={s}", .{ if (i > 0) "," else "", kv.key, kv.value }) catch return 0;
        pos += written.len;
    }
    return canonicalTagHash(scratch[0..pos]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TimeSeries Projection
// ═══════════════════════════════════════════════════════════════════════════════

pub const TSProjection = struct {
    allocator: Allocator,
    /// The shard's store, which the time-series machine changes in apply
    /// (`ts/machine.zig`); or, standing alone, one of its own (`init`).
    store: *Store,
    /// A standalone projection's own store, and the log index its next
    /// write takes.
    own: ?*Own = null,
    stats: Stats = .{},

    const Own = struct {
        store: Store,
        scratch: std.heap.ArenaAllocator,
        next_index: u64 = 1,
    };

    pub const Stats = struct {
        points_inserted: u64 = 0,
        queries: u64 = 0,
    };

    /// A projection over a store of its own, for tests and a handler with
    /// no shard: its writes go through the same machine a shard applies.
    pub fn init(allocator: Allocator) TSProjection {
        const own = allocator.create(Own) catch @panic("out of memory");
        own.* = .{ .store = Store.init(allocator), .scratch = std.heap.ArenaAllocator.init(allocator) };
        return .{ .allocator = allocator, .store = &own.store, .own = own };
    }

    /// A projection reading `store`, which a shard's apply changes.
    pub fn initOn(allocator: Allocator, store: *Store) TSProjection {
        return .{ .allocator = allocator, .store = store };
    }

    pub fn deinit(self: *TSProjection) void {
        const own = self.own orelse return;
        own.store.deinit();
        own.scratch.deinit();
        self.allocator.destroy(own);
        self.own = null;
    }

    fn read(self: *const TSProjection) ReadView {
        return .{ .store = self.store };
    }

    // ─── Standalone writes ────────────────────────────────────────────────

    /// Apply `etype` as the shard would, on a standalone projection's store.
    fn applyOwn(self: *TSProjection, comptime etype: EntryType, ns_id: NamespaceId, key: []const u8, value: []const u8, stamp_ns: u64, out: *Result) !void {
        const own = self.own orelse @panic("a shard's time series are written only by applying entries");
        var pbuf: [MAX_STANDALONE_PAYLOAD]u8 = undefined;
        const e = entry_mod.buildCommandEntry(etype, entry_mod.Flags.NONE, 1, own.next_index, stamp_ns, ns_id, key, value, &pbuf) orelse return error.OutOfMemory;
        own.next_index += 1;
        out.* = Result.start(&e);
        try machine_mod.applyEntry(&ts_machine.machine, &own.store, &own.scratch, &e, out);
    }

    const MAX_STANDALONE_PAYLOAD = 16 * 1024;

    /// Apply an entry to a standalone projection, through the machine a
    /// shard applies it with.
    pub fn applyEntry(self: *TSProjection, e: *const entry_mod.Entry, out: *Result) !void {
        const own = self.own orelse @panic("a shard's time series are written only by applying entries");
        own.next_index = @max(own.next_index, e.header.index + 1);
        try machine_mod.applyEntry(&ts_machine.machine, &own.store, &own.scratch, e, out);
    }

    /// Insert a point into a standalone projection. `tags` is the raw
    /// `k=v,k=v` tag set; the machine canonicalizes it. `ual_index` is
    /// ignored: the point takes the next log index of the projection.
    pub fn insert(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, value: f64, timestamp_ns: u64, ual_index: u64, tags: []const u8) !void {
        _ = ual_index;
        var vbuf: [MAX_TAG_STRING + 1024]u8 = undefined;
        const v = TsWriteValue.encode(&vbuf, .{ .value = value, .timestamp_ns = timestamp_ns, .field_name = field_name, .tags = tags }) orelse return error.OutOfMemory;
        var out: Result = undefined;
        try self.applyOwn(.ts_write, ns_id, measurement, v, if (timestamp_ns == 0) 1 else timestamp_ns, &out);
        if (out.refused() != null) return error.Refused;
        self.stats.points_inserted += 1;
    }

    /// Delete a standalone projection's series of `measurement` whose tags
    /// equal every predicate; returns how many went.
    pub fn deleteSeries(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, preds: []const TagPredicate) usize {
        var fbuf: [MAX_TAG_STRING]u8 = undefined;
        var pos: usize = 0;
        for (preds, 0..) |p, i| {
            const part = std.fmt.bufPrint(fbuf[pos..], "{s}{s}={s}", .{ if (i > 0) "," else "", p.key, p.value }) catch return 0;
            pos += part.len;
        }
        const before = self.read().store.family(ts_machine.Series.ID).count;
        var out: Result = undefined;
        self.applyOwn(.ts_delete, ns_id, measurement, fbuf[0..pos], 1, &out) catch return 0;
        return before - self.store.family(ts_machine.Series.ID).count;
    }

    pub fn deleteMeasurement(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8) usize {
        return self.deleteSeries(ns_id, measurement, &.{});
    }

    /// Remove a standalone projection's points of `measurement` older than
    /// `cutoff_ns`; returns how many went.
    pub fn applyRetention(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, cutoff_ns: u64) usize {
        // The entry carries an age from its stamp: a stamp of the cutoff
        // and an age of 0 trims exactly below the cutoff.
        var age: [8]u8 = undefined;
        std.mem.writeInt(u64, &age, 0, .little);
        const before = self.store.family(ts_machine.Points.ID).count;
        var out: Result = undefined;
        self.applyOwn(.ts_retention, ns_id, measurement, &age, cutoff_ns, &out) catch return 0;
        return before - self.store.family(ts_machine.Points.ID).count;
    }

    // ─── Reads ────────────────────────────────────────────────────────────

    pub const QueryResult = struct {
        points_in_buffer: usize,
    };

    /// A point's place in read order: its time, then its position
    /// (`ts_machine.positionOf`), which no two of a series' points share.
    pub const ReadAt = ts_machine.At;

    /// Which of a field's series a read takes.
    const Pick = union(enum) {
        /// The one with this tag hash.
        exact: u64,
        /// Those whose tags satisfy every predicate (all, when none).
        matching: []const TagPredicate,
    };

    /// One series' points from where a read starts, as the merge holds it.
    const Cursor = struct {
        it: ReadRange(ts_machine.Points),
        head: StoredPoint,
        tag_hash: u64,
        tags: []const u8,

        fn advance(self: *Cursor, max_ns: u64) error{Malformed}!bool {
            return self.step(0, max_ns);
        }

        /// The next point of the series, newest first or oldest, while it
        /// lies in `[min_ns, max_ns]`.
        fn step(self: *Cursor, min_ns: u64, max_ns: u64) error{Malformed}!bool {
            const p = (try self.it.next()) orelse return false;
            const at = ts_machine.pointAt(p.key);
            if (at.ns > max_ns or at.ns < min_ns) return false;
            self.head = .{ .timestamp_ns = at.ns, .field_value = p.value.value, .ual_index = at.ual, .tag_hash = self.tag_hash, .tags = self.tags };
            return true;
        }
    };

    fn cursorLess(_: void, a: *Cursor, b: *Cursor) std.math.Order {
        if (a.head.timestamp_ns != b.head.timestamp_ns) return std.math.order(a.head.timestamp_ns, b.head.timestamp_ns);
        return std.math.order(a.head.ual_index, b.head.ual_index);
    }

    fn cursorMore(_: void, a: *Cursor, b: *Cursor) std.math.Order {
        return cursorLess({}, b, a);
    }

    /// Which way a read goes through a field's points.
    pub const Order = enum(u8) { oldest_first, newest_first };

    /// Every point of the picked series in `[min_ns, max_ns]`, newest
    /// first and strictly before `before` when given, to `visit` until it
    /// says stop: `scan`, the other way.
    fn scanBack(self: *const TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, pick: Pick, before: ?ReadAt, min_ns: u64, max_ns: u64, ctx: anytype, comptime visit: fn (@TypeOf(ctx), StoredPoint) bool) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const rv = self.read();
        var cursors: std.ArrayListUnmanaged(*Cursor) = .empty;
        const field_prefix = try ts_machine.fieldPrefix(a, ns_id, measurement, field_name);
        // No position is maxInt, so (max_ns, maxInt) is past every point at
        // max_ns.
        const end: ReadAt = before orelse .{ .ns = max_ns, .ual = std.math.maxInt(u64) };
        var series_it = rv.range(ts_machine.Series, field_prefix);
        while (try series_it.next()) |s| {
            const name = ts_machine.seriesName(s.key);
            switch (pick) {
                .exact => |h| if (name.tag_hash != h) continue,
                .matching => |preds| if (!tagsMatch(s.value.tags, preds)) continue,
            }
            const c = try a.create(Cursor);
            c.* = .{ .it = rv.rangeBefore(ts_machine.Points, s.key, try ts_machine.pointKey(a, s.key, end.ns, end.ual)), .head = undefined, .tag_hash = name.tag_hash, .tags = s.value.tags };
            if (try c.step(min_ns, max_ns)) try cursors.append(a, c);
        }
        var heap: std.PriorityQueue(*Cursor, void, cursorMore) = .empty;
        try heap.pushSlice(a, cursors.items);
        while (heap.pop()) |c| {
            if (!visit(ctx, c.head)) break;
            if (try c.step(min_ns, max_ns)) try heap.push(a, c);
        }
    }

    /// Up to `out.len` points of a measurement field's tag-series whose
    /// tags satisfy `preds`, in `[min_ns, max_ns]`, in `order`, and past
    /// `after` in that order when given: one page of a read that goes on
    /// from its last point. (time, position) orders every point of a field
    /// across its series, so pages neither repeat nor skip a point that was
    /// there the whole read.
    pub fn readPage(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ns: u64, max_ns: u64, order: Order, after: ?ReadAt, out: []StoredPoint) !usize {
        self.stats.queries += 1;
        if (out.len == 0) return 0;
        var f: Fill = .{ .buf = out };
        const pick: Pick = .{ .matching = preds };
        switch (order) {
            .oldest_first => {
                const from: From = if (after) |at| (if (at.ns < min_ns) .{ .at_ns = min_ns } else .{ .after = at }) else .{ .at_ns = min_ns };
                try self.scan(ns_id, measurement, field_name, pick, from, max_ns, &f, Fill.take);
            },
            .newest_first => {
                const before: ?ReadAt = if (after) |at| (if (at.ns > max_ns) null else at) else null;
                try self.scanBack(ns_id, measurement, field_name, pick, before, min_ns, max_ns, &f, Fill.take);
            },
        }
        return f.n;
    }

    /// Where a read starts: at a time, or past a point.
    const From = union(enum) { at_ns: u64, after: ts_machine.At };

    /// Every point of the picked series from `from` through `max_ns`, in
    /// (time, position) order across them, to `visit` until it says stop.
    /// One seek per series, then a merge: a read of one series costs only
    /// that series' points.
    fn scan(self: *const TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, pick: Pick, from: From, max_ns: u64, ctx: anytype, comptime visit: fn (@TypeOf(ctx), StoredPoint) bool) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const rv = self.read();
        var cursors: std.ArrayListUnmanaged(*Cursor) = .empty;
        const field_prefix = try ts_machine.fieldPrefix(a, ns_id, measurement, field_name);
        var series_it = rv.range(ts_machine.Series, field_prefix);
        while (try series_it.next()) |s| {
            const name = ts_machine.seriesName(s.key);
            switch (pick) {
                .exact => |h| if (name.tag_hash != h) continue,
                .matching => |preds| if (!tagsMatch(s.value.tags, preds)) continue,
            }
            const c = try a.create(Cursor);
            c.* = .{ .it = undefined, .head = undefined, .tag_hash = name.tag_hash, .tags = s.value.tags };
            c.it = switch (from) {
                .at_ns => |t| if (t == 0) rv.range(ts_machine.Points, s.key) else rv.rangeAfter(ts_machine.Points, s.key, try ts_machine.pointKey(a, s.key, t - 1, std.math.maxInt(u64))),
                .after => |at| rv.rangeAfter(ts_machine.Points, s.key, try ts_machine.pointKey(a, s.key, at.ns, at.ual)),
            };
            if (try c.advance(max_ns)) try cursors.append(a, c);
        }
        var heap: std.PriorityQueue(*Cursor, void, cursorLess) = .empty;
        try heap.pushSlice(a, cursors.items);
        while (heap.pop()) |c| {
            if (!visit(ctx, c.head)) break;
            if (try c.advance(max_ns)) try heap.push(a, c);
        }
    }

    /// Every point of a measurement field's tag-series whose tags satisfy
    /// `preds`, in `[min_ts, max_ts]`, time-ordered, to `visit` until it
    /// says stop: however many there are.
    pub fn scanFiltered(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64, ctx: anytype, comptime visit: fn (@TypeOf(ctx), StoredPoint) bool) !void {
        self.stats.queries += 1;
        try self.scan(ns_id, measurement, field_name, .{ .matching = preds }, .{ .at_ns = min_ts }, max_ts, ctx, visit);
    }

    const Fill = struct {
        buf: []StoredPoint,
        n: usize = 0,

        fn take(self: *Fill, p: StoredPoint) bool {
            if (self.n == self.buf.len) return false;
            self.buf[self.n] = p;
            self.n += 1;
            return self.n < self.buf.len;
        }
    };

    fn fill(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, pick: Pick, from: From, max_ts: u64, point_buf: []StoredPoint) !QueryResult {
        self.stats.queries += 1;
        if (point_buf.len == 0) return .{ .points_in_buffer = 0 };
        var f: Fill = .{ .buf = point_buf };
        try self.scan(ns_id, measurement, field_name, pick, from, max_ts, &f, Fill.take);
        return .{ .points_in_buffer = f.n };
    }

    /// The earliest points of one series (`tag_hash`), or of every
    /// tag-series of a measurement's field (null), in `[min_ts, max_ts]`,
    /// time-ordered, up to `point_buf.len`. A point's tags are borrowed from
    /// the store, good until it next changes.
    pub fn queryRange(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, tag_hash: ?u64, min_ts: u64, max_ts: u64, point_buf: []StoredPoint) !QueryResult {
        const pick: Pick = if (tag_hash) |h| .{ .exact = h } else .{ .matching = &.{} };
        return self.fill(ns_id, measurement, field_name, pick, .{ .at_ns = min_ts }, max_ts, point_buf);
    }

    /// `queryRange` over every tag-series whose tags satisfy `preds`.
    /// Predicates constrain only the tags they name, so a partial filter
    /// matches a superset tag set. An empty list means every tag-series.
    pub fn queryRangeFiltered(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64, point_buf: []StoredPoint) !QueryResult {
        return self.fill(ns_id, measurement, field_name, .{ .matching = preds }, .{ .at_ns = min_ts }, max_ts, point_buf);
    }

    /// The earliest points after `after`, in (time, position) order, of
    /// one series (`tag_hash`) or every tag-series of a measurement's field
    /// (null): a reader going on from its last point neither skips nor
    /// repeats one, even among points sharing a time.
    pub fn readAfter(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, tag_hash: ?u64, after: ReadAt, through_ns: u64, out: []StoredPoint) !usize {
        const pick: Pick = if (tag_hash) |h| .{ .exact = h } else .{ .matching = &.{} };
        return (try self.fill(ns_id, measurement, field_name, pick, .{ .after = after }, through_ns, out)).points_in_buffer;
    }

    /// Whether `measurement` has a series of `field_name` whose tags satisfy
    /// every predicate, whether or not it has points in range.
    pub fn hasFieldSeries(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate) !bool {
        var kbuf: [4096]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&kbuf);
        const prefix = try ts_machine.fieldPrefix(fba.allocator(), ns_id, measurement, field_name);
        var it = self.read().range(ts_machine.Series, prefix);
        while (try it.next()) |s| if (tagsMatch(s.value.tags, preds)) return true;
        return false;
    }

    /// A measurement field's record: its newest time and lateness counts;
    /// null when it has no series.
    pub fn fieldRecord(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8) ?ts_machine.FieldValue {
        var kbuf: [4096]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&kbuf);
        const k = ts_machine.fieldPrefix(fba.allocator(), ns_id, measurement, field_name) catch return null;
        return self.read().get(ts_machine.Fields, k) catch null;
    }

    /// Distinct tag-series under one measurement+field, as canonical tag
    /// strings borrowed from the store. Backs per-series rollups and `group_by`.
    pub fn seriesTagsFor(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, out: [][]const u8) !usize {
        var kbuf: [4096]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&kbuf);
        var it = self.read().range(ts_machine.Series, try ts_machine.fieldPrefix(fba.allocator(), ns_id, measurement, field_name));
        var n: usize = 0;
        while (try it.next()) |s| {
            if (n >= out.len) break;
            out[n] = s.value.tags;
            n += 1;
        }
        return n;
    }

    // ─── Aggregations ──────────────────────────────────────────────────────

    const Acc = struct {
        n: u64 = 0,
        sum: f64 = 0,
        lo: f64 = std.math.floatMax(f64),
        hi: f64 = -std.math.floatMax(f64),

        fn add(self: *Acc, p: StoredPoint) bool {
            self.n += 1;
            self.sum += p.field_value;
            self.lo = @min(self.lo, p.field_value);
            self.hi = @max(self.hi, p.field_value);
            return true;
        }
    };

    /// Every matching point in the range, however many.
    fn aggregate(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !Acc {
        self.stats.queries += 1;
        var acc: Acc = .{};
        try self.scan(ns_id, measurement, field_name, .{ .matching = preds }, .{ .at_ns = min_ts }, max_ts, &acc, Acc.add);
        return acc;
    }

    pub fn avg(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !?f64 {
        const acc = try self.aggregate(ns_id, measurement, field_name, preds, min_ts, max_ts);
        return if (acc.n == 0) null else acc.sum / @as(f64, @floatFromInt(acc.n));
    }

    pub fn min(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !?f64 {
        const acc = try self.aggregate(ns_id, measurement, field_name, preds, min_ts, max_ts);
        return if (acc.n == 0) null else acc.lo;
    }

    pub fn max(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !?f64 {
        const acc = try self.aggregate(ns_id, measurement, field_name, preds, min_ts, max_ts);
        return if (acc.n == 0) null else acc.hi;
    }

    pub fn sum(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !?f64 {
        const acc = try self.aggregate(ns_id, measurement, field_name, preds, min_ts, max_ts);
        return if (acc.n == 0) null else acc.sum;
    }

    pub fn count(self: *TSProjection, ns_id: NamespaceId, measurement: []const u8, field_name: []const u8, preds: []const TagPredicate, min_ts: u64, max_ts: u64) !usize {
        return @intCast((try self.aggregate(ns_id, measurement, field_name, preds, min_ts, max_ts)).n);
    }

    // ─── Info ──────────────────────────────────────────────────────────────

    /// Series this shard holds, every namespace's.
    pub fn seriesCount(self: *const TSProjection) usize {
        return self.store.family(ts_machine.Series.ID).count;
    }

    /// Points this shard holds, and what the store keeps for them: the
    /// growth retention bounds.
    pub fn pointCount(self: *const TSProjection) u64 {
        return self.store.family(ts_machine.Points.ID).count;
    }

    pub fn pointBytes(self: *const TSProjection) u64 {
        return self.store.family(ts_machine.Points.ID).encoded_bytes;
    }

    /// Unique measurement names of a namespace, into `buf`, borrowed from
    /// the store: valid while it isn't changed.
    pub fn scanMeasurementNames(self: *const TSProjection, ns_id: NamespaceId, buf: [][]const u8) usize {
        var n: usize = 0;
        const id = ns_id.bytes();
        var it = self.read().range(ts_machine.Series, &id);
        while (it.next() catch null) |s| {
            if (n >= buf.len) break;
            const m = ts_machine.seriesName(s.key).measurement;
            // Series keys sort by measurement, so its series are together.
            if (n > 0 and std.mem.eql(u8, buf[n - 1], m)) continue;
            buf[n] = m;
            n += 1;
        }
        return n;
    }

    /// The most field names a summary lists; past it, `fields` stops
    /// counting and `fields_capped` is set.
    pub const MAX_SUMMARY_FIELDS = 256;

    pub const Summary = struct {
        /// Each field of each tag set is one series.
        series: u32 = 0,
        fields: u32 = 0,
        fields_capped: bool = false,
        points: u64 = 0,
        /// maxInt and 0 when there are no points.
        oldest_ns: u64 = std.math.maxInt(u64),
        newest_ns: u64 = 0,
    };

    /// A listed measurement's series, distinct fields and points.
    pub const ListCounts = struct { series: u32 = 0, fields: u32 = 0, points: u64 = 0 };

    /// `names`' counts into `out`, one per name, each from its own series
    /// records: no point is read.
    pub fn listCounts(self: *const TSProjection, ns_id: NamespaceId, names: []const []const u8, allocator: Allocator, out: []ListCounts) error{OutOfMemory}!void {
        std.debug.assert(out.len == names.len);
        for (names, out) |name, *c| {
            var field_buf: [MAX_SUMMARY_FIELDS][]const u8 = undefined;
            const s = self.summarizeWith(ns_id, name, &field_buf, allocator) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => Summary{},
            };
            c.* = .{ .series = s.series, .fields = s.fields, .points = s.points };
        }
    }

    /// One measurement's series, fields and points, from its series
    /// records: no point is read. Field names go into `field_buf`, at most
    /// `MAX_SUMMARY_FIELDS`, borrowed from the store.
    pub fn summarize(self: *const TSProjection, ns_id: NamespaceId, measurement: []const u8, field_buf: *[MAX_SUMMARY_FIELDS][]const u8) Summary {
        return self.summarizeWith(ns_id, measurement, field_buf, self.allocator) catch .{};
    }

    fn summarizeWith(self: *const TSProjection, ns_id: NamespaceId, measurement: []const u8, field_buf: *[MAX_SUMMARY_FIELDS][]const u8, allocator: Allocator) !Summary {
        var total: Summary = .{};
        const prefix = try ts_machine.measurementPrefix(allocator, ns_id, measurement);
        defer allocator.free(prefix);
        var it = self.read().range(ts_machine.Series, prefix);
        while (try it.next()) |s| {
            total.series += 1;
            const field = ts_machine.seriesName(s.key).field;
            // A field's series sort together.
            if (total.fields > 0 and std.mem.eql(u8, field_buf[total.fields - 1], field)) {} else if (total.fields < MAX_SUMMARY_FIELDS) {
                field_buf[total.fields] = field;
                total.fields += 1;
            } else total.fields_capped = true;
            total.points += s.value.points;
            if (s.value.points > 0) {
                total.oldest_ns = @min(total.oldest_ns, s.value.oldest_ns);
                total.newest_ns = @max(total.newest_ns, s.value.newest_ns);
            }
        }
        return total;
    }

    /// Unique measurement names of a namespace; the caller owns them.
    pub fn listMeasurements(self: *const TSProjection, ns_id: NamespaceId, allocator: Allocator) ![][]const u8 {
        var names: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer {
            for (names.items) |n| allocator.free(n);
            names.deinit(allocator);
        }
        const id = ns_id.bytes();
        var it = self.read().range(ts_machine.Series, &id);
        while (try it.next()) |s| {
            const m = ts_machine.seriesName(s.key).measurement;
            if (names.items.len > 0 and std.mem.eql(u8, names.items[names.items.len - 1], m)) continue;
            try names.append(allocator, try allocator.dupe(u8, m));
        }
        return names.toOwnedSlice(allocator);
    }

    /// Whether any series of `measurement` in the namespace matches.
    pub fn hasSeries(self: *const TSProjection, ns_id: NamespaceId, measurement: []const u8, preds: []const TagPredicate) bool {
        var kbuf: [4096]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&kbuf);
        const prefix = ts_machine.measurementPrefix(fba.allocator(), ns_id, measurement) catch return false;
        var it = self.read().range(ts_machine.Series, prefix);
        while (it.next() catch null) |s| if (tagsMatch(s.value.tags, preds)) return true;
        return false;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

fn applyTest(ts: *TSProjection, e: *const entry_mod.Entry) !void {
    var out = Result.start(e);
    return ts.applyEntry(e, &out);
}

test "ts: basic insert and query" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "cpu", "usage", 82.5, 1000, 1, "");
    try ts.insert(.of(16), "cpu", "usage", 75.0, 2000, 2, "");
    try ts.insert(.of(16), "cpu", "usage", 90.1, 3000, 3, "");

    try testing.expectEqual(@as(u64, 3), ts.stats.points_inserted);
    try testing.expectEqual(@as(usize, 1), ts.seriesCount());

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(.of(16), "cpu", "usage", null, 1000, 3000, &buf);
    try testing.expectEqual(@as(usize, 3), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 82.5), buf[0].field_value);
}

test "ts: range query filters by timestamp" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "mem", "used", 100.0, 1000, 1, "");
    try ts.insert(.of(16), "mem", "used", 200.0, 2000, 2, "");
    try ts.insert(.of(16), "mem", "used", 300.0, 3000, 3, "");
    try ts.insert(.of(16), "mem", "used", 400.0, 4000, 4, "");

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(.of(16), "mem", "used", null, 2000, 3000, &buf);
    try testing.expectEqual(@as(usize, 2), result.points_in_buffer);
    try testing.expectEqual(@as(f64, 200.0), buf[0].field_value);
    try testing.expectEqual(@as(f64, 300.0), buf[1].field_value);
}

test "ts: aggregation — avg" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "cpu", "usage", 80.0, 1000, 1, "");
    try ts.insert(.of(16), "cpu", "usage", 90.0, 2000, 2, "");
    try ts.insert(.of(16), "cpu", "usage", 100.0, 3000, 3, "");

    const average = (try ts.avg(.of(16), "cpu", "usage", &.{}, 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), average, 0.001);
}

test "ts: aggregation — min/max/sum" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "disk", "iops", 10.0, 1000, 1, "");
    try ts.insert(.of(16), "disk", "iops", 50.0, 2000, 2, "");
    try ts.insert(.of(16), "disk", "iops", 30.0, 3000, 3, "");

    const min_val = (try ts.min(.of(16), "disk", "iops", &.{}, 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 10.0), min_val, 0.001);

    const max_val = (try ts.max(.of(16), "disk", "iops", &.{}, 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 50.0), max_val, 0.001);

    const sum_val = (try ts.sum(.of(16), "disk", "iops", &.{}, 1000, 3000)).?;
    try testing.expectApproxEqAbs(@as(f64, 90.0), sum_val, 0.001);
}

test "ts: aggregation — count" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "net", "rx", 1.0, 1000, 1, "");
    try ts.insert(.of(16), "net", "rx", 2.0, 2000, 2, "");
    try ts.insert(.of(16), "net", "rx", 3.0, 3000, 3, "");

    const c = try ts.count(.of(16), "net", "rx", &.{}, 1500, 2500);
    try testing.expectEqual(@as(usize, 1), c); // only ts=2000 in range
}

test "ts: every point a series holds reads back, in order, however many" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    const n = 5000;
    // Written newest first: a read still gives them oldest first.
    for (0..n) |i| try ts.insert(.of(16), "cpu", "usage", @floatFromInt(i), 1000 * (n - i), 0, "");
    const buf = try testing.allocator.alloc(StoredPoint, n + 1);
    defer testing.allocator.free(buf);
    const result = try ts.queryRange(.of(16), "cpu", "usage", null, 0, std.math.maxInt(u64), buf);
    try testing.expectEqual(@as(usize, n), result.points_in_buffer);
    for (buf[1..n], buf[0 .. n - 1]) |p, prev| try testing.expect(p.timestamp_ns > prev.timestamp_ns);
    try testing.expectEqual(@as(usize, n), try ts.count(.of(16), "cpu", "usage", &.{}, 0, std.math.maxInt(u64)));
}

test "ts: multiple series" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(16), "cpu", "usage", 80.0, 1000, 1, "");
    try ts.insert(.of(16), "cpu", "idle", 20.0, 1000, 2, "");
    try ts.insert(.of(16), "mem", "used", 4096.0, 1000, 3, "");

    try testing.expectEqual(@as(usize, 3), ts.seriesCount());
}

test "ts: query nonexistent series" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    var buf: [10]StoredPoint = undefined;
    const result = try ts.queryRange(.of(16), "nonexistent", "field", null, 0, 9999, &buf);
    try testing.expectEqual(@as(usize, 0), result.points_in_buffer);
}

test "ts: a write's answer carries the longest measurement name an entry can hold, whole" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    const name = @as([@import("../storage/persistence.zig").MAX_ENTRY_KEY]u8, @splat('m'));
    const val_bytes: [8]u8 = @bitCast(@as(u64, @bitCast(@as(f64, 1.5))));
    var payload_buf: [name.len + 128]u8 = undefined;
    const e = entry_mod.buildCommandEntry(.ts_write, 0, 1, 1, 1000, .of(16), &name, &val_bytes, &payload_buf).?;
    var out = Result.start(&e);
    try ts.applyEntry(&e, &out);
    try testing.expect(!out.overflowed);
    var r = out.reader();
    try testing.expectEqualSlices(u8, &name, r.slice().?);
    try testing.expectEqual(@as(?u64, 1000), r.int(u64));
}

test "ts: apply entry for ts_write" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    // Build a ts_write UAL entry with key="cpu" and value=f64 bytes
    const val: f64 = 82.5;
    const val_bytes: [8]u8 = @bitCast(@as(u64, @bitCast(val)));
    var payload_buf: [128]u8 = undefined;
    const ual_entry = entry_mod.buildCommandEntry(.ts_write, 0, 1, 1, 1000, .of(16), "cpu", &val_bytes, &payload_buf) orelse unreachable;
    try applyTest(&ts, &ual_entry);

    // A bare value: the field "value", at the entry's stamp.
    var buf: [2]StoredPoint = undefined;
    const r = try ts.queryRange(.of(16), "cpu", "value", null, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 1), r.points_in_buffer);
    try testing.expectEqual(val, buf[0].field_value);
    try testing.expectEqual(@as(u64, 1000), buf[0].timestamp_ns);
}

// ─── Tags as a series dimension (issue #24) ────────────────────────────────

test "ts: tags are a series dimension, not collapsed into one buffer" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    const web1 = canonicalTagHash("host=web-01");
    const web2 = canonicalTagHash("host=web-02");
    try testing.expect(web1 != web2);

    try ts.insert(.of(17), "cpu", "value", 10.0, 1000, 1, "host=web-01");
    try ts.insert(.of(17), "cpu", "value", 20.0, 2000, 2, "host=web-02");
    try ts.insert(.of(17), "cpu", "value", 30.0, 3000, 3, "host=web-01");

    // Same measurement+field, two tag sets → two distinct series.
    try testing.expectEqual(@as(usize, 2), ts.seriesCount());

    // An exact tag query sees only that tag's points.
    var buf: [16]StoredPoint = undefined;
    const r1 = try ts.queryRange(.of(17), "cpu", "value", web1, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 2), r1.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 10.0), buf[0].field_value, 0.001);
    try testing.expectApproxEqAbs(@as(f64, 30.0), buf[1].field_value, 0.001);

    const r2 = try ts.queryRange(.of(17), "cpu", "value", web2, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 1), r2.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 20.0), buf[0].field_value, 0.001);

    // A tag set that was never written matches nothing.
    const r3 = try ts.queryRange(.of(17), "cpu", "value", canonicalTagHash("host=nope"), 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 0), r3.points_in_buffer);
}

test "ts: untagged query still sees tagged writes (no silent regression)" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(17), "cpu", "value", 10.0, 3000, 1, "host=a");
    try ts.insert(.of(17), "cpu", "value", 20.0, 1000, 2, "host=b");
    try ts.insert(.of(17), "cpu", "value", 30.0, 2000, 3, "");

    // `null` = any tags: every tag-series is merged...
    var buf: [16]StoredPoint = undefined;
    const r = try ts.queryRange(.of(17), "cpu", "value", null, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 3), r.points_in_buffer);

    // ...and the merged result is time-ordered, which window/rate rely on.
    try testing.expectEqual(@as(u64, 1000), buf[0].timestamp_ns);
    try testing.expectEqual(@as(u64, 2000), buf[1].timestamp_ns);
    try testing.expectEqual(@as(u64, 3000), buf[2].timestamp_ns);

    // Aggregations follow the same rule: (10+20+30)/3 across all tag-series.
    const a = try ts.avg(.of(17), "cpu", "value", &.{}, 0, std.math.maxInt(u64));
    try testing.expectApproxEqAbs(@as(f64, 20.0), a.?, 0.001);
    const a_b = try ts.avg(.of(17), "cpu", "value", &[_]TagPredicate{.{ .key = "host", .op = .eq, .value = "b" }}, 0, std.math.maxInt(u64));
    try testing.expectApproxEqAbs(@as(f64, 20.0), a_b.?, 0.001);
}

test "ts: tag hash is canonical — order does not fragment a series" {
    // Hashing the raw user string made `host=a,env=b` and `env=b,host=a` two
    // different series; sorting the pairs first makes them one.
    try testing.expectEqual(canonicalTagHash("host=a,env=b"), canonicalTagHash("env=b,host=a"));
    try testing.expectEqual(canonicalTagHash("host=a, env=b"), canonicalTagHash("env=b,host=a"));
    try testing.expect(canonicalTagHash("host=a") != canonicalTagHash("host=b"));
    try testing.expectEqual(@as(u64, 0), canonicalTagHash(""));

    // Pairs and strings must agree, or FloQL filters would never match a write.
    var scratch: [256]u8 = undefined;
    const pairs = [_]TagPair{ .{ .key = "host", .value = "a" }, .{ .key = "env", .value = "b" } };
    try testing.expectEqual(canonicalTagHash("env=b,host=a"), canonicalTagHashPairs(&pairs, &scratch));

    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    try ts.insert(.of(17), "cpu", "value", 1.0, 1000, 1, "host=a,env=b");
    try ts.insert(.of(17), "cpu", "value", 2.0, 2000, 2, "env=b,host=a");
    try testing.expectEqual(@as(usize, 1), ts.seriesCount());
}

test "ts: a series key gives back its parts, and a field's prefix takes no longer field's series" {
    const a = testing.allocator;
    const key = try ts_machine.seriesKey(a, .of(0xAABBCCDD), "cpu", "user", 0x1122334455667788);
    defer a.free(key);
    // The prefix is the id big-endian, as every key below the edge leads.
    try testing.expectEqualSlices(u8, &.{ 0xAA, 0xBB, 0xCC, 0xDD }, key[0..4]);
    const name = ts_machine.seriesName(key);
    try testing.expectEqualStrings("cpu", name.measurement);
    try testing.expectEqualStrings("user", name.field);
    try testing.expectEqual(@as(u64, 0x1122334455667788), name.tag_hash);
    const prefix = try ts_machine.fieldPrefix(a, .of(0xAABBCCDD), "cpu", "user");
    defer a.free(prefix);
    const other = try ts_machine.seriesKey(a, .of(0xAABBCCDD), "cpu", "username", 0);
    defer a.free(other);
    try testing.expect(std.mem.startsWith(u8, key, prefix));
    try testing.expect(!std.mem.startsWith(u8, other, prefix));
}

test "ts: TsWriteValue round-trips field, timestamp and tags" {
    // All three were silently dropped on replay before #24.
    var buf: [256]u8 = undefined;
    const encoded = TsWriteValue.encode(&buf, .{
        .value = 42.5,
        .timestamp_ns = 123_456_789,
        .field_name = "user",
        .tags = "host=web-01,env=prod",
    }).?;

    const decoded = TsWriteValue.decode(encoded, 999).?;
    try testing.expectApproxEqAbs(@as(f64, 42.5), decoded.value, 0.001);
    try testing.expectEqual(@as(u64, 123_456_789), decoded.timestamp_ns);
    try testing.expectEqualStrings("user", decoded.field_name);
    try testing.expectEqualStrings("host=web-01,env=prod", decoded.tags);

    // A bare 8-byte value decodes defensively against the header timestamp.
    var short: [8]u8 = undefined;
    std.mem.writeInt(u64, &short, @bitCast(@as(f64, 7.0)), .little);
    const legacy = TsWriteValue.decode(&short, 555).?;
    try testing.expectApproxEqAbs(@as(f64, 7.0), legacy.value, 0.001);
    try testing.expectEqual(@as(u64, 555), legacy.timestamp_ns);
    try testing.expectEqualStrings("value", legacy.field_name);
    try testing.expectEqualStrings("", legacy.tags);
}

// ─── Tag dictionary: partial / negated / glob matching (issue #24 part 2b) ──

test "ts: partial tag filter matches a superset tag set" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(17), "cpu", "value", 10.0, 1000, 1, "host=web-01,env=prod");
    try ts.insert(.of(17), "cpu", "value", 20.0, 2000, 2, "host=web-02,env=prod");
    try ts.insert(.of(17), "cpu", "value", 30.0, 3000, 3, "host=web-01,env=staging");

    var buf: [16]StoredPoint = undefined;

    // The whole point of 2b: a filter naming ONE tag selects every series whose
    // tag set contains it. Under the 2a tag-hash lookup this matched nothing.
    const host1 = [_]TagPredicate{.{ .key = "host", .op = .eq, .value = "web-01" }};
    const r1 = try ts.queryRangeFiltered(.of(17), "cpu", "value", &host1, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 2), r1.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 10.0), buf[0].field_value, 0.001);
    try testing.expectApproxEqAbs(@as(f64, 30.0), buf[1].field_value, 0.001);

    // Two predicates AND together.
    const both = [_]TagPredicate{
        .{ .key = "host", .op = .eq, .value = "web-01" },
        .{ .key = "env", .op = .eq, .value = "prod" },
    };
    const r2 = try ts.queryRangeFiltered(.of(17), "cpu", "value", &both, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 1), r2.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 10.0), buf[0].field_value, 0.001);

    // No predicates = every tag-series.
    const r3 = try ts.queryRangeFiltered(.of(17), "cpu", "value", &.{}, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 3), r3.points_in_buffer);
}

test "ts: negated and glob tag predicates" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(17), "m", "value", 1.0, 1000, 1, "region=us-east");
    try ts.insert(.of(17), "m", "value", 2.0, 2000, 2, "region=us-west");
    try ts.insert(.of(17), "m", "value", 3.0, 3000, 3, "region=eu-west");
    try ts.insert(.of(17), "m", "value", 4.0, 4000, 4, "");

    var buf: [16]StoredPoint = undefined;

    // `!=` — and an absent tag satisfies a negation, so the untagged point matches.
    const neq = [_]TagPredicate{.{ .key = "region", .op = .neq, .value = "us-east" }};
    const r1 = try ts.queryRangeFiltered(.of(17), "m", "value", &neq, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 3), r1.points_in_buffer);

    // Glob `us-*`.
    const glob = [_]TagPredicate{.{ .key = "region", .op = .glob, .value = "us-*" }};
    const r2 = try ts.queryRangeFiltered(.of(17), "m", "value", &glob, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 2), r2.points_in_buffer);

    // Negated glob `*-west` excludes us-west and eu-west, keeps us-east + untagged.
    const nglob = [_]TagPredicate{.{ .key = "region", .op = .nglob, .value = "*-west" }};
    const r3 = try ts.queryRangeFiltered(.of(17), "m", "value", &nglob, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 2), r3.points_in_buffer);
}

test "ts: glob matcher" {
    try testing.expect(globMatch("web-*", "web-01"));
    try testing.expect(globMatch("*-west", "us-west"));
    try testing.expect(globMatch("web-0?", "web-01"));
    try testing.expect(globMatch("*", "anything"));
    try testing.expect(globMatch("exact", "exact"));
    try testing.expect(!globMatch("web-*", "db-01"));
    try testing.expect(!globMatch("web-0?", "web-012"));
    try testing.expect(!globMatch("exact", "exac"));
    // A `*` may match nothing at all.
    try testing.expect(globMatch("a*b", "ab"));
}

test "ts: tag dictionary resolves a series' tags, and a tag filter matches by them" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    // Written unsorted — the series keeps the canonical (sorted) spelling.
    try ts.insert(.of(17), "cpu", "value", 10.0, 1000, 1, "host=web-01,env=prod");
    // Another namespace has no such series.
    var none: [8][]const u8 = undefined;
    try testing.expectEqual(@as(usize, 0), try ts.seriesTagsFor(.of(18), "cpu", "value", &none));

    var names: [8][]const u8 = undefined;
    const n = try ts.seriesTagsFor(.of(17), "cpu", "value", &names);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqualStrings("env=prod,host=web-01", names[0]);

    var buf: [8]StoredPoint = undefined;
    const preds = [_]TagPredicate{.{ .key = "host", .op = .eq, .value = "web-01" }};
    const r = try ts.queryRangeFiltered(.of(17), "cpu", "value", &preds, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 1), r.points_in_buffer);
    try testing.expectEqualStrings("env=prod,host=web-01", buf[0].tags);
}

test "ts: replay reconstructs the tagged series exactly" {
    // The write path persists TsWriteValue; applyEntry must rebuild the same
    // series, tags included. 2a added the codec but never wired the encode
    // side, so replay still lost field/timestamp/tags.
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    var val_buf: [256]u8 = undefined;
    const encoded = TsWriteValue.encode(&val_buf, .{
        .value = 55.0,
        .timestamp_ns = 7_000_000_000,
        .field_name = "user",
        .tags = "host=web-01,env=prod",
    }).?;

    var payload_buf: [512]u8 = undefined;
    const entry = entry_mod.buildCommandEntry(.ts_write, 0, 1, 1, 123, .of(17), "cpu", encoded, &payload_buf).?;
    try applyTest(&ts, &entry);

    // Field, timestamp and tags all survive, and the point is addressable by
    // a partial tag filter.
    var buf: [8]StoredPoint = undefined;
    const preds = [_]TagPredicate{.{ .key = "env", .op = .eq, .value = "prod" }};
    const r = try ts.queryRangeFiltered(.of(17), "cpu", "user", &preds, 0, std.math.maxInt(u64), &buf);
    try testing.expectEqual(@as(usize, 1), r.points_in_buffer);
    try testing.expectApproxEqAbs(@as(f64, 55.0), buf[0].field_value, 0.001);
    try testing.expectEqual(@as(u64, 7_000_000_000), buf[0].timestamp_ns);
}

test "ts: retention evicts only its own measurement in its own namespace" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();

    try ts.insert(.of(17), "a", "value", 1.0, 1000, 1, "");
    try ts.insert(.of(17), "a", "value", 2.0, 5000, 2, "");
    try ts.insert(.of(17), "b", "value", 3.0, 1000, 3, "");
    try ts.insert(.of(18), "a", "value", 4.0, 1000, 4, "");

    try testing.expectEqual(@as(usize, 1), ts.applyRetention(.of(17), "a", 2000));

    var buf: [10]StoredPoint = undefined;
    try testing.expectEqual(@as(usize, 1), (try ts.queryRange(.of(17), "a", "value", null, 0, 10000, &buf)).points_in_buffer);
    try testing.expectEqual(@as(f64, 2.0), buf[0].field_value);
    try testing.expectEqual(@as(usize, 1), (try ts.queryRange(.of(17), "b", "value", null, 0, 10000, &buf)).points_in_buffer);
    try testing.expectEqual(@as(usize, 1), (try ts.queryRange(.of(18), "a", "value", null, 0, 10000, &buf)).points_in_buffer);
}

test "ts: a retention entry cuts at its stamp less its duration, not at the host's clock" {
    const s_ns = std.time.ns_per_s;
    const host_ns: u64 = @intCast(@import("stdx").time.nanoTimestamp());
    // One stamp far behind the host's clock and one a day ahead: a cut by
    // the host's clock removes all three points or none.
    for ([_]u64{ 1_000 * s_ns, host_ns + std.time.ns_per_day }) |stamp| {
        var ts = TSProjection.init(testing.allocator);
        defer ts.deinit();
        for ([_]u64{ 10, 5, 1 }, 1..) |age_s, i| try ts.insert(.of(17), "m", "value", 1.0, stamp - age_s * s_ns, i, "");

        var age: [8]u8 = undefined;
        var pbuf: [64]u8 = undefined;
        // Older than 3 s at the stamp: the 10 s and 5 s old points.
        std.mem.writeInt(u64, &age, 3_000, .little);
        try applyTest(&ts, &entry_mod.buildCommandEntry(.ts_retention, 0, 1, 10, stamp, .of(17), "m", &age, &pbuf).?);
        var buf: [10]StoredPoint = undefined;
        try testing.expectEqual(@as(usize, 1), (try ts.queryRange(.of(17), "m", "value", null, 0, std.math.maxInt(u64), &buf)).points_in_buffer);

        // A duration past the stamp trims nothing, never everything.
        std.mem.writeInt(u64, &age, stamp / std.time.ns_per_ms + 1, .little);
        try applyTest(&ts, &entry_mod.buildCommandEntry(.ts_retention, 0, 1, 11, stamp, .of(17), "m", &age, &pbuf).?);
        try testing.expectEqual(@as(usize, 1), (try ts.queryRange(.of(17), "m", "value", null, 0, std.math.maxInt(u64), &buf)).points_in_buffer);
    }
}

test "ts: retention trims only its own measurement, and a series it empties goes" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    try ts.insert(.of(17), "a", "value", 1.0, 1000, 1, "");
    try ts.insert(.of(17), "a", "value", 2.0, 1500, 2, "");
    try ts.insert(.of(17), "b", "value", 3.0, 1000, 3, "");
    try ts.insert(.of(17), "b", "value", 4.0, 1500, 4, "");
    var buf: [10]StoredPoint = undefined;
    try testing.expectEqual(@as(usize, 2), ts.applyRetention(.of(17), "a", 2000));
    try testing.expectEqual(@as(usize, 0), (try ts.queryRange(.of(17), "a", "value", null, 0, 10000, &buf)).points_in_buffer);
    try testing.expectEqual(@as(usize, 2), (try ts.queryRange(.of(17), "b", "value", null, 0, 10000, &buf)).points_in_buffer);
    try testing.expectEqual(@as(usize, 1), ts.seriesCount());
    var fields: [TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    try testing.expectEqual(@as(u32, 0), ts.summarize(.of(17), "a", &fields).series);
    // One it only trims stays, from its oldest kept point.
    try testing.expectEqual(@as(usize, 1), ts.applyRetention(.of(17), "b", 1200));
    const b = ts.summarize(.of(17), "b", &fields);
    try testing.expectEqual(@as(u32, 1), b.series);
    try testing.expectEqual(@as(u64, 1), b.points);
    try testing.expectEqual(@as(u64, 1500), b.oldest_ns);
}

test "ts: a tag filter is parsed strictly and canonically" {
    var canon: [MAX_TAG_STRING]u8 = undefined;
    var preds: [MAX_TAGS]TagPredicate = undefined;
    const f = parseTagFilter(" site=x , sensor=A1", &canon, &preds).ok;
    try testing.expectEqualStrings("sensor=A1,site=x", f.canonical);
    try testing.expectEqual(@as(usize, 2), f.preds.len);
    try testing.expectEqualStrings("sensor", f.preds[0].key);
    try testing.expectEqualStrings("A1", f.preds[0].value);
    try testing.expectEqual(@as(usize, 0), parseTagFilter("", &canon, &preds).ok.preds.len);

    for ([_][]const u8{ "sensor", "=A1", "sensor=", "site=x,sensor" }) |bad| {
        try testing.expect(parseTagFilter(bad, &canon, &preds) == .not_pair);
    }
    try testing.expectEqualStrings("sensor", parseTagFilter("site=x,sensor", &canon, &preds).not_pair);

    var many: [MAX_TAGS * 4 + 4]u8 = undefined;
    var n: usize = 0;
    for (0..MAX_TAGS + 1) |i| n += (std.fmt.bufPrint(many[n..], "{c}=v,", .{@as(u8, @intCast('A' + i))}) catch unreachable).len;
    try testing.expect(parseTagFilter(many[0..n], &canon, &preds) == .too_many);
    const long = @as([(MAX_TAG_STRING + 1)]u8, @splat('a'));
    try testing.expect(parseTagFilter(&long, &canon, &preds) == .too_long);
}

test "ts: a delete with a tag filter removes only the series whose tags include it" {
    {
        var ts = TSProjection.init(testing.allocator);
        defer ts.deinit();
        try ts.insert(.of(17), "temp", "value", 1.0, 1000, 1, "sensor=A1,site=x");
        try ts.insert(.of(17), "temp", "value", 2.0, 1000, 2, "sensor=B2,site=x");
        try ts.insert(.of(17), "temp", "value", 3.0, 1000, 3, "");
        try ts.insert(.of(18), "temp", "value", 4.0, 1000, 4, "sensor=A1,site=x");

        var canon: [MAX_TAG_STRING]u8 = undefined;
        var preds: [MAX_TAGS]TagPredicate = undefined;
        const a1 = parseTagFilter("sensor=A1", &canon, &preds).ok.preds;
        try testing.expect(ts.hasSeries(.of(17), "temp", a1));
        try testing.expectEqual(@as(usize, 1), ts.deleteSeries(.of(17), "temp", a1));
        try testing.expect(!ts.hasSeries(.of(17), "temp", a1));
        // The other series, the untagged one, and the same series in
        // another namespace remain.
        var canon2: [MAX_TAG_STRING]u8 = undefined;
        var preds2: [MAX_TAGS]TagPredicate = undefined;
        try testing.expect(ts.hasSeries(.of(17), "temp", parseTagFilter("sensor=B2", &canon2, &preds2).ok.preds));
        try testing.expect(ts.hasSeries(.of(18), "temp", a1));
        var buf: [10]StoredPoint = undefined;
        const untagged = try ts.queryRange(.of(17), "temp", "value", 0, 0, 10000, &buf);
        try testing.expectEqual(@as(usize, 1), untagged.points_in_buffer);
    }
}

test "ts: a tag-filtered delete is refused at apply when no series matches, and runs once one is written ahead of it" {
    var vals: [1][256]u8 = undefined;
    var payloads: [3][512]u8 = undefined;
    const del = entry_mod.buildCommandEntry(.ts_delete, 0, 1, 1, 100, .of(17), "temp", "sensor=A1", &payloads[0]).?;
    const write = entry_mod.buildCommandEntry(.ts_write, 0, 1, 2, 200, .of(17), "temp", TsWriteValue.encode(&vals[0], .{ .value = 1.0, .timestamp_ns = 1000, .field_name = "value", .tags = "sensor=A1" }).?, &payloads[1]).?;
    const del_again = entry_mod.buildCommandEntry(.ts_delete, 0, 1, 3, 300, .of(17), "temp", "sensor=A1", &payloads[2]).?;

    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    var out = Result.start(&del);
    try ts.applyEntry(&del, &out);
    const refused = out.refused().?;
    try testing.expect(refused.status == .not_found);
    try testing.expectEqualStrings("no series of 'temp' has tags sensor=A1; nothing deleted", refused.refusal.message);

    try applyTest(&ts, &write);
    out = Result.start(&del_again);
    try ts.applyEntry(&del_again, &out);
    try testing.expect(out.refused() == null);
    try testing.expectEqual(@as(usize, 0), ts.seriesCount());
}

test "ts: the same tags and measurement in two namespaces stay apart, live and replayed" {
    const tags = "sensor=A1,site=x";
    var vals: [2][256]u8 = undefined;
    var payloads: [3][512]u8 = undefined;
    const entries = [_]entry_mod.Entry{
        entry_mod.buildCommandEntry(.ts_write, 0, 1, 1, 100, .of(17), "temp", TsWriteValue.encode(&vals[0], .{ .value = 1.0, .timestamp_ns = 1000, .field_name = "value", .tags = tags }).?, &payloads[0]).?,
        entry_mod.buildCommandEntry(.ts_write, 0, 1, 2, 200, .of(18), "temp", TsWriteValue.encode(&vals[1], .{ .value = 2.0, .timestamp_ns = 1000, .field_name = "value", .tags = tags }).?, &payloads[1]).?,
        entry_mod.buildCommandEntry(.ts_delete, 0, 1, 3, 300, .of(17), "temp", "sensor=A1", &payloads[2]).?,
    };
    const a1 = [_]TagPredicate{.{ .key = "sensor", .op = .eq, .value = "A1" }};

    const check = struct {
        /// The namespace's one point, or none, as a tag-filtered query sees it.
        fn point(ts: *TSProjection, ns: NamespaceId, want: ?f64) !void {
            var buf: [4]StoredPoint = undefined;
            const r = try ts.queryRangeFiltered(ns, "temp", "value", &a1, 0, std.math.maxInt(u64), &buf);
            if (want) |v| {
                try testing.expectEqual(@as(usize, 1), r.points_in_buffer);
                try testing.expectEqual(v, buf[0].field_value);
                try testing.expectEqualStrings(tags, buf[0].tags);
            } else try testing.expectEqual(@as(usize, 0), r.points_in_buffer);
        }
    };

    // Live: each entry applied in turn.
    var live = TSProjection.init(testing.allocator);
    defer live.deinit();
    try applyTest(&live, &entries[0]);
    try applyTest(&live, &entries[1]);
    try testing.expectEqual(@as(usize, 2), live.seriesCount());
    try check.point(&live, .of(17), 1.0);
    try check.point(&live, .of(18), 2.0);
    try applyTest(&live, &entries[2]);
    try check.point(&live, .of(17), null);
    try check.point(&live, .of(18), 2.0);
    try testing.expect(!live.hasSeries(.of(17), "temp", &.{}));

    // Replayed: the whole log into a fresh projection ends the same.
    var replayed = TSProjection.init(testing.allocator);
    defer replayed.deinit();
    for (&entries) |*e| try applyTest(&replayed, e);
    try check.point(&replayed, .of(17), null);
    try check.point(&replayed, .of(18), 2.0);
    try testing.expectEqual(@as(usize, 1), replayed.seriesCount());
}

test "ts: a list page's counts come from one pass, each name's its own" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    for ([_]u64{ 1000, 2000, 3000 }) |t| try ts.insert(.of(16), "cpu", "usage", 1, t, t, "host=a");
    try ts.insert(.of(16), "cpu", "usage", 1, 500, 4, "host=b");
    try ts.insert(.of(16), "cpu", "idle", 1, 4000, 5, "host=a");
    try ts.insert(.of(16), "mem", "used", 1, 9000, 6, "");
    try ts.insert(.of(23), "cpu", "usage", 1, 9000, 7, "");

    const names = [_][]const u8{ "mem", "cpu", "disk" };
    var out: [names.len]TSProjection.ListCounts = undefined;
    try ts.listCounts(.of(16), &names, testing.allocator, &out);
    try testing.expectEqual(TSProjection.ListCounts{ .series = 1, .fields = 1, .points = 1 }, out[0]);
    // Its series agree with the summary's.
    var fields: [TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    const sum = ts.summarize(.of(16), "cpu", &fields);
    try testing.expectEqual(TSProjection.ListCounts{ .series = sum.series, .fields = sum.fields, .points = sum.points }, out[1]);
    try testing.expectEqual(TSProjection.ListCounts{}, out[2]);
}

test "ts: a measurement's summary counts its series, fields and points" {
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    for ([_]u64{ 1000, 2000, 3000 }) |t| try ts.insert(.of(16), "cpu", "usage", 1, t, t, "host=a");
    try ts.insert(.of(16), "cpu", "usage", 1, 500, 4, "host=b");
    try ts.insert(.of(16), "cpu", "idle", 1, 4000, 5, "host=a");
    // Another measurement, and the same one in another namespace.
    try ts.insert(.of(16), "mem", "used", 1, 9000, 6, "");
    try ts.insert(.of(23), "cpu", "usage", 1, 9000, 7, "");

    var fields: [TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    const s = ts.summarize(.of(16), "cpu", &fields);
    try testing.expectEqual(@as(u32, 3), s.series);
    try testing.expectEqual(@as(u32, 2), s.fields);
    try testing.expect(!s.fields_capped);
    try testing.expectEqual(@as(u64, 5), s.points);
    try testing.expectEqual(@as(u64, 500), s.oldest_ns);
    try testing.expectEqual(@as(u64, 4000), s.newest_ns);
    const none = ts.summarize(.of(16), "disk", &fields);
    try testing.expectEqual(@as(u64, 0), none.points);
    try testing.expectEqual(@as(u32, 0), none.series);
}

test "ts: a delete or retention over thousands of series applies in one linear pass" {
    // Ranging between staged deletes rescans them all: 5000 series of 20
    // points took over 30 s in Debug that way. Timed on the allocator a
    // server runs with: the leak-checking one spends most of it on frees.
    const series = 5000;
    const per = 20;
    const stdx_time = @import("stdx").time;
    var fields: [TSProjection.MAX_SUMMARY_FIELDS][]const u8 = undefined;
    inline for (.{ "delete", "retention" }) |how| {
        var ts = TSProjection.init(std.heap.smp_allocator);
        defer ts.deinit();
        var tags: [32]u8 = undefined;
        for (0..series) |s| for (0..per) |p|
            try ts.insert(.of(17), "m", "value", 1, 1000 + p, 0, try std.fmt.bufPrint(&tags, "host=h{d}", .{s}));
        try testing.expectEqual(@as(usize, series), ts.seriesCount());
        const started = stdx_time.milliTimestamp();
        if (comptime std.mem.eql(u8, how, "delete")) {
            try testing.expectEqual(@as(usize, series), ts.deleteMeasurement(.of(17), "m"));
        } else {
            try testing.expectEqual(@as(usize, series * per), ts.applyRetention(.of(17), "m", 1000 + per));
        }
        const took = stdx_time.milliTimestamp() - started;
        try testing.expectEqual(@as(usize, 0), ts.seriesCount());
        try testing.expectEqual(@as(u32, 0), ts.summarize(.of(17), "m", &fields).series);
        try testing.expectEqual(@as(usize, 0), ts.store.family(ts_machine.Points.ID).count);
        if (took > 5000) {
            std.debug.print("ts {s} of {d} points took {d} ms\n", .{ how, series * per, took });
            return error.TooSlow;
        }
    }
}

test "ts: a newest-first read is an oldest-first one reversed, and pages of any size walk every point once, either way" {
    var prng = std.Random.DefaultPrng.init(0x7E57);
    const rand = prng.random();
    var ts = TSProjection.init(testing.allocator);
    defer ts.deinit();
    const n = 600;
    var tags: [16]u8 = undefined;
    // Five hosts over 60 times: many points share a time, across series
    // and within one.
    for (0..n) |i| try ts.insert(.of(17), "m", "value", @floatFromInt(i), 1 + rand.uintLessThan(u64, 60), 0, try std.fmt.bufPrint(&tags, "host=h{d}", .{rand.uintLessThan(u8, 5)}));
    var fwd: [n + 1]StoredPoint = undefined;
    var back: [n + 1]StoredPoint = undefined;
    for (0..20) |_| {
        const lo = rand.uintLessThan(u64, 70);
        const hi = lo + rand.uintLessThan(u64, 70);
        const nf = try ts.readPage(.of(17), "m", "value", &.{}, lo, hi, .oldest_first, null, &fwd);
        const nb = try ts.readPage(.of(17), "m", "value", &.{}, lo, hi, .newest_first, null, &back);
        try testing.expectEqual(nf, nb);
        for (fwd[0..nf], 0..) |p, i| {
            const q = back[nb - 1 - i];
            try testing.expectEqual(p.timestamp_ns, q.timestamp_ns);
            try testing.expectEqual(p.ual_index, q.ual_index);
            try testing.expect(p.timestamp_ns >= lo and p.timestamp_ns <= hi);
        }
        // Paged by a random size, each way: the same points in the same
        // order, each once.
        inline for (.{ TSProjection.Order.oldest_first, TSProjection.Order.newest_first }) |order| {
            const whole = if (order == .oldest_first) fwd[0..nf] else back[0..nb];
            var after: ?TSProjection.ReadAt = null;
            var got: usize = 0;
            const size = 1 + rand.uintLessThan(usize, 40);
            var page: [41]StoredPoint = undefined;
            while (true) {
                const k = try ts.readPage(.of(17), "m", "value", &.{}, lo, hi, order, after, page[0..size]);
                for (page[0..k]) |p| {
                    try testing.expectEqual(whole[got].timestamp_ns, p.timestamp_ns);
                    try testing.expectEqual(whole[got].ual_index, p.ual_index);
                    got += 1;
                }
                if (k < size) break;
                after = .{ .ns = page[k - 1].timestamp_ns, .ual = page[k - 1].ual_index };
            }
            try testing.expectEqual(whole.len, got);
        }
    }
}
