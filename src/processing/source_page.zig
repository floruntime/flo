//! A read of a source another shard owns, by a processing pipeline or a
//! workflow's stream trigger: the ask, and the owner's page. Only the owner
//! reads its own state, on its own thread, with the reads its clients'
//! reads use: applied data, nothing logged or replicated.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ts_mod = @import("../projection/ts.zig");
const ts_machine = @import("../ts/machine.zig");
const NamespaceId = @import("../node/router.zig").NamespaceId;

pub const Kind = enum(u8) { stream, ts };

/// Who asks, so the page goes back to it.
pub const Asker = enum(u8) { pipeline, trigger };

/// What the owner could read. Only `ok` says where the read reached: the
/// asker moves its cursor on no other answer, and asks again.
pub const Status = enum(u8) {
    ok,
    /// The owner doesn't serve reads now: offline, halted, a leader still
    /// applying earlier terms, or a boot not caught up with its group.
    not_ready,
    /// The owner has no such stream (never made, or deleted).
    no_stream,
};

/// Most points one TS page carries; each is 26 bytes and its tag string.
/// A stream page is what a client's stream read answers: whole appends,
/// up to `MAX_ANSWER_BYTES` (256 KiB) past the first.
pub const TS_PAGE_POINTS: usize = 512;

/// What the asker wants: `limit` records of `source` after `from`.
///
/// `[term u64][seq u64][kind u8][asker u8][key len u16][key][ns len u16][ns]
/// [source len u16][source][field len u16][field][tag_hash u64][from_a u64]
/// [from_b u64][limit u32][behind_ns u64][idle_at u64]`
pub const Read = struct {
    /// The asker's term and the read's number: an answer to another is dropped.
    term: u64,
    seq: u64,
    kind: Kind,
    asker: Asker = .pipeline,
    /// The asker's key on its shard: a pipeline's, or a trigger's workflow.
    key: []const u8,
    namespace: []const u8,
    /// The stream, or the measurement.
    source: []const u8,
    /// TS only.
    field: []const u8 = "",
    /// TS only: the series' tag set, or 0 for every one.
    tag_hash: u64 = 0,
    /// The cursor: a stream id (ms, seq), or a TS point's (time, position).
    from_a: u64,
    from_b: u64,
    limit: u32,
    /// TS only: how far behind the field's newest point the page stops
    /// (the source's lateness window, or 0 to read every point).
    behind_ns: u64 = 0,
    /// TS only: the field's write count at which the asker found it idle
    /// (no write for the window), or 0. Read through to the newest only
    /// while the field's count is still that.
    idle_at: u64 = 0,

    /// Whether every name fits its length field; a pipeline whose names
    /// don't can't ask, and is refused (`ProcessingHandler.askSource`).
    pub fn fits(self: Read) bool {
        const max = std.math.maxInt(u16);
        return self.key.len <= max and self.source.len <= max and self.namespace.len <= max and self.field.len <= max;
    }

    pub fn encode(self: Read, allocator: Allocator) ?[]u8 {
        if (!fits(self)) return null;
        const size = 8 + 8 + 1 + 1 + 2 + self.key.len + 2 + self.namespace.len + 2 + self.source.len + 2 + self.field.len + 8 + 8 + 8 + 4 + 8 + 8;
        const buf = allocator.alloc(u8, size) catch return null;
        var w: Writer = .{ .buf = buf };
        w.int(u64, self.term);
        w.int(u64, self.seq);
        w.int(u8, @backingInt(self.kind));
        w.int(u8, @backingInt(self.asker));
        w.int(u16, @intCast(self.key.len));
        w.bytes(self.key);
        w.int(u16, @intCast(self.namespace.len));
        w.bytes(self.namespace);
        w.int(u16, @intCast(self.source.len));
        w.bytes(self.source);
        w.int(u16, @intCast(self.field.len));
        w.bytes(self.field);
        w.int(u64, self.tag_hash);
        w.int(u64, self.from_a);
        w.int(u64, self.from_b);
        w.int(u32, self.limit);
        w.int(u64, self.behind_ns);
        w.int(u64, self.idle_at);
        std.debug.assert(w.at == buf.len);
        return buf;
    }

    pub fn decode(bytes: []const u8) ?Read {
        var r: Reader = .{ .buf = bytes };
        const term = r.int(u64) orelse return null;
        const seq = r.int(u64) orelse return null;
        const kind = std.enums.fromInt(Kind, r.int(u8) orelse return null) orelse return null;
        const asker = std.enums.fromInt(Asker, r.int(u8) orelse return null) orelse return null;
        const key = r.take(r.int(u16) orelse return null) orelse return null;
        const namespace = r.take(r.int(u16) orelse return null) orelse return null;
        const source = r.take(r.int(u16) orelse return null) orelse return null;
        const field = r.take(r.int(u16) orelse return null) orelse return null;
        const tag_hash = r.int(u64) orelse return null;
        const from_a = r.int(u64) orelse return null;
        const from_b = r.int(u64) orelse return null;
        const limit = r.int(u32) orelse return null;
        const behind_ns = r.int(u64) orelse return null;
        const idle_at = r.int(u64) orelse return null;
        if (r.at != bytes.len) return null;
        return .{ .term = term, .seq = seq, .kind = kind, .asker = asker, .key = key, .namespace = namespace, .source = source, .field = field, .tag_hash = tag_hash, .from_a = from_a, .from_b = from_b, .limit = limit, .behind_ns = behind_ns, .idle_at = idle_at };
    }
};

/// One TS point as a page carries it: its tag string, not the owner's
/// hash of it, which only the owner can look up.
pub const TsPoint = struct { timestamp_ns: u64, ual: u64, value: f64, tags: []const u8 };

/// The owner's answer: the read's own header, then where the read reached
/// and the records.
///
/// `[term u64][seq u64][kind u8][asker u8][key len u16][key][from_a u64]
/// [from_b u64][status u8][made_ns u64][last_a u64][last_b u64][late u64]
/// [count u32]` then each
/// stream record `[ms u64][seq u64][len u32][payload]` or TS point
/// `[ns u64][ual u64][value u64][tags len u16][tags]`.
pub const Page = struct {
    term: u64,
    seq: u64,
    kind: Kind,
    asker: Asker,
    key: []const u8,
    from_a: u64,
    from_b: u64,
    status: Status,
    /// Stream: which stream was read, by the stamp that made it, so an
    /// asker whose cursor is in one since deleted and made again starts
    /// over. 0 for TS.
    made_ns: u64,
    /// Stream: the id the read reached, past appends it couldn't read, so
    /// the cursor still moves past them. TS: the field's newest time and
    /// its write count (0 when it has no series).
    last_a: u64,
    last_b: u64,
    /// TS: the field's late points past the edge the read's window
    /// reports against (`lateEdgeFor`); 0 for a stream.
    late: u64,
    count: u32,
    items: []const u8,

    /// A page being written: the header now, the records as they come.
    pub const Builder = struct {
        list: std.ArrayList(u8) = .empty,
        count_at: usize = 0,
        last_at: usize = 0,
        count: u32 = 0,
        status: Status = .ok,
        made_ns: u64 = 0,

        pub fn begin(b: *Builder, allocator: Allocator, r: Read) !void {
            const w = &b.list;
            try appendInt(w, allocator, u64, r.term);
            try appendInt(w, allocator, u64, r.seq);
            try appendInt(w, allocator, u8, @backingInt(r.kind));
            try appendInt(w, allocator, u8, @backingInt(r.asker));
            try appendInt(w, allocator, u16, @intCast(r.key.len));
            try w.appendSlice(allocator, r.key);
            try appendInt(w, allocator, u64, r.from_a);
            try appendInt(w, allocator, u64, r.from_b);
            b.last_at = w.items.len;
            try appendInt(w, allocator, u8, 0);
            try appendInt(w, allocator, u64, 0);
            try appendInt(w, allocator, u64, 0);
            try appendInt(w, allocator, u64, 0);
            try appendInt(w, allocator, u64, 0);
            b.count_at = w.items.len;
            try appendInt(w, allocator, u32, 0);
        }

        pub fn record(b: *Builder, allocator: Allocator, ms: u64, seq: u64, payload: []const u8) !void {
            try appendInt(&b.list, allocator, u64, ms);
            try appendInt(&b.list, allocator, u64, seq);
            try appendInt(&b.list, allocator, u32, @intCast(payload.len));
            try b.list.appendSlice(allocator, payload);
            b.count += 1;
        }

        pub fn point(b: *Builder, allocator: Allocator, p: TsPoint) !void {
            const tags = p.tags[0..@min(p.tags.len, std.math.maxInt(u16))];
            try appendInt(&b.list, allocator, u64, p.timestamp_ns);
            try appendInt(&b.list, allocator, u64, p.ual);
            try appendInt(&b.list, allocator, u64, @bitCast(p.value));
            try appendInt(&b.list, allocator, u16, @intCast(tags.len));
            try b.list.appendSlice(allocator, tags);
            b.count += 1;
        }

        /// The page's bytes, owned by the caller.
        /// A page whose status isn't `ok` carries no records, and its `last`
        /// is its `from`.
        pub fn finish(b: *Builder, allocator: Allocator, last_a: u64, last_b: u64, late: u64) ![]u8 {
            const s = b.list.items;
            std.debug.assert(b.status == .ok or b.count == 0);
            s[b.last_at] = @backingInt(b.status);
            std.mem.writeInt(u64, s[b.last_at + 1 ..][0..8], b.made_ns, .little);
            std.mem.writeInt(u64, s[b.last_at + 9 ..][0..8], last_a, .little);
            std.mem.writeInt(u64, s[b.last_at + 17 ..][0..8], last_b, .little);
            std.mem.writeInt(u64, s[b.last_at + 25 ..][0..8], late, .little);
            std.mem.writeInt(u32, s[b.count_at..][0..4], b.count, .little);
            return b.list.toOwnedSlice(allocator);
        }

        pub fn deinit(b: *Builder, allocator: Allocator) void {
            b.list.deinit(allocator);
        }

        pub fn bytes(b: *const Builder) usize {
            return b.list.items.len;
        }
    };

    pub fn decode(bytes: []const u8) ?Page {
        var r: Reader = .{ .buf = bytes };
        const term = r.int(u64) orelse return null;
        const seq = r.int(u64) orelse return null;
        const kind = std.enums.fromInt(Kind, r.int(u8) orelse return null) orelse return null;
        const asker = std.enums.fromInt(Asker, r.int(u8) orelse return null) orelse return null;
        const key = r.take(r.int(u16) orelse return null) orelse return null;
        const from_a = r.int(u64) orelse return null;
        const from_b = r.int(u64) orelse return null;
        const status = std.enums.fromInt(Status, r.int(u8) orelse return null) orelse return null;
        const made_ns = r.int(u64) orelse return null;
        const last_a = r.int(u64) orelse return null;
        const last_b = r.int(u64) orelse return null;
        const late = r.int(u64) orelse return null;
        const count = r.int(u32) orelse return null;
        return .{ .term = term, .seq = seq, .kind = kind, .asker = asker, .key = key, .from_a = from_a, .from_b = from_b, .status = status, .made_ns = made_ns, .last_a = last_a, .last_b = last_b, .late = late, .count = count, .items = bytes[r.at..] };
    }

    pub const Record = struct { ms: u64, seq: u64, payload: []const u8 };

    /// The page's stream records, or null where they don't decode.
    pub fn records(self: Page, out: []Record) ?[]Record {
        if (self.kind != .stream or self.count > out.len) return null;
        var r: Reader = .{ .buf = self.items };
        for (out[0..self.count]) |*o| {
            const ms = r.int(u64) orelse return null;
            const seq = r.int(u64) orelse return null;
            const payload = r.take(r.int(u32) orelse return null) orelse return null;
            o.* = .{ .ms = ms, .seq = seq, .payload = payload };
        }
        return if (r.at == self.items.len) out[0..self.count] else null;
    }

    /// The page's TS points, or null where they don't decode.
    pub fn points(self: Page, out: []TsPoint) ?[]TsPoint {
        if (self.kind != .ts or self.count > out.len) return null;
        var r: Reader = .{ .buf = self.items };
        for (out[0..self.count]) |*o| {
            const ns = r.int(u64) orelse return null;
            const ual = r.int(u64) orelse return null;
            const value: f64 = @bitCast(r.int(u64) orelse return null);
            const tags = r.take(r.int(u16) orelse return null) orelse return null;
            o.* = .{ .timestamp_ns = ns, .ual = ual, .value = value, .tags = tags };
        }
        return if (r.at == self.items.len) out[0..self.count] else null;
    }
};

/// Up to `limit` of a series' points after `after` and at or before
/// `through_ns`, in (time, position)
/// order, into `out`. A page may end among points sharing a time: the
/// next goes on from its last point's (time, position). The tag strings
/// are the projection's, good until it next changes.
pub fn readTsPage(ts: *ts_mod.TSProjection, ns: NamespaceId, measurement: []const u8, field: []const u8, tag_hash: u64, after: ts_mod.TSProjection.ReadAt, through_ns: u64, limit: usize, out: *[TS_PAGE_POINTS]TsPoint) usize {
    var buf: [TS_PAGE_POINTS]ts_mod.StoredPoint = undefined;
    const want = @min(limit, buf.len);
    const n = ts.readAfter(ns, measurement, field, if (tag_hash != 0) tag_hash else null, after, through_ns, buf[0..want]) catch return 0;
    for (buf[0..n], out[0..n]) |p, *o| o.* = .{ .timestamp_ns = p.timestamp_ns, .ual = p.ual_index, .value = p.field_value, .tags = p.tags };
    return n;
}

fn appendInt(list: *std.ArrayList(u8), allocator: Allocator, comptime T: type, v: T) !void {
    var b: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &b, v, .little);
    try list.appendSlice(allocator, &b);
}

const Writer = struct {
    buf: []u8,
    at: usize = 0,

    fn int(w: *Writer, comptime T: type, v: T) void {
        std.mem.writeInt(T, w.buf[w.at..][0..@sizeOf(T)], v, .little);
        w.at += @sizeOf(T);
    }
    fn bytes(w: *Writer, b: []const u8) void {
        @memcpy(w.buf[w.at..][0..b.len], b);
        w.at += b.len;
    }
};

const Reader = struct {
    buf: []const u8,
    at: usize = 0,

    fn int(r: *Reader, comptime T: type) ?T {
        if (r.buf.len - r.at < @sizeOf(T)) return null;
        defer r.at += @sizeOf(T);
        return std.mem.readInt(T, r.buf[r.at..][0..@sizeOf(T)], .little);
    }
    fn take(r: *Reader, n: usize) ?[]const u8 {
        if (r.buf.len - r.at < n) return null;
        defer r.at += n;
        return r.buf[r.at..][0..n];
    }
};

test "source read and page: fields come back as sent; a cut one doesn't decode" {
    const a = std.testing.allocator;
    const read: Read = .{ .term = 3, .seq = 9, .kind = .ts, .key = "job\x001", .namespace = "default", .source = "cpu", .field = "v", .tag_hash = 7, .from_a = 100, .from_b = 0, .limit = 50 };
    const rb = read.encode(a).?;
    defer a.free(rb);
    const got = Read.decode(rb).?;
    try std.testing.expectEqualStrings("job\x001", got.key);
    try std.testing.expectEqual(@as(u64, 7), got.tag_hash);
    try std.testing.expectEqual(@as(u32, 50), got.limit);
    try std.testing.expectEqual(@as(?Read, null), Read.decode(rb[0 .. rb.len - 1]));
    try std.testing.expectEqual(Asker.pipeline, got.asker);
    const tr: Read = .{ .term = 1, .seq = 2, .kind = .stream, .asker = .trigger, .key = "wf", .namespace = "default", .source = "s", .from_a = 0, .from_b = 0, .limit = 1 };
    const tb = tr.encode(a).?;
    defer a.free(tb);
    try std.testing.expectEqual(Asker.trigger, Read.decode(tb).?.asker);

    var b: Page.Builder = .{};
    defer b.deinit(a);
    try b.begin(a, .{ .term = 3, .seq = 9, .kind = .stream, .key = "j", .namespace = "default", .source = "s", .from_a = 1, .from_b = 2, .limit = 5 });
    try b.record(a, 10, 0, "x");
    try b.record(a, 10, 1, "");
    const pb = try b.finish(a, 10, 1, 4);
    defer a.free(pb);
    const page = Page.decode(pb).?;
    try std.testing.expectEqual(@as(u32, 2), page.count);
    try std.testing.expectEqual(@as(u64, 4), page.late);
    try std.testing.expectEqual(@as(u64, 2), page.from_b);
    try std.testing.expectEqual(@as(u64, 10), page.last_a);
    try std.testing.expectEqual(@as(u64, 1), page.last_b);
    try std.testing.expectEqual(Status.ok, page.status);

    // A refusal carries its status, its stream's stamp and asker, and no records.
    var nb: Page.Builder = .{ .status = .not_ready, .made_ns = 5 };
    defer nb.deinit(a);
    try nb.begin(a, tr);
    const np = try nb.finish(a, 0, 0, 0);
    defer a.free(np);
    const n = Page.decode(np).?;
    try std.testing.expectEqual(Status.not_ready, n.status);
    try std.testing.expectEqual(@as(u64, 5), n.made_ns);
    try std.testing.expectEqual(Asker.trigger, n.asker);
    try std.testing.expectEqual(@as(u32, 0), n.count);
    var recs: [4]Page.Record = undefined;
    const rs = page.records(&recs).?;
    try std.testing.expectEqualStrings("x", rs[0].payload);
    try std.testing.expectEqual(@as(u64, 1), rs[1].seq);
    try std.testing.expectEqual(@as(?[]Page.Record, null), (Page{ .term = 0, .seq = 0, .kind = .stream, .asker = .pipeline, .key = "", .from_a = 0, .from_b = 0, .status = .ok, .made_ns = 0, .last_a = 0, .last_b = 0, .late = 0, .count = 2, .items = page.items[0 .. page.items.len - 1] }).records(&recs));
}

test "readTsPage: pages may end among points sharing a time, and together read each point once" {
    var ts = ts_mod.TSProjection.init(std.testing.allocator);
    defer ts.deinit();
    // Three points at each of times 10..14, from three tag series.
    var ual: u64 = 1;
    for (0..5) |t| for ([_][]const u8{ "h=a", "h=b", "h=c" }) |tags| {
        try ts.insert(.default, "m", "v", 1, 10 + t, ual, tags);
        ual += 1;
    };
    var out: [TS_PAGE_POINTS]TsPoint = undefined;
    var after: ts_mod.TSProjection.ReadAt = .{ .ns = 0, .ual = 0 };
    var seen: [16]bool = @splat(false);
    var pages: usize = 0;
    while (true) : (pages += 1) {
        const n = readTsPage(&ts, .default, "m", "v", 0, after, std.math.maxInt(u64), 4, &out);
        if (n == 0) break;
        for (out[0..n]) |p| {
            try std.testing.expect(!seen[p.ual >> ts_machine.ORDINAL_BITS]);
            seen[p.ual >> ts_machine.ORDINAL_BITS] = true;
        }
        after = .{ .ns = out[n - 1].timestamp_ns, .ual = out[n - 1].ual };
    }
    try std.testing.expectEqual(@as(usize, 4), pages);
    for (seen[1..16]) |s| try std.testing.expect(s);
}

test "readTsPage: one time with more points than a page is read across pages, none skipped" {
    var ts = ts_mod.TSProjection.init(std.testing.allocator);
    defer ts.deinit();
    var tbuf: [16]u8 = undefined;
    const n_points = TS_PAGE_POINTS * 2 + 7;
    for (0..n_points) |i| try ts.insert(.default, "m", "v", 1, 50, i + 1, try std.fmt.bufPrint(&tbuf, "h={d}", .{i}));
    var out: [TS_PAGE_POINTS]TsPoint = undefined;
    var after: ts_mod.TSProjection.ReadAt = .{ .ns = 0, .ual = 0 };
    var total: usize = 0;
    while (true) {
        const n = readTsPage(&ts, .default, "m", "v", 0, after, std.math.maxInt(u64), TS_PAGE_POINTS, &out);
        if (n == 0) break;
        total += n;
        after = .{ .ns = out[n - 1].timestamp_ns, .ual = out[n - 1].ual };
    }
    try std.testing.expectEqual(n_points, total);
}

test "readTsPage: an earlier point in a series past the first is read first, however many points the others hold at one time" {
    var ts = ts_mod.TSProjection.init(std.testing.allocator);
    defer ts.deinit();
    var tbuf: [16]u8 = undefined;
    for (0..1200) |i| try ts.insert(.default, "m", "v", 1, 1000, i + 1, try std.fmt.bufPrint(&tbuf, "h={d}", .{i}));
    try ts.insert(.default, "m", "v", 2, 5, 5000, "h=early");
    var out: [TS_PAGE_POINTS]TsPoint = undefined;
    const n = readTsPage(&ts, .default, "m", "v", 0, .{ .ns = 0, .ual = 0 }, std.math.maxInt(u64), 100, &out);
    try std.testing.expectEqual(@as(usize, 100), n);
    try std.testing.expectEqual(@as(u64, 5), out[0].timestamp_ns);
    try std.testing.expectEqualStrings("h=early", out[0].tags);
}
