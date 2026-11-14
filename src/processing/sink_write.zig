//! A pipeline's sink write for the shard that owns its target, as an inbox
//! message carries it. Only the owner touches its own state: it proposes
//! the write as a client's would be, on its own thread.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Kind = enum(u8) { kv, queue, stream, ts };

/// `[kind u8][ns len u8][ns][target len u16][target][ttl? u8][ttl u64]
/// [priority u8][timestamp_ns u64][field len u8][field][tags len u16][tags]
/// [value]`. Names are as the sink gave them; the owner finds the
/// namespace's id.
pub const SinkWrite = struct {
    kind: Kind,
    namespace: []const u8,
    /// The KV key, queue, stream or measurement.
    target: []const u8,
    value: []const u8,
    /// KV only.
    ttl_ms: ?u64 = null,
    /// Queue only.
    priority: u8 = 0,
    /// TS only: the point's time, or 0 for the entry's stamp.
    timestamp_ns: u64 = 0,
    /// TS only.
    field: []const u8 = "",
    /// TS only: `k=v,k=v`, as a client's write sends them.
    tags: []const u8 = "",

    const FIXED = 1 + 1 + 2 + 9 + 1 + 8 + 1 + 2;

    /// What a write's order is kept within: its kind, namespace and target.
    pub fn targetKey(self: SinkWrite) u64 {
        var h = std.hash.Wyhash.init(@backingInt(self.kind));
        h.update(self.namespace);
        h.update(&.{0});
        h.update(self.target);
        return h.final();
    }

    /// The message's bytes, owned by the caller (the receiver frees them),
    /// or null out of memory or for a name too long to carry.
    pub fn encode(self: SinkWrite, allocator: Allocator) ?[]u8 {
        if (self.namespace.len > std.math.maxInt(u8) or self.field.len > std.math.maxInt(u8)) return null;
        if (self.target.len > std.math.maxInt(u16) or self.tags.len > std.math.maxInt(u16)) return null;
        const buf = allocator.alloc(u8, FIXED + self.namespace.len + self.target.len + self.field.len + self.tags.len + self.value.len) catch return null;
        var w: Writer = .{ .buf = buf };
        w.byte(@backingInt(self.kind));
        w.byte(@intCast(self.namespace.len));
        w.bytes(self.namespace);
        w.int(u16, @intCast(self.target.len));
        w.bytes(self.target);
        w.byte(@intFromBool(self.ttl_ms != null));
        w.int(u64, self.ttl_ms orelse 0);
        w.byte(self.priority);
        w.int(u64, self.timestamp_ns);
        w.byte(@intCast(self.field.len));
        w.bytes(self.field);
        w.int(u16, @intCast(self.tags.len));
        w.bytes(self.tags);
        w.bytes(self.value);
        std.debug.assert(w.at == buf.len);
        return buf;
    }

    pub fn decode(bytes: []const u8) ?SinkWrite {
        var r: Reader = .{ .buf = bytes };
        const kind = std.enums.fromInt(Kind, r.byte() orelse return null) orelse return null;
        const ns = r.take(r.byte() orelse return null) orelse return null;
        const target = r.take(r.int(u16) orelse return null) orelse return null;
        const has_ttl = r.byte() orelse return null;
        const ttl = r.int(u64) orelse return null;
        const priority = r.byte() orelse return null;
        const timestamp_ns = r.int(u64) orelse return null;
        const field = r.take(r.byte() orelse return null) orelse return null;
        const tags = r.take(r.int(u16) orelse return null) orelse return null;
        return .{
            .kind = kind,
            .namespace = ns,
            .target = target,
            .value = bytes[r.at..],
            .ttl_ms = if (has_ttl != 0) ttl else null,
            .priority = priority,
            .timestamp_ns = timestamp_ns,
            .field = field,
            .tags = tags,
        };
    }
};

const Writer = struct {
    buf: []u8,
    at: usize = 0,

    fn byte(w: *Writer, b: u8) void {
        w.buf[w.at] = b;
        w.at += 1;
    }
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

    fn byte(r: *Reader) ?u8 {
        if (r.at >= r.buf.len) return null;
        r.at += 1;
        return r.buf[r.at - 1];
    }
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

test "SinkWrite: each kind's fields come back as sent, and a cut message doesn't decode" {
    const a = std.testing.allocator;
    const sent = [_]SinkWrite{
        .{ .kind = .kv, .namespace = "default", .target = "k", .value = "v", .ttl_ms = 5 },
        .{ .kind = .queue, .namespace = "app", .target = "q", .value = "", .priority = 7 },
        .{ .kind = .stream, .namespace = "default", .target = "s", .value = "payload" },
        .{ .kind = .ts, .namespace = "default", .target = "cpu", .value = &@as([8]u8, @bitCast(@as(f64, 1.5))), .timestamp_ns = 9, .field = "f", .tags = "host=a" },
    };
    for (sent) |s| {
        const bytes = s.encode(a).?;
        defer a.free(bytes);
        const got = SinkWrite.decode(bytes).?;
        try std.testing.expectEqual(s.kind, got.kind);
        try std.testing.expectEqualStrings(s.namespace, got.namespace);
        try std.testing.expectEqualStrings(s.target, got.target);
        try std.testing.expectEqualStrings(s.value, got.value);
        try std.testing.expectEqual(s.ttl_ms, got.ttl_ms);
        try std.testing.expectEqual(s.priority, got.priority);
        try std.testing.expectEqual(s.timestamp_ns, got.timestamp_ns);
        try std.testing.expectEqualStrings(s.field, got.field);
        try std.testing.expectEqualStrings(s.tags, got.tags);
        // Cut before the value starts: a fixed field is missing.
        try std.testing.expectEqual(@as(?SinkWrite, null), SinkWrite.decode(bytes[0 .. bytes.len - s.value.len - 1]));
    }
    try std.testing.expectEqual(@as(?SinkWrite, null), SinkWrite.decode(&.{9}));
}
