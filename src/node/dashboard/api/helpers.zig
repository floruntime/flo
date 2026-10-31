//! Dashboard API Shared Helpers
//!
//! Reusable utilities for all API domain modules:
//! - DashboardContext: Central context for all dashboard API handlers
//! - BinaryReader: Safe little-endian binary wire format parser
//! - Query parameter parsing (generic over int types + string)
//! - JSON error responses

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
pub const json = @import("../../../util/json.zig");
pub const MetricsRegistry = @import("../../../metrics/registry.zig").MetricsRegistry;
pub const HostStats = @import("../host_stats.zig");
pub const NodeHealth = @import("../../health.zig").NodeHealth;

// =============================================================================
// Dashboard Context — Central context for all dashboard API handlers
// =============================================================================

/// Central context providing data access for dashboard API handlers.
///
/// The dashboard asks each shard over its client ops, as any client does,
/// through the node's own port (`loopback.zig`); it reads MetricsRegistry
/// directly.
pub const DashboardContext = struct {
    allocator: Allocator,
    metrics: *MetricsRegistry,
    num_shards: u32,
    start_time: i64,
    /// The node's own client port and the address it is reachable at from
    /// this process. Mutations from the dashboard thread are issued as a
    /// client request to it, reusing the thread-safe protocol path (the
    /// dashboard thread cannot propose directly).
    listen_port: u16 = 9000,
    client_ip4: [4]u8 = .{ 127, 0, 0, 1 },
    /// Live host telemetry for this node (CPU/MEM/IO), sampled by /cluster/stats.
    host: HostStats.Sampler = .{},
    /// Each shard's published state, for /ready.
    health: ?*const NodeHealth = null,
    /// The shards are built and serving. The dashboard starts before they
    /// replay, and a data API before this would ask shards still being built.
    shards_ready: std.atomic.Value(bool) = .init(false),

    pub fn init(allocator: Allocator, metrics: *MetricsRegistry, num_shards: u32) DashboardContext {
        return .{
            .allocator = allocator,
            .metrics = metrics,
            .num_shards = num_shards,
            .start_time = @import("stdx").time.milliTimestamp(),
        };
    }

    /// Open the data APIs, once the shards are built and wired.
    pub fn markShardsReady(self: *DashboardContext) void {
        self.shards_ready.store(true, .release);
    }

    /// `host:port` of this node's client listener, for the dashboard's own writes.
    pub fn clientEndpoint(self: *const DashboardContext, buf: *[32]u8) ![]const u8 {
        const a = self.client_ip4;
        return std.fmt.bufPrint(buf, "{d}.{d}.{d}.{d}:{d}", .{ a[0], a[1], a[2], a[3], self.listen_port });
    }

    /// Get uptime in seconds
    pub fn uptimeSeconds(self: *const DashboardContext) u64 {
        const now = @import("stdx").time.milliTimestamp();
        // start_time is a ms timestamp — convert the diff to seconds (this was
        // returning milliseconds-as-seconds, making uptime 1000× too large and
        // rps = commands_total/uptime collapse to 0).
        const diff_ms = now - self.start_time;
        return if (diff_ms > 0) @intCast(@divFloor(diff_ms, 1000)) else 0;
    }
};

// =============================================================================
// BinaryReader — Safe little-endian binary wire format parser
// =============================================================================

/// Reads little-endian integers and byte slices from a binary buffer with
/// bounds checking. Returns `null` when buffer is exhausted.
pub const BinaryReader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) BinaryReader {
        return .{ .data = data, .pos = 0 };
    }

    pub fn readU16(self: *BinaryReader) ?u16 {
        if (self.pos + 2 > self.data.len) return null;
        const v = std.mem.readInt(u16, self.data[self.pos..][0..2], .little);
        self.pos += 2;
        return v;
    }

    pub fn readU32(self: *BinaryReader) ?u32 {
        if (self.pos + 4 > self.data.len) return null;
        const v = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return v;
    }

    pub fn readU64(self: *BinaryReader) ?u64 {
        if (self.pos + 8 > self.data.len) return null;
        const v = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return v;
    }

    pub fn readI32(self: *BinaryReader) ?i32 {
        if (self.pos + 4 > self.data.len) return null;
        const v = std.mem.readInt(i32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return v;
    }

    pub fn readI64(self: *BinaryReader) ?i64 {
        if (self.pos + 8 > self.data.len) return null;
        const v = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return v;
    }

    pub fn readByte(self: *BinaryReader) ?u8 {
        if (self.pos >= self.data.len) return null;
        const v = self.data[self.pos];
        self.pos += 1;
        return v;
    }

    pub fn readBytes(self: *BinaryReader, len: usize) ?[]const u8 {
        if (self.pos + len > self.data.len) return null;
        const slice = self.data[self.pos..][0..len];
        self.pos += len;
        return slice;
    }

    /// Read a length-prefixed byte slice.
    pub fn readLenPrefixed(self: *BinaryReader, comptime LenType: type) ?[]const u8 {
        const len: usize = switch (LenType) {
            u16 => @intCast(self.readU16() orelse return null),
            u32 => @intCast(self.readU32() orelse return null),
            else => @compileError("Unsupported length prefix type"),
        };
        return self.readBytes(len);
    }
};

// =============================================================================
// Query Parameter Parsing
// =============================================================================

/// Generic query string parameter parser — works with u32, u64, and []const u8.
pub fn parseQueryParam(comptime T: type, query_string: ?[]const u8, key: []const u8) ?T {
    const qs = query_string orelse return null;
    var pairs = std.mem.splitScalar(u8, qs, '&');
    while (pairs.next()) |pair| {
        if (std.mem.indexOfScalar(u8, pair, '=')) |eq_idx| {
            const k = pair[0..eq_idx];
            const v = pair[eq_idx + 1 ..];
            if (std.mem.eql(u8, k, key)) {
                if (T == []const u8) {
                    return if (v.len == 0) null else v;
                } else {
                    return std.fmt.parseInt(T, v, 10) catch null;
                }
            }
        }
    }
    return null;
}

/// Why a route refuses its query string: a parameter not in `allowed`, one
/// given twice, or one with no value; null when there's none. Written into
/// `buf`. Checked first, so the route's own reads see each name at most once.
pub fn queryRefusal(buf: []u8, query_string: ?[]const u8, comptime allowed: []const []const u8) ?[]const u8 {
    const qs = query_string orelse return null;
    var seen = @as([allowed.len]bool, @splat(false));
    var pairs = std.mem.splitScalar(u8, qs, '&');
    next: while (pairs.next()) |pair| {
        if (pair.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, pair, '=');
        const k = if (eq) |e| pair[0..e] else pair;
        inline for (allowed, 0..) |a, i| if (std.mem.eql(u8, k, a)) {
            if (eq == null or eq.? + 1 == pair.len) return std.fmt.bufPrint(buf, "'{s}' needs a value", .{k}) catch "a parameter needs a value";
            if (seen[i]) return std.fmt.bufPrint(buf, "'{s}' is given more than once", .{k}) catch "a parameter is given more than once";
            seen[i] = true;
            continue :next;
        };
        return std.fmt.bufPrint(buf, "unknown parameter '{s}'", .{k}) catch "unknown parameter";
    }
    return null;
}

/// A boolean query parameter: absent is false; true/1 and false/0 are the
/// only values, so a typo isn't read as false.
pub fn boolQueryParam(query_string: ?[]const u8, key: []const u8) error{InvalidBool}!bool {
    const v = parseQueryParam([]const u8, query_string, key) orelse return false;
    if (std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1")) return true;
    if (std.mem.eql(u8, v, "false") or std.mem.eql(u8, v, "0")) return false;
    return error.InvalidBool;
}

test "queryRefusal names an unknown, repeated or empty parameter" {
    const allowed: []const []const u8 = &.{ "namespace", "max_len", "dry_run" };
    var buf: [64]u8 = undefined;
    try std.testing.expectEqual(@as(?[]const u8, null), queryRefusal(&buf, "namespace=a&max_len=3", allowed));
    try std.testing.expectEqual(@as(?[]const u8, null), queryRefusal(&buf, null, allowed));
    try std.testing.expectEqualStrings("unknown parameter 'max_bytes'", queryRefusal(&buf, "max_len=3&max_bytes=10", allowed).?);
    try std.testing.expectEqualStrings("'max_len' is given more than once", queryRefusal(&buf, "max_len=1&max_len=abc", allowed).?);
    try std.testing.expectEqualStrings("'dry_run' needs a value", queryRefusal(&buf, "max_len=1&dry_run", allowed).?);
    try std.testing.expectEqualStrings("'max_len' needs a value", queryRefusal(&buf, "max_len=", allowed).?);
}

test "boolQueryParam takes only true/false/1/0" {
    try std.testing.expect(try boolQueryParam("dry_run=true", "dry_run"));
    try std.testing.expect(try boolQueryParam("dry_run=1", "dry_run"));
    try std.testing.expect(!try boolQueryParam("dry_run=0", "dry_run"));
    try std.testing.expect(!try boolQueryParam(null, "dry_run"));
    try std.testing.expectError(error.InvalidBool, boolQueryParam("dry_run=yes", "dry_run"));
    try std.testing.expectError(error.InvalidBool, boolQueryParam("dry_run=True", "dry_run"));
}

// =============================================================================
// JSON Error Response
// =============================================================================

/// Create JSON error response: {"error":"<message>"}
pub fn jsonError(allocator: Allocator, message: []const u8) ![]const u8 {
    var json_aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer json_aw.deinit();
    const writer = &json_aw.writer;
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("error", message);
    try obj.end();

    return try json_aw.toOwnedSlice();
}

// =============================================================================
// Percent-Decoding (for URL path segments)
// =============================================================================

/// Percent-decode `input` into `buf` (e.g. `%2F` → `/`). Input with no `%` is
/// returned as is; otherwise the decoded slice of `buf`, or null when input is
/// longer than `buf`.
pub fn percentDecode(buf: []u8, input: []const u8) ?[]const u8 {
    if (std.mem.indexOfScalar(u8, input, '%') == null) return input;
    // Decoding never lengthens, so an input that fits can't overrun buf.
    if (input.len > buf.len) return null;
    var i: usize = 0;
    var o: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]);
            const lo = hexVal(input[i + 2]);
            if (hi != null and lo != null) {
                buf[o] = (hi.? << 4) | lo.?;
                o += 1;
                i += 3;
                continue;
            }
        }
        buf[o] = input[i];
        o += 1;
        i += 1;
    }
    return buf[0..o];
}

fn hexVal(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "BinaryReader reads little-endian integers" {
    var buf: [20]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], 42, .little);
    std.mem.writeInt(u64, buf[4..12], 123456, .little);
    std.mem.writeInt(u16, buf[12..14], 999, .little);

    var reader = BinaryReader.init(&buf);
    try std.testing.expectEqual(@as(u32, 42), reader.readU32().?);
    try std.testing.expectEqual(@as(u64, 123456), reader.readU64().?);
    try std.testing.expectEqual(@as(u16, 999), reader.readU16().?);
}

test "BinaryReader returns null on underflow" {
    var reader = BinaryReader.init(&[_]u8{ 1, 2 });
    try std.testing.expect(reader.readU32() == null);
}

test "BinaryReader readBytes and readLenPrefixed" {
    var buf: [10]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], 3, .little);
    buf[2] = 'a';
    buf[3] = 'b';
    buf[4] = 'c';

    var reader = BinaryReader.init(&buf);
    const data = reader.readLenPrefixed(u16).?;
    try std.testing.expectEqualStrings("abc", data);
}

test "parseQueryParam parses string and integer params" {
    const qs: []const u8 = "limit=100&prefix=test&offset=50";
    try std.testing.expectEqual(@as(u32, 100), parseQueryParam(u32, qs, "limit").?);
    try std.testing.expectEqualStrings("test", parseQueryParam([]const u8, qs, "prefix").?);
    try std.testing.expectEqual(@as(u64, 50), parseQueryParam(u64, qs, "offset").?);
    try std.testing.expect(parseQueryParam(u32, qs, "missing") == null);
}

test "parseQueryParam returns null for empty or missing" {
    try std.testing.expect(parseQueryParam(u32, null, "x") == null);
    try std.testing.expect(parseQueryParam([]const u8, "key=", "key") == null);
}

test "jsonError produces valid JSON" {
    const allocator = std.testing.allocator;
    const result = try jsonError(allocator, "not found");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"error\":\"not found\"}", result);
}

test "DashboardContext init and uptime" {
    const allocator = std.testing.allocator;
    var metrics = MetricsRegistry.init(allocator);
    defer metrics.deinit();

    const ctx = DashboardContext.init(allocator, &metrics, 4);
    try std.testing.expectEqual(@as(u32, 4), ctx.num_shards);
    try std.testing.expect(ctx.uptimeSeconds() <= 1);
}

test "dashboard: the client endpoint follows the bind address" {
    var ctx = DashboardContext{ .allocator = std.testing.allocator, .metrics = undefined, .num_shards = 1, .start_time = 0, .listen_port = 9000 };
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("127.0.0.1:9000", try ctx.clientEndpoint(&buf));
    ctx.client_ip4 = .{ 10, 0, 1, 12 };
    try std.testing.expectEqualStrings("10.0.1.12:9000", try ctx.clientEndpoint(&buf));
}

test "percentDecode refuses an escaped input longer than its buffer" {
    var buf: [8]u8 = undefined;
    try std.testing.expect(percentDecode(&buf, "abcdefg%41") == null);
    try std.testing.expectEqualStrings("abcdeA", percentDecode(&buf, "abcde%41").?);
    try std.testing.expectEqualStrings("no-escapes-at-all", percentDecode(&buf, "no-escapes-at-all").?);
}
