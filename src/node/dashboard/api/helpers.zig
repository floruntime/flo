//! Dashboard API Shared Helpers
//!
//! Reusable utilities for all API domain modules:
//! - DashboardContext: Central context for all dashboard API handlers
//! - BinaryReader: Safe little-endian binary wire format parser
//! - Query parameter parsing (generic over int types + string)
//! - JSON error responses
//! - Run statistics types + JSON helpers

const std = @import("std");
const log = @import("stdx").log;
const Allocator = std.mem.Allocator;
pub const json = @import("../../../util/json.zig");
pub const MetricsRegistry = @import("../../../metrics/registry.zig").MetricsRegistry;

// =============================================================================
// Dashboard Context — Central context for all dashboard API handlers
// =============================================================================

/// Central context providing data access for dashboard API handlers.
///
/// In the new shard architecture, the dashboard thread communicates with
/// shards via direct projection reads (read-only) and MetricsRegistry
/// (mutex-protected metadata).
///
/// `shard_ptrs` holds opaque pointers to the Shard array — each API
/// handler casts to the concrete type when it needs projection data.
/// Read-only access is safe for dashboard display purposes.
pub const DashboardContext = struct {
    allocator: Allocator,
    metrics: *MetricsRegistry,
    num_shards: u32,
    start_time: i64,
    /// Opaque pointers to Shard structs (set by runtime after shard creation).
    /// Handlers cast via `getShard()` to access projections read-only.
    shard_ptrs: ?[]*anyopaque,
    /// The node's own protocol listen port. Mutations from the dashboard thread
    /// are issued as a loopback client request to `127.0.0.1:listen_port`, reusing
    /// the thread-safe protocol path (the dashboard thread cannot propose directly).
    listen_port: u16 = 9000,

    pub fn init(allocator: Allocator, metrics: *MetricsRegistry, num_shards: u32) DashboardContext {
        return .{
            .allocator = allocator,
            .metrics = metrics,
            .num_shards = num_shards,
            .start_time = @import("stdx").time.milliTimestamp(),
            .shard_ptrs = null,
        };
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
// Run Statistics Types + JSON Helpers
// =============================================================================

/// Run statistics counters
pub const RunCounts = struct {
    total: u32 = 0,
    pending: u32 = 0,
    running: u32 = 0,
    completed: u32 = 0,
    failed: u32 = 0,
    cancelled: u32 = 0,
    timed_out: u32 = 0,
};

/// Write RunCounts as a nested JSON object field
pub fn writeRunCountsJson(writer: anytype, counts: RunCounts) !void {
    var runs_obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try runs_obj.begin();
    try runs_obj.intField("total", counts.total);
    try runs_obj.intField("pending", counts.pending);
    try runs_obj.intField("running", counts.running);
    try runs_obj.intField("completed", counts.completed);
    try runs_obj.intField("failed", counts.failed);
    try runs_obj.intField("cancelled", counts.cancelled);
    try runs_obj.intField("timed_out", counts.timed_out);
    try runs_obj.end();
}

// =============================================================================
// Percent-Decoding (for URL path segments)
// =============================================================================

/// Decode percent-encoded bytes in-place (e.g. `%2F` → `/`).
/// Returns a slice into `buf` with the decoded content.
/// If input has no `%` sequences, returns the original slice unchanged.
pub fn percentDecode(buf: []u8, input: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, input, '%') == null) return input;
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
