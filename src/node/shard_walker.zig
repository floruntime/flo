//! ShardWalker — generic cross-shard scan with pagination
//!
//! List and scan operations must visit all shards. Instead of copy-pasted
//! implementations per domain, a single generic `ShardWalker(T)` handles
//! cursor splitting, cross-shard fan-out, result merging, and pagination.
//!
//! ## Cursor Format
//!
//! `[shard_id: u16][local_cursor: variable]`
//!
//! The walker knows which shard to resume from; the local cursor is
//! opaque (each projection defines its own format).
//!
//! ## Walk Strategy
//!
//! Walk shards 0 → N sequentially, accumulating results until the limit
//! is reached. If a shard has more data, encode a cursor and return.
//! The next page resumes from that exact position.
//!
//! ## Usage
//!
//! ```zig
//! const Walker = ShardWalker(KVEntry);
//! var walker = Walker.init(localScanFn, shard_count);
//! const result = walker.walkLocal(0, "ns", null, 10, localScanner);
//! ```

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// ShardWalker
// ═══════════════════════════════════════════════════════════════════════════════

pub fn ShardWalker(comptime ResultT: type) type {
    return struct {
        const Self = @This();

        /// Maximum items in a single scan batch.
        pub const MAX_BATCH: usize = 1024;

        // ─── Cursor ──────────────────────────────────────────────────────

        /// Opaque pagination cursor: shard_id + local cursor bytes.
        pub const Cursor = struct {
            shard_id: u16,
            local_cursor: ?[]const u8,

            /// Encode cursor into a buffer: [shard_id_le: 2][local_cursor...].
            pub fn encode(self: Cursor, buf: []u8) ?[]const u8 {
                const lc = self.local_cursor orelse &[_]u8{};
                const needed = 2 + lc.len;
                if (buf.len < needed) return null;

                std.mem.writeInt(u16, buf[0..2], self.shard_id, .little);
                if (lc.len > 0) {
                    @memcpy(buf[2 .. 2 + lc.len], lc);
                }
                return buf[0..needed];
            }

            /// Decode cursor from raw bytes.
            pub fn decode(data: []const u8) ?Cursor {
                if (data.len < 2) return null;
                const shard_id = std.mem.readInt(u16, data[0..2], .little);
                const local = if (data.len > 2) data[2..] else null;
                return .{
                    .shard_id = shard_id,
                    .local_cursor = local,
                };
            }
        };

        // ─── ScanResult ──────────────────────────────────────────────────

        /// Result of a local scan on one shard.
        pub const ScanResult = struct {
            /// Collected items.
            items: []const ResultT,
            /// Next local cursor (null = shard exhausted).
            next_cursor: ?[]const u8,
        };

        // ─── LocalScanFn ─────────────────────────────────────────────────

        /// Function type for scanning one shard locally.
        /// Each domain (KV, Stream, Queue, TS) provides one.
        ///
        /// Parameters:
        ///   ctx:           domain-specific context (e.g., the projection)
        ///   namespace:     namespace filter
        ///   filter:        prefix/pattern filter (empty = no filter)
        ///   local_cursor:  resume position (null = start of shard)
        ///   limit:         max items to return
        pub const LocalScanFn = *const fn (
            ctx: *anyopaque,
            namespace: []const u8,
            filter: []const u8,
            local_cursor: ?[]const u8,
            limit: u32,
        ) ScanResult;

        // ─── WalkResult ──────────────────────────────────────────────────

        /// Result of a full walk (possibly spanning multiple shards).
        pub const WalkResult = struct {
            /// Collected items from all visited shards.
            items: []const ResultT,
            /// Encoded cursor for the next page (null = all shards exhausted).
            next_cursor: ?[]const u8,
        };

        // ─── Instance ────────────────────────────────────────────────────

        local_scan: LocalScanFn,
        shard_count: u16,

        pub fn init(local_scan: LocalScanFn, shard_count: u16) Self {
            return .{
                .local_scan = local_scan,
                .shard_count = shard_count,
            };
        }

        /// Execute a local-only walk (single node, all shards scanned sequentially).
        ///
        /// This is the non-distributed version. The caller collects results
        /// into a pre-allocated buffer. Cross-shard fan-out via inbox will
        /// be added in a later phase.
        ///
        /// Parameters:
        ///   contexts:   per-shard scan contexts (index = shard_id)
        ///   namespace:  namespace to scan
        ///   filter:     prefix/pattern filter (empty = no filter)
        ///   cursor:     raw encoded cursor (null = start)
        ///   limit:      max items to return
        ///   result_buf: caller-owned buffer for result items
        ///   cursor_buf: caller-owned buffer for encoding the next cursor
        pub fn walk(
            self: *const Self,
            contexts: []const *anyopaque,
            namespace: []const u8,
            filter: []const u8,
            cursor: ?[]const u8,
            limit: u32,
            result_buf: []ResultT,
            cursor_buf: []u8,
        ) WalkResult {
            // Decode starting position
            const start = if (cursor) |raw| Cursor.decode(raw) orelse Cursor{ .shard_id = 0, .local_cursor = null } else Cursor{ .shard_id = 0, .local_cursor = null };

            var remaining: u32 = limit;
            var current_shard = start.shard_id;
            var local_cursor = start.local_cursor;
            var collected: usize = 0;

            while (current_shard < self.shard_count and remaining > 0) {
                const ctx = contexts[@intCast(current_shard)];
                const scan = self.local_scan(ctx, namespace, filter, local_cursor, remaining);

                // Copy results into buffer (bounded by remaining to prevent underflow)
                const to_copy = @min(scan.items.len, @min(result_buf.len - collected, remaining));
                if (to_copy > 0) {
                    @memcpy(result_buf[collected .. collected + to_copy], scan.items[0..to_copy]);
                    collected += to_copy;
                    remaining -= @intCast(to_copy);
                }

                if (scan.next_cursor != null) {
                    // More data on this shard — return cursor
                    const encoded = (Cursor{
                        .shard_id = current_shard,
                        .local_cursor = scan.next_cursor,
                    }).encode(cursor_buf);

                    return .{
                        .items = result_buf[0..collected],
                        .next_cursor = encoded,
                    };
                }

                // Shard exhausted, move to next
                current_shard += 1;
                local_cursor = null;
            }

            // All shards exhausted
            return .{
                .items = result_buf[0..collected],
                .next_cursor = null,
            };
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

// Mock item type for testing
const TestItem = struct {
    key: [8]u8,
    shard: u16,

    fn fromInt(n: u32, shard: u16) TestItem {
        var key: [8]u8 = .{0} ** 8;
        std.mem.writeInt(u32, key[0..4], n, .little);
        return .{ .key = key, .shard = shard };
    }
};

// Mock scan context: holds a fixed array of items
const MockScanCtx = struct {
    items: []const TestItem,
    shard_id: u16,
};

fn mockLocalScan(ctx_raw: *anyopaque, _: []const u8, _: []const u8, local_cursor: ?[]const u8, limit: u32) ShardWalker(TestItem).ScanResult {
    const ctx: *const MockScanCtx = @ptrCast(@alignCast(ctx_raw));

    // Decode local cursor as a u32 offset
    var offset: u32 = 0;
    if (local_cursor) |lc| {
        if (lc.len >= 4) {
            offset = std.mem.readInt(u32, lc[0..4], .little);
        }
    }

    const start_idx: usize = @intCast(offset);
    if (start_idx >= ctx.items.len) {
        return .{ .items = &[_]TestItem{}, .next_cursor = null };
    }

    const end_idx = @min(start_idx + limit, ctx.items.len);
    const items = ctx.items[start_idx..end_idx];

    // If more items remain, encode next cursor
    const has_more = end_idx < ctx.items.len;
    if (has_more) {
        // We need a stable cursor — use a comptime-known static buffer pattern
        // For tests, return a pointer to a thread-local buffer
        const S = struct {
            threadlocal var cursor_buf: [4]u8 = undefined;
        };
        std.mem.writeInt(u32, &S.cursor_buf, @intCast(end_idx), .little);
        return .{ .items = items, .next_cursor = &S.cursor_buf };
    }

    return .{ .items = items, .next_cursor = null };
}

test "ShardWalker: cursor encode/decode roundtrip" {
    const Walker = ShardWalker(TestItem);
    var buf: [64]u8 = undefined;

    const cursor = Walker.Cursor{ .shard_id = 3, .local_cursor = "hello" };
    const encoded = cursor.encode(&buf);
    try std.testing.expect(encoded != null);

    const decoded = Walker.Cursor.decode(encoded.?);
    try std.testing.expect(decoded != null);
    try std.testing.expectEqual(@as(u16, 3), decoded.?.shard_id);
    try std.testing.expectEqualStrings("hello", decoded.?.local_cursor.?);
}

test "ShardWalker: cursor decode with no local cursor" {
    const Walker = ShardWalker(TestItem);
    var buf: [2]u8 = undefined;

    const cursor = Walker.Cursor{ .shard_id = 1, .local_cursor = null };
    const encoded = cursor.encode(&buf);
    try std.testing.expect(encoded != null);

    const decoded = Walker.Cursor.decode(encoded.?);
    try std.testing.expect(decoded != null);
    try std.testing.expectEqual(@as(u16, 1), decoded.?.shard_id);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.?.local_cursor);
}

test "ShardWalker: walk single shard, all items fit" {
    const Walker = ShardWalker(TestItem);

    const items = [_]TestItem{
        TestItem.fromInt(1, 0),
        TestItem.fromInt(2, 0),
        TestItem.fromInt(3, 0),
    };

    var ctx = MockScanCtx{ .items = &items, .shard_id = 0 };
    var contexts = [_]*anyopaque{@ptrCast(&ctx)};

    const walker = Walker.init(mockLocalScan, 1);
    var result_buf: [16]TestItem = undefined;
    var cursor_buf: [64]u8 = undefined;

    const result = walker.walk(&contexts, "", "", null, 10, &result_buf, &cursor_buf);

    try std.testing.expectEqual(@as(usize, 3), result.items.len);
    try std.testing.expectEqual(@as(?[]const u8, null), result.next_cursor);
}

test "ShardWalker: walk with pagination (limit < items)" {
    const Walker = ShardWalker(TestItem);

    const items = [_]TestItem{
        TestItem.fromInt(1, 0),
        TestItem.fromInt(2, 0),
        TestItem.fromInt(3, 0),
        TestItem.fromInt(4, 0),
        TestItem.fromInt(5, 0),
    };

    var ctx = MockScanCtx{ .items = &items, .shard_id = 0 };
    var contexts = [_]*anyopaque{@ptrCast(&ctx)};

    const walker = Walker.init(mockLocalScan, 1);
    var result_buf: [16]TestItem = undefined;
    var cursor_buf: [64]u8 = undefined;

    // First page: limit 3
    const page1 = walker.walk(&contexts, "", "", null, 3, &result_buf, &cursor_buf);
    try std.testing.expectEqual(@as(usize, 3), page1.items.len);
    try std.testing.expect(page1.next_cursor != null); // more items

    // Second page: use cursor
    var result_buf2: [16]TestItem = undefined;
    var cursor_buf2: [64]u8 = undefined;
    const page2 = walker.walk(&contexts, "", "", page1.next_cursor, 3, &result_buf2, &cursor_buf2);
    try std.testing.expectEqual(@as(usize, 2), page2.items.len); // remaining 2
    try std.testing.expectEqual(@as(?[]const u8, null), page2.next_cursor);
}

test "ShardWalker: walk multiple shards" {
    const Walker = ShardWalker(TestItem);

    const items0 = [_]TestItem{
        TestItem.fromInt(10, 0),
        TestItem.fromInt(11, 0),
    };
    const items1 = [_]TestItem{
        TestItem.fromInt(20, 1),
        TestItem.fromInt(21, 1),
        TestItem.fromInt(22, 1),
    };

    var ctx0 = MockScanCtx{ .items = &items0, .shard_id = 0 };
    var ctx1 = MockScanCtx{ .items = &items1, .shard_id = 1 };
    var contexts = [_]*anyopaque{ @ptrCast(&ctx0), @ptrCast(&ctx1) };

    const walker = Walker.init(mockLocalScan, 2);
    var result_buf: [16]TestItem = undefined;
    var cursor_buf: [64]u8 = undefined;

    // Get all items across both shards
    const result = walker.walk(&contexts, "", "", null, 10, &result_buf, &cursor_buf);
    try std.testing.expectEqual(@as(usize, 5), result.items.len);
    try std.testing.expectEqual(@as(?[]const u8, null), result.next_cursor);

    // Verify shard ordering: shard 0 items first, then shard 1
    try std.testing.expectEqual(@as(u16, 0), result.items[0].shard);
    try std.testing.expectEqual(@as(u16, 0), result.items[1].shard);
    try std.testing.expectEqual(@as(u16, 1), result.items[2].shard);
}

test "ShardWalker: walk multiple shards with limit across boundary" {
    const Walker = ShardWalker(TestItem);

    const items0 = [_]TestItem{
        TestItem.fromInt(10, 0),
        TestItem.fromInt(11, 0),
        TestItem.fromInt(12, 0),
    };
    const items1 = [_]TestItem{
        TestItem.fromInt(20, 1),
        TestItem.fromInt(21, 1),
    };

    var ctx0 = MockScanCtx{ .items = &items0, .shard_id = 0 };
    var ctx1 = MockScanCtx{ .items = &items1, .shard_id = 1 };
    var contexts = [_]*anyopaque{ @ptrCast(&ctx0), @ptrCast(&ctx1) };

    const walker = Walker.init(mockLocalScan, 2);
    var result_buf: [16]TestItem = undefined;
    var cursor_buf: [64]u8 = undefined;

    // Limit 4 — should get 3 from shard 0, 1 from shard 1
    const result = walker.walk(&contexts, "", "", null, 4, &result_buf, &cursor_buf);
    try std.testing.expectEqual(@as(usize, 4), result.items.len);

    // Next cursor should point to shard 1 with offset 1
    try std.testing.expect(result.next_cursor != null);

    // Continue from cursor
    var result_buf2: [16]TestItem = undefined;
    var cursor_buf2: [64]u8 = undefined;
    const page2 = walker.walk(&contexts, "", "", result.next_cursor, 10, &result_buf2, &cursor_buf2);
    try std.testing.expectEqual(@as(usize, 1), page2.items.len); // 1 remaining on shard 1
    try std.testing.expectEqual(@as(?[]const u8, null), page2.next_cursor);
}

test "ShardWalker: empty shards" {
    const Walker = ShardWalker(TestItem);

    const empty = [_]TestItem{};
    var ctx0 = MockScanCtx{ .items = &empty, .shard_id = 0 };
    var ctx1 = MockScanCtx{ .items = &empty, .shard_id = 1 };
    var contexts = [_]*anyopaque{ @ptrCast(&ctx0), @ptrCast(&ctx1) };

    const walker = Walker.init(mockLocalScan, 2);
    var result_buf: [16]TestItem = undefined;
    var cursor_buf: [64]u8 = undefined;

    const result = walker.walk(&contexts, "", "", null, 10, &result_buf, &cursor_buf);
    try std.testing.expectEqual(@as(usize, 0), result.items.len);
    try std.testing.expectEqual(@as(?[]const u8, null), result.next_cursor);
}
