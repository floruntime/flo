//! KV WAL — Simple append-only Write-Ahead Log for KV persistence
//!
//! Records every KV put and delete operation as a binary record.
//! On server restart, the WAL is replayed to rebuild the KVProjection.
//!
//! ## Record Format (19-byte header + key + value)
//!
//! ```
//! magic:      u32  (0x464C4F57 "FLOW")
//! op:         u8   (1=PUT, 2=DELETE)
//! key_len:    u16  (key length in bytes)
//! val_len:    u32  (value length, 0 for DELETE)
//! expiry_ns:  u64  (TTL expiry nanos, 0 = no expiry)
//! key:        [key_len]u8
//! value:      [val_len]u8
//! ```
//!
//! On replay, if a header is truncated (partial write before crash),
//! replay stops at the truncation point. All records before it are valid.

const std = @import("std");

// ═══════════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════════

const WAL_MAGIC: u32 = 0x464C4F57; // "FLOW" in little-endian
const WAL_HEADER_SIZE: usize = 19; // 4 + 1 + 2 + 4 + 8
const WAL_OP_PUT: u8 = 1;
const WAL_OP_DELETE: u8 = 2;

// ═══════════════════════════════════════════════════════════════════════════════
// KVWAL
// ═══════════════════════════════════════════════════════════════════════════════

pub const KVWAL = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,

    /// Open or create a WAL file at the given path.
    /// If the file exists, it is opened for appending (not truncated).
    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !KVWAL {
        // createFile with truncate=false: creates if missing, opens if exists
        const file = try std.fs.cwd().createFile(wal_path, .{
            .read = true,
            .truncate = false,
        });
        // Seek to end for appending new records
        try file.seekFromEnd(0);

        return .{
            .file = file,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *KVWAL) void {
        self.file.close();
    }

    /// Append a PUT record to the WAL.
    pub fn appendPut(self: *KVWAL, key: []const u8, value: []const u8, expiry_ns: u64) !void {
        try self.writeRecord(WAL_OP_PUT, key, value, expiry_ns);
    }

    /// Append a DELETE record to the WAL.
    pub fn appendDelete(self: *KVWAL, key: []const u8) !void {
        try self.writeRecord(WAL_OP_DELETE, key, &.{}, 0);
    }

    /// Flush the WAL to disk (fsync).
    pub fn sync(self: *KVWAL) void {
        self.file.sync() catch {};
    }

    /// Write a single WAL record.
    fn writeRecord(self: *KVWAL, op: u8, key: []const u8, value: []const u8, expiry_ns: u64) !void {
        const key_len: u16 = @intCast(key.len);
        const val_len: u32 = @intCast(value.len);

        // Build header
        var hdr: [WAL_HEADER_SIZE]u8 = undefined;
        std.mem.writeInt(u32, hdr[0..4], WAL_MAGIC, .little);
        hdr[4] = op;
        std.mem.writeInt(u16, hdr[5..7], key_len, .little);
        std.mem.writeInt(u32, hdr[7..11], val_len, .little);
        std.mem.writeInt(u64, hdr[11..19], expiry_ns, .little);

        // Write header + key + value using writev for atomicity
        var iovecs = [_]std.posix.iovec_const{
            .{ .base = &hdr, .len = WAL_HEADER_SIZE },
            .{ .base = key.ptr, .len = key.len },
            .{ .base = value.ptr, .len = value.len },
        };
        _ = try self.file.writev(&iovecs);
    }

    /// Replay the WAL, calling `putFn` for each PUT and `deleteFn` for each DELETE.
    /// Returns the number of records successfully replayed.
    pub fn replay(self: *KVWAL, context: anytype, putFn: anytype, deleteFn: anytype) !u64 {
        // Seek to beginning for replay
        try self.file.seekTo(0);

        var replay_count: u64 = 0;

        while (true) {
            // Read header
            var hdr: [WAL_HEADER_SIZE]u8 = undefined;
            const hdr_read = self.file.readAll(&hdr) catch break;
            if (hdr_read < WAL_HEADER_SIZE) break;

            // Validate magic
            const magic = std.mem.readInt(u32, hdr[0..4], .little);
            if (magic != WAL_MAGIC) break;

            const op = hdr[4];
            const key_len = std.mem.readInt(u16, hdr[5..7], .little);
            const val_len = std.mem.readInt(u32, hdr[7..11], .little);
            const expiry_ns = std.mem.readInt(u64, hdr[11..19], .little);

            // Read key + value as a single allocation
            const total: usize = @as(usize, key_len) + @as(usize, val_len);
            if (total == 0) break; // Degenerate record

            const payload = self.allocator.alloc(u8, total) catch break;
            defer self.allocator.free(payload);

            const payload_read = self.file.readAll(payload) catch break;
            if (payload_read < total) break;

            const key = payload[0..key_len];
            const value = if (val_len > 0) payload[key_len..][0..val_len] else &[_]u8{};

            if (op == WAL_OP_PUT) {
                replay_count += 1;
                putFn(context, key, value, replay_count, expiry_ns) catch continue;
            } else if (op == WAL_OP_DELETE) {
                replay_count += 1;
                deleteFn(context, key, replay_count) catch continue;
            }
            // Unknown op → skip (but we already consumed the bytes)
        }

        // Seek back to end for future appends
        try self.file.seekFromEnd(0);

        return replay_count;
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

test "kv_wal: write and replay" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(path);
    const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/test.wal", .{path});
    defer testing.allocator.free(wal_path);

    // Write some records
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        try wal.appendPut("key1", "value1", 0);
        try wal.appendPut("key2", "value2", 1000);
        try wal.appendDelete("key1");
        try wal.appendPut("key3", "value3", 0);
        wal.sync();
    }

    // Replay
    {
        var wal = try KVWAL.init(testing.allocator, wal_path);
        defer wal.deinit();

        var puts: u64 = 0;
        var deletes: u64 = 0;

        const ctx = &.{ &puts, &deletes };

        const count = try wal.replay(ctx, struct {
            fn put(c: @TypeOf(ctx), _key: []const u8, _value: []const u8, _lsn: u64, _expiry: u64) !void {
                _ = _key;
                _ = _value;
                _ = _lsn;
                _ = _expiry;
                c[0].* += 1;
            }
        }.put, struct {
            fn del(c: @TypeOf(ctx), _key: []const u8, _lsn: u64) !void {
                _ = _key;
                _ = _lsn;
                c[1].* += 1;
            }
        }.del);

        try testing.expectEqual(@as(u64, 4), count);
        try testing.expectEqual(@as(u64, 3), puts);
        try testing.expectEqual(@as(u64, 1), deletes);
    }
}
