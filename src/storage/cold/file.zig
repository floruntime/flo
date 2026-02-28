//! File Backend - Local filesystem storage backend
//!
//! A backend implementation that stores data on the local filesystem.
//! Used for:
//! - Testing and development
//! - NFS/shared filesystem archival
//! - Local backup strategies
//!
//! Features:
//! - Atomic writes (write to .tmp, then rename)
//! - Automatic directory creation
//! - Full round-trip support (upload/download)

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend = @import("backend.zig");
const ColdBackend = backend.ColdBackend;
const ObjectMetadata = backend.ObjectMetadata;
const ObjectInfo = backend.ObjectInfo;
const BackendError = backend.BackendError;
const StreamSource = backend.StreamSource;
const StreamSink = backend.StreamSink;
const DEFAULT_CHUNK_SIZE = backend.DEFAULT_CHUNK_SIZE;

/// Configuration for file backend
pub const FileConfig = struct {
    /// Base path for storing objects
    base_path: []const u8,

    /// Whether to create directories automatically
    create_dirs: bool = true,

    /// Whether to sync to disk on write
    sync_on_write: bool = true,
};

/// File backend that stores data on local filesystem
///
/// Objects are stored as files at: {base_path}/{key}
/// Atomic writes ensure no partial files on crash.
pub const FileBackend = struct {
    allocator: Allocator,
    cfg: FileConfig,
    base_path: []const u8,

    const Self = @This();

    /// Maximum path length
    const MAX_PATH_LEN = 4096;

    /// Initialize a new FileBackend
    pub fn init(allocator: Allocator, file_config: FileConfig) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Duplicate base_path for ownership
        const base_path = try allocator.dupe(u8, file_config.base_path);
        errdefer allocator.free(base_path);

        self.* = .{
            .allocator = allocator,
            .cfg = file_config,
            .base_path = base_path,
        };

        // Create base directory if requested
        if (file_config.create_dirs) {
            std.fs.cwd().makePath(base_path) catch |err| {
                // Ignore if already exists
                if (err != error.PathAlreadyExists) {
                    allocator.free(base_path);
                    allocator.destroy(self);
                    return error.IoError;
                }
            };
        }

        return self;
    }

    /// Cleanup resources
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.base_path);
        self.allocator.destroy(self);
    }

    /// Get the ColdBackend interface
    pub fn asBackend(self: *Self) ColdBackend {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Build full path for a key
    fn buildPath(self: *const Self, key: []const u8, buf: *[MAX_PATH_LEN]u8) BackendError![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}", .{ self.base_path, key }) catch {
            return error.InvalidKey;
        };
    }

    /// Build temp path for atomic write
    fn buildTempPath(self: *const Self, key: []const u8, buf: *[MAX_PATH_LEN]u8) BackendError![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}.tmp", .{ self.base_path, key }) catch {
            return error.InvalidKey;
        };
    }

    /// Ensure parent directory exists
    fn ensureParentDir(full_path: []const u8) BackendError!void {
        if (std.fs.path.dirname(full_path)) |dir| {
            std.fs.cwd().makePath(dir) catch |err| {
                if (err != error.PathAlreadyExists) {
                    return error.IoError;
                }
            };
        }
    }

    // ========================================================================
    // VTable Implementation
    // ========================================================================

    const vtable = ColdBackend.VTable{
        .upload_stream = uploadStreamImpl,
        .download_stream = downloadStreamImpl,
        .upload = uploadImpl,
        .download = downloadImpl,
        .exists = existsImpl,
        .delete = deleteImpl,
        .list = listImpl,
        .head = headImpl,
        .deinit = deinitImpl,
    };

    /// Streaming upload - reads from source in chunks, writes to file
    /// Memory bounded: only DEFAULT_CHUNK_SIZE bytes in RAM at a time
    fn uploadStreamImpl(ptr: *anyopaque, key: []const u8, source: *const StreamSource, _: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        var tmp_buf: [MAX_PATH_LEN]u8 = undefined;

        const full_path = try self.buildPath(key, &path_buf);
        const tmp_path = try self.buildTempPath(key, &tmp_buf);

        // Ensure parent directory exists
        if (self.cfg.create_dirs) {
            try ensureParentDir(full_path);
        }

        // Write to temp file first (atomic write pattern)
        const file = std.fs.cwd().createFile(tmp_path, .{}) catch {
            return error.IoError;
        };
        defer file.close();

        // Stream data in chunks - memory bounded
        var chunk_buf: [DEFAULT_CHUNK_SIZE]u8 = undefined;
        while (true) {
            const n = try source.read(&chunk_buf);
            if (n == 0) break;

            file.writeAll(chunk_buf[0..n]) catch {
                std.fs.cwd().deleteFile(tmp_path) catch {};
                return error.IoError;
            };
        }

        // Sync to disk if configured
        if (self.cfg.sync_on_write) {
            file.sync() catch {
                std.fs.cwd().deleteFile(tmp_path) catch {};
                return error.IoError;
            };
        }

        // Atomic rename to final path
        std.fs.cwd().rename(tmp_path, full_path) catch {
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return error.IoError;
        };
    }

    /// Streaming download - reads file in chunks, writes to sink
    /// Memory bounded: only DEFAULT_CHUNK_SIZE bytes in RAM at a time
    fn downloadStreamImpl(ptr: *anyopaque, key: []const u8, sink: *const StreamSink) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const full_path = try self.buildPath(key, &path_buf);

        const file = std.fs.cwd().openFile(full_path, .{}) catch |err| {
            return switch (err) {
                error.FileNotFound => error.ObjectNotFound,
                error.AccessDenied => error.AccessDenied,
                else => error.IoError,
            };
        };
        defer file.close();

        // Stream data in chunks - memory bounded
        var chunk_buf: [DEFAULT_CHUNK_SIZE]u8 = undefined;
        while (true) {
            const n = file.read(&chunk_buf) catch {
                return error.IoError;
            };
            if (n == 0) break;

            try sink.write(chunk_buf[0..n]);
        }
    }

    fn uploadImpl(ptr: *anyopaque, key: []const u8, data: []const u8, _: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        var tmp_buf: [MAX_PATH_LEN]u8 = undefined;

        const full_path = try self.buildPath(key, &path_buf);
        const tmp_path = try self.buildTempPath(key, &tmp_buf);

        // Ensure parent directory exists
        if (self.cfg.create_dirs) {
            try ensureParentDir(full_path);
        }

        // Write to temp file first (atomic write pattern)
        const file = std.fs.cwd().createFile(tmp_path, .{}) catch {
            return error.IoError;
        };
        defer file.close();

        file.writeAll(data) catch {
            // Clean up temp file on error
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return error.IoError;
        };

        // Sync to disk if configured
        if (self.cfg.sync_on_write) {
            file.sync() catch {
                std.fs.cwd().deleteFile(tmp_path) catch {};
                return error.IoError;
            };
        }

        // Atomic rename to final path
        std.fs.cwd().rename(tmp_path, full_path) catch {
            std.fs.cwd().deleteFile(tmp_path) catch {};
            return error.IoError;
        };
    }

    fn downloadImpl(ptr: *anyopaque, key: []const u8, buffer: []u8) BackendError![]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const full_path = try self.buildPath(key, &path_buf);

        const file = std.fs.cwd().openFile(full_path, .{}) catch |err| {
            return switch (err) {
                error.FileNotFound => error.ObjectNotFound,
                error.AccessDenied => error.AccessDenied,
                else => error.IoError,
            };
        };
        defer file.close();

        // Get file size to check buffer
        const stat = file.stat() catch {
            return error.IoError;
        };

        if (stat.size > buffer.len) {
            return error.BufferTooSmall;
        }

        const bytes_read = file.readAll(buffer) catch {
            return error.IoError;
        };

        return buffer[0..bytes_read];
    }

    fn existsImpl(ptr: *anyopaque, key: []const u8) BackendError!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const full_path = try self.buildPath(key, &path_buf);

        std.fs.cwd().access(full_path, .{}) catch |err| {
            return switch (err) {
                error.FileNotFound => false,
                else => error.IoError,
            };
        };

        return true;
    }

    fn deleteImpl(ptr: *anyopaque, key: []const u8) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const full_path = try self.buildPath(key, &path_buf);

        std.fs.cwd().deleteFile(full_path) catch |err| {
            return switch (err) {
                error.FileNotFound => error.ObjectNotFound,
                error.AccessDenied => error.AccessDenied,
                else => error.IoError,
            };
        };
    }

    fn listImpl(ptr: *anyopaque, prefix: []const u8, allocator: Allocator) BackendError![]ObjectInfo {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const search_path = try self.buildPath(prefix, &path_buf);

        // Determine if prefix ends with slash (meaning list directory contents)
        const prefix_ends_with_slash = prefix.len > 0 and prefix[prefix.len - 1] == '/';

        // Find the directory to search and the file prefix filter
        var dir_path: []const u8 = undefined;
        var file_prefix: []const u8 = undefined;

        if (prefix_ends_with_slash) {
            // "flash/" -> list all files in flash/ directory
            dir_path = std.mem.trimRight(u8, search_path, "/");
            file_prefix = "";
        } else {
            // "flash/segment-" -> list files in flash/ starting with "segment-"
            dir_path = std.fs.path.dirname(search_path) orelse self.base_path;
            file_prefix = std.fs.path.basename(search_path);
        }

        var results: std.ArrayListUnmanaged(ObjectInfo) = .empty;
        errdefer {
            for (results.items) |item| {
                allocator.free(item.key);
            }
            results.deinit(allocator);
        }

        // Open directory
        var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| {
            return switch (err) {
                error.FileNotFound => (allocator.alloc(ObjectInfo, 0) catch return error.OutOfMemory),
                error.NotDir => (allocator.alloc(ObjectInfo, 0) catch return error.OutOfMemory),
                else => error.IoError,
            };
        };
        defer dir.close();

        // Iterate and collect matching entries
        var iter = dir.iterate();
        while (iter.next() catch return error.IoError) |entry| {
            // Skip directories, only list files
            if (entry.kind == .directory) continue;

            // Check if name starts with prefix (if prefix is specified)
            if (file_prefix.len > 0 and !std.mem.startsWith(u8, entry.name, file_prefix)) {
                continue;
            }

            // Build the full key (relative to base_path)
            var key_buf: [MAX_PATH_LEN]u8 = undefined;
            const relative_dir = std.mem.trimLeft(u8, dir_path[self.base_path.len..], "/");
            const key = if (relative_dir.len > 0)
                std.fmt.bufPrint(&key_buf, "{s}/{s}", .{ relative_dir, entry.name }) catch continue
            else
                std.fmt.bufPrint(&key_buf, "{s}", .{entry.name}) catch continue;

            // Get file metadata
            var full_file_path_buf: [MAX_PATH_LEN]u8 = undefined;
            const full_file_path = std.fmt.bufPrint(&full_file_path_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;

            const stat = std.fs.cwd().statFile(full_file_path) catch continue;

            const key_dupe = allocator.dupe(u8, key) catch return error.OutOfMemory;
            errdefer allocator.free(key_dupe);

            results.append(allocator, .{
                .key = key_dupe,
                .size = stat.size,
                .last_modified = @intCast(@divFloor(stat.mtime, std.time.ns_per_ms)),
            }) catch return error.OutOfMemory;
        }

        return results.toOwnedSlice(allocator) catch return error.OutOfMemory;
    }

    fn headImpl(ptr: *anyopaque, key: []const u8) BackendError!?ObjectMetadata {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const full_path = try self.buildPath(key, &path_buf);

        const stat = std.fs.cwd().statFile(full_path) catch |err| {
            return switch (err) {
                error.FileNotFound => null,
                else => error.IoError,
            };
        };

        return ObjectMetadata{
            .size = stat.size,
            .last_modified = @intCast(@divFloor(stat.mtime, std.time.ns_per_ms)),
        };
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    /// Free a list of ObjectInfo returned by list()
    pub fn freeObjectInfoList(infos: []ObjectInfo, allocator: Allocator) void {
        for (infos) |info| {
            allocator.free(info.key);
        }
        allocator.free(infos);
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "FileBackend: upload and download round-trip" {
    const allocator = testing.allocator;

    // Clean up test directory
    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Upload
    const test_data = "hello world, this is test data!";
    try cb.upload("test/key.dat", test_data, null);

    // Download
    var buffer: [1024]u8 = undefined;
    const data = try cb.download("test/key.dat", &buffer);

    try testing.expectEqualStrings(test_data, data);
}

test "FileBackend: exists" {
    const allocator = testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_exists") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_exists") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend_exists",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Should not exist initially
    try testing.expect(!try cb.exists("mykey.dat"));

    // Upload
    try cb.upload("mykey.dat", "data", null);

    // Should exist now
    try testing.expect(try cb.exists("mykey.dat"));

    // Different key should not exist
    try testing.expect(!try cb.exists("otherkey.dat"));
}

test "FileBackend: delete" {
    const allocator = testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_delete") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_delete") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend_delete",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Upload
    try cb.upload("to_delete.dat", "data", null);
    try testing.expect(try cb.exists("to_delete.dat"));

    // Delete
    try cb.delete("to_delete.dat");

    // Should not exist anymore
    try testing.expect(!try cb.exists("to_delete.dat"));

    // Deleting non-existent file should error
    try testing.expectError(error.ObjectNotFound, cb.delete("nonexistent.dat"));
}

test "FileBackend: head metadata" {
    const allocator = testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_head") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_head") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend_head",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Head on non-existent file
    const meta_none = try cb.head("nonexistent.dat");
    try testing.expect(meta_none == null);

    // Upload and check metadata
    const test_data = "Test data for head operation";
    try cb.upload("headtest.dat", test_data, null);

    const meta = try cb.head("headtest.dat");
    try testing.expect(meta != null);
    try testing.expectEqual(test_data.len, meta.?.size);
    try testing.expect(meta.?.last_modified > 0);
}

test "FileBackend: streaming upload and download" {
    const allocator = testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_stream") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_stream") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend_stream",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Streaming upload
    const test_data = "Streaming test data - larger payload for streaming test";
    var source = backend.SliceStreamSource.init(test_data);
    const ss = source.asStreamSource();
    try cb.uploadStream("stream_test.dat", &ss, null);

    // Streaming download
    var sink = backend.BufferStreamSink.init(allocator);
    defer sink.deinit();
    const stream_sink = sink.asStreamSink();

    try cb.downloadStream("stream_test.dat", &stream_sink);

    try testing.expectEqualStrings(test_data, sink.getData());
}

test "FileBackend: list objects" {
    const allocator = testing.allocator;

    std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_list") catch {};
    defer std.fs.cwd().deleteTree("/tmp/test_cold_file_backend_list") catch {};

    var fb = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_file_backend_list",
    });
    defer fb.deinit();

    const cb = fb.asBackend();

    // Create multiple files
    try cb.upload("segments/seg-0001.dat", "data1", null);
    try cb.upload("segments/seg-0002.dat", "data2", null);
    try cb.upload("segments/seg-0003.dat", "data3", null);
    try cb.upload("other/file.dat", "other data", null);

    // List with prefix
    const list_result = try cb.list("segments/", allocator);
    defer FileBackend.freeObjectInfoList(list_result, allocator);

    try testing.expectEqual(@as(usize, 3), list_result.len);
}
