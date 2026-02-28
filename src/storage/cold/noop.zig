//! Noop Backend - No-operation storage backend
//!
//! A backend implementation that discards all data. Used for:
//! - Testing without actual storage
//! - Benchmarking (measure overhead without I/O)
//! - Development when cold storage is disabled
//!
//! All uploads succeed but data is discarded.
//! All downloads fail with ObjectNotFound.

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

/// Noop backend that discards all data
///
/// Useful for testing, benchmarking, or disabling cold storage.
/// Tracks operation counts for verification in tests.
pub const NoopBackend = struct {
    allocator: Allocator,

    /// Number of upload operations (for testing verification)
    upload_count: std.atomic.Value(u64),

    /// Number of download attempts (for testing verification)
    download_count: std.atomic.Value(u64),

    /// Number of delete operations (for testing verification)
    delete_count: std.atomic.Value(u64),

    /// Total bytes "uploaded" (discarded)
    bytes_uploaded: std.atomic.Value(u64),

    const Self = @This();

    /// Initialize a new NoopBackend
    pub fn init(allocator: Allocator) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .upload_count = std.atomic.Value(u64).init(0),
            .download_count = std.atomic.Value(u64).init(0),
            .delete_count = std.atomic.Value(u64).init(0),
            .bytes_uploaded = std.atomic.Value(u64).init(0),
        };
        return self;
    }

    /// Cleanup resources
    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self);
    }

    /// Get the ColdBackend interface
    pub fn asBackend(self: *Self) ColdBackend {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Get upload count (for testing)
    pub fn getUploadCount(self: *const Self) u64 {
        return self.upload_count.load(.monotonic);
    }

    /// Get download count (for testing)
    pub fn getDownloadCount(self: *const Self) u64 {
        return self.download_count.load(.monotonic);
    }

    /// Get delete count (for testing)
    pub fn getDeleteCount(self: *const Self) u64 {
        return self.delete_count.load(.monotonic);
    }

    /// Get total bytes uploaded (for testing)
    pub fn getBytesUploaded(self: *const Self) u64 {
        return self.bytes_uploaded.load(.monotonic);
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

    /// Streaming upload - reads and discards all data from source
    fn uploadStreamImpl(ptr: *anyopaque, _: []const u8, source: *const StreamSource, _: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = self.upload_count.fetchAdd(1, .monotonic);

        // Read and discard all data from source (memory-bounded)
        var discard_buf: [DEFAULT_CHUNK_SIZE]u8 = undefined;
        var total_bytes: u64 = 0;

        while (true) {
            const n = try source.read(&discard_buf);
            if (n == 0) break;
            total_bytes += n;
        }

        _ = self.bytes_uploaded.fetchAdd(total_bytes, .monotonic);
    }

    /// Streaming download - nothing is stored, always fails
    fn downloadStreamImpl(ptr: *anyopaque, _: []const u8, _: *const StreamSink) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = self.download_count.fetchAdd(1, .monotonic);
        return error.ObjectNotFound;
    }

    /// Simple upload - data is discarded
    fn uploadImpl(ptr: *anyopaque, _: []const u8, data: []const u8, _: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = self.upload_count.fetchAdd(1, .monotonic);
        _ = self.bytes_uploaded.fetchAdd(data.len, .monotonic);
        // Data is discarded - noop
    }

    fn downloadImpl(ptr: *anyopaque, _: []const u8, _: []u8) BackendError![]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = self.download_count.fetchAdd(1, .monotonic);
        // Nothing is ever stored, so nothing can be downloaded
        return error.ObjectNotFound;
    }

    fn existsImpl(_: *anyopaque, _: []const u8) BackendError!bool {
        // Nothing is ever stored
        return false;
    }

    fn deleteImpl(ptr: *anyopaque, _: []const u8) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = self.delete_count.fetchAdd(1, .monotonic);
        // Nothing to delete - noop
    }

    fn listImpl(_: *anyopaque, _: []const u8, _: Allocator) BackendError![]ObjectInfo {
        // Nothing is ever stored
        return &.{};
    }

    fn headImpl(_: *anyopaque, _: []const u8) BackendError!?ObjectMetadata {
        // Nothing is ever stored
        return null;
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "NoopBackend: upload discards data" {
    const allocator = testing.allocator;

    var nb = try NoopBackend.init(allocator);
    defer nb.deinit();

    const cb = nb.asBackend();

    // Upload some data
    try cb.upload("test/key.dat", "Hello, World!", null);

    // Verify counts
    try testing.expectEqual(@as(u64, 1), nb.getUploadCount());
    try testing.expectEqual(@as(u64, 13), nb.getBytesUploaded());

    // Upload more
    try cb.upload("another/key.dat", "More data", null);
    try testing.expectEqual(@as(u64, 2), nb.getUploadCount());
}

test "NoopBackend: download fails" {
    const allocator = testing.allocator;

    var nb = try NoopBackend.init(allocator);
    defer nb.deinit();

    const cb = nb.asBackend();

    // Upload something
    try cb.upload("test/key.dat", "data", null);

    // Download should fail - data was discarded
    var buffer: [100]u8 = undefined;
    const result = cb.download("test/key.dat", &buffer);
    try testing.expectError(error.ObjectNotFound, result);
}

test "NoopBackend: exists returns false" {
    const allocator = testing.allocator;

    var nb = try NoopBackend.init(allocator);
    defer nb.deinit();

    const cb = nb.asBackend();

    try cb.upload("test/key.dat", "data", null);

    // Even after upload, exists returns false (data was discarded)
    const exists = try cb.exists("test/key.dat");
    try testing.expect(!exists);
}

test "NoopBackend: streaming upload" {
    const allocator = testing.allocator;

    var nb = try NoopBackend.init(allocator);
    defer nb.deinit();

    const cb = nb.asBackend();

    // Create a stream source
    var source = backend.SliceStreamSource.init("Streaming data test!");
    const ss = source.asStreamSource();

    try cb.uploadStream("stream/key.dat", &ss, null);

    try testing.expectEqual(@as(u64, 1), nb.getUploadCount());
    try testing.expectEqual(@as(u64, 20), nb.getBytesUploaded());
}
