//! Cold Storage Backend Interface
//!
//! Provides a pluggable interface for remote storage backends.
//! Supports S3-compatible storage, Azure Blob, local filesystem, and noop (testing).
//!
//! Design Principles:
//! - Vtable-based polymorphism for zero-cost abstraction
//! - All backends implement the same interface
//! - **Memory-bounded operations** - streaming upload/download
//! - Fixed-size chunk processing (~5MB) regardless of file size

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Default chunk size for streaming operations (5MB)
/// Memory per operation: ~2 × CHUNK_SIZE (read + write buffers)
pub const DEFAULT_CHUNK_SIZE: usize = 5 * 1024 * 1024;

/// Object metadata for uploads and HEAD requests
pub const ObjectMetadata = struct {
    /// Size in bytes
    size: u64,
    /// SHA-256 checksum (optional)
    checksum: ?[32]u8 = null,
    /// Content type (e.g., "application/octet-stream")
    content_type: ?[]const u8 = null,
    /// Last modified timestamp (milliseconds since epoch)
    last_modified: i64 = 0,
};

/// Information about a listed object
pub const ObjectInfo = struct {
    /// Object key
    key: []const u8,
    /// Size in bytes
    size: u64,
    /// Last modified timestamp (milliseconds since epoch)
    last_modified: i64,
};

/// Backend error types
pub const BackendError = error{
    /// Object not found in storage
    ObjectNotFound,
    /// Access denied (permissions issue)
    AccessDenied,
    /// Storage backend unavailable
    ServiceUnavailable,
    /// Network error during operation
    NetworkError,
    /// Checksum mismatch
    ChecksumMismatch,
    /// Buffer too small for download
    BufferTooSmall,
    /// Invalid key format
    InvalidKey,
    /// Backend not initialized
    NotInitialized,
    /// Operation timed out
    Timeout,
    /// Generic I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// End of stream reached
    EndOfStream,
    /// Read error from source
    ReadError,
    /// Invalid response from server
    InvalidResponse,
    /// No credentials available
    NoCredentials,
    /// Credentials expired
    CredentialsExpired,
    /// Operation not implemented
    NotImplemented,
};

/// Read function callback for streaming uploads
/// Returns number of bytes read (0 = end of stream)
pub const ReadFn = *const fn (
    ctx: *anyopaque,
    buffer: []u8,
) BackendError!usize;

/// Write function callback for streaming downloads
/// Called repeatedly with chunks of data
pub const WriteFn = *const fn (
    ctx: *anyopaque,
    data: []const u8,
) BackendError!void;

/// Stream source for uploads
/// Wraps a read function and context pointer for vtable compatibility
pub const StreamSource = struct {
    /// Context for the read function
    ctx: *anyopaque,
    /// Read function - fills buffer, returns bytes read (0 = EOF)
    read_fn: ReadFn,
    /// Total size if known (required for S3, optional for file backend)
    total_size: ?u64,

    /// Read from the stream source
    pub fn read(self: *const StreamSource, buffer: []u8) BackendError!usize {
        return self.read_fn(self.ctx, buffer);
    }
};

/// Stream sink for downloads
/// Wraps a write function and context pointer for vtable compatibility
pub const StreamSink = struct {
    /// Context for the write function
    ctx: *anyopaque,
    /// Write function - receives chunks of data
    write_fn: WriteFn,

    /// Write to the stream sink
    pub fn write(self: *const StreamSink, data: []const u8) BackendError!void {
        return self.write_fn(self.ctx, data);
    }
};

/// Helper: StreamSource that reads from a slice
pub const SliceStreamSource = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) SliceStreamSource {
        return .{ .data = data };
    }

    pub fn asStreamSource(self: *SliceStreamSource) StreamSource {
        return .{
            .ctx = self,
            .read_fn = readImpl,
            .total_size = self.data.len,
        };
    }

    fn readImpl(ctx: *anyopaque, buffer: []u8) BackendError!usize {
        const self: *SliceStreamSource = @ptrCast(@alignCast(ctx));
        if (self.pos >= self.data.len) return 0;

        const remaining = self.data.len - self.pos;
        const to_read = @min(remaining, buffer.len);
        @memcpy(buffer[0..to_read], self.data[self.pos..][0..to_read]);
        self.pos += to_read;
        return to_read;
    }
};

/// Helper: StreamSink that writes to a growable buffer
pub const BufferStreamSink = struct {
    allocator: Allocator,
    buffer: std.ArrayList(u8),

    pub fn init(allocator: Allocator) BufferStreamSink {
        return .{
            .allocator = allocator,
            .buffer = .empty,
        };
    }

    pub fn deinit(self: *BufferStreamSink) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn asStreamSink(self: *BufferStreamSink) StreamSink {
        return .{
            .ctx = self,
            .write_fn = writeImpl,
        };
    }

    pub fn getData(self: *const BufferStreamSink) []const u8 {
        return self.buffer.items;
    }

    fn writeImpl(ctx: *anyopaque, data: []const u8) BackendError!void {
        const self: *BufferStreamSink = @ptrCast(@alignCast(ctx));
        self.buffer.appendSlice(self.allocator, data) catch return error.OutOfMemory;
    }
};

/// Cold storage backend interface
///
/// This is a vtable-based interface allowing pluggable storage backends.
/// All operations are synchronous (caller handles async via thread pool if needed).
///
/// **Streaming Design**: Upload and download use streaming interfaces to avoid
/// loading entire files into RAM. This prevents memory spikes with large segments.
pub const ColdBackend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Stream data to remote storage (memory-bounded)
        /// Reads from source in chunks, never loads full file into RAM
        upload_stream: *const fn (
            ptr: *anyopaque,
            key: []const u8,
            source: *const StreamSource,
            metadata: ?ObjectMetadata,
        ) BackendError!void,

        /// Stream data from remote storage (memory-bounded)
        /// Writes to sink in chunks, never loads full file into RAM
        download_stream: *const fn (
            ptr: *anyopaque,
            key: []const u8,
            sink: *const StreamSink,
        ) BackendError!void,

        /// Upload data to remote storage (convenience for small data)
        /// For data that fits in memory - delegates to upload_stream internally
        upload: *const fn (
            ptr: *anyopaque,
            key: []const u8,
            data: []const u8,
            metadata: ?ObjectMetadata,
        ) BackendError!void,

        /// Download data from remote storage into provided buffer
        /// For data that fits in memory - delegates to download_stream internally
        download: *const fn (
            ptr: *anyopaque,
            key: []const u8,
            buffer: []u8,
        ) BackendError![]u8,

        /// Check if object exists
        exists: *const fn (
            ptr: *anyopaque,
            key: []const u8,
        ) BackendError!bool,

        /// Delete object
        delete: *const fn (
            ptr: *anyopaque,
            key: []const u8,
        ) BackendError!void,

        /// List objects with prefix
        /// Caller owns returned slice and must free with allocator
        list: *const fn (
            ptr: *anyopaque,
            prefix: []const u8,
            allocator: Allocator,
        ) BackendError![]ObjectInfo,

        /// Get object metadata without downloading content
        head: *const fn (
            ptr: *anyopaque,
            key: []const u8,
        ) BackendError!?ObjectMetadata,

        /// Cleanup resources
        deinit: *const fn (ptr: *anyopaque) void,
    };

    // ========================================================================
    // Public API (delegates to vtable)
    // ========================================================================

    /// Stream data to storage (memory-bounded)
    pub fn uploadStream(
        self: ColdBackend,
        key: []const u8,
        source: *const StreamSource,
        metadata: ?ObjectMetadata,
    ) BackendError!void {
        return self.vtable.upload_stream(self.ptr, key, source, metadata);
    }

    /// Stream data from storage (memory-bounded)
    pub fn downloadStream(
        self: ColdBackend,
        key: []const u8,
        sink: *const StreamSink,
    ) BackendError!void {
        return self.vtable.download_stream(self.ptr, key, sink);
    }

    /// Upload data (convenience for small data)
    pub fn upload(
        self: ColdBackend,
        key: []const u8,
        data: []const u8,
        metadata: ?ObjectMetadata,
    ) BackendError!void {
        return self.vtable.upload(self.ptr, key, data, metadata);
    }

    /// Download data into buffer
    pub fn download(
        self: ColdBackend,
        key: []const u8,
        buffer: []u8,
    ) BackendError![]u8 {
        return self.vtable.download(self.ptr, key, buffer);
    }

    /// Check if object exists
    pub fn exists(self: ColdBackend, key: []const u8) BackendError!bool {
        return self.vtable.exists(self.ptr, key);
    }

    /// Delete object
    pub fn delete(self: ColdBackend, key: []const u8) BackendError!void {
        return self.vtable.delete(self.ptr, key);
    }

    /// List objects with prefix
    pub fn list(
        self: ColdBackend,
        prefix: []const u8,
        allocator: Allocator,
    ) BackendError![]ObjectInfo {
        return self.vtable.list(self.ptr, prefix, allocator);
    }

    /// Get object metadata
    pub fn head(self: ColdBackend, key: []const u8) BackendError!?ObjectMetadata {
        return self.vtable.head(self.ptr, key);
    }

    /// Cleanup backend resources
    pub fn deinitBackend(self: ColdBackend) void {
        self.vtable.deinit(self.ptr);
    }
};

/// Free a list of ObjectInfo returned by list()
pub fn freeObjectInfoList(allocator: Allocator, list_items: []ObjectInfo) void {
    for (list_items) |item| {
        allocator.free(item.key);
    }
    allocator.free(list_items);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "SliceStreamSource: reads data in chunks" {
    var source = SliceStreamSource.init("Hello, World!");
    const ss = source.asStreamSource();

    var buf: [5]u8 = undefined;

    // Read first chunk
    const n1 = try ss.read(&buf);
    try testing.expectEqual(@as(usize, 5), n1);
    try testing.expectEqualStrings("Hello", buf[0..5]);

    // Read second chunk
    const n2 = try ss.read(&buf);
    try testing.expectEqual(@as(usize, 5), n2);
    try testing.expectEqualStrings(", Wor", buf[0..5]);

    // Read remaining
    const n3 = try ss.read(&buf);
    try testing.expectEqual(@as(usize, 3), n3);
    try testing.expectEqualStrings("ld!", buf[0..3]);

    // EOF
    const n4 = try ss.read(&buf);
    try testing.expectEqual(@as(usize, 0), n4);
}

test "BufferStreamSink: accumulates data" {
    const allocator = testing.allocator;

    var sink = BufferStreamSink.init(allocator);
    defer sink.deinit();

    const ss = sink.asStreamSink();

    try ss.write("Hello");
    try ss.write(", ");
    try ss.write("World!");

    try testing.expectEqualStrings("Hello, World!", sink.getData());
}
