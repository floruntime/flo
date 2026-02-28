//! Cold Storage Module
//!
//! Provides cold storage backends for archiving UAL segments and snapshots
//! to durable object stores. Multiple backends are supported for different
//! deployment scenarios.
//!
//! Backends:
//! - **noop**: Discards all data — useful for testing and benchmarking
//! - **file**: Local filesystem — useful for development and E2E testing
//! - **s3**: AWS S3 / S3-compatible (MinIO, LocalStack)
//! - **azure**: Azure Blob Storage / Azurite emulator

pub const backend = @import("backend.zig");
pub const ColdBackend = backend.ColdBackend;
pub const BackendError = backend.BackendError;
pub const ObjectMetadata = backend.ObjectMetadata;
pub const ObjectInfo = backend.ObjectInfo;
pub const StreamSource = backend.StreamSource;
pub const StreamSink = backend.StreamSink;
pub const SliceStreamSource = backend.SliceStreamSource;
pub const BufferStreamSink = backend.BufferStreamSink;

pub const NoopBackend = @import("noop.zig").NoopBackend;
pub const FileBackend = @import("file.zig").FileBackend;
pub const FileConfig = @import("file.zig").FileConfig;
pub const S3Backend = @import("s3.zig").S3Backend;
pub const S3Config = @import("s3.zig").S3Config;
pub const AzureBackend = @import("azure.zig").AzureBackend;
pub const AzureConfig = @import("azure.zig").AzureConfig;

pub const HttpClient = @import("http_client.zig").HttpClient;
pub const TlsHttpClient = @import("http_client_tls.zig").HttpClient;
pub const aws_sigv4 = @import("aws_sigv4.zig");

pub const manifest = @import("manifest.zig");
pub const ColdManifest = manifest.ColdManifest;
pub const ColdEntry = manifest.ColdEntry;

pub const tier_manager = @import("tier_manager.zig");
pub const ColdTierManager = tier_manager.ColdTierManager;
pub const ColdTierConfig = tier_manager.ColdTierConfig;
pub const ColdTierError = tier_manager.ColdTierError;

/// Backend type selector for configuration
pub const BackendType = enum {
    noop,
    file,
    s3,
    azure,
};

/// Create a cold storage backend from a type and configuration.
///
/// For s3 and azure backends, pass the appropriate config struct.
/// For noop, pass null.
/// For file, pass a FileConfig.
pub fn createBackend(
    allocator: @import("std").mem.Allocator,
    backend_type: BackendType,
    file_config: ?FileConfig,
    s3_config: ?S3Config,
    azure_config: ?AzureConfig,
) !ColdBackend {
    switch (backend_type) {
        .noop => {
            const b = try NoopBackend.init(allocator);
            return b.asBackend();
        },
        .file => {
            const config = file_config orelse return error.InvalidConfiguration;
            const b = try FileBackend.init(allocator, config);
            return b.asBackend();
        },
        .s3 => {
            const config = s3_config orelse return error.InvalidConfiguration;
            const b = try S3Backend.init(allocator, config);
            return b.asBackend();
        },
        .azure => {
            const config = azure_config orelse return error.InvalidConfiguration;
            const b = try AzureBackend.init(allocator, config);
            return b.asBackend();
        },
    }
}

const std = @import("std");
const testing = std.testing;

test "cold storage: refAllDecls" {
    comptime {
        _ = backend;
        _ = NoopBackend;
        _ = FileBackend;
        _ = S3Backend;
        _ = AzureBackend;
        _ = HttpClient;
        _ = TlsHttpClient;
        _ = aws_sigv4;
    }
}

test "cold storage: polymorphism — noop vs file" {
    const allocator = testing.allocator;

    // Create noop backend
    const noop = try NoopBackend.init(allocator);
    const noop_backend = noop.asBackend();
    defer noop_backend.deinitBackend();

    // Upload should succeed (data is discarded)
    try noop_backend.upload("test-key", "hello", null);

    // Download should fail (noop doesn't store)
    var buf: [256]u8 = undefined;
    try testing.expectError(error.ObjectNotFound, noop_backend.download("test-key", &buf));

    // Exists should return false
    const exists = try noop_backend.exists("test-key");
    try testing.expect(!exists);

    // Create file backend with tmp dir
    const file_b = try FileBackend.init(allocator, .{
        .base_path = "/tmp/test_cold_mod_poly",
        .create_dirs = true,
    });
    const file_backend = file_b.asBackend();
    defer {
        file_backend.deinitBackend();
        std.fs.cwd().deleteTree("/tmp/test_cold_mod_poly") catch {};
    }

    // Upload then download should work
    try file_backend.upload("poly-key", "world", null);

    var download_buf: [256]u8 = undefined;
    const data = try file_backend.download("poly-key", &download_buf);
    try testing.expectEqualStrings("world", data);

    // Exists should return true
    const file_exists = try file_backend.exists("poly-key");
    try testing.expect(file_exists);
}
