//! S3 Backend - Amazon S3 storage backend
//!
//! A backend implementation for Amazon S3 cloud storage and S3-compatible
//! services (MinIO, LocalStack, Ceph, etc.).
//!
//! Features:
//! - AWS Signature V4 signing
//! - Streaming upload/download for memory efficiency
//! - Support for custom endpoints (MinIO, LocalStack)
//! - Storage class selection (STANDARD, GLACIER, etc.)
//!
//! Supports both HTTP (for local S3-compatible services) and HTTPS (for production):
//! - MinIO: http://localhost:9000
//! - LocalStack: http://localhost:4566
//! - Production AWS S3: https://s3.{region}.amazonaws.com (default)

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend = @import("backend.zig");
const ColdBackend = backend.ColdBackend;
const ObjectMetadata = backend.ObjectMetadata;
const ObjectInfo = backend.ObjectInfo;
const BackendError = backend.BackendError;
const StreamSource = backend.StreamSource;
const StreamSink = backend.StreamSink;
const http_client = @import("http_client_tls.zig");
const HttpClient = http_client.HttpClient;
const aws_sigv4 = @import("aws_sigv4.zig");

/// Configuration for S3 backend
pub const S3Config = struct {
    /// S3 bucket name
    bucket: []const u8,

    /// AWS region (e.g., "us-east-1")
    region: []const u8,

    /// AWS access key ID (required)
    access_key_id: []const u8,

    /// AWS secret access key (required)
    secret_access_key: []const u8,

    /// Custom endpoint URL (for S3-compatible services)
    /// Example: "http://localhost:9000" for MinIO
    /// If null, uses https://s3.{region}.amazonaws.com
    endpoint: ?[]const u8 = null,

    /// Path prefix for all objects
    prefix: []const u8 = "",

    /// Storage class for uploads
    storage_class: StorageClass = .standard,

    /// Use path-style URLs (required for MinIO and some S3-compatible services)
    /// true: http://endpoint/bucket/key
    /// false: http://bucket.endpoint/key (virtual-hosted style)
    path_style: bool = true,

    pub const StorageClass = enum {
        standard,
        standard_ia,
        onezone_ia,
        glacier,
        glacier_ir,
        deep_archive,

        pub fn toString(self: StorageClass) []const u8 {
            return switch (self) {
                .standard => "STANDARD",
                .standard_ia => "STANDARD_IA",
                .onezone_ia => "ONEZONE_IA",
                .glacier => "GLACIER",
                .glacier_ir => "GLACIER_IR",
                .deep_archive => "DEEP_ARCHIVE",
            };
        }
    };
};

/// S3 backend for cloud object storage
pub const S3Backend = struct {
    allocator: Allocator,
    cfg: S3Config,
    bucket: []const u8,
    region: []const u8,
    prefix: []const u8,
    access_key_id: []const u8,
    secret_access_key: []const u8,
    endpoint: []const u8,
    host: []const u8,

    const Self = @This();
    const MAX_URL_LEN = 2048;

    /// Initialize a new S3Backend
    pub fn init(allocator: Allocator, config: S3Config) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Duplicate strings for ownership
        const bucket_dup = try allocator.dupe(u8, config.bucket);
        errdefer allocator.free(bucket_dup);

        const region_dup = try allocator.dupe(u8, config.region);
        errdefer allocator.free(region_dup);

        const prefix_dup = try allocator.dupe(u8, config.prefix);
        errdefer allocator.free(prefix_dup);

        const access_key_dup = try allocator.dupe(u8, config.access_key_id);
        errdefer allocator.free(access_key_dup);

        const secret_key_dup = try allocator.dupe(u8, config.secret_access_key);
        errdefer allocator.free(secret_key_dup);

        // Build endpoint URL
        var endpoint_buf: [256]u8 = undefined;
        const endpoint_dup = if (config.endpoint) |ep|
            try allocator.dupe(u8, ep)
        else blk: {
            const built = try std.fmt.bufPrint(&endpoint_buf, "https://s3.{s}.amazonaws.com", .{region_dup});
            break :blk try allocator.dupe(u8, built);
        };
        errdefer allocator.free(endpoint_dup);

        // Extract host from endpoint
        var host_buf: [256]u8 = undefined;
        const host_dup = blk: {
            const uri = std.Uri.parse(endpoint_dup) catch return error.InvalidKey;
            const h = uri.host orelse return error.InvalidKey;
            break :blk try allocator.dupe(u8, try std.fmt.bufPrint(&host_buf, "{s}", .{h.percent_encoded}));
        };
        errdefer allocator.free(host_dup);

        self.* = .{
            .allocator = allocator,
            .cfg = config,
            .bucket = bucket_dup,
            .region = region_dup,
            .prefix = prefix_dup,
            .access_key_id = access_key_dup,
            .secret_access_key = secret_key_dup,
            .endpoint = endpoint_dup,
            .host = host_dup,
        };

        return self;
    }

    /// Cleanup resources
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.bucket);
        self.allocator.free(self.region);
        self.allocator.free(self.prefix);
        self.allocator.free(self.access_key_id);
        self.allocator.free(self.secret_access_key);
        self.allocator.free(self.endpoint);
        self.allocator.free(self.host);
        self.allocator.destroy(self);
    }

    /// Get the ColdBackend interface
    pub fn asBackend(self: *Self) ColdBackend {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Build the full object key with prefix
    fn buildKey(self: *const Self, key: []const u8, buf: *[MAX_URL_LEN]u8) []const u8 {
        if (self.prefix.len > 0) {
            return std.fmt.bufPrint(buf, "{s}{s}", .{ self.prefix, key }) catch key;
        }
        return key;
    }

    /// Build the S3 URL for a key
    fn buildUrl(self: *const Self, key: []const u8, buf: *[MAX_URL_LEN]u8) ![]const u8 {
        var key_buf: [MAX_URL_LEN]u8 = undefined;
        const full_key = self.buildKey(key, &key_buf);

        if (self.cfg.path_style) {
            return std.fmt.bufPrint(buf, "{s}/{s}/{s}", .{ self.endpoint, self.bucket, full_key }) catch error.InvalidKey;
        } else {
            return std.fmt.bufPrint(buf, "{s}/{s}", .{ self.endpoint, full_key }) catch error.InvalidKey;
        }
    }

    /// Build URI path for signing
    fn buildUriPath(self: *const Self, key: []const u8, buf: *[MAX_URL_LEN]u8) []const u8 {
        var key_buf: [MAX_URL_LEN]u8 = undefined;
        const full_key = self.buildKey(key, &key_buf);

        if (self.cfg.path_style) {
            return std.fmt.bufPrint(buf, "/{s}/{s}", .{ self.bucket, full_key }) catch "/";
        } else {
            return std.fmt.bufPrint(buf, "/{s}", .{full_key}) catch "/";
        }
    }

    /// Get signing host (includes bucket for virtual-hosted style)
    fn getSigningHost(self: *const Self, buf: *[256]u8) []const u8 {
        if (self.cfg.path_style) {
            return self.host;
        } else {
            return std.fmt.bufPrint(buf, "{s}.{s}", .{ self.bucket, self.host }) catch self.host;
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

    fn uploadStreamImpl(ptr: *anyopaque, key: []const u8, source: *const StreamSource, metadata: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const content_length = source.total_size orelse return error.InvalidKey;

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "PUT",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            null,
            timestamp,
        );

        var headers: [8]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "Authorization", .value = signed.getAuthorization() };
        header_count += 1;
        headers[header_count] = .{ .name = "x-amz-date", .value = signed.getDate() };
        header_count += 1;
        headers[header_count] = .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() };
        header_count += 1;

        if (self.cfg.storage_class != .standard) {
            headers[header_count] = .{ .name = "x-amz-storage-class", .value = self.cfg.storage_class.toString() };
            header_count += 1;
        }

        if (metadata) |m| {
            if (m.content_type) |ct| {
                headers[header_count] = .{ .name = "Content-Type", .value = ct };
                header_count += 1;
            }
        }

        const BodyReaderAdapter = struct {
            source: *const StreamSource,

            fn read(ctx: *anyopaque, buffer: []u8) anyerror!usize {
                const adapter: *@This() = @ptrCast(@alignCast(ctx));
                return adapter.source.read(buffer) catch |e| switch (e) {
                    error.EndOfStream => return 0,
                    else => return e,
                };
            }
        };

        var adapter = BodyReaderAdapter{ .source = source };
        const body_reader = http_client.BodyReader{
            .ctx = &adapter,
            .read_fn = BodyReaderAdapter.read,
            .content_length = content_length,
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .PUT,
            .uri = url,
            .headers = headers[0..header_count],
            .body_reader = &body_reader,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200 and response.status_code != 201) {
            return error.IoError;
        }
    }

    fn downloadStreamImpl(ptr: *anyopaque, key: []const u8, sink: *const StreamSink) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "GET",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        const SinkAdapter = struct {
            sink: *const StreamSink,

            fn write(ctx: *anyopaque, data: []const u8) anyerror!void {
                const sa: *@This() = @ptrCast(@alignCast(ctx));
                return sa.sink.write(data) catch error.IoError;
            }
        };

        var adapter = SinkAdapter{ .sink = sink };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        const status = client.requestStreaming(
            .{
                .method = .GET,
                .uri = url,
                .headers = &headers,
            },
            &adapter,
            SinkAdapter.write,
        ) catch return error.NetworkError;

        if (status == 404) {
            return error.ObjectNotFound;
        }
        if (status != 200) {
            return error.IoError;
        }
    }

    fn uploadImpl(ptr: *anyopaque, key: []const u8, data: []const u8, metadata: ?ObjectMetadata) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        const payload_hash = aws_sigv4.hashPayload(data);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "PUT",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            payload_hash,
            timestamp,
        );

        var headers: [8]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "Authorization", .value = signed.getAuthorization() };
        header_count += 1;
        headers[header_count] = .{ .name = "x-amz-date", .value = signed.getDate() };
        header_count += 1;
        headers[header_count] = .{ .name = "x-amz-content-sha256", .value = &payload_hash };
        header_count += 1;

        if (self.cfg.storage_class != .standard) {
            headers[header_count] = .{ .name = "x-amz-storage-class", .value = self.cfg.storage_class.toString() };
            header_count += 1;
        }

        if (metadata) |m| {
            if (m.content_type) |ct| {
                headers[header_count] = .{ .name = "Content-Type", .value = ct };
                header_count += 1;
            }
        }

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .PUT,
            .uri = url,
            .headers = headers[0..header_count],
            .body = data,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200 and response.status_code != 201) {
            return error.IoError;
        }
    }

    fn downloadImpl(ptr: *anyopaque, key: []const u8, buffer: []u8) BackendError![]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "GET",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .GET,
            .uri = url,
            .headers = &headers,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code == 404) {
            return error.ObjectNotFound;
        }
        if (response.status_code != 200) {
            return error.IoError;
        }

        if (response.body.len > buffer.len) {
            return error.BufferTooSmall;
        }

        @memcpy(buffer[0..response.body.len], response.body);
        return buffer[0..response.body.len];
    }

    fn existsImpl(ptr: *anyopaque, key: []const u8) BackendError!bool {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "HEAD",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .HEAD,
            .uri = url,
            .headers = &headers,
        }) catch return error.NetworkError;
        defer response.deinit();

        return response.status_code == 200;
    }

    fn deleteImpl(ptr: *anyopaque, key: []const u8) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "DELETE",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .DELETE,
            .uri = url,
            .headers = &headers,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200 and response.status_code != 204) {
            return error.IoError;
        }
    }

    fn listImpl(ptr: *anyopaque, prefix: []const u8, allocator: Allocator) BackendError![]ObjectInfo {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var key_buf: [MAX_URL_LEN]u8 = undefined;
        const full_prefix = self.buildKey(prefix, &key_buf);

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = std.fmt.bufPrint(&url_buf, "{s}/{s}?list-type=2&prefix={s}", .{
            self.endpoint,
            self.bucket,
            full_prefix,
        }) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = std.fmt.bufPrint(&path_buf, "/{s}/", .{self.bucket}) catch return error.InvalidKey;

        var query_buf: [512]u8 = undefined;
        const query_string = std.fmt.bufPrint(&query_buf, "list-type=2&prefix={s}", .{full_prefix}) catch return error.InvalidKey;

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "GET",
            uri_path,
            query_string,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .GET,
            .uri = url,
            .headers = &headers,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200) {
            return error.IoError;
        }

        return parseListResponse(response.body, allocator);
    }

    fn headImpl(ptr: *anyopaque, key: []const u8) BackendError!?ObjectMetadata {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var path_buf: [MAX_URL_LEN]u8 = undefined;
        const uri_path = self.buildUriPath(key, &path_buf);

        var host_buf: [256]u8 = undefined;
        const signing_host = self.getSigningHost(&host_buf);

        const timestamp = std.time.timestamp();
        const signed = aws_sigv4.signRequest(
            "HEAD",
            uri_path,
            null,
            signing_host,
            self.region,
            .{
                .access_key_id = self.access_key_id,
                .secret_access_key = self.secret_access_key,
            },
            aws_sigv4.EMPTY_PAYLOAD_HASH,
            timestamp,
        );

        const headers = [_]http_client.Header{
            .{ .name = "Authorization", .value = signed.getAuthorization() },
            .{ .name = "x-amz-date", .value = signed.getDate() },
            .{ .name = "x-amz-content-sha256", .value = signed.getContentSha256() },
        };

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .HEAD,
            .uri = url,
            .headers = &headers,
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code == 404) {
            return null;
        }
        if (response.status_code != 200) {
            return error.IoError;
        }

        const content_length = if (response.getHeader("content-length")) |cl|
            std.fmt.parseInt(u64, cl, 10) catch 0
        else
            0;

        return ObjectMetadata{
            .size = content_length,
            .content_type = response.getHeader("content-type"),
            .last_modified = 0,
        };
    }

    fn deinitImpl(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

/// Parse S3 ListObjectsV2 XML response (simplified parser)
fn parseListResponse(xml: []const u8, allocator: Allocator) BackendError![]ObjectInfo {
    var results: std.ArrayList(ObjectInfo) = .empty;
    errdefer {
        for (results.items) |item| {
            allocator.free(item.key);
        }
        results.deinit(allocator);
    }

    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Contents>")) |contents_start| {
        const contents_end = std.mem.indexOfPos(u8, xml, contents_start, "</Contents>") orelse break;
        const contents = xml[contents_start..contents_end];

        const key_start = (std.mem.indexOf(u8, contents, "<Key>") orelse continue) + 5;
        const key_end = std.mem.indexOfPos(u8, contents, key_start, "</Key>") orelse continue;
        const key = contents[key_start..key_end];

        const size: u64 = blk: {
            const size_start = (std.mem.indexOf(u8, contents, "<Size>") orelse break :blk 0) + 6;
            const size_end = std.mem.indexOfPos(u8, contents, size_start, "</Size>") orelse break :blk 0;
            break :blk std.fmt.parseInt(u64, contents[size_start..size_end], 10) catch 0;
        };

        const duped_key = allocator.dupe(u8, key) catch return error.OutOfMemory;
        results.append(allocator, .{
            .key = duped_key,
            .size = size,
            .last_modified = 0,
        }) catch {
            allocator.free(duped_key);
            return error.OutOfMemory;
        };

        pos = contents_end;
    }

    return results.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "S3Backend: initialization" {
    const allocator = testing.allocator;

    var s3 = try S3Backend.init(allocator, .{
        .bucket = "test-bucket",
        .region = "us-east-1",
        .access_key_id = "AKIAIOSFODNN7EXAMPLE",
        .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        .endpoint = "http://localhost:9000",
        .prefix = "cold-storage/",
    });
    defer s3.deinit();

    try testing.expectEqualStrings("test-bucket", s3.bucket);
    try testing.expectEqualStrings("us-east-1", s3.region);
    try testing.expectEqualStrings("cold-storage/", s3.prefix);
}

test "S3Backend: URL building" {
    const allocator = testing.allocator;

    var s3 = try S3Backend.init(allocator, .{
        .bucket = "mybucket",
        .region = "us-west-2",
        .access_key_id = "AKID",
        .secret_access_key = "SECRET",
        .endpoint = "http://localhost:9000",
        .prefix = "segments/",
        .path_style = true,
    });
    defer s3.deinit();

    var url_buf: [S3Backend.MAX_URL_LEN]u8 = undefined;
    const url = try s3.buildUrl("mykey.dat", &url_buf);
    try testing.expectEqualStrings("http://localhost:9000/mybucket/segments/mykey.dat", url);
}

test "parseListResponse: basic parsing" {
    const allocator = testing.allocator;

    const xml =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<ListBucketResult>
        \\  <Contents>
        \\    <Key>file1.txt</Key>
        \\    <Size>1024</Size>
        \\  </Contents>
        \\  <Contents>
        \\    <Key>file2.txt</Key>
        \\    <Size>2048</Size>
        \\  </Contents>
        \\</ListBucketResult>
    ;

    const results = try parseListResponse(xml, allocator);
    defer {
        for (results) |item| {
            allocator.free(item.key);
        }
        allocator.free(results);
    }

    try testing.expectEqual(@as(usize, 2), results.len);
    try testing.expectEqualStrings("file1.txt", results[0].key);
    try testing.expectEqual(@as(u64, 1024), results[0].size);
    try testing.expectEqualStrings("file2.txt", results[1].key);
    try testing.expectEqual(@as(u64, 2048), results[1].size);
}
