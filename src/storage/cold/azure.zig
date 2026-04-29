//! Azure Blob Storage Backend
//!
//! A backend implementation for Azure Blob Storage.
//! Supports both Shared Key and SAS token authentication.
//!
//! Features:
//! - Shared Key authentication (account key)
//! - SAS token authentication
//! - Streaming upload/download for memory efficiency
//! - Block blob operations
//! - Access tier selection
//!
//! Supports both HTTP (for local emulators) and HTTPS (for production):
//! - Azurite (local emulator): http://127.0.0.1:10000/devstoreaccount1
//! - Production Azure: https://{account}.blob.core.windows.net (default)

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
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// Configuration for Azure Blob Storage backend
pub const AzureConfig = struct {
    /// Storage account name
    account_name: []const u8,

    /// Container name
    container: []const u8,

    /// Account key for Shared Key authentication
    account_key: ?[]const u8 = null,

    /// SAS token (alternative to account key)
    /// Should NOT include the leading '?'
    sas_token: ?[]const u8 = null,

    /// Custom endpoint URL (for Azurite or Azure Stack)
    /// Example: "http://127.0.0.1:10000/devstoreaccount1"
    /// If null, uses https://{account}.blob.core.windows.net
    endpoint: ?[]const u8 = null,

    /// Path prefix for all blobs
    prefix: []const u8 = "",

    /// Access tier for uploaded blobs
    access_tier: AccessTier = .hot,

    pub const AccessTier = enum {
        hot,
        cool,
        cold,
        archive,

        pub fn toString(self: AccessTier) []const u8 {
            return switch (self) {
                .hot => "Hot",
                .cool => "Cool",
                .cold => "Cold",
                .archive => "Archive",
            };
        }
    };
};

/// Azure Blob Storage backend
pub const AzureBackend = struct {
    allocator: Allocator,
    cfg: AzureConfig,
    account_name: []const u8,
    container: []const u8,
    prefix: []const u8,
    account_key: ?[]const u8,
    sas_token: ?[]const u8,
    endpoint: []const u8,

    const Self = @This();
    const MAX_URL_LEN = 2048;

    /// Initialize a new AzureBackend
    pub fn init(allocator: Allocator, config: AzureConfig) !*Self {
        // Must have either account_key or sas_token
        if (config.account_key == null and config.sas_token == null) {
            return error.NoCredentials;
        }

        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const account_name_dup = try allocator.dupe(u8, config.account_name);
        errdefer allocator.free(account_name_dup);

        const container_dup = try allocator.dupe(u8, config.container);
        errdefer allocator.free(container_dup);

        const prefix_dup = try allocator.dupe(u8, config.prefix);
        errdefer allocator.free(prefix_dup);

        const account_key_dup = if (config.account_key) |key|
            try allocator.dupe(u8, key)
        else
            null;
        errdefer if (account_key_dup) |key| allocator.free(key);

        const sas_token_dup = if (config.sas_token) |token|
            try allocator.dupe(u8, token)
        else
            null;
        errdefer if (sas_token_dup) |token| allocator.free(token);

        var endpoint_buf: [256]u8 = undefined;
        const endpoint_dup = if (config.endpoint) |ep|
            try allocator.dupe(u8, ep)
        else blk: {
            const built = try std.fmt.bufPrint(&endpoint_buf, "https://{s}.blob.core.windows.net", .{account_name_dup});
            break :blk try allocator.dupe(u8, built);
        };
        errdefer allocator.free(endpoint_dup);

        self.* = .{
            .allocator = allocator,
            .cfg = config,
            .account_name = account_name_dup,
            .container = container_dup,
            .prefix = prefix_dup,
            .account_key = account_key_dup,
            .sas_token = sas_token_dup,
            .endpoint = endpoint_dup,
        };

        return self;
    }

    /// Cleanup resources
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.account_name);
        self.allocator.free(self.container);
        self.allocator.free(self.prefix);
        if (self.account_key) |key| self.allocator.free(key);
        if (self.sas_token) |token| self.allocator.free(token);
        self.allocator.free(self.endpoint);
        self.allocator.destroy(self);
    }

    /// Get the ColdBackend interface
    pub fn asBackend(self: *Self) ColdBackend {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Build the full blob name with prefix
    fn buildBlobName(self: *const Self, key: []const u8, buf: *[MAX_URL_LEN]u8) []const u8 {
        if (self.prefix.len > 0) {
            return std.fmt.bufPrint(buf, "{s}{s}", .{ self.prefix, key }) catch key;
        }
        return key;
    }

    /// Build the Azure Blob URL for a key
    fn buildUrl(self: *const Self, key: []const u8, buf: *[MAX_URL_LEN]u8) ![]const u8 {
        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        if (self.sas_token) |sas| {
            return std.fmt.bufPrint(buf, "{s}/{s}/{s}?{s}", .{
                self.endpoint,
                self.container,
                blob_name,
                sas,
            }) catch error.InvalidKey;
        } else {
            return std.fmt.bufPrint(buf, "{s}/{s}/{s}", .{
                self.endpoint,
                self.container,
                blob_name,
            }) catch error.InvalidKey;
        }
    }

    /// Build URL for list operation
    fn buildListUrl(self: *const Self, prefix: []const u8, buf: *[MAX_URL_LEN]u8) ![]const u8 {
        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const full_prefix = self.buildBlobName(prefix, &blob_buf);

        if (self.sas_token) |sas| {
            return std.fmt.bufPrint(buf, "{s}/{s}?restype=container&comp=list&prefix={s}&{s}", .{
                self.endpoint,
                self.container,
                full_prefix,
                sas,
            }) catch error.InvalidKey;
        } else {
            return std.fmt.bufPrint(buf, "{s}/{s}?restype=container&comp=list&prefix={s}", .{
                self.endpoint,
                self.container,
                full_prefix,
            }) catch error.InvalidKey;
        }
    }

    /// Sign a request with Shared Key authentication
    fn signRequest(
        self: *const Self,
        method: []const u8,
        blob_name: []const u8,
        content_length: ?usize,
        content_type: ?[]const u8,
        timestamp: []const u8,
        auth_buf: *[512]u8,
    ) ?[]const u8 {
        const account_key = self.account_key orelse return null;

        // Decode base64 account key
        var decoded_key: [64]u8 = undefined;
        const key_len = std.base64.standard.Decoder.calcSizeForSlice(account_key) catch return null;
        if (key_len > decoded_key.len) return null;
        std.base64.standard.Decoder.decode(&decoded_key, account_key) catch return null;

        // Build string to sign
        var sts_buf: [2048]u8 = undefined;
        var sts_fbs: std.Io.Writer = .fixed(&sts_buf);
        const sts_writer = &sts_fbs;

        // VERB
        sts_writer.writeAll(method) catch return null;
        sts_writer.writeByte('\n') catch return null;

        // Content-Encoding (empty)
        sts_writer.writeByte('\n') catch return null;
        // Content-Language (empty)
        sts_writer.writeByte('\n') catch return null;

        // Content-Length
        if (content_length) |len| {
            if (len > 0) {
                sts_writer.print("{d}", .{len}) catch return null;
            }
        }
        sts_writer.writeByte('\n') catch return null;

        // Content-MD5 (empty)
        sts_writer.writeByte('\n') catch return null;

        // Content-Type
        if (content_type) |ct| {
            sts_writer.writeAll(ct) catch return null;
        }
        sts_writer.writeByte('\n') catch return null;

        // Date (empty - using x-ms-date instead)
        sts_writer.writeByte('\n') catch return null;

        // If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since, Range (all empty)
        sts_writer.writeAll("\n\n\n\n\n") catch return null;

        // CanonicalizedHeaders
        sts_writer.print("x-ms-blob-type:BlockBlob\nx-ms-date:{s}\nx-ms-version:2020-10-02\n", .{timestamp}) catch return null;

        // CanonicalizedResource
        sts_writer.print("/{s}/{s}/{s}", .{ self.account_name, self.container, blob_name }) catch return null;

        const string_to_sign = sts_fbs.buffered();

        // HMAC-SHA256
        var signature: [HmacSha256.mac_length]u8 = undefined;
        HmacSha256.create(&signature, string_to_sign, decoded_key[0..key_len]);

        // Base64 encode signature
        var sig_b64: [64]u8 = undefined;
        const sig_b64_slice = std.base64.standard.Encoder.encode(&sig_b64, &signature);

        // Build Authorization header
        return std.fmt.bufPrint(auth_buf, "SharedKey {s}:{s}", .{
            self.account_name,
            sig_b64_slice,
        }) catch null;
    }

    /// Format date for x-ms-date header
    fn formatMsDate(timestamp: i64) [29]u8 {
        const epoch_seconds: u64 = @intCast(timestamp);
        const epoch_day = epoch_seconds / 86400;
        const day_seconds = epoch_seconds % 86400;

        const z = epoch_day + 719468;
        const era: u64 = z / 146097;
        const doe: u64 = z - era * 146097;
        const yoe: u64 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        const y: u64 = yoe + era * 400;
        const doy: u64 = doe - (365 * yoe + yoe / 4 - yoe / 100);
        const mp: u64 = (5 * doy + 2) / 153;
        const d: u64 = doy - (153 * mp + 2) / 5 + 1;
        const m: u64 = if (mp < 10) mp + 3 else mp - 9;
        const year: u64 = if (m <= 2) y + 1 else y;
        const month: u64 = m;
        const day: u64 = d;

        const hour = day_seconds / 3600;
        const minute = (day_seconds % 3600) / 60;
        const second = day_seconds % 60;

        const dow = (epoch_day + 4) % 7;
        const day_names = [_][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
        const month_names = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

        var result: [29]u8 = undefined;
        _ = std.fmt.bufPrint(&result, "{s}, {d:0>2} {s} {d} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
            day_names[dow],
            day,
            month_names[month - 1],
            year,
            hour,
            minute,
            second,
        }) catch {};

        return result;
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

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [10]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-blob-type", .value = "BlockBlob" };
        header_count += 1;

        const content_type = if (metadata) |m| m.content_type else null;
        if (content_type) |ct| {
            headers[header_count] = .{ .name = "Content-Type", .value = ct };
            header_count += 1;
        }

        if (self.cfg.access_tier != .hot) {
            headers[header_count] = .{ .name = "x-ms-access-tier", .value = self.cfg.access_tier.toString() };
            header_count += 1;
        }

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("PUT", blob_name, content_length, content_type, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
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

        if (response.status_code != 201) {
            return error.IoError;
        }
    }

    fn downloadStreamImpl(ptr: *anyopaque, key: []const u8, sink: *const StreamSink) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("GET", blob_name, null, null, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
        }

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
                .headers = headers[0..header_count],
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

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [10]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-blob-type", .value = "BlockBlob" };
        header_count += 1;

        const content_type = if (metadata) |m| m.content_type else null;
        if (content_type) |ct| {
            headers[header_count] = .{ .name = "Content-Type", .value = ct };
            header_count += 1;
        }

        if (self.cfg.access_tier != .hot) {
            headers[header_count] = .{ .name = "x-ms-access-tier", .value = self.cfg.access_tier.toString() };
            header_count += 1;
        }

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("PUT", blob_name, data.len, content_type, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
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

        if (response.status_code != 201) {
            return error.IoError;
        }
    }

    fn downloadImpl(ptr: *anyopaque, key: []const u8, buffer: []u8) BackendError![]u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("GET", blob_name, null, null, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
        }

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .GET,
            .uri = url,
            .headers = headers[0..header_count],
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

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("HEAD", blob_name, null, null, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
        }

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .HEAD,
            .uri = url,
            .headers = headers[0..header_count],
        }) catch return error.NetworkError;
        defer response.deinit();

        return response.status_code == 200;
    }

    fn deleteImpl(ptr: *anyopaque, key: []const u8) BackendError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("DELETE", blob_name, null, null, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
        }

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .DELETE,
            .uri = url,
            .headers = headers[0..header_count],
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200 and response.status_code != 202) {
            return error.IoError;
        }
    }

    fn listImpl(ptr: *anyopaque, prefix: []const u8, allocator: Allocator) BackendError![]ObjectInfo {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildListUrl(prefix, &url_buf) catch return error.InvalidKey;

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .GET,
            .uri = url,
            .headers = headers[0..header_count],
        }) catch return error.NetworkError;
        defer response.deinit();

        if (response.status_code != 200) {
            return error.IoError;
        }

        return parseListBlobsResponse(response.body, allocator);
    }

    fn headImpl(ptr: *anyopaque, key: []const u8) BackendError!?ObjectMetadata {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var url_buf: [MAX_URL_LEN]u8 = undefined;
        const url = self.buildUrl(key, &url_buf) catch return error.InvalidKey;

        var blob_buf: [MAX_URL_LEN]u8 = undefined;
        const blob_name = self.buildBlobName(key, &blob_buf);

        const timestamp = @import("stdx").time.milliTimestamp();
        const ms_date = formatMsDate(timestamp);

        var headers: [5]http_client.Header = undefined;
        var header_count: usize = 0;

        headers[header_count] = .{ .name = "x-ms-version", .value = "2020-10-02" };
        header_count += 1;
        headers[header_count] = .{ .name = "x-ms-date", .value = &ms_date };
        header_count += 1;

        var auth_buf: [512]u8 = undefined;
        if (self.signRequest("HEAD", blob_name, null, null, &ms_date, &auth_buf)) |auth| {
            headers[header_count] = .{ .name = "Authorization", .value = auth };
            header_count += 1;
        }

        var client = HttpClient.init(self.allocator, .{});
        defer client.deinit();
        var response = client.request(.{
            .method = .HEAD,
            .uri = url,
            .headers = headers[0..header_count],
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

/// Parse Azure List Blobs XML response
fn parseListBlobsResponse(xml: []const u8, allocator: Allocator) BackendError![]ObjectInfo {
    var results: std.ArrayList(ObjectInfo) = .empty;
    errdefer {
        for (results.items) |item| {
            allocator.free(item.key);
        }
        results.deinit(allocator);
    }

    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Blob>")) |blob_start| {
        const blob_end = std.mem.indexOfPos(u8, xml, blob_start, "</Blob>") orelse break;
        const blob = xml[blob_start..blob_end];

        const name_start = (std.mem.indexOf(u8, blob, "<Name>") orelse continue) + 6;
        const name_end = std.mem.indexOfPos(u8, blob, name_start, "</Name>") orelse continue;
        const name = blob[name_start..name_end];

        const size: u64 = blk: {
            const size_start = (std.mem.indexOf(u8, blob, "<Content-Length>") orelse break :blk 0) + 16;
            const size_end = std.mem.indexOfPos(u8, blob, size_start, "</Content-Length>") orelse break :blk 0;
            break :blk std.fmt.parseInt(u64, blob[size_start..size_end], 10) catch 0;
        };

        const duped_name = allocator.dupe(u8, name) catch return error.OutOfMemory;
        results.append(allocator, .{
            .key = duped_name,
            .size = size,
            .last_modified = 0,
        }) catch {
            allocator.free(duped_name);
            return error.OutOfMemory;
        };

        pos = blob_end;
    }

    return results.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "AzureBackend: initialization with SAS token" {
    const allocator = testing.allocator;

    var azure = try AzureBackend.init(allocator, .{
        .account_name = "devstoreaccount1",
        .container = "test-container",
        .sas_token = "sv=2020-10-02&ss=b&srt=sco&sp=rwdlacx&sig=xxx",
        .endpoint = "http://127.0.0.1:10000/devstoreaccount1",
        .prefix = "cold-storage/",
    });
    defer azure.deinit();

    try testing.expectEqualStrings("devstoreaccount1", azure.account_name);
    try testing.expectEqualStrings("test-container", azure.container);
    try testing.expectEqualStrings("cold-storage/", azure.prefix);
}

test "AzureBackend: URL building with SAS" {
    const allocator = testing.allocator;

    var azure = try AzureBackend.init(allocator, .{
        .account_name = "devstoreaccount1",
        .container = "mycontainer",
        .sas_token = "sv=2020-10-02",
        .endpoint = "http://127.0.0.1:10000/devstoreaccount1",
        .prefix = "segments/",
    });
    defer azure.deinit();

    var url_buf: [AzureBackend.MAX_URL_LEN]u8 = undefined;
    const url = try azure.buildUrl("mykey.dat", &url_buf);
    try testing.expectEqualStrings(
        "http://127.0.0.1:10000/devstoreaccount1/mycontainer/segments/mykey.dat?sv=2020-10-02",
        url,
    );
}

test "AzureBackend: requires credentials" {
    const allocator = testing.allocator;

    const result = AzureBackend.init(allocator, .{
        .account_name = "test",
        .container = "test",
    });

    try testing.expectError(error.NoCredentials, result);
}

test "parseListBlobsResponse: basic parsing" {
    const allocator = testing.allocator;

    const xml =
        \\<?xml version="1.0" encoding="utf-8"?>
        \\<EnumerationResults>
        \\  <Blobs>
        \\    <Blob>
        \\      <Name>file1.txt</Name>
        \\      <Properties>
        \\        <Content-Length>1024</Content-Length>
        \\      </Properties>
        \\    </Blob>
        \\    <Blob>
        \\      <Name>file2.txt</Name>
        \\      <Properties>
        \\        <Content-Length>2048</Content-Length>
        \\      </Properties>
        \\    </Blob>
        \\  </Blobs>
        \\</EnumerationResults>
    ;

    const results = try parseListBlobsResponse(xml, allocator);
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
