//! BrokerPool — Manages TCP connections to Kafka brokers.
//!
//! Each broker connection multiplexes requests via correlation_id.
//! Connections are established lazily — only to brokers that are
//! leaders for assigned partitions.
//!
//! Phase 1: blocking TCP with timeout. Phase 2+: reactor registration.

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const protocol = @import("protocol.zig");
const auth_mod = @import("auth.zig");
const KafkaWriter = codec.KafkaWriter;
const KafkaReader = codec.KafkaReader;
const ApiKey = codec.ApiKey;
const ApiVersionRange = codec.ApiVersionRange;
const ErrorCode = protocol.ErrorCode;

const log = @import("stdx").log;

// =============================================================================
// BrokerAddress
// =============================================================================

pub const BrokerAddress = struct {
    host: []const u8,
    port: u16,
};

// =============================================================================
// SaslConfig
// =============================================================================

pub const SaslConfig = struct {
    mechanism: []const u8, // "PLAIN", "SCRAM-SHA-256", etc.
    username: []const u8,
    password: []const u8,
};

// =============================================================================
// BrokerConnection
// =============================================================================

pub const ConnectionState = enum(u8) {
    disconnected,
    connecting,
    sasl_handshake,
    sasl_authenticating,
    api_versions,
    ready,
    failed,
};

pub const BrokerConnection = struct {
    broker_id: i32,
    address: BrokerAddress,
    stream: ?std.net.Stream,
    state: ConnectionState,
    last_activity_ms: i64,

    /// Receive buffer — Kafka responses can be large
    recv_buf: []u8,
    recv_pos: usize,

    allocator: Allocator,

    const RECV_BUF_SIZE: usize = 2 * 1024 * 1024; // 2MB

    pub fn init(allocator: Allocator, broker_id: i32, address: BrokerAddress) !BrokerConnection {
        const recv_buf = try allocator.alloc(u8, RECV_BUF_SIZE);
        return .{
            .broker_id = broker_id,
            .address = address,
            .stream = null,
            .state = .disconnected,
            .last_activity_ms = 0,
            .recv_buf = recv_buf,
            .recv_pos = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BrokerConnection) void {
        self.close();
        self.allocator.free(self.recv_buf);
    }

    pub fn close(self: *BrokerConnection) void {
        if (self.stream) |s| {
            s.close();
            self.stream = null;
        }
        self.state = .disconnected;
        self.recv_pos = 0;
    }

    /// Connect to the broker. Returns error if connection fails.
    pub fn connect(self: *BrokerConnection) !void {
        if (self.stream != null) return;
        self.state = .connecting;

        const address = std.net.Address.parseIp4(self.address.host, self.address.port) catch blk: {
            // Try IPv6
            break :blk std.net.Address.parseIp6(self.address.host, self.address.port) catch {
                // Try DNS resolution
                break :blk try resolveDns(self.address.host, self.address.port);
            };
        };

        self.stream = std.net.tcpConnectToAddress(address) catch |err| {
            log.err("Failed to connect to broker {d} at {s}:{d}: {}", .{
                self.broker_id, self.address.host, self.address.port, err,
            });
            self.state = .failed;
            return error.ConnectionFailed;
        };

        self.state = .ready;
        self.recv_pos = 0;
    }

    /// Send a raw request frame (already encoded with length prefix).
    pub fn send(self: *BrokerConnection, frame: []const u8) !void {
        const s = self.stream orelse return error.NotConnected;
        var sent: usize = 0;
        while (sent < frame.len) {
            sent += s.write(frame[sent..]) catch |err| {
                log.err("Broker {d} send failed: {}", .{ self.broker_id, err });
                self.state = .failed;
                return error.SendFailed;
            };
        }
    }

    /// Receive a complete Kafka response frame.
    /// Returns the response body (after 4-byte length prefix).
    pub fn receive(self: *BrokerConnection) ![]const u8 {
        const s = self.stream orelse return error.NotConnected;

        // Read the 4-byte length prefix
        var len_buf: [4]u8 = undefined;
        var len_read: usize = 0;
        while (len_read < 4) {
            const n = s.read(len_buf[len_read..]) catch |err| {
                log.err("Broker {d} recv length failed: {}", .{ self.broker_id, err });
                self.state = .failed;
                return error.RecvFailed;
            };
            if (n == 0) {
                self.state = .failed;
                return error.ConnectionClosed;
            }
            len_read += n;
        }

        const response_len: usize = @intCast(std.mem.readInt(i32, &len_buf, .big));
        if (response_len > self.recv_buf.len) {
            log.err("Broker {d} response too large: {d} bytes", .{ self.broker_id, response_len });
            return error.ResponseTooLarge;
        }

        // Read the response body
        self.recv_pos = 0;
        while (self.recv_pos < response_len) {
            const n = s.read(self.recv_buf[self.recv_pos..response_len]) catch |err| {
                log.err("Broker {d} recv body failed: {}", .{ self.broker_id, err });
                self.state = .failed;
                return error.RecvFailed;
            };
            if (n == 0) {
                self.state = .failed;
                return error.ConnectionClosed;
            }
            self.recv_pos += n;
        }

        self.last_activity_ms = std.time.milliTimestamp();
        return self.recv_buf[0..response_len];
    }
};

// =============================================================================
// BrokerPool
// =============================================================================

pub const BrokerPool = struct {
    allocator: Allocator,

    /// broker_id → BrokerConnection
    connections: std.AutoHashMap(i32, BrokerConnection),

    /// Broker address table from metadata
    broker_addresses: std.AutoHashMap(i32, BrokerAddress),

    /// Next correlation_id (monotonically increasing)
    next_correlation_id: i32,

    /// Negotiated API versions from first ApiVersions response
    api_versions: [64]ApiVersionRange,
    api_versions_initialized: bool,

    /// SASL authentication config
    sasl_config: ?SaslConfig,

    /// Client ID for all requests
    client_id: []const u8,

    pub fn init(allocator: Allocator, sasl_config: ?SaslConfig) BrokerPool {
        return .{
            .allocator = allocator,
            .connections = std.AutoHashMap(i32, BrokerConnection).init(allocator),
            .broker_addresses = std.AutoHashMap(i32, BrokerAddress).init(allocator),
            .next_correlation_id = 1,
            .api_versions = [_]ApiVersionRange{.{}} ** 64,
            .api_versions_initialized = false,
            .sasl_config = sasl_config,
            .client_id = "flo-kafka-source",
        };
    }

    pub fn deinit(self: *BrokerPool) void {
        var conn_iter = self.connections.valueIterator();
        while (conn_iter.next()) |conn| {
            conn.deinit();
        }
        self.connections.deinit();
        self.broker_addresses.deinit();
    }

    /// Get the next correlation ID.
    pub fn nextCorrelationId(self: *BrokerPool) i32 {
        const id = self.next_correlation_id;
        self.next_correlation_id +%= 1;
        return id;
    }

    /// Negotiate the best API version for a given key.
    pub fn negotiateVersion(self: *BrokerPool, api_key: ApiKey) i16 {
        if (!self.api_versions_initialized) return 0;
        const key_idx: usize = @intCast(@intFromEnum(api_key));
        if (key_idx >= 64) return 0;
        const broker_range = self.api_versions[key_idx];
        const our_range = getOurVersionRange(api_key);

        // Find highest mutually-supported version
        const min_version = @max(broker_range.min_version, our_range.min_version);
        const max_version = @min(broker_range.max_version, our_range.max_version);
        if (min_version > max_version) return our_range.min_version;
        return max_version;
    }

    /// Get or create a connection to a broker by ID.
    pub fn getConnection(self: *BrokerPool, broker_id: i32) !*BrokerConnection {
        const gop = try self.connections.getOrPut(broker_id);
        if (!gop.found_existing) {
            const addr = self.broker_addresses.get(broker_id) orelse {
                _ = self.connections.remove(broker_id);
                return error.UnknownBroker;
            };
            gop.value_ptr.* = try BrokerConnection.init(self.allocator, broker_id, addr);
        }
        return gop.value_ptr;
    }

    /// Bootstrap: connect to first reachable broker, do ApiVersions + SASL.
    pub fn bootstrap(self: *BrokerPool, bootstrap_brokers: []const BrokerAddress) !void {
        for (bootstrap_brokers) |addr| {
            var conn = BrokerConnection.init(self.allocator, -1, addr) catch continue;
            errdefer conn.deinit();

            conn.connect() catch continue;

            // SASL handshake (if configured)
            if (self.sasl_config) |sasl| {
                try self.performSasl(&conn, sasl);
            }

            // ApiVersions
            try self.performApiVersions(&conn);

            // Store as bootstrap connection (id=-1)
            try self.connections.put(-1, conn);
            return;
        }
        return error.NoReachableBroker;
    }

    /// Fetch metadata for a topic.
    pub fn fetchMetadata(self: *BrokerPool, topic: []const u8) !protocol.MetadataResponse {
        const conn = try self.getAnyReadyConnection();
        const version = self.negotiateVersion(.Metadata);

        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        try protocol.encodeMetadataRequest(&writer, topic, version);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .Metadata, version, corr_id, writer.getWritten());

        const response_data = try conn.receive();
        const header = try codec.decodeResponseHeader(response_data, corr_id);

        const metadata = try protocol.decodeMetadataResponse(header.body, version, self.allocator);

        // Update broker address table
        for (metadata.brokers) |broker| {
            try self.broker_addresses.put(broker.node_id, .{
                .host = broker.host,
                .port = @intCast(broker.port),
            });
        }

        return metadata;
    }

    /// Send a Fetch request and get the response.
    pub fn fetch(
        self: *BrokerPool,
        broker_id: i32,
        topic: []const u8,
        partitions: []const protocol.FetchPartitionRequest,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
    ) !protocol.FetchResponse {
        const conn = try self.getConnection(broker_id);
        if (conn.state != .ready) try conn.connect();
        if (self.sasl_config) |sasl| {
            if (conn.state == .ready and !self.api_versions_initialized) {
                try self.performSasl(conn, sasl);
                try self.performApiVersions(conn);
            }
        }

        const version = self.negotiateVersion(.Fetch);

        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        try protocol.encodeFetchRequest(&writer, version, topic, partitions, max_wait_ms, min_bytes, max_bytes, isolation_level);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .Fetch, version, corr_id, writer.getWritten());

        const response_data = try conn.receive();
        const header = try codec.decodeResponseHeader(response_data, corr_id);

        return try protocol.decodeFetchResponse(header.body, version, self.allocator);
    }

    /// Send ListOffsets request for a single partition.
    pub fn listOffsets(
        self: *BrokerPool,
        broker_id: i32,
        topic: []const u8,
        partition_id: i32,
        timestamp: i64,
        isolation_level: i8,
    ) !protocol.ListOffsetsResponse {
        const conn = try self.getConnection(broker_id);
        if (conn.state != .ready) try conn.connect();

        const version = self.negotiateVersion(.ListOffsets);

        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        try protocol.encodeListOffsetsRequest(&writer, version, topic, partition_id, timestamp, isolation_level);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .ListOffsets, version, corr_id, writer.getWritten());

        const response_data = try conn.receive();
        const header = try codec.decodeResponseHeader(response_data, corr_id);

        return try protocol.decodeListOffsetsResponse(header.body, version, self.allocator);
    }

    /// Send OffsetCommit for a single partition (fire-and-forget semantics for caller).
    pub fn sendOffsetCommit(
        self: *BrokerPool,
        group_id: []const u8,
        topic: []const u8,
        partition_id: i32,
        committed_offset: i64,
    ) !void {
        const conn = self.getAnyReadyConnection() catch return;
        const version = self.negotiateVersion(.OffsetCommit);

        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        try protocol.encodeOffsetCommitRequest(&writer, version, group_id, topic, partition_id, committed_offset);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .OffsetCommit, version, corr_id, writer.getWritten());

        // Read response but don't fail on errors (fire-and-forget)
        _ = conn.receive() catch {};
    }

    /// Fetch committed offsets for partitions.
    pub fn fetchOffsets(
        self: *BrokerPool,
        group_id: []const u8,
        topic: []const u8,
        partition_ids: []const i32,
    ) !protocol.OffsetFetchResponse {
        const conn = try self.getAnyReadyConnection();
        const version = self.negotiateVersion(.OffsetFetch);

        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        try protocol.encodeOffsetFetchRequest(&writer, version, group_id, topic, partition_ids);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .OffsetFetch, version, corr_id, writer.getWritten());

        const response_data = try conn.receive();
        const header = try codec.decodeResponseHeader(response_data, corr_id);

        return try protocol.decodeOffsetFetchResponse(header.body, version, self.allocator);
    }

    // =======================================================================
    // Internal Helpers
    // =======================================================================

    fn sendRequest(
        self: *BrokerPool,
        conn: *BrokerConnection,
        api_key: ApiKey,
        api_version: i16,
        correlation_id: i32,
        body: []const u8,
    ) !void {
        const frame = try codec.encodeRequest(
            self.allocator,
            api_key,
            api_version,
            correlation_id,
            self.client_id,
            body,
        );
        defer self.allocator.free(frame);
        try conn.send(frame);
    }

    fn performApiVersions(self: *BrokerPool, conn: *BrokerConnection) !void {
        var writer = KafkaWriter.init(self.allocator);
        defer writer.deinit();
        // Start with v0 (always supported)
        try protocol.encodeApiVersionsRequest(&writer, 0);

        const corr_id = self.nextCorrelationId();
        try self.sendRequest(conn, .ApiVersions, 0, corr_id, writer.getWritten());

        const response_data = try conn.receive();
        const header = try codec.decodeResponseHeader(response_data, corr_id);
        const result = try protocol.decodeApiVersionsResponse(header.body, 0);

        if (result.error_code != .none) {
            log.err("ApiVersions failed: {s}", .{result.error_code.toStr()});
            return error.ApiVersionsFailed;
        }

        self.api_versions = result.api_versions;
        self.api_versions_initialized = true;

        log.info("Negotiated API versions with broker ({d} APIs)", .{result.num_versions});
    }

    fn performSasl(self: *BrokerPool, conn: *BrokerConnection, sasl: SaslConfig) !void {
        _ = self;
        try auth_mod.performSaslHandshake(conn, sasl.mechanism, sasl.username, sasl.password);
    }

    fn getAnyReadyConnection(self: *BrokerPool) !*BrokerConnection {
        var iter = self.connections.valueIterator();
        while (iter.next()) |conn| {
            if (conn.state == .ready) return conn;
        }
        return error.NoReadyConnection;
    }
};

// =============================================================================
// Version Ranges We Support
// =============================================================================

fn getOurVersionRange(api_key: ApiKey) ApiVersionRange {
    return switch (api_key) {
        .ApiVersions => .{ .min_version = 0, .max_version = 3 },
        .Metadata => .{ .min_version = 1, .max_version = 12 },
        .Fetch => .{ .min_version = 4, .max_version = 16 },
        .ListOffsets => .{ .min_version = 1, .max_version = 8 },
        .OffsetCommit => .{ .min_version = 2, .max_version = 9 },
        .OffsetFetch => .{ .min_version = 1, .max_version = 9 },
        .SaslHandshake => .{ .min_version = 0, .max_version = 1 },
        .SaslAuthenticate => .{ .min_version = 0, .max_version = 2 },
    };
}

// =============================================================================
// DNS Resolution Helper
// =============================================================================

fn resolveDns(host: []const u8, port: u16) !std.net.Address {
    // Use getAddressList for DNS resolution
    const list = try std.net.getAddressList(std.heap.page_allocator, host, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.DnsResolutionFailed;
    return list.addrs[0];
}

// =============================================================================
// Tests
// =============================================================================

test "BrokerPool init and deinit" {
    var pool = BrokerPool.init(std.testing.allocator, null);
    defer pool.deinit();

    try std.testing.expectEqual(@as(i32, 1), pool.next_correlation_id);
    try std.testing.expect(!pool.api_versions_initialized);
}

test "BrokerPool nextCorrelationId increments" {
    var pool = BrokerPool.init(std.testing.allocator, null);
    defer pool.deinit();

    const id1 = pool.nextCorrelationId();
    const id2 = pool.nextCorrelationId();
    const id3 = pool.nextCorrelationId();

    try std.testing.expectEqual(@as(i32, 1), id1);
    try std.testing.expectEqual(@as(i32, 2), id2);
    try std.testing.expectEqual(@as(i32, 3), id3);
}

test "negotiateVersion without initialization returns 0" {
    var pool = BrokerPool.init(std.testing.allocator, null);
    defer pool.deinit();

    try std.testing.expectEqual(@as(i16, 0), pool.negotiateVersion(.Fetch));
}

test "negotiateVersion picks highest mutual version" {
    var pool = BrokerPool.init(std.testing.allocator, null);
    defer pool.deinit();

    // Simulate broker reporting Fetch v0-v14
    pool.api_versions[@intFromEnum(ApiKey.Fetch)] = .{ .min_version = 0, .max_version = 14 };
    pool.api_versions_initialized = true;

    // Our range for Fetch: v4-v16
    // Mutual: v4-v14, best = v14
    try std.testing.expectEqual(@as(i16, 14), pool.negotiateVersion(.Fetch));
}

test "getOurVersionRange for known APIs" {
    const fetch = getOurVersionRange(.Fetch);
    try std.testing.expectEqual(@as(i16, 4), fetch.min_version);
    try std.testing.expectEqual(@as(i16, 16), fetch.max_version);

    const metadata = getOurVersionRange(.Metadata);
    try std.testing.expectEqual(@as(i16, 1), metadata.min_version);
    try std.testing.expectEqual(@as(i16, 12), metadata.max_version);
}
