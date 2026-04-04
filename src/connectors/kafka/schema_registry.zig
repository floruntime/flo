//! Confluent Schema Registry Client
//!
//! HTTP client for fetching schemas from a Confluent-compatible Schema Registry.
//! Used by Avro and Protobuf deserializers to resolve schema IDs from the
//! Confluent wire format: [0x00][schema_id: i32 big-endian][payload...]
//!
//! Features:
//!   - LRU-bounded schema cache (max 100 entries)
//!   - Basic auth support
//!   - Thread-safe (one instance per KafkaSource, single-shard)

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

// =============================================================================
// Configuration
// =============================================================================

pub const SchemaRegistryConfig = struct {
    /// Schema Registry base URL (e.g. "http://localhost:8081")
    url: []const u8,
    /// Basic auth username (optional)
    username: []const u8 = "",
    /// Basic auth password (optional)
    password: []const u8 = "",
};

// =============================================================================
// Schema Types
// =============================================================================

pub const SchemaType = enum {
    avro,
    protobuf,
    json_schema,
};

pub const Schema = struct {
    id: i32,
    schema_type: SchemaType,
    schema: []const u8, // raw schema text (JSON for Avro, proto for Protobuf)
    /// Tracks LRU ordering — lower = older
    last_used: u64,
};

// =============================================================================
// Confluent Wire Format
// =============================================================================

/// Parse the Confluent wire format header from a Kafka record value.
/// Returns the schema_id and a slice pointing to the payload after the header.
/// Wire format: [0x00][schema_id: i32 BE][payload...]
pub fn parseConfluentHeader(data: []const u8) ?struct { schema_id: i32, payload: []const u8 } {
    if (data.len < 5) return null;
    if (data[0] != 0x00) return null; // magic byte
    const schema_id = std.mem.readInt(i32, data[1..5], .big);
    return .{ .schema_id = schema_id, .payload = data[5..] };
}

// =============================================================================
// Schema Registry Client
// =============================================================================

pub const SchemaRegistryClient = struct {
    allocator: Allocator,
    config: SchemaRegistryConfig,
    cache: std.AutoHashMap(i32, Schema),
    access_counter: u64,

    const MAX_CACHE_SIZE: usize = 100;

    pub fn init(allocator: Allocator, config: SchemaRegistryConfig) SchemaRegistryClient {
        return .{
            .allocator = allocator,
            .config = config,
            .cache = std.AutoHashMap(i32, Schema).init(allocator),
            .access_counter = 0,
        };
    }

    pub fn deinit(self: *SchemaRegistryClient) void {
        var it = self.cache.valueIterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.schema);
        }
        self.cache.deinit();
    }

    /// Fetch a schema by ID. Returns cached version if available.
    pub fn getSchema(self: *SchemaRegistryClient, schema_id: i32) !*const Schema {
        // Check cache first
        if (self.cache.getPtr(schema_id)) |entry| {
            self.access_counter += 1;
            entry.last_used = self.access_counter;
            return entry;
        }

        // Fetch from registry
        const schema_text = try self.fetchSchemaFromRegistry(schema_id);
        errdefer self.allocator.free(schema_text);

        // Evict if at capacity
        if (self.cache.count() >= MAX_CACHE_SIZE) {
            self.evictLru();
        }

        self.access_counter += 1;
        self.cache.put(schema_id, .{
            .id = schema_id,
            .schema_type = .avro, // default; could parse from registry response
            .schema = schema_text,
            .last_used = self.access_counter,
        }) catch return error.SchemaRegistryError;

        return self.cache.getPtr(schema_id) orelse return error.SchemaRegistryError;
    }

    /// Evict the least-recently-used cache entry.
    fn evictLru(self: *SchemaRegistryClient) void {
        var min_used: u64 = std.math.maxInt(u64);
        var evict_id: ?i32 = null;

        var it = self.cache.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.last_used < min_used) {
                min_used = entry.value_ptr.last_used;
                evict_id = entry.key_ptr.*;
            }
        }

        if (evict_id) |id| {
            if (self.cache.fetchRemove(id)) |kv| {
                self.allocator.free(kv.value.schema);
            }
        }
    }

    /// HTTP GET /schemas/ids/{id} from the Schema Registry.
    fn fetchSchemaFromRegistry(self: *SchemaRegistryClient, schema_id: i32) ![]const u8 {
        // Build URL: {base_url}/schemas/ids/{schema_id}
        var url_buf: [512]u8 = undefined;
        const url = std.fmt.bufPrint(&url_buf, "{s}/schemas/ids/{d}", .{
            self.config.url, schema_id,
        }) catch return error.SchemaRegistryError;

        // Use std.http.Client for the HTTP request
        var client: std.http.Client = .{ .allocator = self.allocator };
        defer client.deinit();

        // Prepare response body collector
        var response_body = std.Io.Writer.Allocating.init(self.allocator);
        defer response_body.deinit();

        // Build extra headers for auth
        var auth_header_buf: [256]u8 = undefined;
        var extra_headers_buf: [1]std.http.Header = undefined;
        var extra_headers: []const std.http.Header = &.{};

        if (self.config.username.len > 0) {
            const auth_value = buildBasicAuth(
                self.config.username,
                self.config.password,
                &auth_header_buf,
            );
            if (auth_value) |val| {
                extra_headers_buf[0] = .{ .name = "Authorization", .value = val };
                extra_headers = extra_headers_buf[0..1];
            }
        }

        const result = client.fetch(.{
            .location = .{ .url = url },
            .method = .GET,
            .response_writer = &response_body.writer,
            .extra_headers = extra_headers,
        }) catch |err| {
            log.err("Schema Registry HTTP request failed for schema {d}: {}", .{ schema_id, err });
            return error.SchemaRegistryError;
        };

        if (result.status != .ok) {
            log.err("Schema Registry returned HTTP {d} for schema {d}", .{
                @intFromEnum(result.status), schema_id,
            });
            return error.SchemaRegistryError;
        }

        // Parse the response JSON: {"schema": "..."}
        const body = response_body.written();
        return self.extractSchemaFromResponse(body) catch {
            log.err("Failed to parse Schema Registry response for schema {d}", .{schema_id});
            return error.SchemaRegistryError;
        };
    }

    /// Extract the "schema" field from the Schema Registry JSON response.
    /// Response format: {"schema": "{\"type\":\"record\",...}", "schemaType": "AVRO", ...}
    fn extractSchemaFromResponse(self: *SchemaRegistryClient, json_body: []const u8) ![]const u8 {
        // Simple extraction: find "schema":" and extract the value
        // The schema field is a JSON-encoded string (escaped JSON within JSON)
        const parsed = std.json.parseFromSlice(SchemaResponse, self.allocator, json_body, .{
            .ignore_unknown_fields = true,
        }) catch return error.SchemaRegistryError;
        defer parsed.deinit();

        // Dupe the schema string since parsed will be freed
        return self.allocator.dupe(u8, parsed.value.schema) catch return error.SchemaRegistryError;
    }
};

const SchemaResponse = struct {
    schema: []const u8,
};

/// Build a Basic Auth header value: "Basic base64(username:password)"
fn buildBasicAuth(username: []const u8, password: []const u8, buf: *[256]u8) ?[]const u8 {
    if (username.len == 0) return null;

    // Build "username:password"
    var cred_buf: [128]u8 = undefined;
    const creds = std.fmt.bufPrint(&cred_buf, "{s}:{s}", .{ username, password }) catch return null;

    // Base64 encode
    const prefix = "Basic ";
    const encoded_len = std.base64.standard.Encoder.calcSize(creds.len);
    if (prefix.len + encoded_len > buf.len) return null;

    @memcpy(buf[0..prefix.len], prefix);
    _ = std.base64.standard.Encoder.encode(buf[prefix.len..], creds);
    return buf[0 .. prefix.len + encoded_len];
}

// =============================================================================
// Tests
// =============================================================================

test "parseConfluentHeader valid" {
    const data = [_]u8{
        0x00, // magic byte
        0x00, 0x00, 0x00, 0x2A, // schema_id = 42
        'h', 'e', 'l', 'l', 'o', // payload
    };
    const result = parseConfluentHeader(&data).?;
    try std.testing.expectEqual(@as(i32, 42), result.schema_id);
    try std.testing.expectEqualStrings("hello", result.payload);
}

test "parseConfluentHeader too short" {
    const data = [_]u8{ 0x00, 0x01, 0x02 };
    try std.testing.expect(parseConfluentHeader(&data) == null);
}

test "parseConfluentHeader wrong magic" {
    const data = [_]u8{ 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF };
    try std.testing.expect(parseConfluentHeader(&data) == null);
}

test "SchemaRegistryClient cache" {
    const allocator = std.testing.allocator;
    var client = SchemaRegistryClient.init(allocator, .{
        .url = "http://localhost:8081",
    });
    defer client.deinit();

    // Manually insert a schema into cache
    const schema_text = try allocator.dupe(u8, "{\"type\":\"string\"}");
    try client.cache.put(1, .{
        .id = 1,
        .schema_type = .avro,
        .schema = schema_text,
        .last_used = 1,
    });

    // Should be retrievable from cache
    const schema = try client.getSchema(1);
    try std.testing.expectEqual(@as(i32, 1), schema.id);
    try std.testing.expectEqualStrings("{\"type\":\"string\"}", schema.schema);
}

test "SchemaRegistryClient LRU eviction" {
    const allocator = std.testing.allocator;
    var client = SchemaRegistryClient.init(allocator, .{
        .url = "http://localhost:8081",
    });
    defer client.deinit();

    // Fill cache to MAX_CACHE_SIZE
    for (0..SchemaRegistryClient.MAX_CACHE_SIZE) |i| {
        const id: i32 = @intCast(i);
        const schema_text = try allocator.dupe(u8, "schema");
        client.access_counter += 1;
        try client.cache.put(id, .{
            .id = id,
            .schema_type = .avro,
            .schema = schema_text,
            .last_used = client.access_counter,
        });
    }
    try std.testing.expectEqual(SchemaRegistryClient.MAX_CACHE_SIZE, client.cache.count());

    // Access schema 0 to make it recently used
    _ = try client.getSchema(0);

    // Evict — should remove schema 1 (lowest last_used after 0 was bumped)
    client.evictLru();
    try std.testing.expectEqual(SchemaRegistryClient.MAX_CACHE_SIZE - 1, client.cache.count());
    try std.testing.expect(client.cache.get(1) == null); // schema 1 was evicted
    try std.testing.expect(client.cache.get(0) != null); // schema 0 was kept
}

test "buildBasicAuth" {
    var buf: [256]u8 = undefined;
    const result = buildBasicAuth("alice", "s3cret", &buf).?;
    try std.testing.expect(std.mem.startsWith(u8, result, "Basic "));

    // Decode and verify
    var dec_buf: [128]u8 = undefined;
    const encoded = result["Basic ".len..];
    try std.base64.standard.Decoder.decode(&dec_buf, encoded);
    const decoded = dec_buf[0 .. std.base64.standard.Decoder.calcSizeForSlice(encoded) catch unreachable];
    try std.testing.expectEqualStrings("alice:s3cret", decoded);
}

test "buildBasicAuth empty username" {
    var buf: [256]u8 = undefined;
    try std.testing.expect(buildBasicAuth("", "", &buf) == null);
}
