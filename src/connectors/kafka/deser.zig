//! Kafka Record Value Deserializer
//!
//! Handles format-specific deserialization of Kafka record values
//! into the []const u8 that ProcessingRecord expects.
//!
//! Phase 1 formats: raw, json, string.
//! Phase 3: avro, protobuf (requires Schema Registry).

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = @import("stdx").log;

const schema_registry = @import("schema_registry.zig");
const avro = @import("avro.zig");
const protobuf = @import("protobuf.zig");

pub const Format = enum(u8) {
    raw = 0,
    json = 1,
    string = 2,
    avro = 3,
    protobuf = 4,
};

pub const DeserializeError = enum(u8) {
    skip = 0,
    dead_letter = 1,
    fail = 2,
};

pub const Deserializer = struct {
    format: Format,
    on_error: DeserializeError,
    registry_client: ?*schema_registry.SchemaRegistryClient = null,

    pub fn init(format: Format, on_error: DeserializeError) Deserializer {
        return .{ .format = format, .on_error = on_error };
    }

    /// Deserialize a Kafka record value according to the configured format.
    /// Returns the value for ProcessingRecord. The returned slice may alias
    /// the input (for raw/json) or be newly allocated (for string wrapping).
    pub fn deserialize(self: *const Deserializer, value: ?[]const u8, allocator: Allocator) !?[]const u8 {
        const v = value orelse return null;
        if (v.len == 0) return v;

        return switch (self.format) {
            .raw => v,
            .json => self.deserializeJson(v, allocator),
            .string => self.deserializeString(v, allocator),
            .avro => return self.deserializeAvro(v, allocator),
            .protobuf => return self.deserializeProtobuf(v, allocator),
        };
    }

    fn deserializeJson(self: *const Deserializer, data: []const u8, allocator: Allocator) !?[]const u8 {
        // Validate JSON by checking basic structure
        if (!isValidJson(data)) {
            return self.handleError("invalid JSON", data, allocator);
        }
        return data; // JSON passes through as-is
    }

    fn deserializeString(self: *const Deserializer, data: []const u8, allocator: Allocator) !?[]const u8 {
        _ = self;
        // If it's already valid JSON, pass through
        if (isValidJson(data)) return data;

        // Otherwise wrap in {"value": "..."} with JSON escaping
        var result: std.ArrayList(u8) = .{};
        defer result.deinit(allocator);

        try result.appendSlice(allocator, "{\"value\":\"");
        for (data) |c| {
            switch (c) {
                '"' => try result.appendSlice(allocator, "\\\""),
                '\\' => try result.appendSlice(allocator, "\\\\"),
                '\n' => try result.appendSlice(allocator, "\\n"),
                '\r' => try result.appendSlice(allocator, "\\r"),
                '\t' => try result.appendSlice(allocator, "\\t"),
                else => {
                    if (c < 0x20) {
                        // Control character — encode as \u00XX
                        var esc: [6]u8 = undefined;
                        _ = std.fmt.bufPrint(&esc, "\\u{x:0>4}", .{c}) catch unreachable;
                        try result.appendSlice(allocator, &esc);
                    } else {
                        try result.append(allocator, c);
                    }
                },
            }
        }
        try result.appendSlice(allocator, "\"}");

        return try result.toOwnedSlice(allocator);
    }

    fn handleError(self: *const Deserializer, reason: []const u8, data: []const u8, allocator: Allocator) !?[]const u8 {
        switch (self.on_error) {
            .skip => {
                log.warn("Skipping record: {s} (length={d})", .{ reason, data.len });
                return null;
            },
            .dead_letter => {
                // Return the raw data — caller routes to DLQ
                log.warn("Dead-lettering record: {s} (length={d})", .{ reason, data.len });
                return data;
            },
            .fail => {
                _ = allocator;
                log.err("Deserialize error (fail mode): {s}", .{reason});
                return error.DeserializationFailed;
            },
        }
    }

    fn deserializeAvro(self: *const Deserializer, data: []const u8, allocator: Allocator) !?[]const u8 {
        // Parse Confluent wire header: [0x00][schema_id:i32 BE][avro_bytes...]
        const header = schema_registry.parseConfluentHeader(data) orelse {
            return self.handleError("invalid Confluent Avro header", data, allocator);
        };

        // Get schema from registry (required for Avro)
        const client = self.registry_client orelse {
            log.err("Avro format requires Schema Registry client", .{});
            return self.handleError("no schema registry configured", data, allocator);
        };

        const schema_obj = client.getSchema(header.schema_id) catch {
            return self.handleError("schema registry lookup failed", data, allocator);
        };

        // Parse Avro schema JSON → AvroType
        var schema_parser = avro.SchemaParser.init(allocator);
        defer schema_parser.deinit();
        const avro_type = schema_parser.parse(schema_obj.schema) catch {
            return self.handleError("invalid Avro schema", data, allocator);
        };

        // Decode Avro binary → JSON
        var avro_decoder = avro.AvroDecoder.init(allocator);
        const json = avro_decoder.decode(header.payload, avro_type) catch {
            return self.handleError("Avro decode failed", data, allocator);
        };

        return json;
    }

    fn deserializeProtobuf(self: *const Deserializer, data: []const u8, allocator: Allocator) !?[]const u8 {
        // Parse Confluent protobuf header (magic + schema_id + message indexes)
        const header = protobuf.parseConfluentProtobufHeader(data) orelse {
            // No Confluent header — try raw protobuf decode
            var decoder = protobuf.ProtobufDecoder.init(allocator);
            const json = decoder.decode(data) catch {
                return self.handleError("protobuf decode failed", data, allocator);
            };
            return json;
        };

        // Decode protobuf payload → JSON (self-describing, schema-less)
        var decoder = protobuf.ProtobufDecoder.init(allocator);
        const json = decoder.decode(header.payload) catch {
            return self.handleError("protobuf decode failed", data, allocator);
        };

        return json;
    }
};

/// Simple JSON validation — checks that the data starts with { or [ and is
/// well-balanced. Not a full parser; sufficient for format detection.
fn isValidJson(data: []const u8) bool {
    if (data.len == 0) return false;

    // Trim leading whitespace
    var start: usize = 0;
    while (start < data.len and (data[start] == ' ' or data[start] == '\t' or data[start] == '\n' or data[start] == '\r')) {
        start += 1;
    }
    if (start >= data.len) return false;

    const first = data[start];
    if (first != '{' and first != '[' and first != '"' and first != 't' and first != 'f' and first != 'n' and (first < '0' or first > '9') and first != '-') {
        return false;
    }

    // Use std.json to validate
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, data, .{}) catch return false;
    parsed.deinit();
    return true;
}

// =============================================================================
// Tests
// =============================================================================

test "Deserializer raw passthrough" {
    const deser = Deserializer.init(.raw, .skip);
    const data = "hello world";
    const result = try deser.deserialize(data, std.testing.allocator);
    try std.testing.expectEqualStrings("hello world", result.?);
}

test "Deserializer raw null value" {
    const deser = Deserializer.init(.raw, .skip);
    const result = try deser.deserialize(null, std.testing.allocator);
    try std.testing.expect(result == null);
}

test "Deserializer json valid" {
    const deser = Deserializer.init(.json, .skip);
    const data = "{\"key\":\"value\"}";
    const result = try deser.deserialize(data, std.testing.allocator);
    try std.testing.expectEqualStrings(data, result.?);
}

test "Deserializer json invalid skips" {
    const deser = Deserializer.init(.json, .skip);
    const data = "not json at all";
    const result = try deser.deserialize(data, std.testing.allocator);
    try std.testing.expect(result == null);
}

test "Deserializer string wraps non-JSON" {
    const deser = Deserializer.init(.string, .skip);
    const data = "plain text";
    const result = try deser.deserialize(data, std.testing.allocator);
    defer std.testing.allocator.free(result.?);
    try std.testing.expectEqualStrings("{\"value\":\"plain text\"}", result.?);
}

test "Deserializer string passes JSON through" {
    const deser = Deserializer.init(.string, .skip);
    const data = "{\"already\":\"json\"}";
    const result = try deser.deserialize(data, std.testing.allocator);
    // Should not allocate — passes through as-is
    try std.testing.expectEqualStrings(data, result.?);
}

test "Deserializer string escapes special characters" {
    const deser = Deserializer.init(.string, .skip);
    const data = "line1\nline2\ttab\"quote";
    const result = try deser.deserialize(data, std.testing.allocator);
    defer std.testing.allocator.free(result.?);
    try std.testing.expectEqualStrings("{\"value\":\"line1\\nline2\\ttab\\\"quote\"}", result.?);
}

test "isValidJson with various inputs" {
    try std.testing.expect(isValidJson("{\"key\":1}"));
    try std.testing.expect(isValidJson("[1,2,3]"));
    try std.testing.expect(isValidJson("\"hello\""));
    try std.testing.expect(isValidJson("42"));
    try std.testing.expect(isValidJson("true"));
    try std.testing.expect(isValidJson("null"));
    try std.testing.expect(!isValidJson(""));
    try std.testing.expect(!isValidJson("not json"));
    try std.testing.expect(!isValidJson("{broken"));
}
