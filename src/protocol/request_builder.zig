//! Request Builder
//!
//! Helper for building protocol requests with auto-incrementing request IDs.
//! Provides a convenient API for tests, benchmarks, and CLI client.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const Request = proto.Request;
const OpCode = proto.OpCode;
const MAGIC = proto.MAGIC;
const VERSION = proto.VERSION;

/// Helper to build requests easily
pub const RequestBuilder = struct {
    allocator: Allocator,
    next_request_id: u64,

    pub fn init(allocator: Allocator) RequestBuilder {
        return .{
            .allocator = allocator,
            .next_request_id = 1,
        };
    }

    fn nextId(self: *RequestBuilder) u64 {
        const id = self.next_request_id;
        self.next_request_id += 1;
        return id;
    }

    // =========================================================================
    // Generic request builder (used by CLI and for full flexibility)
    // =========================================================================

    /// Build a request with all parameters including options
    pub fn build(
        self: *RequestBuilder,
        op_code: OpCode,
        namespace: []const u8,
        key: []const u8,
        value: []const u8,
        options: []const u8,
    ) Request {
        const request_id = self.nextId();

        return Request{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .op_code = @intFromEnum(op_code),
                .flags = 0,
                .reserved = .{0} ** 8,
                .payload_length = 0,
                .request_id = request_id,
                .crc32 = 0,
            },
            .namespace = namespace,
            .key = key,
            .value = value,
            .options = options,
        };
    }

    /// Build a request without options (convenience wrapper)
    pub fn buildSimple(
        self: *RequestBuilder,
        op_code: OpCode,
        namespace: []const u8,
        key: []const u8,
        value: []const u8,
    ) Request {
        return self.build(op_code, namespace, key, value, "");
    }

    // =========================================================================
    // Simple API: use "default" namespace (common case)
    // =========================================================================

    pub fn get(self: *RequestBuilder, key: []const u8) Request {
        return self.getNamespace("default", key);
    }

    pub fn put(self: *RequestBuilder, key: []const u8, value: []const u8) Request {
        return self.putNamespace("default", key, value);
    }

    pub fn delete(self: *RequestBuilder, key: []const u8) Request {
        return self.deleteNamespace("default", key);
    }

    // =========================================================================
    // Durability
    // =========================================================================

    /// Durability mode for per-connection override
    pub const DurabilityMode = enum(u8) {
        sync = 0,
        async_mode = 1,
        use_default = 255, // Clear override, use server config
    };

    /// Set durability mode for this connection
    ///
    /// Call once after connecting to override the server's default durability.
    /// - `.sync`: Full ACID (waits for fdatasync) - payments, workflows
    /// - `.async_mode`: High throughput (kernel page cache) - logs, analytics
    /// - `.use_default`: Clear override, use server's config default
    pub fn setDurability(self: *RequestBuilder, mode: DurabilityMode) Request {
        const request_id = self.nextId();

        return Request{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .op_code = @intFromEnum(OpCode.set_durability),
                .flags = 0,
                .reserved = .{0} ** 8,
                .payload_length = 0,
                .request_id = request_id,
                .crc32 = 0,
            },
            .namespace = &[_]u8{},
            .key = &[_]u8{},
            .value = &[_]u8{@intFromEnum(mode)},
        };
    }

    // =========================================================================
    // Namespace-aware API
    // =========================================================================

    pub fn getNamespace(self: *RequestBuilder, namespace: []const u8, key: []const u8) Request {
        const request_id = self.nextId();

        return Request{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .op_code = @intFromEnum(OpCode.kv_get),
                .flags = 0,
                .reserved = .{0} ** 8,
                .payload_length = 0,
                .request_id = request_id,
                .crc32 = 0,
            },
            .namespace = namespace,
            .key = key,
            .value = &[_]u8{},
        };
    }

    pub fn putNamespace(self: *RequestBuilder, namespace: []const u8, key: []const u8, value: []const u8) Request {
        const request_id = self.nextId();

        return Request{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .op_code = @intFromEnum(OpCode.kv_put),
                .flags = 0,
                .reserved = .{0} ** 8,
                .payload_length = 0,
                .request_id = request_id,
                .crc32 = 0,
            },
            .namespace = namespace,
            .key = key,
            .value = value,
        };
    }

    pub fn deleteNamespace(self: *RequestBuilder, namespace: []const u8, key: []const u8) Request {
        const request_id = self.nextId();

        return Request{
            .header = .{
                .magic = MAGIC,
                .version = VERSION,
                .op_code = @intFromEnum(OpCode.kv_delete),
                .flags = 0,
                .reserved = .{0} ** 8,
                .payload_length = 0,
                .request_id = request_id,
                .crc32 = 0,
            },
            .namespace = namespace,
            .key = key,
            .value = &[_]u8{},
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RequestBuilder basic operations" {
    const allocator = std.testing.allocator;
    var builder = RequestBuilder.init(allocator);

    // Test auto-incrementing IDs
    const req1 = builder.get("key1");
    try std.testing.expectEqual(@as(u64, 1), req1.header.request_id);

    const req2 = builder.put("key2", "value2");
    try std.testing.expectEqual(@as(u64, 2), req2.header.request_id);

    const req3 = builder.delete("key3");
    try std.testing.expectEqual(@as(u64, 3), req3.header.request_id);
}

test "RequestBuilder namespace operations" {
    const allocator = std.testing.allocator;
    var builder = RequestBuilder.init(allocator);

    const req = builder.getNamespace("prod", "mykey");
    try std.testing.expectEqualStrings("prod", req.namespace);
    try std.testing.expectEqualStrings("mykey", req.key);
}
