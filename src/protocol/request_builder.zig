//! Request Builder
//!
//! Helper for building protocol requests with auto-incrementing request IDs.
//! Provides a convenient API for tests, benchmarks, and CLI client.

const std = @import("std");
const Allocator = std.mem.Allocator;
const proto = @import("proto.zig");
const Request = proto.Request;
const OpCode = proto.OpCode;

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
            .header = proto.RequestHeader.init(op_code, request_id),
            .namespace = namespace,
            .key = key,
            .value = value,
            .options = options,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RequestBuilder numbers its requests and keeps what it's given" {
    var builder = RequestBuilder.init(std.testing.allocator);
    const first = builder.build(.kv_delete, "prod", "mykey", "", "");
    const second = builder.build(.kv_exists, "prod", "other", "", "");
    try std.testing.expectEqual(@as(u64, 1), first.header.request_id);
    try std.testing.expectEqual(@as(u64, 2), second.header.request_id);
    try std.testing.expectEqualStrings("prod", first.namespace);
    try std.testing.expectEqualStrings("mykey", first.key);
}
