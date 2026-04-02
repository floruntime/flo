//! SASL Authentication for Kafka connections.
//!
//! Phase 1 supports: SASL/PLAIN (plaintext mechanism).
//! Phase 2: SCRAM-SHA-256, SCRAM-SHA-512.
//! Phase 3: AWS MSK IAM.

const std = @import("std");
const Allocator = std.mem.Allocator;
const codec = @import("codec.zig");
const KafkaWriter = codec.KafkaWriter;
const KafkaReader = codec.KafkaReader;
const broker_mod = @import("broker.zig");
const BrokerConnection = broker_mod.BrokerConnection;

const log = @import("stdx").log;

// =============================================================================
// SASL/PLAIN Token
// =============================================================================

/// Build a SASL/PLAIN token: \0{username}\0{password}
pub fn buildPlainToken(username: []const u8, password: []const u8, allocator: Allocator) ![]u8 {
    const token_len = 1 + username.len + 1 + password.len;
    const token = try allocator.alloc(u8, token_len);
    token[0] = 0; // authzid (empty)
    @memcpy(token[1..][0..username.len], username);
    token[1 + username.len] = 0;
    @memcpy(token[2 + username.len ..][0..password.len], password);
    return token;
}

// =============================================================================
// SASL Handshake + Authenticate Flow
// =============================================================================

/// Perform the full SASL handshake and authentication on a broker connection.
///
/// Flow:
///   1. SaslHandshake request → verify mechanism supported
///   2. SaslAuthenticate request → send PLAIN token
///   3. Connection is now authenticated
pub fn performSaslHandshake(
    conn: *BrokerConnection,
    mechanism: []const u8,
    username: []const u8,
    password: []const u8,
) !void {
    conn.state = .sasl_handshake;

    // Step 1: SaslHandshake
    {
        var writer = KafkaWriter.init(conn.allocator);
        defer writer.deinit();

        // SaslHandshake v1: compact string mechanism
        try writer.writeString(mechanism);

        const frame = try codec.encodeRequest(
            conn.allocator,
            .SaslHandshake,
            1, // v1
            0, // correlation_id
            "flo-kafka-source",
            writer.getWritten(),
        );
        defer conn.allocator.free(frame);

        try conn.send(frame);
        const response_data = try conn.receive();

        // Parse response: error_code(i16) + enabled_mechanisms(array of string)
        const header = try codec.decodeResponseHeader(response_data, 0);
        var reader = KafkaReader.init(header.body);
        const error_code = try reader.readInt16();

        if (error_code != 0) {
            // Read supported mechanisms for error message
            const mech_count = try reader.readArrayLen();
            log.err("SASL mechanism '{s}' not supported by broker. Supported mechanisms ({d}):", .{ mechanism, mech_count });
            if (mech_count > 0) {
                var i: i32 = 0;
                while (i < mech_count) : (i += 1) {
                    if (try reader.readString()) |m| {
                        log.err("  - {s}", .{m});
                    }
                }
            }
            return error.SaslMechanismNotSupported;
        }
    }

    // Step 2: SaslAuthenticate
    conn.state = .sasl_authenticating;
    {
        const token = try buildPlainToken(username, password, conn.allocator);
        defer conn.allocator.free(token);

        var writer = KafkaWriter.init(conn.allocator);
        defer writer.deinit();

        // SaslAuthenticate v1: auth_bytes (bytes)
        try writer.writeBytes(token);

        const frame = try codec.encodeRequest(
            conn.allocator,
            .SaslAuthenticate,
            1, // v1
            1, // correlation_id
            "flo-kafka-source",
            writer.getWritten(),
        );
        defer conn.allocator.free(frame);

        try conn.send(frame);
        const response_data = try conn.receive();

        const header = try codec.decodeResponseHeader(response_data, 1);
        var reader = KafkaReader.init(header.body);
        const error_code = try reader.readInt16();

        if (error_code != 0) {
            const error_message = try reader.readString();
            if (error_message) |msg| {
                log.err("SASL authentication failed: {s}", .{msg});
            } else {
                log.err("SASL authentication failed with error code {d}", .{error_code});
            }
            conn.state = .failed;
            return error.SaslAuthenticationFailed;
        }
    }

    conn.state = .ready;
    log.info("SASL/{s} authentication successful", .{mechanism});
}

// =============================================================================
// Tests
// =============================================================================

test "buildPlainToken format" {
    const token = try buildPlainToken("alice", "s3cret", std.testing.allocator);
    defer std.testing.allocator.free(token);

    // Format: \0alice\0s3cret
    try std.testing.expectEqual(@as(usize, 13), token.len);
    try std.testing.expectEqual(@as(u8, 0), token[0]);
    try std.testing.expectEqualStrings("alice", token[1..6]);
    try std.testing.expectEqual(@as(u8, 0), token[6]);
    try std.testing.expectEqualStrings("s3cret", token[7..13]);
}

test "buildPlainToken empty credentials" {
    const token = try buildPlainToken("", "", std.testing.allocator);
    defer std.testing.allocator.free(token);

    // Format: \0\0
    try std.testing.expectEqual(@as(usize, 2), token.len);
    try std.testing.expectEqual(@as(u8, 0), token[0]);
    try std.testing.expectEqual(@as(u8, 0), token[1]);
}
