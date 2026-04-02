//! SASL Authentication for Kafka connections.
//!
//! Phase 1: SASL/PLAIN (plaintext mechanism).
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

// Crypto imports for SCRAM
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const HmacSha512 = std.crypto.auth.hmac.sha2.HmacSha512;
const Sha256 = std.crypto.hash.sha2.Sha256;
const Sha512 = std.crypto.hash.sha2.Sha512;
const pbkdf2 = std.crypto.pwhash.pbkdf2;

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
// SCRAM-SHA-256 / SCRAM-SHA-512 (RFC 5802)
// =============================================================================
//
// SCRAM flow over Kafka's SaslAuthenticate API:
//   1. client-first-message  → SaslAuthenticate request
//   2. server-first-message  ← SaslAuthenticate response
//   3. client-final-message  → SaslAuthenticate request
//   4. server-final-message  ← SaslAuthenticate response
//
// client-first: n,,n=<user>,r=<client-nonce>
// server-first: r=<nonce>,s=<salt>,i=<iterations>
// client-final:  c=biws,r=<nonce>,p=<proof>
// server-final:  v=<server-signature>

pub fn performScramHandshake(
    conn: *BrokerConnection,
    mechanism: []const u8,
    username: []const u8,
    password: []const u8,
) !void {
    const is_sha512 = std.mem.eql(u8, mechanism, "SCRAM-SHA-512");
    const digest_len: usize = if (is_sha512) 64 else 32;

    // Step 1: SaslHandshake (same as PLAIN)
    conn.state = .sasl_handshake;
    try sendSaslHandshakeRequest(conn, mechanism);

    // Step 2: client-first-message
    conn.state = .sasl_authenticating;
    const client_nonce = generateNonce();
    const gs2_header = "n,,";

    // Build client-first-message-bare: n=<user>,r=<nonce>
    var client_first_bare_buf: [512]u8 = undefined;
    const client_first_bare = std.fmt.bufPrint(&client_first_bare_buf, "n={s},r={s}", .{
        username, &client_nonce,
    }) catch return error.SaslAuthenticationFailed;

    // Full client-first: gs2-header + bare
    var client_first_buf: [600]u8 = undefined;
    const client_first = std.fmt.bufPrint(&client_first_buf, "{s}{s}", .{
        gs2_header, client_first_bare,
    }) catch return error.SaslAuthenticationFailed;

    // Send client-first
    const server_first_data = try sendSaslAuthenticateAndReceive(conn, client_first, 2);

    // Step 3: Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
    const server_first = parseServerFirst(server_first_data) orelse {
        log.err("SCRAM: Failed to parse server-first-message", .{});
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    };

    // Validate server nonce starts with our client nonce
    if (!std.mem.startsWith(u8, server_first.nonce, &client_nonce)) {
        log.err("SCRAM: Server nonce doesn't start with client nonce", .{});
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    }

    // Decode salt from base64
    var salt_buf: [128]u8 = undefined;
    const salt = base64Decode(server_first.salt, &salt_buf) orelse {
        log.err("SCRAM: Invalid base64 salt", .{});
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    };

    // Step 4: Compute SCRAM proof
    // SaltedPassword = PBKDF2(password, salt, iterations)
    var salted_password: [64]u8 = undefined; // max SHA-512 = 64 bytes
    const sp = salted_password[0..digest_len];

    if (is_sha512) {
        pbkdf2(sp, password, salt, server_first.iterations, HmacSha512) catch {
            return error.SaslAuthenticationFailed;
        };
    } else {
        pbkdf2(sp, password, salt, server_first.iterations, HmacSha256) catch {
            return error.SaslAuthenticationFailed;
        };
    }

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [64]u8 = undefined;
    const ck = client_key[0..digest_len];
    if (is_sha512) {
        HmacSha512.create(ck[0..HmacSha512.mac_length], "Client Key", sp);
    } else {
        HmacSha256.create(ck[0..HmacSha256.mac_length], "Client Key", sp);
    }

    // StoredKey = Hash(ClientKey)
    var stored_key: [64]u8 = undefined;
    const sk = stored_key[0..digest_len];
    if (is_sha512) {
        Sha512.hash(ck, sk[0..Sha512.digest_length], .{});
    } else {
        Sha256.hash(ck, sk[0..Sha256.digest_length], .{});
    }

    // channel-binding = "biws" (base64 of "n,,")
    // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
    var client_final_no_proof_buf: [512]u8 = undefined;
    const client_final_no_proof = std.fmt.bufPrint(&client_final_no_proof_buf, "c=biws,r={s}", .{
        server_first.nonce,
    }) catch return error.SaslAuthenticationFailed;

    var auth_message_buf: [2048]u8 = undefined;
    const auth_message = std.fmt.bufPrint(&auth_message_buf, "{s},{s},{s}", .{
        client_first_bare, server_first_data, client_final_no_proof,
    }) catch return error.SaslAuthenticationFailed;

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    var client_sig: [64]u8 = undefined;
    const cs = client_sig[0..digest_len];
    if (is_sha512) {
        HmacSha512.create(cs[0..HmacSha512.mac_length], auth_message, sk);
    } else {
        HmacSha256.create(cs[0..HmacSha256.mac_length], auth_message, sk);
    }

    // ClientProof = ClientKey XOR ClientSignature
    var client_proof: [64]u8 = undefined;
    for (0..digest_len) |i| {
        client_proof[i] = ck[i] ^ cs[i];
    }

    // Base64-encode the proof
    var proof_b64_buf: [128]u8 = undefined;
    const proof_b64 = base64Encode(client_proof[0..digest_len], &proof_b64_buf);

    // Build client-final-message: c=biws,r=<nonce>,p=<proof>
    var client_final_buf: [700]u8 = undefined;
    const client_final = std.fmt.bufPrint(&client_final_buf, "{s},p={s}", .{
        client_final_no_proof, proof_b64,
    }) catch return error.SaslAuthenticationFailed;

    // Send client-final
    const server_final_data = try sendSaslAuthenticateAndReceive(conn, client_final, 3);

    // Step 5: Verify server-final-message: v=<server-signature>
    // ServerKey = HMAC(SaltedPassword, "Server Key")
    var server_key: [64]u8 = undefined;
    const svk = server_key[0..digest_len];
    if (is_sha512) {
        HmacSha512.create(svk[0..HmacSha512.mac_length], "Server Key", sp);
    } else {
        HmacSha256.create(svk[0..HmacSha256.mac_length], "Server Key", sp);
    }

    // ServerSignature = HMAC(ServerKey, AuthMessage)
    var expected_server_sig: [64]u8 = undefined;
    const ess = expected_server_sig[0..digest_len];
    if (is_sha512) {
        HmacSha512.create(ess[0..HmacSha512.mac_length], auth_message, svk);
    } else {
        HmacSha256.create(ess[0..HmacSha256.mac_length], auth_message, svk);
    }

    var expected_b64_buf: [128]u8 = undefined;
    const expected_b64 = base64Encode(ess, &expected_b64_buf);

    // Parse server-final: v=<base64>
    if (!std.mem.startsWith(u8, server_final_data, "v=")) {
        // Check for error indication
        if (std.mem.startsWith(u8, server_final_data, "e=")) {
            log.err("SCRAM server error: {s}", .{server_final_data[2..]});
        } else {
            log.err("SCRAM: Unexpected server-final: {s}", .{server_final_data});
        }
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    }

    const server_sig_b64 = server_final_data[2..];
    if (!std.mem.eql(u8, server_sig_b64, expected_b64)) {
        log.err("SCRAM: Server signature verification failed", .{});
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    }

    conn.state = .ready;
    log.info("SASL/{s} authentication successful", .{mechanism});
}

/// Generate a 24-byte random nonce encoded as 32 base64 chars.
fn generateNonce() [32]u8 {
    var raw: [24]u8 = undefined;
    std.crypto.random.bytes(&raw);
    var encoded: [32]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&encoded, &raw);
    return encoded;
}

const ServerFirstMessage = struct {
    nonce: []const u8,
    salt: []const u8,
    iterations: u32,
};

fn parseServerFirst(data: []const u8) ?ServerFirstMessage {
    var nonce: ?[]const u8 = null;
    var salt: ?[]const u8 = null;
    var iterations: ?u32 = null;

    var iter = std.mem.splitScalar(u8, data, ',');
    while (iter.next()) |part| {
        if (part.len < 2) continue;
        if (part[0] == 'r' and part[1] == '=') {
            nonce = part[2..];
        } else if (part[0] == 's' and part[1] == '=') {
            salt = part[2..];
        } else if (part[0] == 'i' and part[1] == '=') {
            iterations = std.fmt.parseInt(u32, part[2..], 10) catch null;
        }
    }

    if (nonce != null and salt != null and iterations != null) {
        return .{ .nonce = nonce.?, .salt = salt.?, .iterations = iterations.? };
    }
    return null;
}

fn base64Decode(encoded: []const u8, buf: *[128]u8) ?[]const u8 {
    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return null;
    if (decoded_len > buf.len) return null;
    std.base64.standard.Decoder.decode(buf, encoded) catch return null;
    return buf[0..decoded_len];
}

fn base64Encode(data: []const u8, buf: *[128]u8) []const u8 {
    const encoded_len = std.base64.standard.Encoder.calcSize(data.len);
    if (encoded_len > buf.len) return "";
    _ = std.base64.standard.Encoder.encode(buf, data);
    return buf[0..encoded_len];
}

fn sendSaslHandshakeRequest(conn: *BrokerConnection, mechanism: []const u8) !void {
    var writer = KafkaWriter.init(conn.allocator);
    defer writer.deinit();

    try writer.writeString(mechanism);

    const frame = try codec.encodeRequest(
        conn.allocator,
        .SaslHandshake,
        1,
        0,
        "flo-kafka-source",
        writer.getWritten(),
    );
    defer conn.allocator.free(frame);

    try conn.send(frame);
    const response_data = try conn.receive();

    const header = try codec.decodeResponseHeader(response_data, 0);
    var reader = KafkaReader.init(header.body);
    const error_code = try reader.readInt16();

    if (error_code != 0) {
        const mech_count = try reader.readArrayLen();
        log.err("SASL mechanism '{s}' not supported by broker. Supported ({d}):", .{ mechanism, mech_count });
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

/// Send a SaslAuthenticate request and return the response auth_bytes as a slice.
fn sendSaslAuthenticateAndReceive(conn: *BrokerConnection, auth_bytes: []const u8, corr_id: i32) ![]const u8 {
    var writer = KafkaWriter.init(conn.allocator);
    defer writer.deinit();

    try writer.writeBytes(auth_bytes);

    const frame = try codec.encodeRequest(
        conn.allocator,
        .SaslAuthenticate,
        1,
        corr_id,
        "flo-kafka-source",
        writer.getWritten(),
    );
    defer conn.allocator.free(frame);

    try conn.send(frame);
    const response_data = try conn.receive();

    const header = try codec.decodeResponseHeader(response_data, corr_id);
    var reader = KafkaReader.init(header.body);
    const error_code = try reader.readInt16();

    if (error_code != 0) {
        const error_message = try reader.readString();
        if (error_message) |msg| {
            log.err("SASL authentication step failed: {s}", .{msg});
        }
        conn.state = .failed;
        return error.SaslAuthenticationFailed;
    }

    // Skip error_message (should be null on success)
    _ = try reader.readString();

    // Read auth_bytes field
    const response_auth = try reader.readBytes();
    return response_auth orelse return error.SaslAuthenticationFailed;
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

test "parseServerFirst valid" {
    const msg = "r=clientNonce123serverNonce456,s=c2FsdHZhbHVl,i=4096";
    const result = parseServerFirst(msg).?;
    try std.testing.expectEqualStrings("clientNonce123serverNonce456", result.nonce);
    try std.testing.expectEqualStrings("c2FsdHZhbHVl", result.salt);
    try std.testing.expectEqual(@as(u32, 4096), result.iterations);
}

test "parseServerFirst missing field" {
    const msg = "r=nonce,s=salt";
    try std.testing.expect(parseServerFirst(msg) == null);
}

test "parseServerFirst invalid iterations" {
    const msg = "r=nonce,s=salt,i=notanumber";
    try std.testing.expect(parseServerFirst(msg) == null);
}

test "base64 round-trip" {
    const original = "Hello, SCRAM!";
    var encode_buf: [128]u8 = undefined;
    const encoded = base64Encode(original, &encode_buf);
    try std.testing.expect(encoded.len > 0);

    var decode_buf: [128]u8 = undefined;
    const decoded = base64Decode(encoded, &decode_buf).?;
    try std.testing.expectEqualStrings(original, decoded);
}

test "generateNonce produces valid base64" {
    const nonce = generateNonce();
    // Should be 32 base64 chars encoding 24 random bytes
    try std.testing.expectEqual(@as(usize, 32), nonce.len);
    // Verify it's valid base64
    var decode_buf: [128]u8 = undefined;
    const decoded = base64Decode(&nonce, &decode_buf);
    try std.testing.expect(decoded != null);
    try std.testing.expectEqual(@as(usize, 24), decoded.?.len);
}

test "SCRAM-SHA-256 key derivation" {
    // RFC 5802 test vector: password = "pencil", salt = "QSXCR+Q6sek8bf92" (base64), i = 4096
    // We verify that the PBKDF2 + HMAC chain produces deterministic output
    const password = "pencil";
    const salt = "salt-value"; // simplified test salt
    const iterations: u32 = 4096;

    var salted_password: [32]u8 = undefined;
    try pbkdf2(&salted_password, password, salt, iterations, HmacSha256);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted_password);

    // StoredKey = SHA-256(ClientKey)
    var stored_key: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(&client_key, &stored_key, .{});

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    var server_key: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&server_key, "Server Key", &salted_password);

    // Verify determinism — same inputs produce same outputs
    var salted_password2: [32]u8 = undefined;
    try pbkdf2(&salted_password2, password, salt, iterations, HmacSha256);
    try std.testing.expectEqualSlices(u8, &salted_password, &salted_password2);

    var client_key2: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&client_key2, "Client Key", &salted_password2);
    try std.testing.expectEqualSlices(u8, &client_key, &client_key2);
}
