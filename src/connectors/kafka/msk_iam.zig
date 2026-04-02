//! AWS MSK IAM Authentication (SigV4 SASL)
//!
//! Implements the custom `AWS_MSK_IAM` SASL mechanism used by Amazon MSK.
//! The SASL handshake uses AWS Signature Version 4 to authenticate.
//!
//! Flow:
//!   1. SaslHandshake with mechanism = "AWS_MSK_IAM"
//!   2. Client sends a JSON authentication payload:
//!      {"version":"2020_10_22","host":"<broker>","user-agent":"flo-kafka-source",
//!       "action":"kafka-cluster:Connect","x-amz-algorithm":"AWS4-HMAC-SHA256",
//!       "x-amz-credential":"<access_key>/<date>/<region>/kafka-cluster/aws4_request",
//!       "x-amz-date":"<iso8601>","x-amz-signedheaders":"host",
//!       "x-amz-security-token":"<session_token>",  // optional
//!       "x-amz-signature":"<hex_signature>"}
//!   3. Server validates and responds with success/failure
//!
//! Reference: https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html

const std = @import("std");
const Allocator = std.mem.Allocator;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;
const log = @import("stdx").log;

const codec = @import("codec.zig");
const broker_mod = @import("broker.zig");
const BrokerConnection = broker_mod.BrokerConnection;
const KafkaWriter = codec.KafkaWriter;
const KafkaReader = codec.KafkaReader;
const auth = @import("auth.zig");

// =============================================================================
// Configuration
// =============================================================================

pub const MskIamConfig = struct {
    /// AWS Access Key ID
    access_key_id: []const u8,
    /// AWS Secret Access Key
    secret_access_key: []const u8,
    /// AWS Session Token (optional, for temporary credentials)
    session_token: []const u8 = "",
    /// AWS region (e.g. "us-east-1")
    region: []const u8,
    /// Broker hostname (for the Host header in SigV4)
    broker_host: []const u8 = "",
};

// =============================================================================
// MSK IAM Handshake
// =============================================================================

pub fn performMskIamHandshake(
    conn: *BrokerConnection,
    config: MskIamConfig,
) !void {
    // Step 1: SaslHandshake
    conn.state = .sasl_handshake;
    try auth.sendSaslHandshakeRequest(conn, "AWS_MSK_IAM");

    // Step 2: Build and send the SigV4-signed auth payload
    conn.state = .sasl_authenticating;

    const now_s = @divTrunc(std.time.timestamp(), 1);
    const payload = try buildAuthPayload(conn.allocator, config, now_s);
    defer conn.allocator.free(payload);

    _ = try auth.sendSaslAuthenticateAndReceive(conn, payload, 1);

    conn.state = .ready;
    log.info("SASL/AWS_MSK_IAM authentication successful", .{});
}

// =============================================================================
// SigV4 Signing
// =============================================================================

/// Build the JSON authentication payload with SigV4 signature.
pub fn buildAuthPayload(allocator: Allocator, config: MskIamConfig, timestamp: i64) ![]const u8 {
    // Format timestamps
    var date_buf: [8]u8 = undefined;
    var datetime_buf: [16]u8 = undefined;
    formatDate(timestamp, &date_buf);
    formatDateTime(timestamp, &datetime_buf);

    const date = date_buf[0..8];
    const datetime = datetime_buf[0..16];

    // Credential scope: <date>/<region>/kafka-cluster/aws4_request
    var scope_buf: [128]u8 = undefined;
    const scope = std.fmt.bufPrint(&scope_buf, "{s}/{s}/kafka-cluster/aws4_request", .{
        date, config.region,
    }) catch return error.SaslAuthenticationFailed;

    // Credential: <access_key_id>/<scope>
    var cred_buf: [256]u8 = undefined;
    const credential = std.fmt.bufPrint(&cred_buf, "{s}/{s}", .{
        config.access_key_id, scope,
    }) catch return error.SaslAuthenticationFailed;

    // Canonical request
    const action = "kafka-cluster:Connect";
    const signed_headers = "host";

    // Canonical headers: host:<broker_host>\n
    var canonical_headers_buf: [256]u8 = undefined;
    const canonical_headers = std.fmt.bufPrint(&canonical_headers_buf, "host:{s}\n", .{
        config.broker_host,
    }) catch return error.SaslAuthenticationFailed;

    // Build canonical query string (sorted)
    var canonical_qs_buf: [1024]u8 = undefined;
    const canonical_qs = blk: {
        // Parameters sorted alphabetically
        var parts: [7][]const u8 = undefined;
        var part_bufs: [7][256]u8 = undefined;
        var count: usize = 0;

        // Action
        parts[count] = std.fmt.bufPrint(&part_bufs[count], "Action={s}", .{action}) catch return error.SaslAuthenticationFailed;
        count += 1;

        // X-Amz-Algorithm
        parts[count] = "X-Amz-Algorithm=AWS4-HMAC-SHA256";
        count += 1;

        // X-Amz-Credential (URL-encoded)
        var cred_enc_buf: [2048]u8 = undefined;
        const cred_encoded = percentEncode(credential, &cred_enc_buf);
        parts[count] = std.fmt.bufPrint(&part_bufs[count], "X-Amz-Credential={s}", .{cred_encoded}) catch return error.SaslAuthenticationFailed;
        count += 1;

        // X-Amz-Date
        parts[count] = std.fmt.bufPrint(&part_bufs[count], "X-Amz-Date={s}", .{datetime}) catch return error.SaslAuthenticationFailed;
        count += 1;

        // X-Amz-Expires
        parts[count] = "X-Amz-Expires=900";
        count += 1;

        // X-Amz-Security-Token (if present)
        if (config.session_token.len > 0) {
            var token_enc_buf: [2048]u8 = undefined;
            const token_encoded = percentEncode(config.session_token, &token_enc_buf);
            parts[count] = std.fmt.bufPrint(&part_bufs[count], "X-Amz-Security-Token={s}", .{token_encoded}) catch return error.SaslAuthenticationFailed;
            count += 1;
        }

        // X-Amz-SignedHeaders
        parts[count] = std.fmt.bufPrint(&part_bufs[count], "X-Amz-SignedHeaders={s}", .{signed_headers}) catch return error.SaslAuthenticationFailed;
        count += 1;

        // Join with &
        var written: usize = 0;
        for (parts[0..count], 0..) |part, i| {
            if (i > 0) {
                canonical_qs_buf[written] = '&';
                written += 1;
            }
            @memcpy(canonical_qs_buf[written..][0..part.len], part);
            written += part.len;
        }
        break :blk canonical_qs_buf[0..written];
    };

    // Hash of empty body
    const empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Canonical request = METHOD\nPATH\nQS\nHEADERS\nSIGNED_HEADERS\nPAYLOAD_HASH
    var canonical_req_buf: [2048]u8 = undefined;
    const canonical_request = std.fmt.bufPrint(&canonical_req_buf, "GET\n/\n{s}\n{s}\n{s}\n{s}", .{
        canonical_qs, canonical_headers, signed_headers, empty_hash,
    }) catch return error.SaslAuthenticationFailed;

    // Hash canonical request
    var canonical_hash: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(canonical_request, &canonical_hash, .{});
    const canonical_hash_hex = std.fmt.bytesToHex(canonical_hash, .lower);

    // String to sign
    var sts_buf: [512]u8 = undefined;
    const string_to_sign = std.fmt.bufPrint(&sts_buf, "AWS4-HMAC-SHA256\n{s}\n{s}\n{s}", .{
        datetime, scope, &canonical_hash_hex,
    }) catch return error.SaslAuthenticationFailed;

    // Signing key derivation
    // kDate = HMAC("AWS4" + secret, date)
    // kRegion = HMAC(kDate, region)
    // kService = HMAC(kRegion, "kafka-cluster")
    // kSigning = HMAC(kService, "aws4_request")
    var k_secret_buf: [256]u8 = undefined;
    const k_secret_prefix = "AWS4";
    @memcpy(k_secret_buf[0..k_secret_prefix.len], k_secret_prefix);
    @memcpy(k_secret_buf[k_secret_prefix.len..][0..config.secret_access_key.len], config.secret_access_key);
    const k_secret = k_secret_buf[0 .. k_secret_prefix.len + config.secret_access_key.len];

    var k_date: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_date, date, k_secret);

    var k_region: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_region, config.region, &k_date);

    var k_service: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_service, "kafka-cluster", &k_region);

    var k_signing: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_signing, "aws4_request", &k_service);

    // Final signature
    var signature: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&signature, string_to_sign, &k_signing);

    const sig_hex = std.fmt.bytesToHex(signature, .lower);

    // Build JSON payload
    var json_buf: [2048]u8 = undefined;
    const json = if (config.session_token.len > 0)
        std.fmt.bufPrint(&json_buf,
            \\{{"version":"2020_10_22","host":"{s}","user-agent":"flo-kafka-source","action":"{s}","x-amz-algorithm":"AWS4-HMAC-SHA256","x-amz-credential":"{s}","x-amz-date":"{s}","x-amz-signedheaders":"{s}","x-amz-security-token":"{s}","x-amz-signature":"{s}"}}
        , .{
            config.broker_host,
            action,
            credential,
            datetime,
            signed_headers,
            config.session_token,
            &sig_hex,
        }) catch return error.SaslAuthenticationFailed
    else
        std.fmt.bufPrint(&json_buf,
            \\{{"version":"2020_10_22","host":"{s}","user-agent":"flo-kafka-source","action":"{s}","x-amz-algorithm":"AWS4-HMAC-SHA256","x-amz-credential":"{s}","x-amz-date":"{s}","x-amz-signedheaders":"{s}","x-amz-signature":"{s}"}}
        , .{
            config.broker_host,
            action,
            credential,
            datetime,
            signed_headers,
            &sig_hex,
        }) catch return error.SaslAuthenticationFailed;

    return allocator.dupe(u8, json) catch return error.SaslAuthenticationFailed;
}

// =============================================================================
// Date Formatting
// =============================================================================

/// Format timestamp as YYYYMMDD
fn formatDate(timestamp: i64, buf: *[8]u8) void {
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(timestamp) };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    _ = std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}", .{
        yd.year, md.month.numeric(), md.day_index + 1,
    }) catch {};
}

/// Format timestamp as YYYYMMDDTHHMMSSZ
fn formatDateTime(timestamp: i64, buf: *[16]u8) void {
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(timestamp) };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    _ = std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        yd.year,
        md.month.numeric(),
        md.day_index + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
    }) catch {};
}

/// Percent-encode a string for AWS query parameters.
fn percentEncode(input: []const u8, buf: *[2048]u8) []const u8 {
    var pos: usize = 0;
    for (input) |c| {
        if (isUnreserved(c)) {
            if (pos >= buf.len) break;
            buf[pos] = c;
            pos += 1;
        } else {
            if (pos + 3 > buf.len) break;
            buf[pos] = '%';
            const hex = "0123456789ABCDEF";
            buf[pos + 1] = hex[c >> 4];
            buf[pos + 2] = hex[c & 0x0F];
            pos += 3;
        }
    }
    return buf[0..pos];
}

fn isUnreserved(c: u8) bool {
    return (c >= 'A' and c <= 'Z') or (c >= 'a' and c <= 'z') or
        (c >= '0' and c <= '9') or c == '-' or c == '_' or c == '.' or c == '~';
}

// =============================================================================
// Tests
// =============================================================================

test "formatDate" {
    // 2024-01-15 = 1705276800
    var buf: [8]u8 = undefined;
    formatDate(1705276800, &buf);
    try std.testing.expectEqualStrings("20240115", &buf);
}

test "formatDateTime" {
    // 2024-01-15T12:30:45Z
    var buf: [16]u8 = undefined;
    formatDateTime(1705321845, &buf);
    try std.testing.expectEqualStrings("20240115T123045Z", &buf);
}

test "percentEncode" {
    var buf: [2048]u8 = undefined;
    const result = percentEncode("AKID/20240115/us-east-1/kafka-cluster/aws4_request", &buf);
    try std.testing.expect(std.mem.indexOf(u8, result, "%2F") != null);
    try std.testing.expect(std.mem.startsWith(u8, result, "AKID"));
}

test "buildAuthPayload structure" {
    const allocator = std.testing.allocator;
    const payload = try buildAuthPayload(allocator, .{
        .access_key_id = "AKIAIOSFODNN7EXAMPLE",
        .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        .region = "us-east-1",
        .broker_host = "b-1.mycluster.kafka.us-east-1.amazonaws.com",
    }, 1705276800);
    defer allocator.free(payload);

    // Verify it's valid JSON-ish (starts/ends with braces)
    try std.testing.expect(payload.len > 0);
    try std.testing.expect(payload[0] == '{');
    try std.testing.expect(payload[payload.len - 1] == '}');

    // Verify required fields are present
    try std.testing.expect(std.mem.indexOf(u8, payload, "\"version\":\"2020_10_22\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, payload, "\"x-amz-algorithm\":\"AWS4-HMAC-SHA256\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, payload, "AKIAIOSFODNN7EXAMPLE") != null);
    try std.testing.expect(std.mem.indexOf(u8, payload, "us-east-1") != null);
    try std.testing.expect(std.mem.indexOf(u8, payload, "x-amz-signature") != null);
}

test "buildAuthPayload with session token" {
    const allocator = std.testing.allocator;
    const payload = try buildAuthPayload(allocator, .{
        .access_key_id = "AKID",
        .secret_access_key = "SECRET",
        .session_token = "TOKEN123",
        .region = "eu-west-1",
        .broker_host = "broker.kafka.eu-west-1.amazonaws.com",
    }, 1705276800);
    defer allocator.free(payload);

    try std.testing.expect(std.mem.indexOf(u8, payload, "x-amz-security-token") != null);
    try std.testing.expect(std.mem.indexOf(u8, payload, "TOKEN123") != null);
}

test "SigV4 signing key derivation is deterministic" {
    const secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const date = "20240115";
    const region = "us-east-1";

    var k_secret_buf: [256]u8 = undefined;
    const prefix = "AWS4";
    @memcpy(k_secret_buf[0..prefix.len], prefix);
    @memcpy(k_secret_buf[prefix.len..][0..secret.len], secret);
    const k_secret = k_secret_buf[0 .. prefix.len + secret.len];

    var k_date: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_date, date, k_secret);

    var k_region: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_region, region, &k_date);

    var k_service: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_service, "kafka-cluster", &k_region);

    var k_signing: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_signing, "aws4_request", &k_service);

    // Repeat to verify determinism
    var k_date2: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_date2, date, k_secret);
    var k_region2: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_region2, region, &k_date2);
    var k_service2: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_service2, "kafka-cluster", &k_region2);
    var k_signing2: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_signing2, "aws4_request", &k_service2);

    try std.testing.expectEqualSlices(u8, &k_signing, &k_signing2);
}
