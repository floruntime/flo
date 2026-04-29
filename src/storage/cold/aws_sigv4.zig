//! AWS Signature Version 4 - Request Signing for S3 API
//!
//! Implements AWS Signature V4 signing algorithm required for S3 API calls.
//! Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
//!
//! The signing process:
//! 1. Create a canonical request
//! 2. Create a string to sign
//! 3. Calculate the signature
//! 4. Add signature to request headers

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// AWS credentials
pub const Credentials = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null,
};

/// Signed request headers
pub const SignedHeaders = struct {
    authorization: [512]u8,
    authorization_len: usize,
    x_amz_date: [16]u8, // "YYYYMMDDTHHMMSSZ"
    x_amz_content_sha256: [64]u8,
    x_amz_security_token: ?[]const u8,

    pub fn getAuthorization(self: *const SignedHeaders) []const u8 {
        return self.authorization[0..self.authorization_len];
    }

    pub fn getDate(self: *const SignedHeaders) []const u8 {
        return &self.x_amz_date;
    }

    pub fn getContentSha256(self: *const SignedHeaders) []const u8 {
        return &self.x_amz_content_sha256;
    }
};

/// Sign an S3 request
pub fn signRequest(
    method: []const u8,
    uri_path: []const u8,
    query_string: ?[]const u8,
    host: []const u8,
    region: []const u8,
    credentials: Credentials,
    payload_hash: ?[64]u8, // hex-encoded SHA256, or null for UNSIGNED-PAYLOAD
    timestamp: i64, // Unix timestamp
) SignedHeaders {
    var result: SignedHeaders = undefined;

    // Format timestamp
    const datetime = formatAmzDate(timestamp);
    result.x_amz_date = datetime;
    const date_stamp = datetime[0..8]; // YYYYMMDD

    // Content hash
    if (payload_hash) |ph| {
        result.x_amz_content_sha256 = ph;
    } else {
        // UNSIGNED-PAYLOAD for streaming uploads
        const unsigned = "UNSIGNED-PAYLOAD";
        @memcpy(result.x_amz_content_sha256[0..unsigned.len], unsigned);
        @memset(result.x_amz_content_sha256[unsigned.len..], 0);
    }

    // Session token
    result.x_amz_security_token = credentials.session_token;

    // Create canonical request
    var canonical_buf: [4096]u8 = undefined;
    var canonical_fbs: std.Io.Writer = .fixed(&canonical_buf);
    const canonical_writer = &canonical_fbs;

    // HTTPMethod
    canonical_writer.writeAll(method) catch {};
    canonical_writer.writeByte('\n') catch {};

    // CanonicalURI (URL-encoded path)
    canonical_writer.writeAll(uri_path) catch {};
    canonical_writer.writeByte('\n') catch {};

    // CanonicalQueryString
    if (query_string) |qs| {
        canonical_writer.writeAll(qs) catch {};
    }
    canonical_writer.writeByte('\n') catch {};

    // CanonicalHeaders (must be sorted, lowercase)
    canonical_writer.print("host:{s}\n", .{host}) catch {};
    canonical_writer.print("x-amz-content-sha256:{s}\n", .{result.x_amz_content_sha256[0..contentHashLen(&result.x_amz_content_sha256)]}) catch {};
    canonical_writer.print("x-amz-date:{s}\n", .{datetime}) catch {};
    if (credentials.session_token) |token| {
        canonical_writer.print("x-amz-security-token:{s}\n", .{token}) catch {};
    }
    canonical_writer.writeByte('\n') catch {};

    // SignedHeaders
    const signed_headers = if (credentials.session_token != null)
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
    else
        "host;x-amz-content-sha256;x-amz-date";
    canonical_writer.writeAll(signed_headers) catch {};
    canonical_writer.writeByte('\n') catch {};

    // HashedPayload
    canonical_writer.writeAll(result.x_amz_content_sha256[0..contentHashLen(&result.x_amz_content_sha256)]) catch {};

    const canonical_request = canonical_fbs.buffered();

    // Hash canonical request
    var canonical_hash: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(canonical_request, &canonical_hash, .{});
    const canonical_hash_hex = bytesToHex(&canonical_hash);

    // Create string to sign
    var sts_buf: [512]u8 = undefined;
    var sts_fbs: std.Io.Writer = .fixed(&sts_buf);
    const sts_writer = &sts_fbs;

    sts_writer.writeAll("AWS4-HMAC-SHA256\n") catch {};
    sts_writer.writeAll(&datetime) catch {};
    sts_writer.writeByte('\n') catch {};

    // Credential scope
    sts_writer.writeAll(date_stamp) catch {};
    sts_writer.print("/{s}/s3/aws4_request\n", .{region}) catch {};
    sts_writer.writeAll(&canonical_hash_hex) catch {};

    const string_to_sign = sts_fbs.buffered();

    // Calculate signing key
    // kSecret = "AWS4" + SecretAccessKey
    var k_secret: [4 + 256]u8 = undefined;
    @memcpy(k_secret[0..4], "AWS4");
    @memcpy(k_secret[4..][0..credentials.secret_access_key.len], credentials.secret_access_key);
    const k_secret_len = 4 + credentials.secret_access_key.len;

    // kDate = HMAC-SHA256(kSecret, DateStamp)
    var k_date: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_date, date_stamp, k_secret[0..k_secret_len]);

    // kRegion = HMAC-SHA256(kDate, Region)
    var k_region: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_region, region, &k_date);

    // kService = HMAC-SHA256(kRegion, "s3")
    var k_service: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_service, "s3", &k_region);

    // kSigning = HMAC-SHA256(kService, "aws4_request")
    var k_signing: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&k_signing, "aws4_request", &k_service);

    // Calculate signature
    var signature: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&signature, string_to_sign, &k_signing);
    const signature_hex = bytesToHex(&signature);

    // Build Authorization header
    var auth_fbs: std.Io.Writer = .fixed(&result.authorization);
    const auth_writer = &auth_fbs;

    auth_writer.print("AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/s3/aws4_request, SignedHeaders={s}, Signature={s}", .{
        credentials.access_key_id,
        date_stamp,
        region,
        signed_headers,
        &signature_hex,
    }) catch {};

    result.authorization_len = auth_fbs.end;

    return result;
}

/// Hash payload and return hex-encoded string
pub fn hashPayload(data: []const u8) [64]u8 {
    var hash: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(data, &hash, .{});
    return bytesToHex(&hash);
}

/// Empty payload hash (for GET, DELETE, HEAD requests)
/// SHA256 of empty string
pub const EMPTY_PAYLOAD_HASH: [64]u8 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".*;

/// Convert bytes to lowercase hex string
fn bytesToHex(bytes: []const u8) [64]u8 {
    const hex_chars = "0123456789abcdef";
    var result: [64]u8 = undefined;
    for (bytes, 0..) |b, i| {
        result[i * 2] = hex_chars[b >> 4];
        result[i * 2 + 1] = hex_chars[b & 0x0f];
    }
    return result;
}

/// Format timestamp as AWS date string "YYYYMMDDTHHMMSSZ"
fn formatAmzDate(timestamp: i64) [16]u8 {
    const epoch_seconds: u64 = @intCast(timestamp);
    const epoch_day = epoch_seconds / 86400;
    const day_seconds = epoch_seconds % 86400;

    // Calculate date from epoch day (civil_from_days algorithm)
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

    // Calculate time
    const hour = day_seconds / 3600;
    const minute = (day_seconds % 3600) / 60;
    const second = day_seconds % 60;

    var result: [16]u8 = undefined;
    _ = std.fmt.bufPrint(&result, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        year,
        month,
        day,
        hour,
        minute,
        second,
    }) catch {};

    return result;
}

fn contentHashLen(hash: *const [64]u8) usize {
    // Find actual length (for UNSIGNED-PAYLOAD which is shorter)
    for (hash, 0..) |c, i| {
        if (c == 0) return i;
    }
    return 64;
}

// ============================================================================
// Tests
// ============================================================================

test "formatAmzDate" {
    // Unix timestamp for 2024-01-15T10:30:00Z
    const ts: i64 = 1705314600;
    const result = formatAmzDate(ts);
    try std.testing.expectEqualStrings("20240115T103000Z", &result);
}

test "hashPayload empty" {
    const hash = hashPayload("");
    try std.testing.expectEqualStrings("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", &hash);
}

test "hashPayload with data" {
    const hash = hashPayload("hello world");
    try std.testing.expectEqualStrings("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", &hash);
}

test "signRequest basic" {
    const signed = signRequest(
        "GET",
        "/mybucket/mykey",
        null,
        "s3.us-east-1.amazonaws.com",
        "us-east-1",
        .{
            .access_key_id = "AKIAIOSFODNN7EXAMPLE",
            .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        },
        EMPTY_PAYLOAD_HASH,
        1705314600, // 2024-01-15T10:30:00Z
    );

    // Verify authorization header starts correctly
    try std.testing.expect(std.mem.startsWith(u8, signed.getAuthorization(), "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240115/us-east-1/s3/aws4_request"));
    try std.testing.expectEqualStrings("20240115T103000Z", signed.getDate());
}
