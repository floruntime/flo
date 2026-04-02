//! TLS transport for Kafka broker connections.
//!
//! Wraps a TCP `std.net.Stream` with `std.crypto.tls.Client` to provide
//! encrypted connections to Kafka brokers. Used for:
//!   - SSL/TLS (one-way — server cert verification)
//!   - mTLS (mutual TLS — client cert is configured but requires runtime
//!     support for CertificateRequest; Zig stdlib does not yet support
//!     sending client certificates during TLS handshake)
//!
//! Configuration:
//!   - `ca_cert_path`: Path to CA certificate bundle (PEM) for server verification.
//!     If empty, uses system root CAs.
//!   - `client_cert_path`: Path to client certificate (PEM) for mTLS.
//!   - `client_key_path`: Path to client private key (PEM) for mTLS.
//!   - `skip_verify`: Disable server certificate verification (NOT recommended).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Certificate = std.crypto.Certificate;
const tls = std.crypto.tls;
const Io = std.Io;
const log = @import("stdx").log;

// =============================================================================
// TLS Configuration
// =============================================================================

pub const TlsConfig = struct {
    /// Path to CA certificate file (PEM) for server verification.
    /// Empty = use system root CAs.
    ca_cert_path: []const u8 = "",

    /// Path to client certificate file (PEM) for mTLS.
    client_cert_path: []const u8 = "",

    /// Path to client private key file (PEM) for mTLS.
    client_key_path: []const u8 = "",

    /// Skip server certificate verification (insecure — for testing only).
    skip_verify: bool = false,

    /// Validate that the config is self-consistent.
    pub fn validate(self: TlsConfig) !void {
        // If client cert is set, key must also be set (and vice versa)
        const has_cert = self.client_cert_path.len > 0;
        const has_key = self.client_key_path.len > 0;
        if (has_cert != has_key) {
            return error.MtlsRequiresBothCertAndKey;
        }
    }

    pub fn isMtls(self: TlsConfig) bool {
        return self.client_cert_path.len > 0 and self.client_key_path.len > 0;
    }
};

// =============================================================================
// TLS Stream
// =============================================================================

pub const TlsStream = struct {
    tls_client: tls.Client,
    tcp_stream: std.net.Stream,

    // Buffers owned by us (needed by tls.Client for read/write)
    read_buf: []u8,
    write_buf: []u8,

    // Io.Reader/Writer backed by the TCP stream
    tcp_reader: std.net.Stream.Reader,
    tcp_writer: std.net.Stream.Writer,

    allocator: Allocator,

    const READ_BUF_SIZE = tls.Client.min_buffer_len;
    const WRITE_BUF_SIZE = tls.max_ciphertext_len;

    /// Establish a TLS connection over an existing TCP stream.
    pub fn init(
        allocator: Allocator,
        tcp_stream: std.net.Stream,
        host: []const u8,
        config: TlsConfig,
    ) !TlsStream {
        const read_buf = try allocator.alloc(u8, READ_BUF_SIZE);
        errdefer allocator.free(read_buf);

        const write_buf = try allocator.alloc(u8, WRITE_BUF_SIZE);
        errdefer allocator.free(write_buf);

        var tcp_reader = tcp_stream.reader(read_buf);
        var tcp_writer = tcp_stream.writer(write_buf);

        // Load CA bundle
        const ca = if (config.skip_verify) @as(@TypeOf((tls.Client.Options{}).ca), .no_verification) else blk: {
            if (config.ca_cert_path.len > 0) {
                var bundle: Certificate.Bundle = .{};
                errdefer bundle.deinit(allocator);
                bundle.addCertsFromFilePathAbsolute(allocator, config.ca_cert_path) catch |err| {
                    log.err("Failed to load CA cert from {s}: {}", .{ config.ca_cert_path, err });
                    return error.CaCertLoadFailed;
                };
                break :blk @as(@TypeOf((tls.Client.Options{}).ca), .{ .bundle = bundle });
            } else {
                // Use system certs
                var bundle: Certificate.Bundle = .{};
                errdefer bundle.deinit(allocator);
                bundle.rescan(allocator) catch |err| {
                    log.err("Failed to load system CA certs: {}", .{err});
                    return error.SystemCaCertLoadFailed;
                };
                break :blk @as(@TypeOf((tls.Client.Options{}).ca), .{ .bundle = bundle });
            }
        };

        const host_opt = if (config.skip_verify)
            @as(@TypeOf((tls.Client.Options{}).host), .no_verification)
        else
            @as(@TypeOf((tls.Client.Options{}).host), .{ .explicit = host });

        if (config.isMtls()) {
            log.warn("mTLS configured but Zig stdlib TLS does not yet support " ++
                "sending client certificates. Server-side TLS will be used.", .{});
        }

        const tls_client = tls.Client.init(&tcp_reader.interface, &tcp_writer.interface, .{
            .host = host_opt,
            .ca = ca,
            .read_buffer = read_buf,
            .write_buffer = write_buf,
        }) catch |err| {
            log.err("TLS handshake failed with {s}: {}", .{ host, err });
            return error.TlsHandshakeFailed;
        };

        return .{
            .tls_client = tls_client,
            .tcp_stream = tcp_stream,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .tcp_reader = tcp_reader,
            .tcp_writer = tcp_writer,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TlsStream) void {
        self.allocator.free(self.read_buf);
        self.allocator.free(self.write_buf);
    }

    /// Write data over the TLS connection.
    pub fn write(self: *TlsStream, data: []const u8) !usize {
        self.tls_client.writer.writerWrite(data) catch {
            return error.TlsWriteFailed;
        };
        return data.len;
    }

    /// Read data from the TLS connection.
    pub fn read(self: *TlsStream, buffer: []u8) !usize {
        return self.tls_client.reader.readerRead(buffer) catch {
            return error.TlsReadFailed;
        };
    }
};

// =============================================================================
// Helper: Load PEM file contents
// =============================================================================

pub fn loadPemFile(allocator: Allocator, path: []const u8) ![]u8 {
    const file = std.fs.openFileAbsolute(path, .{}) catch |err| {
        log.err("Failed to open PEM file {s}: {}", .{ path, err });
        return error.PemFileNotFound;
    };
    defer file.close();

    const stat = try file.stat();
    if (stat.size > 1024 * 1024) return error.PemFileTooLarge; // 1MB max

    const contents = try allocator.alloc(u8, stat.size);
    errdefer allocator.free(contents);

    const bytes_read = try file.readAll(contents);
    if (bytes_read != stat.size) return error.PemFileReadIncomplete;

    return contents;
}

// =============================================================================
// Tests
// =============================================================================

test "TlsConfig validate — cert without key fails" {
    const config = TlsConfig{
        .client_cert_path = "/path/to/cert.pem",
        .client_key_path = "",
    };
    try std.testing.expectError(error.MtlsRequiresBothCertAndKey, config.validate());
}

test "TlsConfig validate — key without cert fails" {
    const config = TlsConfig{
        .client_cert_path = "",
        .client_key_path = "/path/to/key.pem",
    };
    try std.testing.expectError(error.MtlsRequiresBothCertAndKey, config.validate());
}

test "TlsConfig validate — both set is valid" {
    const config = TlsConfig{
        .client_cert_path = "/path/to/cert.pem",
        .client_key_path = "/path/to/key.pem",
    };
    try config.validate();
}

test "TlsConfig validate — neither set is valid" {
    const config = TlsConfig{};
    try config.validate();
}

test "TlsConfig isMtls" {
    try std.testing.expect(!(TlsConfig{}).isMtls());
    try std.testing.expect((TlsConfig{
        .client_cert_path = "cert.pem",
        .client_key_path = "key.pem",
    }).isMtls());
}
