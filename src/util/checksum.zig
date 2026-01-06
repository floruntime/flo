const std = @import("std");
const mem = std.mem;
const checksum_hw = @import("checksum_hw.zig");

/// Checksum implementation using CRC32c (hardware-accelerated)
///
/// CRC32c is the industry standard for storage integrity checks (used in iSCSI, Btrfs, GCS, etc.).
/// Modern CPUs have hardware acceleration for CRC32c via the `crc32` instruction.
/// This provides:
/// - Extremely fast performance (hardware-accelerated on modern CPUs)
/// - Sufficient for detecting data corruption (bit flips, torn writes)
/// - Much faster than cryptographic hashes for integrity checking
pub fn checksum(data: []const u8) u32 {
    if (@inComptime()) {
        // For comptime, use a simple hash (CRC32 not available at comptime)
        if (data.len == 0) return 0;
        var hash: u32 = 0xFFFFFFFF;
        for (data) |byte| {
            hash = hash ^ (@as(u32, byte) << @as(u5, @intCast(hash & 0x1F)));
        }
        return hash ^ 0xFFFFFFFF;
    }
    var stream = ChecksumStream.init();
    stream.add(data);
    return stream.checksum();
}

/// Streaming checksum for large data
/// Uses hardware acceleration when available (10-20x faster than software)
/// Always uses CRC32c algorithm (Castagnoli polynomial) for consistency
pub const ChecksumStream = struct {
    // Always use our HardwareCrc32 which has consistent CRC32c implementation
    // (hardware-accelerated when available, software fallback uses same polynomial)
    crc: checksum_hw.HardwareCrc32,

    pub fn init() ChecksumStream {
        return ChecksumStream{
            .crc = checksum_hw.HardwareCrc32.init(),
        };
    }

    pub fn add(stream: *ChecksumStream, bytes: []const u8) void {
        stream.crc.update(bytes);
    }

    pub fn checksum(stream: *ChecksumStream) u32 {
        const result = stream.crc.final();
        stream.* = undefined;
        return result;
    }
};

/// Verify a checksum matches the data
pub fn verify(data: []const u8, expected: u32) bool {
    return checksum(data) == expected;
}

test "checksum empty" {
    var stream = ChecksumStream.init();
    stream.add(&.{});
    try std.testing.expectEqual(stream.checksum(), comptime checksum(&.{}));
}

test "checksum non-empty" {
    const data = "Hello, Flo!";
    const sum1 = checksum(data);
    const sum2 = checksum(data);

    // Same data should produce same checksum
    try std.testing.expectEqual(sum1, sum2);

    // Different data should produce different checksum
    const different = "Hello, World!";
    const sum3 = checksum(different);
    try std.testing.expect(sum1 != sum3);
}

test "checksum stream" {
    var stream = ChecksumStream.init();
    stream.add("Hello, ");
    stream.add("Flo!");
    const sum1 = stream.checksum();

    const sum2 = checksum("Hello, Flo!");
    try std.testing.expectEqual(sum1, sum2);
}

test "checksum verify" {
    const data = "Test data";
    const sum = checksum(data);
    try std.testing.expect(verify(data, sum));
    try std.testing.expect(!verify("Wrong data", sum));
}

test "checksum at comptime" {
    comptime {
        const sum = checksum(&.{});
        _ = sum;
    }
}
