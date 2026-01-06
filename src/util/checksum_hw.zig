/// Hardware-accelerated CRC32c implementation
/// Uses inline assembly for maximum performance and falls back to a software
/// implementation if hardware support is not available.
const std = @import("std");
const builtin = @import("builtin");

/// Streaming checksum using hardware acceleration if available.
pub const HardwareCrc32 = struct {
    crc: u32,
    update_fn: *const fn (*HardwareCrc32, []const u8) void,

    pub fn init() HardwareCrc32 {
        return HardwareCrc32{
            .crc = 0xFFFFFFFF,
            .update_fn = selectUpdateFn(),
        };
    }

    pub fn update(self: *HardwareCrc32, data: []const u8) void {
        self.update_fn(self, data);
    }

    pub fn final(self: *HardwareCrc32) u32 {
        return self.crc ^ 0xFFFFFFFF;
    }

    /// Selects the best implementation at runtime based on CPU features.
    /// Note: For now, we use compile-time detection. True runtime detection would require CPUID.
    fn selectUpdateFn() *const fn (*HardwareCrc32, []const u8) void {
        switch (builtin.cpu.arch) {
            .x86_64 => {
                // Check if SSE4.2 is in the compile-time feature set
                // Since we build with skylake CPU model, SSE4.2 should be available
                if (builtin.cpu.features.isEnabled(@intFromEnum(std.Target.x86.Feature.sse4_2))) {
                    return update_hw_x86_64;
                }
            },
            .aarch64 => {
                // Check if CRC is in the compile-time feature set
                if (builtin.cpu.features.isEnabled(@intFromEnum(std.Target.aarch64.Feature.crc))) {
                    return update_hw_aarch64;
                }
            },
            else => {},
        }
        // Fallback for all other cases.
        return update_sw;
    }

    /// Hardware-accelerated update for x86_64 SSE4.2.
    /// Uses inline assembly for portability (no C dependencies).
    fn update_hw_x86_64(self: *HardwareCrc32, data: []const u8) void {
        var crc: u32 = self.crc;
        var i: usize = 0;

        // Process 8 bytes at a time using crc32q instruction
        // x86 syntax: crc32q source, dest (dest = crc32(dest, source))
        // Note: crc32q needs the destination to be a 64-bit register even though only lower 32 bits are used
        while (i + 8 <= data.len) : (i += 8) {
            const chunk = std.mem.readInt(u64, data[i..][0..8], .little);
            // Use explicit register allocation to avoid size mismatch
            var crc64: u64 = crc; // Zero-extend to 64-bit
            asm volatile ("crc32q %[data], %[crc_out]"
                : [crc_out] "+r" (crc64),
                : [data] "r" (chunk),
            );
            crc = @as(u32, @truncate(crc64));
        }

        self.crc = crc;

        // Process remaining bytes using software fallback
        if (i < data.len) {
            self.update_sw(data[i..]);
        }
    }

    /// Hardware-accelerated update for aarch64 (ARMv8).
    fn update_hw_aarch64(self: *HardwareCrc32, data: []const u8) void {
        var crc = self.crc;
        var i: usize = 0;

        // Process 8 bytes at a time.
        while (i + 8 <= data.len) : (i += 8) {
            const chunk = std.mem.readInt(u64, data[i..][0..8], .little);
            // crc32cx uses 64-bit data, operates on 32-bit CRC
            // ARM assembly doesn't use % prefix like x86
            crc = asm volatile ("crc32cx w0, w1, x2"
                : [ret] "={w0}" (-> u32),
                : [crc_in] "{w1}" (crc),
                  [data] "{x2}" (chunk),
            );
        }

        // Process remaining bytes.
        while (i < data.len) : (i += 1) {
            // crc32cb uses 8-bit data
            crc = asm volatile ("crc32cb w0, w1, w2"
                : [ret] "={w0}" (-> u32),
                : [crc_in] "{w1}" (crc),
                  [byte] "{w2}" (data[i]),
            );
        }

        self.crc = crc;
    }

    /// Correct, incremental software fallback implementation.
    fn update_sw(self: *HardwareCrc32, data: []const u8) void {
        var crc = self.crc;
        for (data) |byte| {
            const table_index = @as(u8, @truncate(crc)) ^ byte;
            crc = (crc >> 8) ^ crc32c_table[table_index];
        }
        self.crc = crc;
    }
};

/// Precomputed table for the software fallback implementation.
const crc32c_table = blk: {
    @setEvalBranchQuota(10000);
    var table: [256]u32 = undefined;
    const polynomial: u32 = 0x82F63B78; // Reversed polynomial for CRC32C (Castagnoli)

    for (0..256) |i| {
        var crc = @as(u32, @intCast(i));
        var j: u8 = 0;
        while (j < 8) : (j += 1) {
            if ((crc & 1) == 1) {
                crc = (crc >> 1) ^ polynomial;
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }

    break :blk table;
};

/// Streaming checksum using hardware acceleration
pub const HardwareChecksumStream = struct {
    crc: HardwareCrc32,

    pub fn init() HardwareChecksumStream {
        return HardwareChecksumStream{ .crc = HardwareCrc32.init() };
    }

    pub fn add(self: *HardwareChecksumStream, bytes: []const u8) void {
        self.crc.update(bytes);
    }

    pub fn checksum(self: *HardwareChecksumStream) u32 {
        return self.crc.final();
    }
};

/// Detect if CPU supports hardware CRC32c acceleration
/// Note: This uses compile-time feature detection. True runtime detection would require CPUID.
pub fn hasHardwareAcceleration() bool {
    return switch (builtin.cpu.arch) {
        .x86_64 => builtin.cpu.features.isEnabled(@intFromEnum(std.Target.x86.Feature.sse4_2)),
        .aarch64 => builtin.cpu.features.isEnabled(@intFromEnum(std.Target.aarch64.Feature.crc)),
        else => false,
    };
}
