/// CRC32C (Castagnoli), with a hardware instruction where the build can use
/// one (see `hw_path`) and a table-driven software path otherwise.
const std = @import("std");
const builtin = @import("builtin");

/// The CRC32C instruction this build uses, decided at comptime so a build
/// that can't use one never compiles its asm: the target must have the
/// instruction, and the code generator must be able to emit it (Zig 0.16's
/// self-hosted x86_64 backend, the Debug default there, can't encode crc32).
/// The release targets are baseline CPUs, which have neither SSE4.2 nor ARM
/// CRC, so they take the software path.
const hw_path: enum { x86_64, aarch64, none } = switch (builtin.target.cpu.arch) {
    .x86_64 => if (std.Target.x86.featureSetHas(builtin.target.cpu.features, .sse4_2) and builtin.zig_backend != .stage2_x86_64) .x86_64 else .none,
    .aarch64 => if (std.Target.aarch64.featureSetHas(builtin.target.cpu.features, .crc) and builtin.zig_backend != .stage2_aarch64) .aarch64 else .none,
    else => .none,
};

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

    fn selectUpdateFn() *const fn (*HardwareCrc32, []const u8) void {
        return switch (hw_path) {
            .x86_64 => update_hw_x86_64,
            .aarch64 => update_hw_aarch64,
            .none => update_sw,
        };
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

test "CRC32C: the check value, and the hardware path agrees with software" {
    var c = HardwareCrc32.init();
    c.update("123456789");
    try std.testing.expectEqual(@as(u32, 0xE3069283), c.final());

    var data: [4096 + 9]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i *% 31 +% 7);
    for ([_]usize{ 0, 1, 7, 8, 9, 15, 16, 4096, data.len }) |n| {
        var hw = HardwareCrc32.init();
        hw.update(data[0..n]);
        var sw = HardwareCrc32{ .crc = 0xFFFFFFFF, .update_fn = HardwareCrc32.update_sw };
        sw.update(data[0..n]);
        try std.testing.expectEqual(sw.final(), hw.final());
    }
}
