const std = @import("std");

/// Deterministic PRNG for VOPR-style testing
///
/// Based on TigerBeetle's approach:
/// - Seed-based for reproducibility
/// - Fast (xorshift-based)
/// - Good distribution for testing
///
/// Usage:
/// ```zig
/// var prng = PRNG.init(12345);
/// const random_u64 = prng.random().int(u64);
/// const random_bool = prng.random().boolean();
/// ```
pub const PRNG = struct {
    state: u64,

    /// Initialize with a seed
    pub fn init(seed: u64) PRNG {
        return PRNG{
            .state = if (seed == 0) 1 else seed, // Avoid zero state
        };
    }

    /// Get a std.Random interface
    pub fn random(self: *PRNG) std.Random {
        return std.Random.init(self, fill);
    }

    /// Fill buffer with random bytes
    fn fill(self: *PRNG, buf: []u8) void {
        var i: usize = 0;
        while (i < buf.len) {
            const val = self.next();
            const remaining = buf.len - i;
            const to_copy = @min(remaining, @sizeOf(u64));

            const bytes = std.mem.asBytes(&val);
            @memcpy(buf[i..][0..to_copy], bytes[0..to_copy]);
            i += to_copy;
        }
    }

    /// Generate next random u64 (xorshift64)
    fn next(self: *PRNG) u64 {
        var x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        return x;
    }

    /// Generate random integer in range [min, max)
    pub fn intRange(self: *PRNG, comptime T: type, min: T, max: T) T {
        return self.random().intRangeAtMost(T, min, max - 1);
    }

    /// Generate random boolean
    pub fn boolean(self: *PRNG) bool {
        return self.random().boolean();
    }

    /// Generate random float in [0, 1)
    pub fn float(self: *PRNG, comptime T: type) T {
        return self.random().float(T);
    }

    /// Choose random element from slice
    pub fn choose(self: *PRNG, comptime T: type, slice: []const T) T {
        const index = self.random().uintLessThan(usize, slice.len);
        return slice[index];
    }

    /// Shuffle slice in place
    pub fn shuffle(self: *PRNG, comptime T: type, slice: []T) void {
        self.random().shuffle(T, slice);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "PRNG: deterministic" {
    var prng1 = PRNG.init(12345);
    var prng2 = PRNG.init(12345);

    // Same seed should produce same sequence
    for (0..100) |_| {
        const val1 = prng1.random().int(u64);
        const val2 = prng2.random().int(u64);
        try std.testing.expectEqual(val1, val2);
    }
}

test "PRNG: different seeds produce different sequences" {
    var prng1 = PRNG.init(12345);
    var prng2 = PRNG.init(54321);

    var different = false;
    for (0..100) |_| {
        const val1 = prng1.random().int(u64);
        const val2 = prng2.random().int(u64);
        if (val1 != val2) {
            different = true;
            break;
        }
    }

    try std.testing.expect(different);
}

test "PRNG: intRange" {
    var prng = PRNG.init(12345);

    for (0..1000) |_| {
        const val = prng.intRange(u32, 10, 20);
        try std.testing.expect(val >= 10);
        try std.testing.expect(val < 20);
    }
}

test "PRNG: boolean distribution" {
    var prng = PRNG.init(12345);

    var true_count: usize = 0;
    const iterations = 10000;

    for (0..iterations) |_| {
        if (prng.boolean()) {
            true_count += 1;
        }
    }

    // Should be roughly 50/50 (allow 45-55% range)
    const true_percent = (true_count * 100) / iterations;
    try std.testing.expect(true_percent >= 45);
    try std.testing.expect(true_percent <= 55);
}

test "PRNG: choose" {
    var prng = PRNG.init(12345);

    const items = [_]u32{ 1, 2, 3, 4, 5 };

    for (0..100) |_| {
        const chosen = prng.choose(u32, &items);

        // Verify chosen is in items
        var found = false;
        for (items) |item| {
            if (item == chosen) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "PRNG: shuffle" {
    var prng = PRNG.init(12345);

    var items = [_]u32{ 1, 2, 3, 4, 5 };
    const original = items;

    prng.shuffle(u32, &items);

    // Should be different order (with high probability)
    var different = false;
    for (items, 0..) |item, i| {
        if (item != original[i]) {
            different = true;
            break;
        }
    }
    try std.testing.expect(different);

    // Should contain same elements
    std.mem.sort(u32, &items, {}, comptime std.sort.asc(u32));
    try std.testing.expectEqualSlices(u32, &original, &items);
}

test "PRNG: reproducibility across runs" {
    const seed: u64 = 42;

    // Run 1
    var prng1 = PRNG.init(seed);
    var results1: [10]u64 = undefined;
    for (&results1) |*r| {
        r.* = prng1.random().int(u64);
    }

    // Run 2 (same seed)
    var prng2 = PRNG.init(seed);
    var results2: [10]u64 = undefined;
    for (&results2) |*r| {
        r.* = prng2.random().int(u64);
    }

    // Should be identical
    try std.testing.expectEqualSlices(u64, &results1, &results2);
}
