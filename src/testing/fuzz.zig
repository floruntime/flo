const std = @import("std");
const Simulator = @import("vopr.zig").Simulator;
const PRNG = @import("prng.zig").PRNG;

/// Fuzzing harness for VOPR.
/// Generates random configurations and runs simulations to find bugs.
pub const FuzzOptions = struct {
    /// Number of fuzzing iterations.
    iterations: usize = 100,
    /// Base seed for the fuzzer.
    seed: u64 = 0,
    /// Minimum operations per simulation.
    operations_min: usize = 10,
    /// Maximum operations per simulation.
    operations_max: usize = 1000,
};

pub fn fuzz(allocator: std.mem.Allocator, options: FuzzOptions) !void {
    var prng = PRNG.init(if (options.seed == 0) @intCast(std.time.milliTimestamp()) else options.seed);

    std.log.info("Starting fuzzer: iterations={} seed={}", .{ options.iterations, options.seed });

    for (0..options.iterations) |i| {
        const sim_seed = prng.random().int(u64);
        const operations = prng.random().intRangeAtMost(usize, options.operations_min, options.operations_max);
        const crash_prob = prng.random().intRangeAtMost(u8, 0, 50); // 0-50%
        const restart_prob = prng.random().intRangeAtMost(u8, 1, 100); // 1-100%
        const crash_stability = prng.random().intRangeAtMost(u32, 1, 100);
        const restart_stability = prng.random().intRangeAtMost(u32, 1, 50);
        const read_fault = prng.random().intRangeAtMost(u8, 0, 30);
        const write_fault = prng.random().intRangeAtMost(u8, 0, 30);
        const crash_fault = prng.random().intRangeAtMost(u8, 0, 80);

        std.log.info("Fuzz iteration {}/{}: seed={} ops={} crash={}% restart={}%", .{
            i + 1,
            options.iterations,
            sim_seed,
            operations,
            crash_prob,
            restart_prob,
        });

        var sim = try Simulator.init(allocator, .{
            .seed = sim_seed,
            .operations_max = operations,
            .crash_probability = crash_prob,
            .restart_probability = restart_prob,
            .crash_stability = crash_stability,
            .restart_stability = restart_stability,
            .storage_read_fault_probability = read_fault,
            .storage_write_fault_probability = write_fault,
            .storage_crash_fault_probability = crash_fault,
        });
        defer sim.deinit();

        sim.run() catch |err| {
            std.log.err("Simulation failed: seed={} error={}", .{ sim_seed, err });
            return err;
        };

        // Verify invariants
        if (sim.operations_completed != operations) {
            std.log.err("Invariant violation: expected {} operations, got {}", .{
                operations,
                sim.operations_completed,
            });
            return error.InvariantViolation;
        }

        if (sim.crashes != sim.restarts and sim.state == .crashed) {
            std.log.err("Invariant violation: crashes={} restarts={} state={}", .{
                sim.crashes,
                sim.restarts,
                sim.state,
            });
            return error.InvariantViolation;
        }
    }

    std.log.info("Fuzzer completed successfully: {} iterations passed", .{options.iterations});
}

test "fuzz: basic fuzzing" {
    const testing = std.testing;
    const allocator = testing.allocator;

    try fuzz(allocator, .{
        .iterations = 10,
        .seed = 42,
        .operations_min = 10,
        .operations_max = 50,
    });
}

test "fuzz: stress fuzzing" {
    const testing = std.testing;
    const allocator = testing.allocator;

    try fuzz(allocator, .{
        .iterations = 5,
        .seed = 123456,
        .operations_min = 100,
        .operations_max = 500,
    });
}
