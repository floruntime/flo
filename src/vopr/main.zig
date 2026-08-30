//! vopr — deterministic cluster simulation runner
//!
//! One seed reproduces an entire run. Usage:
//!
//!   vopr [--seed=N]                 run one seed (random when omitted)
//!        [--iterations=K]           swarm: K seeds, each in a watchdogged
//!                                   child process (a hang inside the unit
//!                                   under test cannot be interrupted from
//!                                   within, so the parent kills on timeout)
//!        [--seed-timeout=SECS]      per-seed watchdog budget (default 300)
//!        [--mode=volatile|persisted] hard-state model (persisted is the default)
//!        [--no-timer-reset]         drop harness timer re-arming (livelock demo)
//!        [--small-ring]             shrink the ring so eviction outruns
//!                                   replication (fails by design)
//!        [--scenario-out=PATH]      write the scenario JSON, then run
//!        [--scenario-in=PATH]       run a pinned scenario instead of a seed
//!        [--verbose]
//!
//! Exit codes: 0 clean, 1 violations or failing/hanging seeds, 2 usage.

const std = @import("std");
const src = @import("src");
const stdx = @import("stdx");

const vopr = src.vopr;
const Scenario = vopr.Scenario;
const Simulator = vopr.simulator.Simulator;

const Args = struct {
    seed: ?u64 = null,
    iterations: ?u64 = null,
    seed_timeout_s: u64 = 300,
    volatile_mode: bool = false,
    no_timer_reset: bool = false,
    small_ring: bool = false,
    scenario_out: ?[]const u8 = null,
    scenario_in: ?[]const u8 = null,
    verbose: bool = false,
    self_exe: []const u8 = "",
};

fn usage() void {
    std.debug.print(
        "usage: vopr [--seed=N] [--iterations=K] [--seed-timeout=SECS] " ++
            "[--mode=volatile|persisted] [--no-timer-reset] [--small-ring] " ++
            "[--scenario-out=PATH] [--scenario-in=PATH] [--verbose]\n",
        .{},
    );
}

fn parseArgs(argv: []const []const u8) !Args {
    var args = Args{ .self_exe = argv[0] };
    for (argv[1..]) |arg| {
        if (std.mem.startsWith(u8, arg, "--seed=")) {
            args.seed = try std.fmt.parseInt(u64, arg["--seed=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--iterations=")) {
            args.iterations = try std.fmt.parseInt(u64, arg["--iterations=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--seed-timeout=")) {
            args.seed_timeout_s = try std.fmt.parseInt(u64, arg["--seed-timeout=".len..], 10);
        } else if (std.mem.eql(u8, arg, "--mode=volatile")) {
            args.volatile_mode = true;
        } else if (std.mem.eql(u8, arg, "--mode=persisted")) {
            args.volatile_mode = false;
        } else if (std.mem.eql(u8, arg, "--no-timer-reset")) {
            args.no_timer_reset = true;
        } else if (std.mem.eql(u8, arg, "--small-ring")) {
            args.small_ring = true;
        } else if (std.mem.startsWith(u8, arg, "--scenario-out=")) {
            args.scenario_out = arg["--scenario-out=".len..];
        } else if (std.mem.startsWith(u8, arg, "--scenario-in=")) {
            args.scenario_in = arg["--scenario-in=".len..];
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            args.verbose = true;
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            return error.Usage;
        }
    }
    return args;
}

// ── Scenario JSON loading (the pin format written by writeJson) ────────

fn jsonU64(obj: std.json.ObjectMap, key: []const u8) !u64 {
    const v = obj.get(key) orelse return error.MissingField;
    return @intCast(v.integer);
}

fn loadScenario(allocator: std.mem.Allocator, path: []const u8) !Scenario {
    const bytes = try stdx.fs.readFileAlloc(allocator, path, 64 * 1024);
    defer allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;

    const hard = (obj.get("hard_state") orelse return error.MissingField).string;
    const dura = (obj.get("durability") orelse return error.MissingField).string;
    return .{
        .seed = try jsonU64(obj, "seed"),
        .node_count = @intCast(try jsonU64(obj, "node_count")),
        .log_capacity = @intCast(try jsonU64(obj, "log_capacity")),
        .small_ring = (obj.get("small_ring") orelse return error.MissingField).bool,
        .ticks_safety = try jsonU64(obj, "ticks_safety"),
        .ticks_convergence = try jsonU64(obj, "ticks_convergence"),
        .hard_state = if (std.mem.eql(u8, hard, "volatile")) .volatile_state else .persisted,
        .durability = if (std.mem.eql(u8, dura, "async_flush")) .async_flush else .sync,
        .flush_interval_ms = try jsonU64(obj, "flush_interval_ms"),
        .msg_delay_min_ms = try jsonU64(obj, "msg_delay_min_ms"),
        .msg_delay_max_ms = try jsonU64(obj, "msg_delay_max_ms"),
        .drop_percent = @intCast(try jsonU64(obj, "drop_percent")),
        .duplicate_percent = @intCast(try jsonU64(obj, "duplicate_percent")),
        .partition_permille = @intCast(try jsonU64(obj, "partition_permille")),
        .partition_min_ms = try jsonU64(obj, "partition_min_ms"),
        .partition_max_ms = try jsonU64(obj, "partition_max_ms"),
        .crash_permille = @intCast(try jsonU64(obj, "crash_permille")),
        .restart_permille = @intCast(try jsonU64(obj, "restart_permille")),
        .request_percent = @intCast(try jsonU64(obj, "request_percent")),
        .payload_min = @intCast(try jsonU64(obj, "payload_min")),
        .payload_max = @intCast(try jsonU64(obj, "payload_max")),
        .election_timeout_min_ms = try jsonU64(obj, "election_timeout_min_ms"),
        .election_timeout_max_ms = try jsonU64(obj, "election_timeout_max_ms"),
        .heartbeat_interval_ms = try jsonU64(obj, "heartbeat_interval_ms"),
        .rpc_timeout_ms = try jsonU64(obj, "rpc_timeout_ms"),
    };
}

fn writeScenario(scenario: *const Scenario, path: []const u8) !void {
    var buf: [4096]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try scenario.writeJson(&w);
    const file = try stdx.fs.createFile(path, .{});
    defer stdx.fs.closeFile(file);
    try stdx.fs.writeAll(file, w.buffered());
}

// ── Single-seed run ────────────────────────────────────────────────────

fn runOne(allocator: std.mem.Allocator, args: Args, seed: u64) !bool {
    var scenario = if (args.scenario_in) |path|
        try loadScenario(allocator, path)
    else if (args.small_ring)
        Scenario.smallRing(seed)
    else
        Scenario.fromSeed(seed);
    if (args.volatile_mode) scenario.hard_state = .volatile_state;

    // The scenario prints before the run: a hang inside the unit under
    // test cannot be interrupted from in here, so the repro must already
    // be on screen when it happens.
    var jbuf: [4096]u8 = undefined;
    var jw: std.Io.Writer = .fixed(&jbuf);
    scenario.writeJson(&jw) catch {};
    std.debug.print("[vopr] seed={d} scenario:\n{s}", .{ scenario.seed, jw.buffered() });

    if (args.scenario_out) |path| {
        try writeScenario(&scenario, path);
        std.debug.print("[vopr] scenario written to {s}\n", .{path});
    }

    var sim = try Simulator.init(allocator, scenario, .{
        .volatile_hard_state = args.volatile_mode or scenario.hard_state == .volatile_state,
        .no_timer_reset = args.no_timer_reset,
        .verbose = args.verbose,
    });
    defer sim.deinit();

    const s = try sim.run();
    std.debug.print(
        "[vopr] seed={d} {s}: ticks={d} ops={d} acked={d} lost={d} committed={d} " ++
            "elections={d} crashes={d} restarts={d} delivered={d} dropped={d} stalls={d}\n",
        .{
            s.seed,
            if (s.ok) "OK" else "FAILED",
            s.ticks,
            s.ops_submitted,
            s.ops_acked,
            s.ops_lost,
            s.max_committed,
            s.elections_won,
            s.crashes,
            s.restarts,
            s.messages_delivered,
            s.messages_dropped,
            s.apply_stalls,
        },
    );
    if (!s.ok) {
        var vbuf: [16384]u8 = undefined;
        var vw: std.Io.Writer = .fixed(&vbuf);
        sim.printViolations(&vw) catch {};
        sim.printNodeStates(&vw) catch {};
        std.debug.print("{s}", .{vw.buffered()});
        std.debug.print("[vopr] reproduce with: vopr --seed={d}{s}{s}\n", .{
            s.seed,
            if (args.small_ring) " --small-ring" else "",
            if (args.volatile_mode) " --mode=volatile" else "",
        });
    }
    return s.ok;
}

// ── Swarm mode: one watchdogged child process per seed ─────────────────

const SeedResult = enum { ok, failed, hung };

fn runChildSeed(allocator: std.mem.Allocator, args: Args, seed: u64) !SeedResult {
    var seed_arg_buf: [32]u8 = undefined;
    const seed_arg = try std.fmt.bufPrint(&seed_arg_buf, "--seed={d}", .{seed});

    var argv: std.ArrayListUnmanaged([]const u8) = .empty;
    defer argv.deinit(allocator);
    try argv.appendSlice(allocator, &.{ args.self_exe, seed_arg });
    if (args.volatile_mode) try argv.append(allocator, "--mode=volatile");
    if (args.no_timer_reset) try argv.append(allocator, "--no-timer-reset");
    if (args.small_ring) try argv.append(allocator, "--small-ring");
    if (args.verbose) try argv.append(allocator, "--verbose");

    var child = stdx.process.Child.init(argv.items, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
    try child.spawn();
    const pid = child.id;

    const deadline = stdx.time.milliTimestamp() + @as(i64, @intCast(args.seed_timeout_s * 1000));
    while (stdx.time.milliTimestamp() < deadline) {
        var status: c_int = 0;
        const rc = std.c.waitpid(pid, &status, std.posix.W.NOHANG);
        if (rc == pid) {
            if (std.posix.W.IFEXITED(@bitCast(status)) and
                std.posix.W.EXITSTATUS(@bitCast(status)) == 0)
            {
                return .ok;
            }
            return .failed;
        }
        stdx.time.sleep(50 * std.time.ns_per_ms);
    }
    // Hung inside the unit under test — the class of bug no in-process
    // budget can catch.
    _ = std.c.kill(pid, .KILL);
    var status: c_int = 0;
    _ = std.c.waitpid(pid, &status, 0);
    return .hung;
}

fn runSwarm(allocator: std.mem.Allocator, args: Args) !bool {
    const base = args.seed orelse @as(u64, @bitCast(@as(i64, @truncate(stdx.time.nanoTimestamp()))));
    const iterations = args.iterations.?;
    std.debug.print("[vopr] swarm: {d} seeds from base {d}, {d}s watchdog per seed\n", .{
        iterations, base, args.seed_timeout_s,
    });

    var failed: std.ArrayListUnmanaged(u64) = .empty;
    defer failed.deinit(allocator);
    var hung: std.ArrayListUnmanaged(u64) = .empty;
    defer hung.deinit(allocator);

    for (0..iterations) |k| {
        const seed = base +% k;
        switch (try runChildSeed(allocator, args, seed)) {
            .ok => {},
            .failed => try failed.append(allocator, seed),
            .hung => try hung.append(allocator, seed),
        }
    }

    std.debug.print("[vopr] swarm done: {d} seeds, {d} failed, {d} hung\n", .{
        iterations, failed.items.len, hung.items.len,
    });
    for (failed.items) |s| std.debug.print("[vopr]   FAILED seed={d}\n", .{s});
    for (hung.items) |s| std.debug.print("[vopr]   HUNG   seed={d}\n", .{s});
    return failed.items.len == 0 and hung.items.len == 0;
}

pub fn main(init: std.process.Init) !void {
    stdx.io.bootFromInit(init.io);
    const raw_args = try init.minimal.args.toSlice(init.arena.allocator());
    const argv = try init.gpa.alloc([]const u8, raw_args.len);
    defer init.gpa.free(argv);
    for (raw_args, 0..) |a, i| argv[i] = a;

    const args = parseArgs(argv) catch {
        usage();
        std.process.exit(2);
    };

    // Swarm children each derive their own scenario from their seed; a
    // pinned scenario or an output path cannot apply to all of them, and
    // silently ignoring the flag is this codebase's signature bug shape.
    if (args.iterations != null and (args.scenario_in != null or args.scenario_out != null)) {
        std.debug.print("--iterations cannot combine with --scenario-in/--scenario-out\n", .{});
        std.process.exit(2);
    }

    const ok = if (args.iterations != null)
        try runSwarm(init.gpa, args)
    else blk: {
        const seed = args.seed orelse
            @as(u64, @bitCast(@as(i64, @truncate(stdx.time.nanoTimestamp()))));
        break :blk try runOne(init.gpa, args, seed);
    };
    if (!ok) std.process.exit(1);
}
