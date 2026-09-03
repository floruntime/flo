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
//!        [--jobs=N]                 swarm children to run concurrently (default 1)
//!        [--out-dir=DIR]            per-seed child logs (default under /tmp);
//!                                   the parent reads them to group failures
//!                                   by invariant and to tell slow from hung
//!        [--mode=volatile|persisted] hard-state model (persisted is the default)
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
    small_ring: bool = false,
    scenario_out: ?[]const u8 = null,
    scenario_in: ?[]const u8 = null,
    verbose: bool = false,
    jobs: u64 = 1,
    out_dir: ?[]const u8 = null,
    /// Child-only: redirect this process's stderr to a file the swarm
    /// parent can read back.
    log_path: ?[]const u8 = null,
    self_exe: []const u8 = "",
};

fn usage() void {
    std.debug.print(
        "usage: vopr [--seed=N] [--iterations=K] [--seed-timeout=SECS] [--jobs=N] [--out-dir=DIR] " ++
            "[--mode=volatile|persisted] [--small-ring] " ++
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
        } else if (std.mem.eql(u8, arg, "--small-ring")) {
            args.small_ring = true;
        } else if (std.mem.startsWith(u8, arg, "--scenario-out=")) {
            args.scenario_out = arg["--scenario-out=".len..];
        } else if (std.mem.startsWith(u8, arg, "--scenario-in=")) {
            args.scenario_in = arg["--scenario-in=".len..];
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            args.verbose = true;
        } else if (std.mem.startsWith(u8, arg, "--jobs=")) {
            args.jobs = try std.fmt.parseInt(u64, arg["--jobs=".len..], 10);
            if (args.jobs == 0) return error.InvalidArgument;
        } else if (std.mem.startsWith(u8, arg, "--out-dir=")) {
            args.out_dir = arg["--out-dir=".len..];
        } else if (std.mem.startsWith(u8, arg, "--log=")) {
            args.log_path = arg["--log=".len..];
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            return error.Usage;
        }
    }
    return args;
}

// ── Scenario file IO (parsing lives with Scenario itself) ─────────────

fn loadScenario(allocator: std.mem.Allocator, path: []const u8) !Scenario {
    const bytes = try stdx.fs.readFileAlloc(allocator, path, 64 * 1024);
    defer allocator.free(bytes);
    return Scenario.fromJsonSlice(allocator, bytes);
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
        .verbose = args.verbose,
        .progress_every = if (args.log_path != null) 10_000 else 0,
    });
    defer sim.deinit();

    const s = try sim.run();
    std.debug.print(
        "[vopr] seed={d} {s}: ticks={d} ops={d} acked={d} lost={d} committed={d} " ++
            "elections={d} crashes={d} restarts={d} delivered={d} dropped={d} stalls={d} evictions={d}\n",
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
            s.eviction_stalls,
        },
    );
    if (!s.ok) {
        var vbuf: [16384]u8 = undefined;
        var vw: std.Io.Writer = .fixed(&vbuf);
        sim.printViolations(&vw) catch {};
        sim.printNodeStates(&vw) catch {};
        std.debug.print("{s}", .{vw.buffered()});
        // A pinned run reproduces from the pin, not the seed — the file may
        // be hand-edited and no longer match fromSeed's sampling.
        if (args.scenario_in) |pin| {
            std.debug.print("[vopr] reproduce with: vopr --scenario-in={s}{s}\n", .{
                pin,
                if (args.volatile_mode) " --mode=volatile" else "",
            });
        } else {
            std.debug.print("[vopr] reproduce with: vopr --seed={d}{s}{s}\n", .{
                s.seed,
                if (args.small_ring) " --small-ring" else "",
                if (args.volatile_mode) " --mode=volatile" else "",
            });
        }
    }
    return s.ok;
}

// ── Swarm mode: one watchdogged child process per seed ─────────────────

const Outcome = enum { ok, failed, timeout };

const Running = struct {
    seed: u64,
    pid: std.c.pid_t,
    deadline_ms: i64,
    log_path: []const u8,
};

fn spawnChild(allocator: std.mem.Allocator, args: Args, seed: u64, log_path: []const u8) !std.c.pid_t {
    var seed_arg_buf: [32]u8 = undefined;
    const seed_arg = try std.fmt.bufPrint(&seed_arg_buf, "--seed={d}", .{seed});
    const log_arg = try std.fmt.allocPrint(allocator, "--log={s}", .{log_path});
    defer allocator.free(log_arg);

    var argv: std.ArrayListUnmanaged([]const u8) = .empty;
    defer argv.deinit(allocator);
    try argv.appendSlice(allocator, &.{ args.self_exe, seed_arg, log_arg });
    if (args.volatile_mode) try argv.append(allocator, "--mode=volatile");
    if (args.small_ring) try argv.append(allocator, "--small-ring");
    if (args.verbose) try argv.append(allocator, "--verbose");

    var child = stdx.process.Child.init(argv.items, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    try child.spawn();
    return child.id;
}

/// Triage from the child's log: the first violation tag, or for a timeout
/// the last progress tick (a hung child stops advancing; a slow one keeps
/// going right up to the kill).
fn classify(allocator: std.mem.Allocator, log_path: []const u8) struct { class: []const u8, last_tick: ?u64, last_elapsed_ms: ?u64 } {
    const text = stdx.fs.readFileAlloc(allocator, log_path, 4 * 1024 * 1024) catch return .{ .class = "unreadable", .last_tick = null, .last_elapsed_ms = null };
    defer allocator.free(text);
    var class: []const u8 = "unknown";
    var last_tick: ?u64 = null;
    var last_elapsed_ms: ?u64 = null;
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |line| {
        const t = std.mem.trimStart(u8, line, " ");
        if (std.mem.startsWith(u8, t, "[") and std.mem.indexOfScalar(u8, t, ']') != null and
            !std.mem.startsWith(u8, t, "[vopr]") and std.mem.eql(u8, class, "unknown"))
        {
            const end = std.mem.indexOfScalar(u8, t, ']').?;
            class = allocator.dupe(u8, t[1..end]) catch "unknown";
        }
        if (std.mem.startsWith(u8, t, "[vopr] progress tick=")) {
            const rest = t["[vopr] progress tick=".len..];
            const sp = std.mem.indexOfScalar(u8, rest, ' ') orelse rest.len;
            last_tick = std.fmt.parseInt(u64, rest[0..sp], 10) catch last_tick;
            if (std.mem.indexOf(u8, rest, "elapsed_ms=")) |e| {
                last_elapsed_ms = std.fmt.parseInt(u64, rest[e + "elapsed_ms=".len ..], 10) catch last_elapsed_ms;
            }
        }
    }
    return .{ .class = class, .last_tick = last_tick, .last_elapsed_ms = last_elapsed_ms };
}

/// A child killed by the watchdog either stopped advancing (hung) or was
/// still going (slow): silence longer than this before the kill is a hang.
const HANG_SILENCE_MS: u64 = 60_000;

const Failure = struct { seed: u64, class: []const u8, last_tick: ?u64, outcome: Outcome };

fn runSwarm(allocator: std.mem.Allocator, args: Args) !bool {
    const base = args.seed orelse @as(u64, @bitCast(@as(i64, @truncate(stdx.time.nanoTimestamp()))));
    const iterations = args.iterations.?;

    // Child logs live under /tmp by default — never inside the repo.
    const out_dir = args.out_dir orelse try std.fmt.allocPrint(allocator, "/tmp/flo-vopr-swarm-{d}", .{base});
    try stdx.fs.makePath(out_dir);
    std.debug.print("[vopr] swarm: {d} seeds from base {d}, {d} jobs, {d}s watchdog per seed, logs in {s}\n", .{
        iterations, base, args.jobs, args.seed_timeout_s, out_dir,
    });

    var failures: std.ArrayListUnmanaged(Failure) = .empty;
    defer failures.deinit(allocator);
    var running: std.ArrayListUnmanaged(Running) = .empty;
    defer running.deinit(allocator);
    var next: u64 = 0;
    var ok_count: u64 = 0;

    while (next < iterations or running.items.len > 0) {
        // Top up the pool.
        while (next < iterations and running.items.len < args.jobs) : (next += 1) {
            const seed = base +% next;
            const log_path = try std.fmt.allocPrint(allocator, "{s}/seed-{d}.log", .{ out_dir, seed });
            const pid = try spawnChild(allocator, args, seed, log_path);
            try running.append(allocator, .{
                .seed = seed,
                .pid = pid,
                .deadline_ms = stdx.time.milliTimestamp() + @as(i64, @intCast(args.seed_timeout_s * 1000)),
                .log_path = log_path,
            });
        }

        // Reap whatever finished or expired.
        var i: usize = 0;
        while (i < running.items.len) {
            const r = running.items[i];
            var status: c_int = 0;
            const rc = std.c.waitpid(r.pid, &status, std.posix.W.NOHANG);
            var outcome: ?Outcome = null;
            if (rc == r.pid) {
                outcome = if (std.posix.W.IFEXITED(@bitCast(status)) and std.posix.W.EXITSTATUS(@bitCast(status)) == 0) .ok else .failed;
            } else if (stdx.time.milliTimestamp() >= r.deadline_ms) {
                // Only the parent can interrupt a hang inside the unit under test.
                _ = std.c.kill(r.pid, .KILL);
                _ = std.c.waitpid(r.pid, &status, 0);
                outcome = .timeout;
            }
            if (outcome) |o| {
                switch (o) {
                    .ok => {
                        ok_count += 1;
                        std.debug.print("[vopr] seed={d} OK\n", .{r.seed});
                    },
                    .failed, .timeout => {
                        var c = classify(allocator, r.log_path);
                        if (o == .timeout) {
                            const budget_ms = args.seed_timeout_s * 1000;
                            const silence = budget_ms -| (c.last_elapsed_ms orelse 0);
                            c.class = if (silence >= HANG_SILENCE_MS) "hung" else "slow";
                            std.debug.print("[vopr] seed={d} TIMEOUT ({s}) last progress tick={?d} silent for {d}s before the kill\n", .{ r.seed, c.class, c.last_tick, silence / 1000 });
                        }
                        try failures.append(allocator, .{ .seed = r.seed, .class = c.class, .last_tick = c.last_tick, .outcome = o });
                        if (o == .timeout) {} else {
                            std.debug.print("[vopr] seed={d} FAILED [{s}]\n", .{ r.seed, c.class });
                        }
                    },
                }
                allocator.free(r.log_path);
                _ = running.swapRemove(i);
            } else {
                i += 1;
            }
        }
        if (running.items.len > 0) stdx.time.sleep(50 * std.time.ns_per_ms);
    }

    std.debug.print("[vopr] swarm done: {d} seeds, {d} ok, {d} failed/timed out\n", .{
        iterations, ok_count, failures.items.len,
    });
    // Group by class so a swarm reads as findings, not as a seed list.
    var printed: std.ArrayListUnmanaged([]const u8) = .empty;
    defer printed.deinit(allocator);
    for (failures.items) |f| {
        var seen = false;
        for (printed.items) |pc| seen = seen or std.mem.eql(u8, pc, f.class);
        if (seen) continue;
        try printed.append(allocator, f.class);
        std.debug.print("[vopr]   [{s}]:", .{f.class});
        for (failures.items) |g| if (std.mem.eql(u8, g.class, f.class)) {
            if (g.outcome == .timeout) {
                std.debug.print(" {d}(timeout@{?d})", .{ g.seed, g.last_tick });
            } else {
                std.debug.print(" {d}", .{g.seed});
            }
        };
        std.debug.print("\n", .{});
    }
    return failures.items.len == 0;
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
    if (args.log_path) |lp| {
        // Everything this process prints goes to the swarm parent's per-seed
        // file, so failures can be classified after the fact.
        const file = stdx.fs.createFile(lp, .{}) catch {
            std.debug.print("cannot open --log path {s}\n", .{lp});
            std.process.exit(2);
        };
        if (std.c.dup2(file.handle, std.posix.STDERR_FILENO) < 0) std.process.exit(2);
    }

    // Swarm children each derive their own scenario from their seed; a
    // pinned scenario or an output path cannot apply to all of them, and
    // silently ignoring the flag is this codebase's signature bug shape.
    if (args.iterations != null and (args.scenario_in != null or args.scenario_out != null)) {
        std.debug.print("--iterations cannot combine with --scenario-in/--scenario-out\n", .{});
        std.process.exit(2);
    }
    // A pinned scenario is the whole input; a seed or ring flag beside it
    // would be silently overridden — refuse rather than guess.
    if (args.scenario_in != null and (args.seed != null or args.small_ring)) {
        std.debug.print("--scenario-in is exclusive with --seed/--small-ring (the pin carries both)\n", .{});
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
