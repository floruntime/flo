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
//!        [--skip=N,M,...]           swarm: seeds not run (a pinned bug's), at most 8
//!        [--out-dir=DIR]            per-seed child logs (default under /tmp);
//!                                   the parent reads them to group failures
//!                                   by invariant and to tell slow from hung
//!        [--mode=volatile|persisted] hard-state model (persisted is the default)
//!        [--small-ring]             shrink the ring so eviction outruns
//!                                   replication and repairs read below it
//!        [--pending-reads]          the small ring, with a leader's reads below it
//!                                   for a peer answered pending and done later
//!        [--wipes]                  crashes often lose a disk or its hard
//!                                   state, up to two nodes at once
//!        [--membership]             the leader changes the membership often
//!        [--byzantine]              a node with the secret but no seat sends
//!                                   forged votes and appends
//!        [--halts]                  one op halts every node that applies it
//!        [--actions]                a share of the ops are action entries;
//!                                   a swarm fails if no seed settles or
//!                                   expires a lease from an earlier term
//!        [--family-mutant=NAME]     plant a family mutant (`families.Mutant`):
//!                                   the run must fail
//!        [--catching-up]            namespace creates, and nodes that fall
//!                                   behind on applying and win elections
//!        [--txn-limits]             each node has its own limit on the KV
//!                                   batches it proposes, some up to the most
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
    pending_reads: bool = false,
    wipes: bool = false,
    membership: bool = false,
    byzantine: bool = false,
    halts: bool = false,
    actions: bool = false,
    family_mutant: vopr.families.Mutant = .none,
    catching_up: bool = false,
    txn_limits: bool = false,
    max_delivered: u64 = 0,
    skip: [8]u64 = undefined,
    skip_len: usize = 0,
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
            "[--skip=N,M,...] [--mode=volatile|persisted] [--small-ring] [--pending-reads] [--wipes] [--membership] [--byzantine] [--halts] [--actions] [--catching-up] [--txn-limits] [--max-delivered=N] " ++
            "[--family-mutant=NAME] [--scenario-out=PATH] [--scenario-in=PATH] [--verbose]\n",
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
        } else if (std.mem.eql(u8, arg, "--pending-reads")) {
            args.pending_reads = true;
        } else if (std.mem.eql(u8, arg, "--wipes")) {
            args.wipes = true;
        } else if (std.mem.eql(u8, arg, "--membership")) {
            args.membership = true;
        } else if (std.mem.eql(u8, arg, "--byzantine")) {
            args.byzantine = true;
        } else if (std.mem.eql(u8, arg, "--halts")) {
            args.halts = true;
        } else if (std.mem.eql(u8, arg, "--actions")) {
            args.actions = true;
        } else if (std.mem.startsWith(u8, arg, "--family-mutant=")) {
            args.family_mutant = std.meta.stringToEnum(vopr.families.Mutant, arg["--family-mutant=".len..]) orelse return error.Usage;
        } else if (std.mem.eql(u8, arg, "--catching-up")) {
            args.catching_up = true;
        } else if (std.mem.startsWith(u8, arg, "--max-delivered=")) {
            args.max_delivered = try std.fmt.parseInt(u64, arg["--max-delivered=".len..], 10);
        } else if (std.mem.eql(u8, arg, "--txn-limits")) {
            args.txn_limits = true;
        } else if (std.mem.startsWith(u8, arg, "--skip=")) {
            var it = std.mem.splitScalar(u8, arg["--skip=".len..], ',');
            while (it.next()) |n| {
                if (args.skip_len == args.skip.len) return error.TooManySkipped;
                args.skip[args.skip_len] = try std.fmt.parseInt(u64, n, 10);
                args.skip_len += 1;
            }
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
    else if (args.pending_reads)
        Scenario.pendingReads(seed)
    else if (args.small_ring)
        Scenario.smallRing(seed)
    else if (args.wipes)
        Scenario.wipes(seed)
    else if (args.membership)
        Scenario.memberships(seed)
    else if (args.byzantine)
        Scenario.outsider(seed)
    else if (args.halts)
        Scenario.halting(seed)
    else if (args.actions)
        Scenario.actions(seed)
    else if (args.catching_up)
        Scenario.catchingUp(seed)
    else if (args.txn_limits)
        Scenario.txnLimits(seed)
    else
        Scenario.fromSeed(seed);
    if (args.volatile_mode) scenario.hard_state = .volatile_state;
    // A planted bug the run never reaches would pass, and say nothing.
    if (args.family_mutant.needsTxnLimits() and !scenario.hasTxnLimits()) {
        std.debug.print("--family-mutant={t} is in a node's transaction limit: run it with --txn-limits, or a scenario that has them\n", .{args.family_mutant});
        std.process.exit(2);
    }
    if (args.family_mutant.inActions() and scenario.action_percent == 0) {
        std.debug.print("--family-mutant={t} is in the actions machine: run it with --actions, or a scenario with action entries\n", .{args.family_mutant});
        std.process.exit(2);
    }

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
        .max_delivered = args.max_delivered,
        .volatile_hard_state = args.volatile_mode or scenario.hard_state == .volatile_state,
        .family_mutant = args.family_mutant,
        .verbose = args.verbose,
        .progress_every = if (args.log_path != null) 10_000 else 0,
    });
    defer sim.deinit();

    const s = try sim.run();
    if (scenario.halt_op != 0) {
        if (sim.halt) |h| std.debug.print("[vopr] halted at index {d} (term {d}) on every core node\n", .{ h.index, h.term }) else std.debug.print("[vopr] the halting op never committed\n", .{});
    }
    std.debug.print(
        "[vopr] seed={d} {s}: ticks={d} ops={d} acked={d} lost={d} committed={d} " ++
            "elections={d} crashes={d} wipes={d} restarts={d} warm_restarts={d} delivered={d} dropped={d} stalls={d} catch_up_reads={d} divergences={d} beyond_tolerance={d} vouches_taken={d} stranger_frames={d} state_checks={d} rebuilt_checks={d} family_checks={d} kv_refusals={d} kv_staged_refusals={d} kv_merges={d} kv_field_deletes={d} kv_condition_refusals={d} kv_conditions_held={d} kv_batch_condition_refusals={d} kv_batch_conditions_held={d}\n",
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
            s.wipes,
            s.restarts,
            s.warm_restarts,
            s.messages_delivered,
            s.messages_dropped,
            s.apply_stalls,
            s.catch_up_reads,
            s.divergences,
            s.divergences_beyond_tolerance,
            s.vouches_taken,
            sim.stranger_frames,
            s.state_checks,
            s.rebuilt_checks,
            s.family_checks,
            s.kv.refusals,
            s.kv.staged_refusals,
            s.kv.merges,
            s.kv.field_deletes,
            s.kv.condition_refusals,
            s.kv.conditions_held,
            s.kv.batch_condition_refusals,
            s.kv.batch_conditions_held,
        },
    );
    if (scenario.action_percent > 0) printActionCounts(s.actions);
    if (scenario.hasTxnLimits()) printTxnCounts(s.txn);
    if (scenario.pending_read_permille > 0) std.debug.print("[vopr] pending reads served: {d}\n", .{s.pending_reads});
    if (scenario.namespace_creates) std.debug.print("[vopr] namespace creates: proposed={d} catching_up={d} refused={d} stale_host={d}; applied created={d} past_next={d} stale_id={d} (from ready leaders {d}) refused={d}\n", .{ s.ns_submits.proposed, s.ns_submits.catching_up, s.ns_submits.refused, s.ns_submits.stale_host, s.ns.created, s.ns.past_next, s.ns.stale_ids, s.ns_stale_from_ready, s.ns.refusals });
    if (scenario.outsider_permille > 0) std.debug.print("[vopr] outsider sent {d} vote requests, {d} appends, {d} term checks\n", .{ sim.outsider_sent[0], sim.outsider_sent[1], sim.outsider_sent[2] });
    if (!s.ok) {
        var vbuf: [16384]u8 = undefined;
        var vw: std.Io.Writer = .fixed(&vbuf);
        sim.printViolations(&vw) catch {};
        sim.printNodeStates(&vw) catch {};
        std.debug.print("{s}", .{vw.buffered()});
        // A pinned run reproduces from the pin, not the seed — the file may
        // be hand-edited and no longer match fromSeed's sampling.
        var mb: [64]u8 = undefined;
        const mutant = if (args.family_mutant == .none) "" else std.fmt.bufPrint(&mb, " --family-mutant={t}", .{args.family_mutant}) catch "";
        if (args.scenario_in) |pin| {
            std.debug.print("[vopr] reproduce with: vopr --scenario-in={s}{s}{s}\n", .{
                pin,
                mutant,
                if (args.volatile_mode) " --mode=volatile" else "",
            });
        } else {
            std.debug.print("[vopr] reproduce with: vopr --seed={d}{s}{s}{s}{s}{s}{s}{s}{s}{s}{s}{s}\n", .{
                s.seed,
                mutant,
                if (args.actions) " --actions" else "",
                if (args.small_ring) " --small-ring" else "",
                if (args.pending_reads) " --pending-reads" else "",
                if (args.wipes) " --wipes" else "",
                if (args.membership) " --membership" else "",
                if (args.byzantine) " --byzantine" else "",
                if (args.halts) " --halts" else "",
                if (args.catching_up) " --catching-up" else "",
                if (args.txn_limits) " --txn-limits" else "",
                if (args.volatile_mode) " --mode=volatile" else "",
            });
        }
    }
    return s.ok;
}

const ActionCounts = vopr.families.Actions.Counts;
/// How many seeds of an actions swarm must reach each takeover path.
const MIN_TAKEOVER_SEEDS = 3;
const ACTION_KINDS = std.enums.values(vopr.families.Actions.Kind);

/// One line a parent can read back: each kind as applied/refused.
fn printActionCounts(c: ActionCounts) void {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    w.writeAll("[vopr] actions:") catch {};
    for (ACTION_KINDS, c.applied, c.refused) |k, a, r| w.print(" {t}={d}/{d}", .{ k, a, r }) catch {};
    w.print(" cross_term_settles={d} cross_term_expires={d}\n", .{ c.cross_term_settles, c.cross_term_expires }) catch {};
    std.debug.print("{s}", .{w.buffered()});
}

/// A child's action counts, from its log.
fn readActionCounts(allocator: std.mem.Allocator, log_path: []const u8) ?ActionCounts {
    const text = stdx.fs.readFileAlloc(allocator, log_path, 4 * 1024 * 1024) catch return null;
    defer allocator.free(text);
    const at = std.mem.indexOf(u8, text, "[vopr] actions:") orelse return null;
    const line = text[at..(std.mem.indexOfScalarPos(u8, text, at, '\n') orelse text.len)];
    var c: ActionCounts = .{};
    var it = std.mem.tokenizeScalar(u8, line["[vopr] actions:".len..], ' ');
    while (it.next()) |field| {
        const eq = std.mem.indexOfScalar(u8, field, '=') orelse return null;
        const name = field[0..eq];
        const value = field[eq + 1 ..];
        if (std.mem.eql(u8, name, "cross_term_settles")) {
            c.cross_term_settles = std.fmt.parseInt(u64, value, 10) catch return null;
        } else if (std.mem.eql(u8, name, "cross_term_expires")) {
            c.cross_term_expires = std.fmt.parseInt(u64, value, 10) catch return null;
        } else {
            const k = std.meta.stringToEnum(vopr.families.Actions.Kind, name) orelse return null;
            const slash = std.mem.indexOfScalar(u8, value, '/') orelse return null;
            c.applied[@backingInt(k)] = std.fmt.parseInt(u64, value[0..slash], 10) catch return null;
            c.refused[@backingInt(k)] = std.fmt.parseInt(u64, value[slash + 1 ..], 10) catch return null;
        }
    }
    return c;
}

const TxnCounts = vopr.families.TxnCounts;
/// How many seeds of a limits swarm must reach each path.
const MIN_TXN_SEEDS = 3;
/// Fewer for a batch reaching a later leader below it: more crashes reduce it
/// as much as they raise it, so it's thin on any 20 seeds.
const MIN_TXN_ACROSS_LEADERS_SEEDS = 2;

/// One line a parent can read back.
fn printTxnCounts(c: TxnCounts) void {
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    w.writeAll("[vopr] txn:") catch {};
    inline for (comptime std.meta.fieldNames(TxnCounts)) |name| w.print(" {s}={d}", .{ name, @field(c, name) }) catch {};
    std.debug.print("{s}\n", .{w.buffered()});
}

/// A child's transaction counts, from its log.
fn readTxnCounts(allocator: std.mem.Allocator, log_path: []const u8) ?TxnCounts {
    const text = stdx.fs.readFileAlloc(allocator, log_path, 4 * 1024 * 1024) catch return null;
    defer allocator.free(text);
    const at = std.mem.indexOf(u8, text, "[vopr] txn:") orelse return null;
    const line = text[at..(std.mem.indexOfScalarPos(u8, text, at, '\n') orelse text.len)];
    var c: TxnCounts = .{};
    var it = std.mem.tokenizeScalar(u8, line["[vopr] txn:".len..], ' ');
    while (it.next()) |field| {
        const eq = std.mem.indexOfScalar(u8, field, '=') orelse return null;
        const value = std.fmt.parseInt(u64, field[eq + 1 ..], 10) catch return null;
        inline for (comptime std.meta.fieldNames(TxnCounts)) |name| {
            if (std.mem.eql(u8, field[0..eq], name)) @field(c, name) = value;
        }
    }
    return c;
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
    if (args.pending_reads) try argv.append(allocator, "--pending-reads");
    if (args.wipes) try argv.append(allocator, "--wipes");
    if (args.membership) try argv.append(allocator, "--membership");
    if (args.byzantine) try argv.append(allocator, "--byzantine");
    if (args.halts) try argv.append(allocator, "--halts");
    if (args.actions) try argv.append(allocator, "--actions");
    var mutant_arg: [64]u8 = undefined;
    if (args.family_mutant != .none) try argv.append(allocator, try std.fmt.bufPrint(&mutant_arg, "--family-mutant={t}", .{args.family_mutant}));
    if (args.catching_up) try argv.append(allocator, "--catching-up");
    if (args.txn_limits) try argv.append(allocator, "--txn-limits");
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

/// A passing child whose run was excused from converging: an async crash
/// cost a diverged voter no config could spare.
fn excused(allocator: std.mem.Allocator, log_path: []const u8) bool {
    const text = stdx.fs.readFileAlloc(allocator, log_path, 4 * 1024 * 1024) catch return false;
    defer allocator.free(text);
    const at = std.mem.indexOf(u8, text, " beyond_tolerance=") orelse return false;
    const rest = text[at + " beyond_tolerance=".len ..];
    return rest.len > 0 and rest[0] != '0';
}

/// A child killed by the watchdog either stopped advancing (hung) or was
/// still going (slow): silence longer than this before the kill is a hang.
const HANG_SILENCE_MS: u64 = 60_000;

const Failure = struct { seed: u64, class: []const u8, last_tick: ?u64, outcome: Outcome };

fn runSwarm(allocator: std.mem.Allocator, args: Args) !bool {
    const base = args.seed orelse @as(u64, @bitCast(@as(i64, @truncate(stdx.time.nanoTimestamp()))));
    const iterations = args.iterations.?;

    // Child logs live under /tmp by default — never inside the repo.
    const made_dir = if (args.out_dir == null) try std.fmt.allocPrint(allocator, "/tmp/flo-vopr-swarm-{d}", .{base}) else null;
    defer if (made_dir) |d| allocator.free(d);
    const out_dir = args.out_dir orelse made_dir.?;
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
    var skipped: u64 = 0;
    var excused_seeds: std.ArrayListUnmanaged(u64) = .empty;
    defer excused_seeds.deinit(allocator);
    var action_totals: ActionCounts = .{};
    var settle_seeds: u64 = 0;
    var expire_seeds: u64 = 0;
    var unread_counts: u64 = 0;
    var txn_totals: TxnCounts = .{};
    var txn_seeds: struct { refused: u64 = 0, large: u64 = 0, past_own: u64 = 0, replayed: u64 = 0, across_leaders: u64 = 0 } = .{};

    while (next < iterations or running.items.len > 0) {
        // Top up the pool.
        while (next < iterations and running.items.len < args.jobs) : (next += 1) {
            const seed = base +% next;
            if (std.mem.indexOfScalar(u64, args.skip[0..args.skip_len], seed) != null) {
                std.debug.print("[vopr] seed={d} skipped\n", .{seed});
                skipped += 1;
                continue;
            }
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
                        if (args.actions) {
                            if (readActionCounts(allocator, r.log_path)) |c| {
                                action_totals.add(c);
                                if (c.cross_term_settles > 0) settle_seeds += 1;
                                if (c.cross_term_expires > 0) expire_seeds += 1;
                            } else unread_counts += 1;
                        }
                        if (args.txn_limits) {
                            if (readTxnCounts(allocator, r.log_path)) |c| {
                                txn_totals.add(c);
                                if (c.leader_refused > 0) txn_seeds.refused += 1;
                                if (c.over_half > 0) txn_seeds.large += 1;
                                if (c.large_over_own_live > 0) txn_seeds.past_own += 1;
                                if (c.large_replayed > 0) txn_seeds.replayed += 1;
                                if (c.large_across_leaders > 0) txn_seeds.across_leaders += 1;
                            } else unread_counts += 1;
                        }
                        if (excused(allocator, r.log_path)) try excused_seeds.append(allocator, r.seed);
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

    std.debug.print("[vopr] swarm done: {d} seeds, {d} ok, {d} failed/timed out, {d} skipped\n", .{
        iterations, ok_count, failures.items.len, skipped,
    });
    if (excused_seeds.items.len > 0) {
        std.debug.print("[vopr]   ok but excused from converging (a voter lost to an async crash beyond what its config tolerates): {d}:", .{excused_seeds.items.len});
        for (excused_seeds.items) |seed| std.debug.print(" {d}", .{seed});
        std.debug.print("\n", .{});
    }
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
    // A batch whose leases never outlive a leader tested none of the
    // takeover paths: it fails, though every seed passed.
    var covered = true;
    if (args.actions) {
        std.debug.print("[vopr] actions, applied/refused (refused share):", .{});
        for (ACTION_KINDS, action_totals.applied, action_totals.refused) |k, a, rf| {
            std.debug.print(" {t}={d}/{d} ({d}%)", .{ k, a, rf, if (a + rf == 0) 0 else rf * 100 / (a + rf) });
        }
        std.debug.print("\n[vopr] across a term change: {d} settles in {d} seeds, {d} expires in {d} seeds\n", .{ action_totals.cross_term_settles, settle_seeds, action_totals.cross_term_expires, expire_seeds });
        if (unread_counts > 0) {
            std.debug.print("[vopr] FAILED: {d} passing seeds printed no action counts\n", .{unread_counts});
            covered = false;
        }
        // A few seeds, not one: a workload change that loses the path in
        // most seeds should fail here, not pass on a lucky one.
        const want = @min(MIN_TAKEOVER_SEEDS, ok_count);
        if (settle_seeds < want or expire_seeds < want) {
            std.debug.print("[vopr] FAILED: fewer than {d} seeds settled and expired leases from an earlier term\n", .{want});
            covered = false;
        }
    }
    // Limits that no batch ever went past tested nothing: it fails, as above.
    if (args.txn_limits) {
        printTxnCounts(txn_totals);
        std.debug.print("[vopr] seeds reaching: a leader's refusal {d}; large batches applied {d}, past a replica's own limit {d}, replayed {d}, at a later leader below them {d}\n", .{ txn_seeds.refused, txn_seeds.large, txn_seeds.past_own, txn_seeds.replayed, txn_seeds.across_leaders });
        if (unread_counts > 0) {
            std.debug.print("[vopr] FAILED: {d} passing seeds printed no transaction counts\n", .{unread_counts});
            covered = false;
        }
        const want = @min(MIN_TXN_SEEDS, ok_count);
        if (txn_seeds.refused < want or txn_seeds.large < want or txn_seeds.past_own < want or txn_seeds.replayed < want or txn_seeds.across_leaders < @min(MIN_TXN_ACROSS_LEADERS_SEEDS, ok_count)) {
            std.debug.print("[vopr] FAILED: too few seeds had a leader refuse a batch, or applied large ones past a replica's limit, on replay ({d} of each) or at a later leader below them ({d})\n", .{ want, @min(MIN_TXN_ACROSS_LEADERS_SEEDS, ok_count) });
            covered = false;
        }
    }
    return failures.items.len == 0 and covered;
}

// std's default gpa is SafeAllocator in ReleaseSafe, which flo's own main
// avoids for speed; the simulator keeps it so CI's swarms catch misuse.
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
    if (args.scenario_in != null and (args.seed != null or args.small_ring or args.pending_reads or args.wipes or args.membership or args.byzantine or args.halts or args.actions or args.catching_up or args.txn_limits)) {
        std.debug.print("--scenario-in is exclusive with --seed/--small-ring/--pending-reads/--wipes/--membership/--byzantine/--halts/--actions/--catching-up/--txn-limits (the pin carries them)\n", .{});
        std.process.exit(2);
    }

    if (@as(u8, @intFromBool(args.small_ring)) + @intFromBool(args.pending_reads) + @intFromBool(args.wipes) + @intFromBool(args.membership) + @intFromBool(args.byzantine) + @intFromBool(args.halts) + @intFromBool(args.actions) + @intFromBool(args.catching_up) + @intFromBool(args.txn_limits) > 1) {
        std.debug.print("--small-ring, --pending-reads, --wipes, --membership, --byzantine, --halts, --actions, --catching-up and --txn-limits are separate slices; pick one\n", .{});
        std.process.exit(2);
    }

    if (args.iterations != null and args.max_delivered != 0) {
        std.debug.print("--max-delivered is for one run\n", .{});
        std.process.exit(2);
    }
    // A swarm's children would each refuse these, and read as caught.
    if (args.iterations != null and args.family_mutant.needsTxnLimits() and !args.txn_limits) {
        std.debug.print("--family-mutant={t} is in a node's transaction limit: run the swarm with --txn-limits\n", .{args.family_mutant});
        std.process.exit(2);
    }
    if (args.iterations != null and args.family_mutant.inActions() and !args.actions) {
        std.debug.print("--family-mutant={t} is in the actions machine: run the swarm with --actions\n", .{args.family_mutant});
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
