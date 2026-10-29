const std = @import("std");

// Another compiler fails on this file with a wall of unrelated-looking errors.
comptime {
    const v = @import("builtin").zig_version;
    if (v.major != 0 or v.minor != 17)
        @compileError(std.fmt.comptimePrint("flo needs Zig 0.17.x, found {d}.{d}.{d}", .{ v.major, v.minor, v.patch }));
}

pub fn build(b: *std.Build) void {
    // Configure does I/O the build system can't see (git describe, the test
    // root and dashboard asset walks), so a cached configuration would miss
    // new test files and assets. Re-run it every build.
    b.graph.poisonCache();

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });

    // ── Modules ──

    // stdx: standard library extensions (logging, etc.)
    const stdx_module = b.createModule(.{
        .root_source_file = b.path("src/stdx/mod.zig"),
        .target = target,
        .optimize = optimize,
        // stdx wraps libc directly (std.c sockets, fcntl, file IO), so it needs
        // the link wherever it is rooted. Consumers set this on their own root
        // module, which covers stdx as a dependency but not the test artifact
        // rooted at stdx itself — that one fails to compile without it on
        // targets where libc is not linked implicitly.
        .link_libc = true,
    });

    // ── Version (git describe) ──

    const build_options = b.addOptions();
    build_options.addOption([]const u8, "version", getGitVersion(b));
    build_options.addOption(bool, "require_hw_crc", b.option(bool, "require-hw-crc", "Fail unless the target CPU has a CRC32C instruction (release builds set this)") orelse false);
    // Faults e2e drives to reach states no valid request can (a halted
    // shard). Off unless asked for, so no release or image carries them.
    const test_faults = b.option(bool, "test-faults", "Compile in the faults e2e tests drive; never for a release") orelse false;
    build_options.addOption(bool, "test_faults", test_faults);

    // ── Dashboard Assets ──
    // Generate assets.zig by scanning src/node/dashboard/dist at configure time.
    // The file is @import'd by http_server.zig via relative path.
    _ = generateDashboardAssetsModule(b);

    // ── Main Executable ──

    const exe = b.addExecutable(.{
        .name = "flo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    exe.root_module.addImport("stdx", stdx_module);
    exe.root_module.addOptions("build_options", build_options);

    b.installArtifact(exe);

    // ── Generated descriptions ──

    // `zig build gen`: ops.json, the op table as SDK generators and the
    // reference docs read it, into zig-out/gen.
    const gen_exe = b.addExecutable(.{
        .name = "gen-ops",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/gen_ops.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
            .link_libc = true,
        }),
    });
    const gen_stdx = b.createModule(.{ .root_source_file = b.path("src/stdx/mod.zig"), .target = b.graph.host, .optimize = .Debug, .link_libc = true });
    gen_exe.root_module.addImport("stdx", gen_stdx);
    const gen_run = b.addRunArtifact(gen_exe);
    const ops_json = gen_run.captureStdOut(.{});
    const gen_step = b.step("gen", "Write zig-out/gen/ops.json from the op table");
    gen_step.dependOn(&b.addInstallFile(ops_json, "gen/ops.json").step);

    // ── Run Step ──

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // ── Install to System PATH ──

    const install_dir_option = b.option([]const u8, "install_dir", "Directory to install flo binary");
    const default_install_dir = if (b.graph.host.result.os.tag == .macos) "/opt/homebrew/bin" else "/usr/local/bin";
    const install_dir = install_dir_option orelse default_install_dir;

    const system_install_step = b.step("install-system", "Install flo to system PATH directory (default: /usr/local/bin on Linux, /opt/homebrew/bin on macOS)");
    const install_cmd = b.addSystemCommand(&.{ "sh", "-c" });
    const build_root_str = b.root.joinString(b.allocator, "zig-out/bin/flo") catch @panic("OOM");
    const install_script = std.fmt.allocPrint(b.allocator,
        \\mkdir -p "{s}" && cp "{s}" "{s}/flo" && chmod +x "{s}/flo" && echo "✓ Installed flo to {s}/flo"
    , .{ install_dir, build_root_str, install_dir, install_dir, install_dir }) catch unreachable;
    install_cmd.addArg(install_script);
    install_cmd.step.dependOn(b.getInstallStep());
    system_install_step.dependOn(&install_cmd.step);

    // ── Library Module (for tests) ──

    const src_module = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    src_module.addImport("stdx", stdx_module);
    src_module.addOptions("build_options", build_options);

    // ── Test Filter ──

    const test_filters = b.option([]const []const u8, "test-filter", "Run only tests whose name contains this substring; repeat to match any of several") orelse &.{};
    for (test_filters) |f| if (std.mem.indexOfScalar(u8, f, '|') != null)
        std.process.fatal("-Dtest-filter={s}: filters are plain substrings; repeat -Dtest-filter to match several", .{f});

    // ── Unit Tests ──

    // Each test root is generated from a walk of its directory, so a file
    // with tests can't be left out of a hand-kept list and never run.
    const e2e_shard = parseShard(b.option([]const u8, "e2e-shard", "Run e2e shard i of N (\"i/N\"): files split by the times in tests/e2e/shard_times.tsv"));
    const unit_root = generateTestRoot(b, "src", &.{"stdx"}, test_filters, null);
    const stdx_root = generateTestRoot(b, "src/stdx", &.{}, test_filters, null);
    const e2e_root = generateTestRoot(b, "tests/e2e", &.{"helpers"}, test_filters, e2e_shard);
    const integration_root = generateTestRoot(b, "tests/integration", &.{}, test_filters, null);
    // CI's final e2e job sums these across shards: every shard ran, and their
    // tests add up to the whole suite.
    if (e2e_root.shard) |info| {
        const wf = b.addWriteFiles();
        const text = b.fmt("shard={d}/{d} declared={d} total={d}\n", .{ info.index, info.count, info.declared, info.total });
        b.getInstallStep().dependOn(&b.addInstallFile(wf.add("e2e-shard.txt", text), "e2e-shard.txt").step);
    }

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = unit_root.source,
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = test_filters,
    });
    unit_tests.root_module.addImport("src", src_module);
    unit_tests.root_module.addImport("stdx", stdx_module);
    unit_tests.root_module.addOptions("build_options", build_options);
    unit_tests.root_module.addOptions("test_root", unit_root.options);

    const run_unit_tests = b.addRunArtifact(unit_tests);
    // Some tests read source files (the CLI lint), by paths from the root.
    run_unit_tests.setCwd(b.path("."));

    // stdx is its own module, and module dependencies contribute no tests to
    // a test root — everything in stdx (PRNG, log, helpers) was invisible to
    // the src test runner and had never run. Compile stdx as its own root.
    stdx_module.addOptions("test_root", stdx_root.options);
    const stdx_tests = b.addTest(.{
        .root_module = stdx_module,
        .filters = test_filters,
    });
    const run_stdx_tests = b.addRunArtifact(stdx_tests);

    // ── E2E Tests ──

    const e2e_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = e2e_root.source,
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = test_filters,
        .test_runner = .{ .path = b.path("tests/e2e/helpers/test_runner.zig"), .mode = .server },
    });
    e2e_tests.root_module.addImport("src", src_module);
    e2e_tests.root_module.addImport("stdx", stdx_module);
    e2e_tests.root_module.addOptions("build_options", build_options);
    e2e_tests.root_module.addOptions("test_root", e2e_root.options);

    const run_e2e_tests = b.addRunArtifact(e2e_tests);
    run_e2e_tests.step.dependOn(b.getInstallStep());
    // harness_test.zig runs it: a process that starts servers, then dies
    // without cleanup.
    if (e2e_root.missing == null) {
        const orphan = b.addTest(.{
            .name = "e2e-orphan",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/e2e/helpers/orphan_main.zig"),
                .target = target,
                .optimize = optimize,
                .link_libc = true,
            }),
        });
        orphan.root_module.addImport("stdx", stdx_module);
        run_e2e_tests.step.dependOn(&b.addInstallArtifact(orphan, .{}).step);
    }
    // The tests spawn the installed flo. Its bytes are part of the run's
    // cache key, or a change to flo alone would replay the last result
    // without running anything.
    run_e2e_tests.addFileInput(exe.getEmittedBin());

    // ── Integration Tests ──

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = integration_root.source,
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = test_filters,
    });
    integration_tests.root_module.addImport("src", src_module);
    integration_tests.root_module.addImport("stdx", stdx_module);
    integration_tests.root_module.addOptions("build_options", build_options);
    integration_tests.root_module.addOptions("test_root", integration_root.options);

    const run_integration_tests = b.addRunArtifact(integration_tests);

    // ── Test Steps ──

    addTestStep(b, "test", "Run all tests", &.{ &run_unit_tests.step, &run_stdx_tests.step }, &.{ unit_root, stdx_root }, test_filters);
    addTestStep(b, "test-unit", "Run unit tests only", &.{ &run_unit_tests.step, &run_stdx_tests.step }, &.{ unit_root, stdx_root }, test_filters);
    addTestStep(b, "test-e2e", "Run end-to-end tests", &.{&run_e2e_tests.step}, &.{e2e_root}, test_filters);
    addTestStep(b, "test-integration", "Run integration tests", &.{&run_integration_tests.step}, &.{integration_root}, test_filters);

    // ── VOPR simulator ──

    const vopr_exe = b.addExecutable(.{
        .name = "vopr",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/vopr/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    vopr_exe.root_module.addImport("src", src_module);
    vopr_exe.root_module.addImport("stdx", stdx_module);
    b.installArtifact(vopr_exe);

    const run_vopr = b.addRunArtifact(vopr_exe);
    run_vopr.step.dependOn(b.getInstallStep());
    run_vopr.addPassthruArgs();

    const vopr_step = b.step("vopr", "Run the deterministic cluster simulator (-- --seed=N | --iterations=K)");
    vopr_step.dependOn(&run_vopr.step);

    // ── Benchmarks ──

    const bench_step = b.step("bench", "Build all benchmarks");

    const bench_sources = [_]struct { name: []const u8, source: []const u8 }{
        .{ .name = "bench-ual", .source = "bench/bench_ual.zig" },
        .{ .name = "bench-kv", .source = "bench/bench_kv.zig" },
        .{ .name = "bench-inbox", .source = "bench/bench_inbox.zig" },
        .{ .name = "bench-write-path", .source = "bench/bench_write_path.zig" },
    };

    for (bench_sources) |def| {
        const bench_exe = b.addExecutable(.{
            .name = def.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(def.source),
                .target = target,
                .optimize = .ReleaseFast,
                .link_libc = true,
            }),
        });
        bench_exe.root_module.addImport("src", src_module);
        bench_exe.root_module.addImport("stdx", stdx_module);
        const install_bench = b.addInstallArtifact(bench_exe, .{});
        bench_step.dependOn(&install_bench.step);
    }

    // ── Documentation ──

    const lib = b.addObject(.{
        .name = "flo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const install_docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });

    const docs_step = b.step("docs", "Generate API documentation");
    docs_step.dependOn(&install_docs.step);
}

// ── Test Roots ──

const TestRoot = struct {
    /// The generated root.
    source: std.Build.LazyPath,
    /// `declared` (top-level test declarations the walk found), for the
    /// root's check that every one of them was compiled; `filters` and
    /// `matched`, for its check that a filtered build compiled exactly the
    /// tests predicted here.
    options: *std.Build.Step.Options,
    /// Named tests under this root whose name contains a filter.
    matched: usize,
    /// The directory, when this checkout doesn't have it.
    missing: ?[]const u8 = null,
    /// For a shard: its place, and its test declarations against the whole
    /// suite's (the sentinel, which every shard compiles, in neither).
    shard: ?struct { index: usize, count: usize, declared: usize, total: usize } = null,
};

const Shard = struct { index: usize, count: usize };

fn parseShard(arg: ?[]const u8) ?Shard {
    const text = arg orelse return null;
    const slash = std.mem.indexOfScalar(u8, text, '/') orelse
        std.process.fatal("-De2e-shard={s}: expected i/N", .{text});
    const i = std.fmt.parseUnsigned(usize, text[0..slash], 10) catch
        std.process.fatal("-De2e-shard={s}: expected i/N", .{text});
    const n = std.fmt.parseUnsigned(usize, text[slash + 1 ..], 10) catch
        std.process.fatal("-De2e-shard={s}: expected i/N", .{text});
    if (i < 1 or i > n) std.process.fatal("-De2e-shard={s}: i must be 1 to N", .{text});
    return .{ .index = i, .count = n };
}

/// A file not in shard_times.tsv yet weighs this much per test: the measured
/// mean, so a new file lands somewhere sensible until the times are refreshed.
const default_ms_per_test = 900;

/// The shard (0-based) of each of `files`: longest first, each to the
/// least-loaded shard, ties to the lower shard and then by name, so every
/// shard job computes the same split.
fn assignShards(b: *std.Build, root_dir: std.Io.Dir, files: []const TestFile, count: usize) []usize {
    const io = b.graph.io;
    var times: std.StringHashMapUnmanaged(u64) = .empty;
    if (root_dir.readFileAlloc(io, "shard_times.tsv", b.allocator, .unlimited)) |content| {
        var lines = std.mem.splitScalar(u8, content, '\n');
        while (lines.next()) |line| {
            if (line.len == 0 or line[0] == '#') continue;
            const tab = std.mem.indexOfScalar(u8, line, '\t') orelse
                std.process.fatal("tests/e2e/shard_times.tsv: expected <ms>\\t<file>, got {s}", .{line});
            const ms = std.fmt.parseUnsigned(u64, line[0..tab], 10) catch
                std.process.fatal("tests/e2e/shard_times.tsv: bad milliseconds in {s}", .{line});
            times.put(b.allocator, line[tab + 1 ..], ms) catch @panic("out of memory");
        }
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => std.debug.panic("cannot read tests/e2e/shard_times.tsv: {t}", .{err}),
    }

    const order = b.allocator.alloc(usize, files.len) catch @panic("out of memory");
    const weight = b.allocator.alloc(u64, files.len) catch @panic("out of memory");
    for (files, 0..) |f, i| {
        order[i] = i;
        weight[i] = times.get(f.stem) orelse default_ms_per_test * f.declared;
    }
    const Ctx = struct {
        files: []const TestFile,
        weight: []const u64,
        fn lessThan(c: @This(), a: usize, z: usize) bool {
            if (c.weight[a] != c.weight[z]) return c.weight[a] > c.weight[z];
            return std.mem.lessThan(u8, c.files[a].path, c.files[z].path);
        }
    };
    std.mem.sort(usize, order, Ctx{ .files = files, .weight = weight }, Ctx.lessThan);

    const load = b.allocator.alloc(u64, count) catch @panic("out of memory");
    @memset(load, 0);
    const shard_of = b.allocator.alloc(usize, files.len) catch @panic("out of memory");
    for (order) |i| {
        var best: usize = 0;
        for (load, 0..) |l, s| if (l < load[best]) {
            best = s;
        };
        shard_of[i] = best;
        load[best] += weight[i];
    }
    return shard_of;
}

const TestFile = struct {
    path: []const u8,
    /// The path as Zig names its tests: no .zig, dots for slashes.
    stem: []const u8,
    declared: usize,
    matched: usize,
};

/// A checkout without a test directory (a source tarball, an image build)
/// still builds the binary; a step that would run those tests fails instead,
/// without building anything first.
fn addTestStep(b: *std.Build, name: []const u8, description: []const u8, runs: []const *std.Build.Step, roots: []const TestRoot, filters: []const []const u8) void {
    const step = b.step(name, description);
    for (roots) |root| if (root.missing) |dir| {
        step.dependOn(&b.addFail(b.fmt("{s} needs {s}, which this checkout doesn't have", .{ name, dir })).step);
        return;
    };
    for (runs) |run| step.dependOn(run);
    failIfNoTestMatched(b, step, name, filters, roots);
}

/// A filtered test step whose roots hold no test matching any filter would
/// run only the nameless tests and pass, so fail it instead, naming the
/// filters.
fn failIfNoTestMatched(b: *std.Build, step: *std.Build.Step, name: []const u8, filters: []const []const u8, roots: []const TestRoot) void {
    if (filters.len == 0) return;
    for (roots) |root| if (root.matched > 0) return;
    var msg: std.Io.Writer.Allocating = .init(b.allocator);
    msg.writer.print("no test in {s} matched", .{name}) catch @panic("out of memory");
    for (filters) |f| msg.writer.print(" -Dtest-filter={s}", .{f}) catch @panic("out of memory");
    msg.writer.writeAll(" (filters are plain substrings of the test name") catch @panic("out of memory");
    if (std.mem.eql(u8, name, "test")) msg.writer.writeAll("; test-e2e and test-integration are separate steps") catch @panic("out of memory");
    msg.writer.writeAll(")") catch @panic("out of memory");
    step.dependOn(&b.addFail(msg.written()).step);
}

/// Write `<dir>/all_tests.zig`, a test root importing every .zig file under
/// `dir` (sorted, skipping `skip` subdirectories and build output) that
/// declares a test, plus a test that fails unless the sentinel two
/// directories down (`test_root_sentinel/nested/sentinel.zig`) was compiled
/// and at least as many tests as the walk counted at the top level.
/// Called at configure time, like the dashboard assets; the file is
/// gitignored. A missing `dir` gives an empty root marked `missing`.
fn generateTestRoot(b: *std.Build, dir: []const u8, skip: []const []const u8, filters: []const []const u8, shard: ?Shard) TestRoot {
    const io = b.graph.io;
    const out_name = "all_tests.zig";
    var root_dir = b.root.root_dir.handle.openDir(io, dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            const stub = b.addWriteFiles().add(out_name, "test {}\n");
            return .{ .source = stub, .options = b.addOptions(), .matched = 0, .missing = dir };
        },
        else => std.debug.panic("cannot open {s} to find its tests: {t}", .{ dir, err }),
    };
    defer root_dir.close(io);

    var found: std.ArrayListUnmanaged(TestFile) = .empty;
    var walker = root_dir.walkSelectively(b.allocator) catch @panic("out of memory");
    defer walker.deinit();
    while (walker.next(io) catch |err| std.debug.panic("cannot walk {s}: {t}", .{ dir, err })) |entry| {
        switch (entry.kind) {
            .directory => {
                const name = entry.basename;
                const skipped = name[0] == '.' or std.mem.eql(u8, name, "zig-out") or
                    (std.mem.indexOfScalar(u8, entry.path, '/') == null and for (skip) |s| {
                        if (std.mem.eql(u8, name, s)) break true;
                    } else false);
                if (!skipped) walker.enter(io, entry) catch |err|
                    std.debug.panic("cannot open {s}/{s}: {t}", .{ dir, entry.path, err });
            },
            .file => {
                if (!std.mem.endsWith(u8, entry.path, ".zig")) continue;
                if (std.mem.eql(u8, entry.path, out_name)) continue;
                const content = root_dir.readFileAlloc(io, entry.path, b.allocator, .unlimited) catch |err|
                    std.debug.panic("cannot read {s}/{s} to find its tests: {t}", .{ dir, entry.path, err });
                if (!hasTestDecl(content)) continue;
                refuseUnpredictableTests(dir, entry.path, content);
                const path = b.dupe(entry.path);
                const stem = b.allocator.dupe(u8, path[0 .. path.len - ".zig".len]) catch @panic("out of memory");
                std.mem.replaceScalar(u8, stem, '/', '.');
                found.append(b.allocator, .{
                    .path = path,
                    .stem = stem,
                    .declared = countTopLevelTestDecls(content),
                    .matched = countMatchingTests(b, entry.path, content, filters),
                }) catch @panic("out of memory");
            },
            else => {},
        }
    }
    std.mem.sort(TestFile, found.items, {}, struct {
        fn lessThan(_: void, a: TestFile, c: TestFile) bool {
            return std.mem.lessThan(u8, a.path, c.path);
        }
    }.lessThan);

    // A shard compiles its share of the files, plus the sentinel the root's
    // own check looks for.
    const sentinel = "test_root_sentinel/nested/sentinel.zig";
    var files: std.ArrayListUnmanaged([]const u8) = .empty;
    var declared: usize = 0;
    var matched: usize = 0;
    var shard_declared: usize = 0;
    var total_declared: usize = 0;
    var spread: std.ArrayListUnmanaged(TestFile) = .empty;
    for (found.items) |f| if (!std.mem.eql(u8, f.path, sentinel)) {
        spread.append(b.allocator, f) catch @panic("out of memory");
    };
    const shard_of = if (shard) |s| assignShards(b, root_dir, spread.items, s.count) else null;
    var next_spread: usize = 0;
    for (found.items) |f| {
        const is_sentinel = std.mem.eql(u8, f.path, sentinel);
        if (!is_sentinel) {
            defer next_spread += 1;
            total_declared += f.declared;
            if (shard_of) |so| if (so[next_spread] != shard.?.index - 1) continue;
            shard_declared += f.declared;
        }
        declared += f.declared;
        matched += f.matched;
        files.append(b.allocator, f.path) catch @panic("out of memory");
    }
    if (shard) |s| if (shard_declared == 0)
        std.process.fatal("-De2e-shard={d}/{d}: this shard has no tests; use fewer shards", .{ s.index, s.count });

    var aw: std.Io.Writer.Allocating = .init(b.allocator);
    const w = &aw.writer;
    w.writeAll(
        \\// AUTO-GENERATED by build.zig from every file under this directory that
        \\// declares a test (`test "…"`, `test {` or `test name {`, at any depth).
        \\// Do not edit; it is rewritten on every build.
        \\
        \\const std = @import("std");
        \\const builtin = @import("builtin");
        \\const test_root = @import("test_root");
        \\
        \\test {
        \\
    ) catch @panic("out of memory");
    for (files.items) |f| w.print("    _ = @import(\"{s}\");\n", .{f}) catch @panic("out of memory");
    w.writeAll(
        \\    // A filter drops named tests at compile time; this nameless test
        \\    // stays. The build predicted from their names which tests the
        \\    // filters keep; a different count here means that prediction, and
        \\    // the step's "no test matched" check built on it, is wrong.
        \\    if (test_root.filters.len > 0) {
        \\        var compiled: usize = 0;
        \\        for (builtin.test_functions) |t| {
        \\            if (std.mem.indexOf(u8, t.name, ".test.") != null) compiled += 1;
        \\        }
        \\        if (compiled != test_root.matched) {
        \\            std.debug.print("the build predicted {d} tests matching the filters here, but {d} were compiled\n", .{ test_root.matched, compiled });
        \\            return error.TestFilterPredictionWrong;
        \\        }
        \\    }
        \\}
        \\
        \\test "every test declared under this directory is compiled" {
        \\    if (test_root.filters.len > 0) return error.SkipZigTest;
        \\    // The sentinel sits two directories down, so a walk that finds
        \\    // nothing, or doesn't descend, fails here: the count below would
        \\    // shrink with it.
        \\    for (builtin.test_functions) |t| {
        \\        if (std.mem.endsWith(u8, t.name, "test_root_sentinel.nested.sentinel.test.test root sentinel")) break;
        \\    } else return error.TestRootSentinelNotCompiled;
        \\    try std.testing.expect(builtin.test_functions.len >= test_root.declared);
        \\}
        \\
    ) catch @panic("out of memory");

    var file = root_dir.createFile(io, out_name, .{}) catch |err|
        std.debug.panic("cannot write {s}/{s}: {t}", .{ dir, out_name, err });
    defer file.close(io);
    var buf: [4096]u8 = undefined;
    var fw = file.writer(io, &buf);
    fw.interface.writeAll(aw.written()) catch |err| std.debug.panic("cannot write {s}/{s}: {t}", .{ dir, out_name, err });
    fw.interface.flush() catch |err| std.debug.panic("cannot write {s}/{s}: {t}", .{ dir, out_name, err });

    const options = b.addOptions();
    options.addOption(usize, "declared", declared);
    options.addOption([]const []const u8, "filters", filters);
    // The root's own named test is compiled when a filter matches its name.
    matched += countMatchingTests(b, out_name, "test \"every test declared under this directory is compiled\" {", filters);
    options.addOption(usize, "matched", matched);
    return .{
        .source = b.path(b.fmt("{s}/{s}", .{ dir, out_name })),
        .options = options,
        .matched = matched,
        .shard = if (shard) |s| .{ .index = s.index, .count = s.count, .declared = shard_declared, .total = total_declared } else null,
    };
}

/// `countMatchingTests` reads a test's full name off its line, which holds
/// only for `test "…"` at the top level: a decltest is named `X.decltest.y`
/// and a nested test carries its container's name, so a filter could match
/// one without the build knowing. Refuse both rather than miscount.
fn refuseUnpredictableTests(dir: []const u8, path: []const u8, content: []const u8) void {
    var lines = std.mem.splitScalar(u8, content, '\n');
    var line_no: usize = 0;
    while (lines.next()) |line| {
        line_no += 1;
        const trimmed = std.mem.trimStart(u8, line, " \t");
        if (!isTestDecl(trimmed)) continue;
        const rest = trimmed["test ".len..];
        if (rest[0] != '"' and rest[0] != '{')
            std.process.fatal("{s}/{s}:{d}: a decltest's name can't be predicted for -Dtest-filter; write it as test \"…\"", .{ dir, path, line_no });
        if (rest[0] == '"' and trimmed.len != line.len)
            std.process.fatal("{s}/{s}:{d}: a test nested in a container can't be predicted for -Dtest-filter; move it to the top level", .{ dir, path, line_no });
    }
}

/// Named tests in `content` whose full name contains one of `filters`. Zig
/// names a test `<path, with . for / and no .zig>.test.<its string>` and
/// filters by substring of that; `refuseUnpredictableTests` keeps decltests
/// and nested tests out, so the name is fully known from the line.
fn countMatchingTests(b: *std.Build, path: []const u8, content: []const u8, filters: []const []const u8) usize {
    if (filters.len == 0) return 0;
    const stem = b.allocator.dupe(u8, path[0 .. path.len - ".zig".len]) catch @panic("out of memory");
    std.mem.replaceScalar(u8, stem, '/', '.');
    var n: usize = 0;
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "test \"")) continue;
        var str: std.ArrayListUnmanaged(u8) = .empty;
        var i: usize = "test \"".len;
        while (i < line.len and line[i] != '"') : (i += 1) {
            if (line[i] == '\\' and i + 1 < line.len) i += 1;
            str.append(b.allocator, line[i]) catch @panic("out of memory");
        }
        const name = b.fmt("{s}.test.{s}", .{ stem, str.items });
        for (filters) |f| if (std.mem.indexOf(u8, name, f) != null) {
            n += 1;
            break;
        };
    }
    return n;
}

/// Whether `content` declares a test anywhere: `test "…"`, `test {` or a
/// decltest `test name {`, at the top level or nested in a container.
fn hasTestDecl(content: []const u8) bool {
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |raw| {
        if (isTestDecl(std.mem.trimStart(u8, raw, " \t"))) return true;
    }
    return false;
}

/// Test declarations at the top level of `content`. One nested in a
/// container isn't counted, so this is a lower bound on what the file
/// compiles.
fn countTopLevelTestDecls(content: []const u8) usize {
    var n: usize = 0;
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (isTestDecl(line)) n += 1;
    }
    return n;
}

fn isTestDecl(line: []const u8) bool {
    if (!std.mem.startsWith(u8, line, "test ")) return false;
    const rest = line["test ".len..];
    return rest.len > 0 and (rest[0] == '"' or rest[0] == '{' or std.ascii.isAlphabetic(rest[0]) or rest[0] == '_');
}

// ── Dashboard Asset Embedding ──

/// Generate a Zig module that embeds all files from src/node/dashboard/dist.
/// Called at build-configure time; the resulting assets.zig is @import'd
/// by http_server.zig via relative path.
fn generateDashboardAssetsModule(b: *std.Build) *std.Build.Module {
    var aw: std.Io.Writer.Allocating = .init(b.allocator);
    defer aw.deinit();
    const writer = &aw.writer;

    // Module header
    writer.writeAll(
        \\// AUTO-GENERATED FILE. DO NOT EDIT.
        \\// Generated by build.zig from the contents of dist/.
        \\// To update, rebuild the web dashboard first.
        \\const std = @import("std");
        \\
        \\pub const Asset = struct {
        \\    content: []const u8,
        \\    mime_type: []const u8,
        \\    /// The same bytes gzipped, served to a client that accepts gzip.
        \\    gzip: ?[]const u8 = null,
        \\};
        \\
        \\
    ) catch unreachable;

    var entries: std.ArrayListUnmanaged([]const u8) = .empty;
    var kept_gz: std.ArrayListUnmanaged([]const u8) = .empty;
    var asset_idx: usize = 0;
    var precompressed: usize = 0;

    const dist_path = "src/node/dashboard/dist";
    const io = b.graph.io;
    var dist_dir = b.root.root_dir.handle.openDir(io, dist_path, .{ .iterate = true }) catch |err| {
        std.debug.print("Warning: Cannot open {s}: {}. Run 'zake build.dashboard' first.\n", .{ dist_path, err });
        // Return a minimal stub so compilation succeeds without assets.
        writer.writeAll(
            \\pub fn get(path: []const u8) ?Asset {
            \\    _ = path;
            \\    return null;
            \\}
            \\
        ) catch unreachable;
        writeAssetsFile(b, aw.written());
        return b.createModule(.{ .root_source_file = b.path("src/node/dashboard/assets.zig") });
    };
    defer dist_dir.close(io);

    // The assets are listed in full before any copy is written: writing into
    // dist while walking it could hand the walk its own new files.
    var asset_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    {
        var walker = dist_dir.walk(b.allocator) catch unreachable;
        defer walker.deinit();
        while (walker.next(io) catch null) |entry| {
            if (entry.kind != .file) continue;
            // Source maps, and gzipped copies (a killed build's .tmp too).
            if (std.mem.endsWith(u8, entry.path, ".map")) continue;
            if (std.mem.endsWith(u8, entry.path, ".gz") or std.mem.endsWith(u8, entry.path, ".gz.tmp")) continue;
            asset_paths.append(b.allocator, b.dupe(entry.path)) catch unreachable;
        }
    }
    std.mem.sort([]const u8, asset_paths.items, {}, struct {
        fn lessThan(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lessThan);

    for (asset_paths.items) |file_path| {
        const mime = getMimeType(file_path);
        const var_name = b.fmt("asset_{d}", .{asset_idx});
        const compressible = isCompressible(file_path);

        writer.print("const {s} = @embedFile(\"dist/{s}\");\n", .{ var_name, file_path }) catch unreachable;
        var is_precompressed: u8 = 0;
        if (compressible) {
            // Embed a gzipped copy too, for clients that accept gzip. If it
            // can't be made only the raw bytes go in; release.yml refuses a
            // binary whose assets have no compressed copy.
            if (gzipFile(b, dist_dir, file_path)) |gz_path| {
                writer.print("const {s}_gz = @embedFile(\"dist/{s}\");\n", .{ var_name, gz_path }) catch unreachable;
                kept_gz.append(b.allocator, gz_path) catch unreachable;
                is_precompressed = 1;
                precompressed += 1;
            }
        }

        const entry_line = b.fmt("{s}\x00{s}\x00{s}\x00{d}", .{ file_path, var_name, mime, is_precompressed });
        entries.append(b.allocator, entry_line) catch unreachable;
        asset_idx += 1;
    }
    deleteStaleGz(b, dist_dir, kept_gz.items);

    // get() function with if-else chain (Zig has no string switch)
    writer.writeAll(
        \\
        \\pub fn get(path: []const u8) ?Asset {
        \\    const mem = @import("std").mem;
        \\    const normalized = if (path.len > 0 and path[0] == '/') path[1..] else path;
        \\    const lookup = if (normalized.len == 0) "index.html" else normalized;
        \\
        \\
    ) catch unreachable;

    var first = true;
    for (entries.items) |raw| {
        var it = std.mem.splitScalar(u8, raw, 0);
        const epath = it.next().?;
        const vname = it.next().?;
        const emime = it.next().?;
        const compressed = it.next().?;
        const is_gz = std.mem.eql(u8, compressed, "1");

        if (first) {
            writer.print("    if (mem.eql(u8, lookup, \"{s}\")) {{\n", .{epath}) catch unreachable;
            first = false;
        } else {
            writer.print("    }} else if (mem.eql(u8, lookup, \"{s}\")) {{\n", .{epath}) catch unreachable;
        }
        writer.print("        return {s};\n", .{assetLiteral(b, vname, emime, is_gz)}) catch unreachable;
    }

    // SPA fallback — serve index.html for non-API paths
    if (entries.items.len > 0) {
        writer.writeAll(
            \\    } else {
            \\        if (!mem.startsWith(u8, lookup, "api/")) {
            \\
        ) catch unreachable;

        for (entries.items) |raw| {
            var it = std.mem.splitScalar(u8, raw, 0);
            const epath = it.next().?;
            const vname = it.next().?;
            const emime = it.next().?;
            const compressed = it.next().?;
            const is_gz = std.mem.eql(u8, compressed, "1");
            if (std.mem.eql(u8, epath, "index.html")) {
                writer.print("            return {s};\n", .{assetLiteral(b, vname, emime, is_gz)}) catch unreachable;
                break;
            }
        }

        writer.writeAll(
            \\        }
            \\        return null;
            \\    }
            \\}
            \\
        ) catch unreachable;
    } else {
        writer.writeAll(
            \\    return null;
            \\}
            \\
        ) catch unreachable;
    }

    writeAssetsFile(b, aw.written());
    std.debug.print("Dashboard: Embedded {d} assets from {s}, {d} precompressed\n", .{ asset_idx, dist_path, precompressed });

    return b.createModule(.{ .root_source_file = b.path("src/node/dashboard/assets.zig") });
}

fn assetLiteral(b: *std.Build, var_name: []const u8, mime: []const u8, gzipped: bool) []const u8 {
    if (gzipped) return b.fmt(".{{ .content = {s}, .mime_type = \"{s}\", .gzip = {s}_gz }}", .{ var_name, mime, var_name });
    return b.fmt(".{{ .content = {s}, .mime_type = \"{s}\" }}", .{ var_name, mime });
}

fn writeAssetsFile(b: *std.Build, content: []const u8) void {
    const assets_path = "src/node/dashboard/assets.zig";
    const io = b.graph.io;
    var file = b.root.root_dir.handle.createFile(io, assets_path, .{}) catch unreachable;
    defer file.close(io);
    var buf: [4096]u8 = undefined;
    var fw = file.writer(io, &buf);
    fw.interface.writeAll(content) catch unreachable;
    fw.interface.flush() catch unreachable;
}

fn getMimeType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".ico")) return "image/x-icon";
    if (std.mem.endsWith(u8, path, ".woff")) return "font/woff";
    if (std.mem.endsWith(u8, path, ".woff2")) return "font/woff2";
    if (std.mem.endsWith(u8, path, ".map")) return "application/json";
    return "application/octet-stream";
}

fn isCompressible(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".js") or
        std.mem.endsWith(u8, path, ".css") or
        std.mem.endsWith(u8, path, ".html") or
        std.mem.endsWith(u8, path, ".svg") or
        std.mem.endsWith(u8, path, ".json");
}

/// The gzipped copy of `src_path`, made in-process and named by a hash of
/// the source bytes: `<asset>.<hash>.gz`. A copy that exists is the copy of
/// exactly these bytes, whatever the files' mtimes say after a checkout, so
/// it is reused; otherwise it is written. The header has no name or mtime,
/// so a commit always gives the same binary. Null, with a warning, if it
/// can't be made.
fn gzipFile(b: *std.Build, dir: std.Io.Dir, src_path: []const u8) ?[]const u8 {
    const io = b.graph.io;
    const data = dir.readFileAlloc(io, src_path, b.allocator, .unlimited) catch |err| {
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: {t}\n", .{ src_path, err });
        return null;
    };
    // The seed names the compressor and its level: changing either makes
    // every name new.
    const gz_path = b.fmt("{s}.{x:0>16}.gz", .{ src_path, std.hash.Wyhash.hash(0x67_7a_62_65_73_74, data) });
    if (dir.access(io, gz_path, .{})) |_| return gz_path else |_| {}

    var out: std.Io.Writer.Allocating = std.Io.Writer.Allocating.initCapacity(b.allocator, data.len / 2 + 64) catch @panic("out of memory");
    const window = b.allocator.alloc(u8, std.compress.flate.max_window_len) catch @panic("out of memory");
    var c = std.compress.flate.Compress.init(&out.writer, window, .gzip, .best) catch |err| {
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: gzip: {t}\n", .{ src_path, err });
        return null;
    };
    c.writer.writeAll(data) catch |err| {
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: gzip: {t}\n", .{ src_path, err });
        return null;
    };
    c.finish() catch |err| {
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: gzip: {t}\n", .{ src_path, err });
        return null;
    };
    // Written under a temporary name and renamed, so a killed build never
    // leaves a truncated copy under the real one.
    const tmp_path = b.fmt("{s}.tmp", .{gz_path});
    dir.writeFile(io, .{ .sub_path = tmp_path, .data = out.written() }) catch |err| {
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: {t}\n", .{ src_path, err });
        return null;
    };
    dir.rename(tmp_path, dir, gz_path, io) catch |err| {
        dir.deleteFile(io, tmp_path) catch {};
        std.debug.print("Warning: dashboard asset {s} embedded uncompressed: {t}\n", .{ src_path, err });
        return null;
    };
    return gz_path;
}

/// Remove the .gz files no asset uses any more: copies of older sources, and
/// any left by the earlier `<asset>.gz` naming.
fn deleteStaleGz(b: *std.Build, dir: std.Io.Dir, kept: []const []const u8) void {
    const io = b.graph.io;
    var stale: std.ArrayListUnmanaged([]const u8) = .empty;
    var walker = dir.walk(b.allocator) catch return;
    defer walker.deinit();
    while (walker.next(io) catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".gz") and !std.mem.endsWith(u8, entry.path, ".gz.tmp")) continue;
        for (kept) |k| {
            if (std.mem.eql(u8, k, entry.path)) break;
        } else stale.append(b.allocator, b.dupe(entry.path)) catch return;
    }
    for (stale.items) |path| dir.deleteFile(io, path) catch {};
}

/// `-Dversion`, or else `git describe` without the leading `v`. A build
/// where git can't say (no git, not a checkout) warns and is "dev";
/// release.yml checks that a release binary reports its tag.
fn getGitVersion(b: *std.Build) []const u8 {
    const version_override = b.option([]const u8, "version", "Override version string");
    if (version_override) |v| return v;

    const result = std.process.run(b.allocator, b.graph.io, .{
        .argv = &.{ "git", "describe", "--tags", "--always", "--dirty" },
    }) catch |err| {
        std.debug.print("Warning: version is \"dev\": can't run git describe: {t}\n", .{err});
        return "dev";
    };
    const out = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    if (result.term != .exited or result.term.exited != 0 or out.len == 0) {
        std.debug.print("Warning: version is \"dev\": git describe: {s}\n", .{std.mem.trim(u8, result.stderr, &std.ascii.whitespace)});
        return "dev";
    }
    return if (out[0] == 'v') out[1..] else out;
}
