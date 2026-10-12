const std = @import("std");

// Another compiler fails on this file with a wall of unrelated-looking errors.
comptime {
    const v = @import("builtin").zig_version;
    if (v.major != 0 or v.minor != 17)
        @compileError(std.fmt.comptimePrint("flo needs Zig 0.17.x, found {d}.{d}.{d}", .{ v.major, v.minor, v.patch }));
}

/// A single-threaded `std.Io` instance used at build configure time for
/// `std.process.run`, `std.Io.Dir`, etc. Constructed once per build.zig run.
var build_threaded_io: std.Io.Threaded = .init_single_threaded;

fn buildIo() std.Io {
    return build_threaded_io.io();
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
    const unit_root = generateTestRoot(b, "src", &.{"stdx"}, test_filters);
    const stdx_root = generateTestRoot(b, "src/stdx", &.{}, test_filters);
    const e2e_root = generateTestRoot(b, "tests/e2e", &.{}, test_filters);
    const integration_root = generateTestRoot(b, "tests/integration", &.{}, test_filters);

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
    });
    e2e_tests.root_module.addImport("src", src_module);
    e2e_tests.root_module.addImport("stdx", stdx_module);
    e2e_tests.root_module.addOptions("build_options", build_options);
    e2e_tests.root_module.addOptions("test_root", e2e_root.options);

    const run_e2e_tests = b.addRunArtifact(e2e_tests);
    run_e2e_tests.step.dependOn(b.getInstallStep());

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

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_stdx_tests.step);
    failIfNoTestMatched(b, test_step, "test", test_filters, &.{ unit_root, stdx_root });
    failIfMissing(b, test_step, "test", &.{ unit_root, stdx_root });

    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_unit_tests.step);
    unit_test_step.dependOn(&run_stdx_tests.step);
    failIfNoTestMatched(b, unit_test_step, "test-unit", test_filters, &.{ unit_root, stdx_root });
    failIfMissing(b, unit_test_step, "test-unit", &.{ unit_root, stdx_root });

    const e2e_test_step = b.step("test-e2e", "Run end-to-end tests");
    e2e_test_step.dependOn(&run_e2e_tests.step);
    failIfNoTestMatched(b, e2e_test_step, "test-e2e", test_filters, &.{e2e_root});
    failIfMissing(b, e2e_test_step, "test-e2e", &.{e2e_root});

    const integration_test_step = b.step("test-integration", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);
    failIfNoTestMatched(b, integration_test_step, "test-integration", test_filters, &.{integration_root});
    failIfMissing(b, integration_test_step, "test-integration", &.{integration_root});

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
};

/// A checkout without a test directory (a source tarball, an image build)
/// still builds the binary; a step that would run those tests fails instead.
fn failIfMissing(b: *std.Build, step: *std.Build.Step, name: []const u8, roots: []const TestRoot) void {
    for (roots) |root| if (root.missing) |dir|
        step.dependOn(&b.addFail(b.fmt("{s} needs {s}, which this checkout doesn't have", .{ name, dir })).step);
}

/// A filtered test step whose roots hold no test matching any filter would
/// run only the nameless tests and pass, so fail it instead, naming the
/// filters.
fn failIfNoTestMatched(b: *std.Build, step: *std.Build.Step, name: []const u8, filters: []const []const u8, roots: []const TestRoot) void {
    if (filters.len == 0) return;
    for (roots) |root| if (root.matched > 0 or root.missing != null) return;
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
fn generateTestRoot(b: *std.Build, dir: []const u8, skip: []const []const u8, filters: []const []const u8) TestRoot {
    const io = buildIo();
    const out_name = "all_tests.zig";
    var root_dir = b.root.root_dir.handle.openDir(io, dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            const options = b.addOptions();
            options.addOption(usize, "declared", 0);
            options.addOption([]const []const u8, "filters", filters);
            options.addOption(usize, "matched", 0);
            const stub = b.addWriteFiles().add(out_name, "test {}\n");
            return .{ .source = stub, .options = options, .matched = 0, .missing = dir };
        },
        else => std.debug.panic("cannot open {s} to find its tests: {t}", .{ dir, err }),
    };
    defer root_dir.close(io);

    var files: std.ArrayListUnmanaged([]const u8) = .empty;
    var declared: usize = 0;
    var matched: usize = 0;
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
                declared += countTopLevelTestDecls(content);
                matched += countMatchingTests(b, entry.path, content, filters);
                files.append(b.allocator, b.dupe(entry.path)) catch @panic("out of memory");
            },
            else => {},
        }
    }
    std.mem.sort([]const u8, files.items, {}, struct {
        fn lessThan(_: void, a: []const u8, c: []const u8) bool {
            return std.mem.lessThan(u8, a, c);
        }
    }.lessThan);

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
    return .{ .source = b.path(b.fmt("{s}/{s}", .{ dir, out_name })), .options = options, .matched = matched };
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
        \\    is_precompressed: bool = false,
        \\};
        \\
        \\
    ) catch unreachable;

    var entries: std.ArrayListUnmanaged([]const u8) = .empty;
    var asset_idx: usize = 0;

    const dist_path = "src/node/dashboard/dist";
    const io = buildIo();
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

    var walker = dist_dir.walk(b.allocator) catch unreachable;
    defer walker.deinit();

    while (walker.next(io) catch null) |entry| {
        if (entry.kind != .file) continue;
        const file_path = entry.path;
        // Skip source maps and previously-generated .gz files
        if (std.mem.endsWith(u8, file_path, ".map")) continue;
        if (std.mem.endsWith(u8, file_path, ".gz")) continue;
        const mime = getMimeType(file_path);
        const var_name = b.fmt("asset_{d}", .{asset_idx});
        const compressible = isCompressible(file_path);

        var is_precompressed: u8 = 0;
        if (compressible) {
            // Gzip the file at configure time and embed the compressed version.
            // Fall back to raw embed if gzip is not available.
            const gz_path = b.fmt("{s}.gz", .{file_path});
            if (gzipFile(b.allocator, dist_dir, file_path, gz_path)) {
                writer.print("const {s} = @embedFile(\"dist/{s}\");\n", .{
                    var_name, gz_path,
                }) catch unreachable;
                is_precompressed = 1;
            } else {
                writer.print("const {s} = @embedFile(\"dist/{s}\");\n", .{
                    var_name, file_path,
                }) catch unreachable;
            }
        } else {
            writer.print("const {s} = @embedFile(\"dist/{s}\");\n", .{
                var_name, file_path,
            }) catch unreachable;
        }

        const entry_line = b.fmt("{s}\x00{s}\x00{s}\x00{d}", .{ file_path, var_name, mime, is_precompressed });
        entries.append(b.allocator, entry_line) catch unreachable;
        asset_idx += 1;
    }

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
        writer.print("        return .{{ .content = {s}, .mime_type = \"{s}\", .is_precompressed = {s} }};\n", .{ vname, emime, if (is_gz) "true" else "false" }) catch unreachable;
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
                writer.print("            return .{{ .content = {s}, .mime_type = \"{s}\", .is_precompressed = {s} }};\n", .{ vname, emime, if (is_gz) "true" else "false" }) catch unreachable;
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
    std.debug.print("Dashboard: Embedded {d} assets from {s}\n", .{ asset_idx, dist_path });

    return b.createModule(.{ .root_source_file = b.path("src/node/dashboard/assets.zig") });
}

fn writeAssetsFile(b: *std.Build, content: []const u8) void {
    const assets_path = "src/node/dashboard/assets.zig";
    const io = buildIo();
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

fn gzipFile(_: std.mem.Allocator, dir: std.Io.Dir, src_path: []const u8, dest_path: []const u8) bool {
    const io = buildIo();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path_len = dir.realPath(io, &path_buf) catch return false;
    const dir_path = path_buf[0..dir_path_len];

    const abs_src = std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ dir_path, src_path }) catch return false;
    defer std.heap.page_allocator.free(abs_src);

    const abs_dest = std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ dir_path, dest_path }) catch return false;
    defer std.heap.page_allocator.free(abs_dest);

    const cmd = std.fmt.allocPrint(std.heap.page_allocator, "gzip -9 -c \"{s}\" > \"{s}\"", .{ abs_src, abs_dest }) catch return false;
    defer std.heap.page_allocator.free(cmd);

    _ = std.process.run(std.heap.page_allocator, buildIo(), .{
        .argv = &.{ "sh", "-c", cmd },
    }) catch return false;
    return true;
}

fn getGitVersion(b: *std.Build) []const u8 {
    const version_override = b.option([]const u8, "version", "Override version string");
    if (version_override) |v| return v;

    const result = std.process.run(b.allocator, buildIo(), .{
        .argv = &.{ "git", "describe", "--tags", "--always", "--dirty" },
    }) catch return "dev";

    const out = std.mem.trim(u8, result.stdout, &std.ascii.whitespace);
    if (out.len == 0) return "dev";

    // Strip leading 'v' prefix (e.g. "v0.1.0-dev.3" → "0.1.0-dev.3")
    return if (out[0] == 'v') out[1..] else out;
}
