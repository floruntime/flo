const std = @import("std");

/// A single-threaded `std.Io` instance used at build configure time for
/// `std.process.run`, `std.Io.Dir`, etc. Constructed once per build.zig run.
var build_threaded_io: std.Io.Threaded = .init_single_threaded;

fn buildIo() std.Io {
    return build_threaded_io.io();
}

pub fn build(b: *std.Build) void {
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
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // ── Install to System PATH ──

    const install_dir_option = b.option([]const u8, "install_dir", "Directory to install flo binary");
    const default_install_dir = if (b.graph.host.result.os.tag == .macos) "/opt/homebrew/bin" else "/usr/local/bin";
    const install_dir = install_dir_option orelse default_install_dir;

    const system_install_step = b.step("install-system", "Install flo to system PATH directory (default: /usr/local/bin on Linux, /opt/homebrew/bin on macOS)");
    const install_cmd = b.addSystemCommand(&.{ "sh", "-c" });
    const build_root_str = b.pathFromRoot("zig-out/bin/flo");
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

    const test_filter = b.option([]const u8, "test-filter", "Filter tests by name pattern");

    // ── Unit Tests ──

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    unit_tests.root_module.addImport("src", src_module);
    unit_tests.root_module.addImport("stdx", stdx_module);
    unit_tests.root_module.addOptions("build_options", build_options);

    const run_unit_tests = b.addRunArtifact(unit_tests);

    // stdx is its own module, and module dependencies contribute no tests to
    // a test root — everything in stdx (PRNG, log, helpers) was invisible to
    // the src test runner and had never run. Compile stdx as its own root.
    const stdx_tests = b.addTest(.{
        .root_module = stdx_module,
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    const run_stdx_tests = b.addRunArtifact(stdx_tests);

    // ── E2E Tests ──

    const e2e_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/mod.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    e2e_tests.root_module.addImport("src", src_module);
    e2e_tests.root_module.addImport("stdx", stdx_module);
    e2e_tests.root_module.addOptions("build_options", build_options);

    const run_e2e_tests = b.addRunArtifact(e2e_tests);
    run_e2e_tests.step.dependOn(b.getInstallStep());

    // ── Integration Tests ──

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration/mod.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    integration_tests.root_module.addImport("src", src_module);
    integration_tests.root_module.addImport("stdx", stdx_module);
    integration_tests.root_module.addOptions("build_options", build_options);

    const run_integration_tests = b.addRunArtifact(integration_tests);

    // ── Test Steps ──

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_stdx_tests.step);

    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_unit_tests.step);
    unit_test_step.dependOn(&run_stdx_tests.step);

    const e2e_test_step = b.step("test-e2e", "Run end-to-end tests");
    e2e_test_step.dependOn(&run_e2e_tests.step);

    const integration_test_step = b.step("test-integration", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);

    // ── Benchmarks ──

    const bench_step = b.step("bench", "Build all benchmarks");

    const bench_sources = [_]struct { name: []const u8, source: []const u8 }{
        .{ .name = "bench-ual", .source = "bench/bench_ual.zig" },
        .{ .name = "bench-kv", .source = "bench/bench_kv.zig" },
        .{ .name = "bench-inbox", .source = "bench/bench_inbox.zig" },
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
    var dist_dir = std.Io.Dir.cwd().openDir(io, dist_path, .{ .iterate = true }) catch |err| {
        std.debug.print("Warning: Cannot open {s}: {}. Run 'zake build.dashboard' first.\n", .{ dist_path, err });
        // Return a minimal stub so compilation succeeds without assets.
        writer.writeAll(
            \\pub fn get(path: []const u8) ?Asset {
            \\    _ = path;
            \\    return null;
            \\}
        ) catch unreachable;
        writeAssetsFile(aw.written());
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

    writeAssetsFile(aw.written());
    std.debug.print("Dashboard: Embedded {d} assets from {s}\n", .{ asset_idx, dist_path });

    return b.createModule(.{ .root_source_file = b.path("src/node/dashboard/assets.zig") });
}

fn writeAssetsFile(content: []const u8) void {
    const assets_path = "src/node/dashboard/assets.zig";
    const io = buildIo();
    var file = std.Io.Dir.cwd().createFile(io, assets_path, .{}) catch unreachable;
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
