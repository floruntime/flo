const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSafe });

    // ── Modules ──

    // stdx: standard library extensions (logging, etc.)
    const stdx_module = b.createModule(.{
        .root_source_file = b.path("src/stdx.zig"),
        .target = target,
        .optimize = optimize,
    });

    // zware: WebAssembly runtime (for WASM actions)
    const zware_dep = b.dependency("zware", .{
        .target = target,
        .optimize = optimize,
    });
    const zware_module = zware_dep.module("zware");

    // ── Main Executable ──

    const exe = b.addExecutable(.{
        .name = "flo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("stdx", stdx_module);
    exe.root_module.addImport("zware", zware_module);
    exe.linkLibC();

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
    src_module.addImport("zware", zware_module);

    // ── Test Filter ──

    const test_filter = b.option([]const u8, "test-filter", "Filter tests by name pattern");

    // ── Unit Tests ──

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/test.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    unit_tests.root_module.addImport("src", src_module);
    unit_tests.root_module.addImport("stdx", stdx_module);
    unit_tests.root_module.addImport("zware", zware_module);
    unit_tests.linkLibC();

    const run_unit_tests = b.addRunArtifact(unit_tests);

    // ── E2E Tests ──

    const e2e_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/mod.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    e2e_tests.root_module.addImport("src", src_module);
    e2e_tests.root_module.addImport("stdx", stdx_module);
    e2e_tests.root_module.addImport("zware", zware_module);
    e2e_tests.linkLibC();

    const run_e2e_tests = b.addRunArtifact(e2e_tests);
    run_e2e_tests.step.dependOn(b.getInstallStep());

    // ── Integration Tests ──

    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration/mod.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = if (test_filter) |f| &.{f} else &.{},
    });
    integration_tests.root_module.addImport("src", src_module);
    integration_tests.root_module.addImport("stdx", stdx_module);
    integration_tests.root_module.addImport("zware", zware_module);
    integration_tests.linkLibC();

    const run_integration_tests = b.addRunArtifact(integration_tests);

    // ── Test Steps ──

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);

    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_unit_tests.step);

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
            }),
        });
        bench_exe.root_module.addImport("src", src_module);
        bench_exe.root_module.addImport("stdx", stdx_module);
        bench_exe.linkLibC();
        b.installArtifact(bench_exe);
        bench_step.dependOn(&bench_exe.step);
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
