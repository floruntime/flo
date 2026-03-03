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
