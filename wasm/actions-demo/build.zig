const std = @import("std");

pub fn build(b: *std.Build) void {
    // Build the rules engine WASM module
    const lib = b.addExecutable(.{
        .name = "rules_engine",
        .root_module = b.createModule(.{
            .root_source_file = b.path("rules_engine.zig"),
            .target = b.resolveTargetQuery(.{
                .cpu_arch = .wasm32,
                .os_tag = .freestanding,
            }),
            .optimize = .ReleaseSmall,
        }),
    });

    // Export the ABI functions
    lib.entry = .disabled;
    lib.rdynamic = true;

    b.installArtifact(lib);

    // Add a run step for convenience
    const build_step = b.step("wasm", "Build the rules engine WASM module");
    build_step.dependOn(&lib.step);
}
