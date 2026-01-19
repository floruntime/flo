const std = @import("std");

pub fn build(b: *std.Build) void {
    const lib = b.addExecutable(.{
        .name = "txn_classifier",
        .root_module = b.createModule(.{
            .root_source_file = b.path("txn_classifier.zig"),
            .target = b.resolveTargetQuery(.{
                .cpu_arch = .wasm32,
                .os_tag = .freestanding,
            }),
            .optimize = .ReleaseSmall,
        }),
    });

    lib.entry = .disabled;
    lib.rdynamic = true;

    b.installArtifact(lib);

    const build_step = b.step("wasm", "Build the transaction classifier WASM module");
    build_step.dependOn(&lib.step);
}
