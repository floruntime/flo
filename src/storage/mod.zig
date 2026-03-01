// Storage layer — UAL, Partition, Snapshot Manager, Memory Controller, Cold Storage

pub const ual = struct {
    pub const ual = @import("ual/ual.zig");
    pub const entry = @import("ual/entry.zig");
    pub const segment = @import("ual/segment.zig");
    pub const writer = @import("ual/writer.zig");
    pub const reader = @import("ual/reader.zig");
};

pub const cold = @import("cold/mod.zig");

/// Convenience alias — cold manifest is part of the cold module.
pub const cold_manifest = cold.manifest;

pub const partition = @import("partition.zig");
pub const snapshot = @import("snapshot.zig");
pub const memory = @import("memory.zig");
