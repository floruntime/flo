// Test root file that imports all modules with tests
// This ensures all tests are discovered by `zig build test`

// Logging - structured logging with multiple formats
test {
    _ = @import("stdx");
}

// Core utilities
test {
    _ = @import("util/checksum.zig");
    _ = @import("util/json.zig");
    _ = @import("util/validation.zig");
}

// Metrics
test {
    _ = @import("metrics/registry.zig");
}

// Protocol
test {
    _ = @import("protocol/proto.zig");
}

// Node layer — Phase 1
test {
    _ = @import("node/reactor.zig");
    _ = @import("node/inbox.zig");
    _ = @import("node/slab.zig");
    _ = @import("node/router.zig");
    _ = @import("node/dispatcher.zig");
    _ = @import("node/connection.zig");
    _ = @import("node/shard.zig");
    _ = @import("node/acceptor.zig");
    _ = @import("node/shard_walker.zig");
    _ = @import("node/task_scheduler.zig");
}
