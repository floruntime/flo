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
    _ = @import("storage/ual/entry.zig");
    _ = @import("storage/ual/ual.zig");
    _ = @import("storage/ual/segment.zig");
    _ = @import("storage/ual/writer.zig");
    _ = @import("storage/ual/reader.zig");
    _ = @import("storage/snapshot.zig");
    _ = @import("storage/memory.zig");
    _ = @import("storage/partition.zig");
}

// Raft consensus — Phase 3
test {
    _ = @import("raft/log.zig");
    _ = @import("raft/node.zig");
    _ = @import("raft/election.zig");
    _ = @import("raft/replication.zig");
    _ = @import("raft/transport.zig");
}
