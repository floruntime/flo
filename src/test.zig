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
    _ = @import("protocol/resp.zig");
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
    _ = @import("node/ws_handler.zig");
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
    _ = @import("raft/snapshot.zig");
}

// Projections — Phase 4
test {
    _ = @import("projection/router.zig");
    _ = @import("projection/kv.zig");
    _ = @import("projection/queue.zig");
    _ = @import("projection/stream.zig");
    _ = @import("projection/ts.zig");
}

// Handlers — Phase 5
test {
    _ = @import("kv/handler.zig");
    _ = @import("stream/handler.zig");
    _ = @import("queue/handler.zig");
    _ = @import("ts/handler.zig");
    _ = @import("namespace/handler.zig");
    _ = @import("actions/handler.zig");
}

// Dashboard — Phase 6.1
test {
    _ = @import("node/dashboard/api/helpers.zig");
    _ = @import("node/dashboard/api/system.zig");
    _ = @import("node/dashboard/api/queues.zig");
    _ = @import("node/dashboard/api/kv.zig");
    _ = @import("node/dashboard/api/streams.zig");
    _ = @import("node/dashboard/api/namespaces.zig");
    _ = @import("node/dashboard/api/timeseries.zig");
    _ = @import("node/dashboard/api/actions.zig");
    _ = @import("node/dashboard/api/workflows.zig");
    _ = @import("node/dashboard/api/processing.zig");
    _ = @import("node/dashboard/api.zig");
    _ = @import("node/dashboard/http_server.zig");
}

// Cluster — Phase 7
test {
    _ = @import("cluster/coordinator.zig");
}
