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

// Node layer
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
    _ = @import("node/network/jwt.zig");
    _ = @import("node/network/jwks.zig");
    _ = @import("node/manifest.zig");
    _ = @import("node/shard_manifest.zig");
    _ = @import("storage/ual/entry.zig");
    _ = @import("storage/ual/ual.zig");
    _ = @import("storage/ual/segment.zig");
    _ = @import("storage/ual/writer.zig");
    _ = @import("storage/ual/reader.zig");
    _ = @import("storage/snapshot.zig");
    _ = @import("storage/memory.zig");
    _ = @import("storage/partition.zig");
    _ = @import("storage/cold/manifest.zig");
    _ = @import("storage/cold/backend.zig");
    _ = @import("storage/cold/noop.zig");
    _ = @import("storage/cold/file.zig");
    _ = @import("storage/cold/http_client.zig");
    _ = @import("storage/cold/http_client_tls.zig");
    _ = @import("storage/cold/aws_sigv4.zig");
    _ = @import("storage/cold/s3.zig");
    _ = @import("storage/cold/azure.zig");
    _ = @import("storage/cold/mod.zig");
    _ = @import("storage/cold/tier_manager.zig");
}

// Raft consensus
test {
    _ = @import("raft/log.zig");
    _ = @import("raft/node.zig");
    _ = @import("raft/election.zig");
    _ = @import("raft/replication.zig");
    _ = @import("raft/transport.zig");
    _ = @import("raft/snapshot.zig");
}

// Projections
test {
    _ = @import("projection/router.zig");
    _ = @import("projection/kv.zig");
    _ = @import("projection/queue.zig");
    _ = @import("projection/stream.zig");
    _ = @import("projection/ts.zig");
}

// Handlers
test {
    _ = @import("kv/handler.zig");
    _ = @import("stream/handler.zig");
    _ = @import("queue/handler.zig");
    _ = @import("ts/handler.zig");
    _ = @import("namespace/handler.zig");
    _ = @import("actions/handler.zig");
    _ = @import("workflow/handler.zig");
    _ = @import("processing/handler.zig");
    _ = @import("processing/operator.zig");
    _ = @import("processing/collector.zig");
    _ = @import("processing/context.zig");
    // Processing: topology & chain
    _ = @import("processing/topology.zig");
    _ = @import("processing/chain.zig");
    // Processing: endpoints
    _ = @import("processing/endpoints/source.zig");
    _ = @import("processing/endpoints/sink.zig");
    // Processing: keyed state
    _ = @import("processing/state.zig");
    // Processing: declarative operators
    _ = @import("processing/operators/expr_filter.zig");
    _ = @import("processing/operators/passthrough.zig");
    _ = @import("processing/operators/json_keyby.zig");
    _ = @import("processing/operators/json_aggregate.zig");
    _ = @import("processing/operators/json_map.zig");
    _ = @import("processing/operators/json_flatmap.zig");
    _ = @import("processing/operators/native_registry.zig");
    // Processing: windowing
    _ = @import("processing/window/assigner.zig");
    _ = @import("processing/window/trigger.zig");
    _ = @import("processing/window/function.zig");
    _ = @import("processing/window/operator.zig");
    _ = @import("processing/window/session.zig");
    _ = @import("processing/window/lateness.zig");
    // Processing: time
    _ = @import("processing/time/watermark.zig");
    _ = @import("processing/time/tracker.zig");
    _ = @import("processing/time/timer.zig");
    // Processing: checkpointing
    _ = @import("processing/checkpoint/storage.zig");
    _ = @import("processing/checkpoint/offsets.zig");
    _ = @import("processing/checkpoint/snapshot.zig");
    _ = @import("processing/checkpoint/coordinator.zig");
    _ = @import("processing/checkpoint/alignment.zig");
    _ = @import("processing/checkpoint/recovery.zig");
    // Processing: side outputs & metrics
    _ = @import("processing/side_output.zig");
    _ = @import("processing/metrics.zig");
    // Processing: definition & parser
    _ = @import("processing/definition.zig");
    _ = @import("processing/parser.zig");
}

// Dashboard
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

// Cluster
test {
    _ = @import("cluster/coordinator.zig");
    _ = @import("cluster/partition_table.zig");
    _ = @import("cluster/forwarder.zig");
    _ = @import("cluster/gossip.zig");
    _ = @import("cluster/membership.zig");
}

// Auth
test {
    _ = @import("auth/mod.zig");
}
