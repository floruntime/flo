// Library root for importing Flo modules
// Used by integration tests and external consumers

// =============================================================================
// Logging: Structured logging with multiple formats (text, JSON, compact)
// =============================================================================
pub const log = @import("stdx").log;

// =============================================================================
// Protocol Layer: Wire protocol, RESP, request building
// =============================================================================
pub const protocol = struct {
    pub const proto = @import("protocol/proto.zig");
    pub const request_builder = @import("protocol/request_builder.zig");
    pub const result = @import("protocol/result.zig");
    pub const resp = @import("protocol/resp.zig");
};

// =============================================================================
// Node Layer: Acceptor, Reactor, Shard, Dispatcher, Router, Inbox (NEW)
// =============================================================================
pub const node = @import("node/mod.zig");

// =============================================================================
// Storage Layer: UAL, Partition, Snapshot, Memory Controller (NEW)
// =============================================================================
pub const storage = @import("storage/mod.zig");

// =============================================================================
// Raft Consensus (NEW)
// =============================================================================
pub const raft = @import("raft/mod.zig");

// =============================================================================
// VOPR: deterministic simulation & scenario testing
// =============================================================================
pub const vopr = @import("vopr/mod.zig");

// =============================================================================
// Projection Engines: KV, Queue, Stream, TimeSeries (NEW)
// =============================================================================
pub const projection = @import("projection/mod.zig");

// =============================================================================
// KV subsystem internals (handler + per-shard txn table)
// =============================================================================
pub const kv = struct {
    pub const handler = @import("kv/handler.zig");
    pub const txn = @import("kv/txn.zig");
};

// =============================================================================
// Cluster: Controller Raft, Partition Table, Gossip (NEW)
// =============================================================================
pub const cluster = @import("cluster/mod.zig");

// =============================================================================
// Configuration (ADAPT)
// =============================================================================
pub const config = @import("config/mod.zig");

// =============================================================================
// Utilities (KEEP)
// =============================================================================
pub const util = struct {
    pub const checksum = @import("util/checksum.zig");
    pub const json = @import("util/json.zig");
    pub const json_path = @import("util/json_path.zig");
    pub const validation = @import("util/validation.zig");
    pub const constants = @import("util/constants.zig");
};

// =============================================================================
// TS (FloQL, Line Protocol) — parsers are KEEP, executor is ADAPT
// =============================================================================
pub const ts = struct {
    pub const line_protocol = @import("ts/line_protocol.zig");
    pub const floql = struct {
        pub const parser = @import("ts/floql/parser.zig");
        pub const ast = @import("ts/floql/ast.zig");
    };
};

// =============================================================================
// Workflow (parsers KEEP, executor REWRITE)
// =============================================================================
pub const workflow = struct {
    pub const parser = @import("workflow/parser.zig");
    pub const definition = @import("workflow/definition.zig");
    pub const types = @import("workflow/types.zig");
};

// =============================================================================
// Processing (parsers KEEP, executor REWRITE)
// =============================================================================
pub const processing = struct {
    pub const parser = @import("processing/parser.zig");
    pub const definition = @import("processing/definition.zig");
};

// =============================================================================
// Auth: API keys, session tokens, key store
// =============================================================================
pub const auth = @import("auth/mod.zig");

// =============================================================================
// CLI client primitives (raw TCP protocol client) — exposed for e2e/integration
// tests that need to drive multiple concurrent connections against a live
// server. Does NOT pull in the heavier `cli` commander/commands tree.
// =============================================================================
pub const cli_client = @import("cli/client/mod.zig");
