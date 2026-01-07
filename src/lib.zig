// Library root for importing Flo modules
// Used by integration tests and external consumers

// =============================================================================
// Logging: Structured logging with multiple formats (text, JSON, compact)
// =============================================================================
pub const log = @import("stdx").log;

// =============================================================================
// Protocol Layer: Wire protocol, RESP, request building (KEEP — do not modify)
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
// pub const node = @import("node/mod.zig"); // TODO: Phase 1

// =============================================================================
// Storage Layer: UAL, Partition, Snapshot, Memory Controller (NEW)
// =============================================================================
// pub const storage = @import("storage/mod.zig"); // TODO: Phase 2

// =============================================================================
// Raft Consensus (NEW)
// =============================================================================
// pub const raft = @import("raft/mod.zig"); // TODO: Phase 3

// =============================================================================
// Projection Engines: KV, Queue, Stream, TimeSeries (NEW)
// =============================================================================
// pub const projection = @import("projection/mod.zig"); // TODO: Phase 4

// =============================================================================
// Cluster: Controller Raft, Partition Table, Gossip (NEW)
// =============================================================================
// pub const cluster = @import("cluster/mod.zig"); // TODO: Phase 7

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
