//! Flo CLI Client - Public API
//!
//! This module composes all client primitives (KV, Queue, Stream, etc.)
//! into a unified namespace-based API.
//!
//! Usage:
//!   const client = @import("client/mod.zig");
//!   var c = client.Client.init(allocator, endpoint);
//!   try c.connect();
//!   var result = try client.kv.get(&c, "mykey");
//!   var result = try client.queue.enqueue(&c, "myqueue", "payload");

// Re-export base types
pub const Client = @import("base.zig").Client;
pub const Response = @import("base.zig").Response;
/// StreamID used across the stream client API (groupAck/groupClaim ids, cursors).
pub const StreamID = @import("../../stream/stream_id.zig").StreamID;

// Re-export wire utilities from shared protocol module
pub const wire = @import("../../util/wire.zig");
pub const WireWriter = wire.WireWriter;
pub const WireReader = wire.WireReader;
pub const FixedWireWriter = wire.FixedWireWriter;

// Re-export primitive namespaces
pub const kv = @import("kv.zig");
pub const queue = @import("queue.zig");
pub const stream = @import("stream.zig");
pub const action = @import("action.zig");
pub const namespace = @import("namespace.zig");
pub const workflow = @import("workflow.zig");
pub const processing = @import("processing.zig");
pub const ts = @import("ts.zig");
