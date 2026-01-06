//! Command modules for Flo CLI using Commander framework
//!
//! Each module exports a `createXxxCommand(allocator)` function that returns
//! a pre-built `*commander.Command` tree that can be added to the root CLI.

pub const server = @import("server.zig");
pub const cluster = @import("cluster.zig");
pub const kv = @import("kv.zig");
pub const queue = @import("queue.zig");
pub const stream = @import("stream.zig");
pub const status = @import("status.zig");
pub const action = @import("action.zig");
pub const repl = @import("repl.zig");
pub const namespace = @import("namespace.zig");
pub const workflow = @import("workflow.zig");
pub const processing = @import("processing.zig");
pub const ts = @import("ts.zig");

// Re-export convenience functions
pub const createServerCommand = server.createServerCommand;
pub const createClusterCommand = cluster.createClusterCommand;
pub const createKvCommand = kv.createKvCommand;
pub const createQueueCommand = queue.createQueueCommand;
pub const createStreamCommand = stream.createStreamCommand;
pub const createStatusCommand = status.createStatusCommand;
pub const createActionCommand = action.createActionCommand;
pub const createWorkerCommand = action.createWorkerCommand;
pub const createReplCommand = repl.createReplCommand;
pub const createNamespaceCommand = namespace.createNamespaceCommand;
pub const createWorkflowCommand = workflow.createWorkflowCommand;
pub const createProcessingCommand = processing.createProcessingCommand;
pub const createTsCommand = ts.createTsCommand;

// ==================== Testing ====================

test "all command modules" {
    _ = server;
    _ = cluster;
    _ = kv;
    _ = queue;
    _ = stream;
    _ = status;
    _ = action;
    _ = repl;
    _ = namespace;
    _ = workflow;
    _ = processing;
    _ = ts;
}
