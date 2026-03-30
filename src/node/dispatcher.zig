//! Table-Driven Dispatcher — opcode → handler lookup
//!
//! Replaces the old 2,900-line `dispatch()` god function with a flat
//! 512-entry function pointer table. Each subsystem (KV, Stream, Queue,
//! etc.) self-registers its handlers at startup.
//!
//! ## Handler Signature
//!
//! ```zig
//! fn handleGet(shard: *anyopaque, conn: *anyopaque, req: proto.Request) void {
//!     const s: *Shard = @ptrCast(@alignCast(shard));
//!     const c: *Connection = @ptrCast(@alignCast(conn));
//!     // ... handle get ...
//! }
//! ```
//!
//! The `*anyopaque` parameters allow testing with mocks. Real handlers
//! cast to `*Shard` and `*Connection` (defined in later phases).
//!
//! ## Pre-Route Hooks
//!
//! Each opcode may have a `PreRouteFn` that computes a routing hash
//! before dispatch. Three modes:
//!
//! | `pre_route` | Return    | Behavior                        |
//! |-------------|-----------|---------------------------------|
//! | null        | —         | System command, no routing      |
//! | set         | `?u64`    | Single-partition route           |
//! | set         | `null`    | Multi-shard → ShardWalker       |
//!
//! ## Registration
//!
//! ```zig
//! pub fn registerHandlers(d: *Dispatcher) void {
//!     d.register(.kv_get, handleGet);
//!     d.register(.kv_put, handlePut);
//!     d.registerRange(0x30, 0x3F, handleKvRange);  // bulk range
//! }
//! ```

const std = @import("std");
const proto = @import("../protocol/proto.zig");
const shard_walker_mod = @import("shard_walker.zig");

/// ShardWalker specialized for name-list operations (ts_list, stream_list, etc.).
/// All list/scan walk opcodes return `[]const u8` names.
pub const NameWalker = shard_walker_mod.ShardWalker([]const u8);

// ═══════════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════════

/// Handler function — dispatched for a matched opcode.
///
/// Parameters are `*anyopaque` to allow mock testing. Real handlers
/// cast to `*Shard` and `*Connection`.
pub const HandlerFn = *const fn (shard: *anyopaque, conn: *anyopaque, req: proto.Request) void;

/// Pre-route function — computes routing hash for a request.
/// Returns `null` to trigger ShardWalker (multi-shard scan).
pub const PreRouteFn = *const fn (req: proto.Request) ?u64;

/// Error callback — invoked when no handler is registered for an opcode.
pub const ErrorFn = *const fn (conn: *anyopaque, request_id: u64, op_code: u16) void;

// ═══════════════════════════════════════════════════════════════════════════════
// Dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

pub const Dispatcher = struct {
    /// Handler lookup table — indexed by OpCode (u16, capped at MAX_OPCODES).
    handlers: [proto.MAX_OPCODES]?HandlerFn,

    /// Pre-route hooks — indexed by OpCode (u16).
    pre_route: [proto.MAX_OPCODES]?PreRouteFn,

    /// ShardWalker local-scan functions — indexed by OpCode (u16).
    /// Set for opcodes that need cross-shard walking (list/scan).
    /// Matches `NameWalker.LocalScanFn` signature.
    walk_fn: [proto.MAX_OPCODES]?NameWalker.LocalScanFn,

    /// Per-opcode cross-shard walk contexts.
    /// walk_contexts[opcode] = slice of per-shard *anyopaque (one per shard).
    /// Set by runtime after all shards are created.
    walk_contexts: [proto.MAX_OPCODES]?[]const *anyopaque,

    /// Error callback for unknown opcodes.
    on_error: ?ErrorFn,

    /// Number of registered handlers (for diagnostics).
    handler_count: u16,

    /// Initialize with empty tables.
    pub fn init() Dispatcher {
        return .{
            .handlers = [_]?HandlerFn{null} ** proto.MAX_OPCODES,
            .pre_route = [_]?PreRouteFn{null} ** proto.MAX_OPCODES,
            .walk_fn = [_]?NameWalker.LocalScanFn{null} ** proto.MAX_OPCODES,
            .walk_contexts = [_]?[]const *anyopaque{null} ** proto.MAX_OPCODES,
            .on_error = null,
            .handler_count = 0,
        };
    }

    // ─── Registration ────────────────────────────────────────────────────

    /// Register a handler for a single opcode.
    pub fn register(self: *Dispatcher, opcode: proto.OpCode, handler: HandlerFn) void {
        self.registerWithRoute(opcode, handler, null);
    }

    /// Register a handler with an optional pre-route hook.
    pub fn registerWithRoute(self: *Dispatcher, opcode: proto.OpCode, handler: HandlerFn, pre_route_fn: ?PreRouteFn) void {
        const idx = @intFromEnum(opcode);
        if (self.handlers[idx] == null) {
            self.handler_count += 1;
        }
        self.handlers[idx] = handler;
        self.pre_route[idx] = pre_route_fn;
    }

    /// Register a walk (list/scan) opcode — handler + ShardWalker local-scan function.
    /// The handler is used as fallback for single-shard mode (walk_contexts not set).
    /// When walk_contexts are wired, Shard.executeWalk() drives ShardWalker with scan_fn.
    pub fn registerWalk(self: *Dispatcher, opcode: proto.OpCode, handler: HandlerFn, scan_fn: NameWalker.LocalScanFn) void {
        const idx = @intFromEnum(opcode);
        if (self.handlers[idx] == null) {
            self.handler_count += 1;
        }
        self.handlers[idx] = handler;
        self.walk_fn[idx] = scan_fn;
        // pre_route stays null — walk opcodes have no hash routing
    }

    /// Register a walk opcode with a pre-route hook for dual-path routing.
    /// When pre_route returns a hash → single-shard dispatch via handler.
    /// When pre_route returns null → multi-shard walk via scan_fn.
    pub fn registerWalkWithRoute(self: *Dispatcher, opcode: proto.OpCode, handler: HandlerFn, scan_fn: NameWalker.LocalScanFn, pre_route_fn: PreRouteFn) void {
        const idx = @intFromEnum(opcode);
        if (self.handlers[idx] == null) {
            self.handler_count += 1;
        }
        self.handlers[idx] = handler;
        self.walk_fn[idx] = scan_fn;
        self.pre_route[idx] = pre_route_fn;
    }

    /// Set walk contexts for a walk-registered opcode (called by runtime).
    /// contexts is a slice of per-shard *anyopaque — one projection per shard.
    pub fn setWalkContexts(self: *Dispatcher, opcode: proto.OpCode, contexts: []const *anyopaque) void {
        self.walk_contexts[@intFromEnum(opcode)] = contexts;
    }

    /// Check if an opcode is a walk (list/scan) operation.
    pub fn isWalkOp(self: *const Dispatcher, op_code: u16) bool {
        if (op_code >= proto.MAX_OPCODES) return false;
        return self.walk_fn[op_code] != null;
    }

    /// Register a handler for a contiguous range of opcode values [lo, hi] inclusive.
    pub fn registerRange(self: *Dispatcher, lo: u16, hi: u16, handler: HandlerFn) void {
        var i: u16 = lo;
        while (i <= hi) : (i += 1) {
            if (self.handlers[@intCast(i)] == null) {
                self.handler_count += 1;
            }
            self.handlers[@intCast(i)] = handler;
        }
    }

    /// Set the error handler for unrecognized opcodes.
    pub fn setErrorHandler(self: *Dispatcher, err_fn: ErrorFn) void {
        self.on_error = err_fn;
    }

    // ─── Dispatch ────────────────────────────────────────────────────────

    /// Dispatch a request to the registered handler.
    ///
    /// If no handler is registered, invokes the error callback (if set).
    pub fn dispatch(self: *const Dispatcher, shard: *anyopaque, conn: *anyopaque, req: proto.Request) void {
        const idx = req.header.op_code;
        if (idx >= proto.MAX_OPCODES) {
            if (self.on_error) |err_fn| err_fn(conn, req.header.request_id, idx);
            return;
        }
        if (self.handlers[idx]) |handler| {
            handler(shard, conn, req);
        } else if (self.on_error) |err_fn| {
            err_fn(conn, req.header.request_id, idx);
        }
    }

    /// Check if an opcode has a registered handler.
    pub fn hasHandler(self: *const Dispatcher, opcode: proto.OpCode) bool {
        return self.handlers[@intFromEnum(opcode)] != null;
    }

    /// Get the pre-route function for an opcode (if any).
    pub fn getPreRoute(self: *const Dispatcher, opcode: proto.OpCode) ?PreRouteFn {
        return self.pre_route[@intFromEnum(opcode)];
    }

    /// Unregister a handler (for testing).
    pub fn unregister(self: *Dispatcher, opcode: proto.OpCode) void {
        const idx = @intFromEnum(opcode);
        if (self.handlers[idx] != null) {
            self.handlers[idx] = null;
            self.pre_route[idx] = null;
            self.walk_fn[idx] = null;
            self.walk_contexts[idx] = null;
            self.handler_count -= 1;
        }
    }
};

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

// Test helpers — mock Shard/Connection as simple counters
const TestContext = struct {
    dispatch_count: u32 = 0,
    last_opcode: u16 = 0,
    error_count: u32 = 0,
    last_error_opcode: u16 = 0,
};

fn mockHandler(shard: *anyopaque, _: *anyopaque, req: proto.Request) void {
    const ctx: *TestContext = @ptrCast(@alignCast(shard));
    ctx.dispatch_count += 1;
    ctx.last_opcode = req.header.op_code;
}

fn mockHandler2(shard: *anyopaque, _: *anyopaque, req: proto.Request) void {
    const ctx: *TestContext = @ptrCast(@alignCast(shard));
    ctx.dispatch_count += 10; // distinguishable from mockHandler
    ctx.last_opcode = req.header.op_code;
}

fn mockErrorHandler(conn: *anyopaque, _: u64, op_code: u16) void {
    const ctx: *TestContext = @ptrCast(@alignCast(conn));
    ctx.error_count += 1;
    ctx.last_error_opcode = op_code;
}

fn makeRequest(opcode: proto.OpCode) proto.Request {
    var header: proto.RequestHeader = undefined;
    @memset(std.mem.asBytes(&header), 0);
    header.op_code = @intFromEnum(opcode);
    header.request_id = 42;
    return .{
        .header = header,
        .namespace = "",
        .key = "",
        .value = "",
    };
}

test "Dispatcher: init empty" {
    const d = Dispatcher.init();
    try std.testing.expectEqual(@as(u16, 0), d.handler_count);
    try std.testing.expect(!d.hasHandler(.ping));
    try std.testing.expect(!d.hasHandler(.kv_get));
}

test "Dispatcher: register and dispatch single opcode" {
    var d = Dispatcher.init();
    d.register(.ping, mockHandler);

    try std.testing.expect(d.hasHandler(.ping));
    try std.testing.expectEqual(@as(u16, 1), d.handler_count);

    var ctx = TestContext{};
    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.ping));
    try std.testing.expectEqual(@as(u32, 1), ctx.dispatch_count);
    try std.testing.expectEqual(@intFromEnum(proto.OpCode.ping), ctx.last_opcode);
}

test "Dispatcher: unrecognized opcode calls error handler" {
    var d = Dispatcher.init();
    d.setErrorHandler(mockErrorHandler);

    var ctx = TestContext{};
    // kv_get not registered
    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.kv_get));
    try std.testing.expectEqual(@as(u32, 0), ctx.dispatch_count);
    try std.testing.expectEqual(@as(u32, 1), ctx.error_count);
    try std.testing.expectEqual(@intFromEnum(proto.OpCode.kv_get), ctx.last_error_opcode);
}

test "Dispatcher: unrecognized opcode without error handler is silent" {
    const d = Dispatcher.init();
    var ctx = TestContext{};
    // Should not crash
    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.kv_get));
    try std.testing.expectEqual(@as(u32, 0), ctx.dispatch_count);
}

test "Dispatcher: register range" {
    var d = Dispatcher.init();
    // KV opcodes: 0x30–0x3F
    d.registerRange(0x30, 0x3F, mockHandler);

    try std.testing.expectEqual(@as(u16, 16), d.handler_count);
    try std.testing.expect(d.hasHandler(.kv_get));
    try std.testing.expect(d.hasHandler(.kv_put));

    var ctx = TestContext{};
    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.kv_get));
    try std.testing.expectEqual(@as(u32, 1), ctx.dispatch_count);
}

test "Dispatcher: multiple handlers don't interfere" {
    var d = Dispatcher.init();
    d.register(.ping, mockHandler);
    d.register(.kv_get, mockHandler2);

    var ctx = TestContext{};
    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.ping));
    try std.testing.expectEqual(@as(u32, 1), ctx.dispatch_count); // +1

    d.dispatch(@ptrCast(&ctx), @ptrCast(&ctx), makeRequest(.kv_get));
    try std.testing.expectEqual(@as(u32, 11), ctx.dispatch_count); // +10
}

test "Dispatcher: unregister" {
    var d = Dispatcher.init();
    d.register(.ping, mockHandler);
    try std.testing.expect(d.hasHandler(.ping));
    try std.testing.expectEqual(@as(u16, 1), d.handler_count);

    d.unregister(.ping);
    try std.testing.expect(!d.hasHandler(.ping));
    try std.testing.expectEqual(@as(u16, 0), d.handler_count);
}

test "Dispatcher: pre-route hook" {
    var d = Dispatcher.init();

    const preRoute = struct {
        fn preFn(req: proto.Request) ?u64 {
            _ = req;
            return 12345; // deterministic hash
        }
    }.preFn;

    d.registerWithRoute(.kv_get, mockHandler, preRoute);

    const prf = d.getPreRoute(.kv_get);
    try std.testing.expect(prf != null);

    const hash = prf.?(makeRequest(.kv_get));
    try std.testing.expectEqual(@as(?u64, 12345), hash);
}

test "Dispatcher: pre-route null means shard walker" {
    var d = Dispatcher.init();

    const preRoute = struct {
        fn preFn(_: proto.Request) ?u64 {
            return null; // scan → shard walker
        }
    }.preFn;

    d.registerWithRoute(.kv_scan, mockHandler, preRoute);

    const prf = d.getPreRoute(.kv_scan);
    try std.testing.expect(prf != null);

    const hash = prf.?(makeRequest(.kv_scan));
    try std.testing.expectEqual(@as(?u64, null), hash);
}
