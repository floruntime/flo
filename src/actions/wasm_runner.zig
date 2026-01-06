//! Actions WASM Runner
//!
//! Executes WASM modules for `action_type = .wasm` actions.
//! Simpler than the processing WASM operator — this is a stateless
//! request/response model: bytes in → bytes out.
//!
//! The runner is format-agnostic. Input and output are raw `[]const u8` —
//! the runner never inspects or validates the contents. Guests may use
//! JSON, MessagePack, Protobuf, raw audio, images, or any other format.
//!
//! Reuses zware (pure Zig WASM 2.0 interpreter) from the processing module,
//! but with a minimal ABI designed for actions:
//!
//! ## Guest Exports (Required)
//!
//!   handle(input_ptr: u32, input_len: u32) -> i64
//!     Returns packed (output_ptr << 32) | output_len, or negative error code.
//!
//!   alloc(size: u32) -> u32
//!     Allocate bytes in guest linear memory. Returns 0 on failure.
//!
//!   dealloc(ptr: u32, size: u32) -> void
//!     Free previously allocated memory.
//!
//! ## Guest Exports (Optional)
//!
//!   init() -> i32
//!     Called once after instantiation. Returns 0 on success.
//!
//!   describe() -> i64
//!     Return packed ptr|len to a description string.
//!
//! ## Host Imports
//!
//!   flo.log(level: u32, msg_ptr: u32, msg_len: u32) -> void
//!     Log a message. Levels: 0=debug, 1=info, 2=warn, 3=error.
//!
//!   flo.kv_get(key_ptr: u32, key_len: u32, buf_ptr: u32, buf_len: u32) -> i32
//!     Read a KV value. Returns bytes written (>= 0), -1 (not found),
//!     or -3 (buffer too small).
//!
//!   flo.kv_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
//!     Write a KV value. Returns 0 on success, negative on error.
//!
//!   flo.kv_delete(key_ptr: u32, key_len: u32) -> i32
//!     Delete a KV key. Returns 0 on success, -1 (not found).
//!
//! ## Concurrency
//!
//! The runner enforces a per-shard concurrency limit (default: 4).
//! `tryAcquire()` / `release()` must bracket each execution. When
//! the limit is reached, callers should return `error.ConcurrencyLimitReached`.
//!
//! ## Usage
//!
//! ```zig
//! var runner = try ActionWasmRunner.init(allocator);
//! defer runner.deinit();
//!
//! var module = try runner.loadModule(wasm_bytes, .{});
//! defer module.deinit();
//!
//! if (!runner.tryAcquire()) return error.ConcurrencyLimitReached;
//! defer runner.release();
//!
//! var result = try runner.execute(&module, raw_input_bytes);
//! defer result.deinit();
//! ```
//!
//! ## Thread Safety
//!
//! One ActionWasmRunner per shard (matches thread-per-shard model).
//! Not thread-safe — each shard has its own runner.

const std = @import("std");
const Allocator = std.mem.Allocator;
const zware = @import("zware");
const Store = zware.Store;
const Module = zware.Module;
const Instance = zware.Instance;
const ValType = zware.ValType;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for WASM action execution sandbox.
pub const ActionWasmConfig = struct {
    /// Maximum WASM linear memory in 64KB pages.
    /// Default: 256 pages = 16 MB.
    max_memory_pages: u32 = 256,

    /// Maximum output size in bytes.
    max_output_size: u32 = 1024 * 1024, // 1MB

    /// Action name (for logging)
    action_name: []const u8 = "wasm-action",

    /// Maximum concurrent WASM executions per shard.
    /// Prevents runaway resource usage from parallel invocations.
    max_concurrent_executions: u32 = 4,

    /// Whether KV host functions are enabled for this action.
    /// When true, WASM guest can call flo.kv_get/kv_set/kv_delete.
    enable_kv_access: bool = true,

    /// Default config
    pub const default: ActionWasmConfig = .{};

    /// Minimal config for testing (must accommodate demo WASM module's 18-page minimum)
    pub const testing_minimal: ActionWasmConfig = .{
        .max_memory_pages = 32, // 2MB (demo module needs at least 18 pages)
        .max_output_size = 64 * 1024, // 64KB
        .action_name = "wasm-test",
    };

    /// Compute maximum memory in bytes
    pub fn maxMemoryBytes(self: ActionWasmConfig) u64 {
        return @as(u64, self.max_memory_pages) * 65536;
    }
};

// =============================================================================
// Error codes from guest handle() function
// =============================================================================

pub const GUEST_ERROR_INVALID_INPUT: i64 = -1;
pub const GUEST_ERROR_ALLOC_FAILED: i64 = -2;
pub const GUEST_ERROR_EXECUTION: i64 = -3;

// =============================================================================
// Host function ABI constants
// =============================================================================

const flo_module = "flo";
const wasi_module = "wasi_snapshot_preview1";

// Return codes (matching processing/wasm/abi.zig)
pub const RC_SUCCESS: i32 = 0;
pub const RC_NOT_FOUND: i32 = -1;
pub const RC_BUFFER_TOO_SMALL: i32 = -3;
pub const RC_STATE_ERROR: i32 = -5;
pub const RC_INVALID_ARGS: i32 = -6;

const log_params = &[_]ValType{ .I32, .I32, .I32 };
const log_results = &[_]ValType{};

// KV host function signatures
const kv_get_params = &[_]ValType{ .I32, .I32, .I32, .I32 }; // key_ptr, key_len, buf_ptr, buf_len
const kv_get_results = &[_]ValType{.I32}; // bytes written or error
const kv_set_params = &[_]ValType{ .I32, .I32, .I32, .I32 }; // key_ptr, key_len, val_ptr, val_len
const kv_set_results = &[_]ValType{.I32}; // RC_SUCCESS or error
const kv_delete_params = &[_]ValType{ .I32, .I32 }; // key_ptr, key_len
const kv_delete_results = &[_]ValType{.I32}; // RC_SUCCESS or error

// WASI shim signatures (for modules that import WASI)
const wasi_clock_time_get_params = &[_]ValType{ .I32, .I64, .I32 };
const wasi_clock_time_get_results = &[_]ValType{.I32};
const wasi_random_get_params = &[_]ValType{ .I32, .I32 };
const wasi_random_get_results = &[_]ValType{.I32};
const wasi_fd_write_params = &[_]ValType{ .I32, .I32, .I32, .I32 };
const wasi_fd_write_results = &[_]ValType{.I32};
const wasi_args_sizes_get_params = &[_]ValType{ .I32, .I32 };
const wasi_args_sizes_get_results = &[_]ValType{.I32};
const wasi_args_get_params = &[_]ValType{ .I32, .I32 };
const wasi_args_get_results = &[_]ValType{.I32};
const wasi_environ_sizes_get_params = &[_]ValType{ .I32, .I32 };
const wasi_environ_sizes_get_results = &[_]ValType{.I32};
const wasi_environ_get_params = &[_]ValType{ .I32, .I32 };
const wasi_environ_get_results = &[_]ValType{.I32};
const wasi_proc_exit_params = &[_]ValType{.I32};
const wasi_proc_exit_results = &[_]ValType{};

// =============================================================================
// Execution Result
// =============================================================================

/// Result of a WASM action execution.
pub const ExecutionResult = struct {
    /// Output bytes (owned by caller). Format-agnostic — may be JSON, binary, etc.
    output: []u8,
    /// Allocator used for output (for dealloc)
    allocator: Allocator,

    pub fn deinit(self: *ExecutionResult) void {
        self.allocator.free(self.output);
    }

    /// Get output as string slice
    pub fn outputStr(self: ExecutionResult) []const u8 {
        return self.output;
    }
};

// =============================================================================
// ActionWasmModule — A decoded WASM module ready for action execution
// =============================================================================

/// A loaded and decoded WASM module for action execution.
pub const ActionWasmModule = struct {
    module: Module,
    config: ActionWasmConfig,
    has_init: bool,
    has_describe: bool,

    pub fn deinit(self: *ActionWasmModule) void {
        self.module.deinit();
    }

    /// Check if the module exports a function with the given name
    pub fn hasExport(self: *const ActionWasmModule, name: []const u8) bool {
        for (self.module.exports.list.items) |exp| {
            if (exp.tag == .Func and std.mem.eql(u8, exp.name, name)) {
                return true;
            }
        }
        return false;
    }
};

// =============================================================================
// ActionWasmRunner — Per-shard WASM execution engine for actions
// =============================================================================

/// KV dispatch function type for host functions.
/// Called by flo.kv_get/kv_set/kv_delete to interact with Layer 1.
pub const KvDispatchFn = *const fn (ctx: *anyopaque, op: KvOp, allocator: Allocator) KvResult;

pub const KvOp = union(enum) {
    get: struct { namespace: []const u8, key: []const u8 },
    set: struct { namespace: []const u8, key: []const u8, value: []const u8 },
    delete: struct { namespace: []const u8, key: []const u8 },
};

pub const KvResult = struct {
    status: enum { success, not_found, err },
    /// For get: the value bytes (valid until next dispatch call)
    value: ?[]const u8 = null,
};

/// Context passed to KV host functions via the usize context parameter.
/// Lives for the duration of a single WASM execute() call.
pub const WasmKvContext = struct {
    /// Namespace for KV operations (scoped to the action's namespace)
    namespace: []const u8,
    /// KV dispatch function (routes through handler's command dispatcher)
    kv_dispatch_fn: ?KvDispatchFn,
    /// KV dispatch context
    kv_dispatch_ctx: ?*anyopaque,
    /// Allocator for temporary host-side allocations
    allocator: Allocator,
    /// Whether KV access is enabled
    kv_enabled: bool,

    pub fn fromContext(context: usize) *WasmKvContext {
        return @ptrFromInt(context);
    }

    pub fn toContext(self: *WasmKvContext) usize {
        return @intFromPtr(self);
    }
};

/// Per-shard WASM runtime for action execution.
///
/// Owns the zware Store and registers minimal host functions.
/// Each shard has one runner that can execute multiple WASM modules.
/// Enforces a per-shard concurrency limit via tryAcquire()/release().
pub const ActionWasmRunner = struct {
    allocator: Allocator,
    store: Store,
    host_functions_registered: bool,
    /// Number of currently executing WASM actions on this shard
    active_executions: u32 = 0,
    /// Maximum concurrent executions (from config)
    max_concurrent: u32 = 4,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        return initWithConfig(allocator, ActionWasmConfig.default);
    }

    pub fn initWithConfig(allocator: Allocator, config: ActionWasmConfig) !Self {
        var runner = Self{
            .allocator = allocator,
            .store = Store.init(allocator),
            .host_functions_registered = false,
            .max_concurrent = config.max_concurrent_executions,
        };

        try runner.registerHostFunctions();
        return runner;
    }

    pub fn deinit(self: *Self) void {
        self.store.deinit();
    }

    // =========================================================================
    // Concurrency Control
    // =========================================================================

    /// Try to acquire an execution slot. Returns true if acquired.
    /// Caller MUST call release() after execution completes (use defer).
    pub fn tryAcquire(self: *Self) bool {
        if (self.active_executions >= self.max_concurrent) return false;
        self.active_executions += 1;
        return true;
    }

    /// Release an execution slot after WASM execution completes.
    pub fn release(self: *Self) void {
        self.active_executions -= 1;
    }

    /// Number of currently active WASM executions on this shard.
    pub fn activeCount(self: *const Self) u32 {
        return self.active_executions;
    }

    /// Number of available execution slots.
    pub fn availableSlots(self: *const Self) u32 {
        return self.max_concurrent - self.active_executions;
    }

    /// Register minimal host functions for action execution.
    /// flo.log + flo.kv_get/kv_set/kv_delete + WASI shims.
    fn registerHostFunctions(self: *Self) !void {
        if (self.host_functions_registered) return;

        // flo.log
        try self.store.exposeHostFunction(
            flo_module,
            "log",
            hostFnLog,
            0,
            log_params,
            log_results,
        );

        // flo.kv_get / flo.kv_set / flo.kv_delete
        try self.store.exposeHostFunction(flo_module, "kv_get", hostFnKvGet, 0, kv_get_params, kv_get_results);
        try self.store.exposeHostFunction(flo_module, "kv_set", hostFnKvSet, 0, kv_set_params, kv_set_results);
        try self.store.exposeHostFunction(flo_module, "kv_delete", hostFnKvDelete, 0, kv_delete_params, kv_delete_results);

        // WASI shims — for modules compiled with WASI target
        try self.store.exposeHostFunction(wasi_module, "clock_time_get", hostFnWasiClockTimeGet, 0, wasi_clock_time_get_params, wasi_clock_time_get_results);
        try self.store.exposeHostFunction(wasi_module, "random_get", hostFnWasiRandomGet, 0, wasi_random_get_params, wasi_random_get_results);
        try self.store.exposeHostFunction(wasi_module, "fd_write", hostFnWasiFdWrite, 0, wasi_fd_write_params, wasi_fd_write_results);
        try self.store.exposeHostFunction(wasi_module, "args_sizes_get", hostFnWasiArgsSizesGet, 0, wasi_args_sizes_get_params, wasi_args_sizes_get_results);
        try self.store.exposeHostFunction(wasi_module, "args_get", hostFnWasiArgsGet, 0, wasi_args_get_params, wasi_args_get_results);
        try self.store.exposeHostFunction(wasi_module, "environ_sizes_get", hostFnWasiEnvironSizesGet, 0, wasi_environ_sizes_get_params, wasi_environ_sizes_get_results);
        try self.store.exposeHostFunction(wasi_module, "environ_get", hostFnWasiEnvironGet, 0, wasi_environ_get_params, wasi_environ_get_results);
        try self.store.exposeHostFunction(wasi_module, "proc_exit", hostFnWasiProcExit, 0, wasi_proc_exit_params, wasi_proc_exit_results);

        self.host_functions_registered = true;
    }

    /// Load a WASM module from raw bytes.
    ///
    /// Validates against config limits and checks for required exports.
    pub fn loadModule(self: *Self, wasm_bytes: []const u8, config: ActionWasmConfig) !ActionWasmModule {
        _ = self;

        var module = Module.init(std.heap.page_allocator, wasm_bytes);
        errdefer module.deinit();

        try module.decode();

        // Validate memory limits
        if (module.memories.list.items.len > 0) {
            const mem_def = module.memories.list.items[0];
            if (mem_def.limits.max) |max| {
                if (max > config.max_memory_pages) {
                    return error.MemoryLimitExceeded;
                }
            }
            if (mem_def.limits.min > config.max_memory_pages) {
                return error.MemoryLimitExceeded;
            }
        }

        var action_module = ActionWasmModule{
            .module = module,
            .config = config,
            .has_init = false,
            .has_describe = false,
        };

        // Check for optional exports
        action_module.has_init = action_module.hasExport("init");
        action_module.has_describe = action_module.hasExport("describe");

        // Validate required exports exist
        if (!action_module.hasExport("handle")) {
            return error.MissingHandleExport;
        }
        if (!action_module.hasExport("alloc")) {
            return error.MissingAllocExport;
        }

        return action_module;
    }

    /// Execute a WASM action module with the given input bytes.
    ///
    /// Input and output are opaque byte slices — the runner does not
    /// inspect or validate the format (may be JSON, binary, audio, etc.).
    ///
    /// For KV host function access, use `executeWithKv()` instead.
    pub fn execute(self: *Self, wasm_module: *ActionWasmModule, input: []const u8) !ExecutionResult {
        return self.executeWithKv(wasm_module, input, null);
    }

    /// Execute with optional KV context for host function access.
    /// The `kv_ctx` enables flo.kv_get/set/delete during execution.
    /// Pass null to disable KV access (KV host calls will return RC_STATE_ERROR).
    pub fn executeWithKv(self: *Self, wasm_module: *ActionWasmModule, input: []const u8, kv_ctx: ?*WasmKvContext) !ExecutionResult {
        // Set KV context on host functions via store context pointer.
        const context_val: usize = if (kv_ctx) |ctx| ctx.toContext() else 0;
        self.updateHostFunctionContexts(context_val);
        defer self.updateHostFunctionContexts(0);

        // Create instance
        var instance = Instance.init(
            self.allocator,
            &self.store,
            wasm_module.module,
        );
        errdefer instance.deinit();
        try instance.instantiate();
        defer instance.deinit();

        // Call init() if exported
        if (wasm_module.has_init) {
            var init_in = [0]u64{};
            var init_out = [1]u64{0};
            try instance.invoke("init", init_in[0..], init_out[0..], .{});
            const init_result: i32 = @truncate(@as(i64, @bitCast(init_out[0])));
            if (init_result != 0) return error.GuestInitFailed;
        }

        // Allocate input in guest memory
        const input_len: u32 = @intCast(input.len);
        var alloc_in = [1]u64{@as(u64, input_len)};
        var alloc_out = [1]u64{0};
        try instance.invoke("alloc", alloc_in[0..], alloc_out[0..], .{});
        const input_ptr: u32 = @truncate(alloc_out[0]);
        if (input_ptr == 0) return error.GuestAllocFailed;

        // Write input to guest memory
        const memory = try instance.getMemory(0);
        const mem_data = memory.memory();
        const input_end = @as(u64, input_ptr) + @as(u64, input_len);
        if (input_end > mem_data.len) return error.OutOfBoundsMemoryAccess;
        @memcpy(mem_data[input_ptr .. input_ptr + input_len], input);

        // Call handle(input_ptr, input_len) → i64
        var handle_in = [2]u64{ @as(u64, input_ptr), @as(u64, input_len) };
        var handle_out = [1]u64{0};
        try instance.invoke("handle", handle_in[0..], handle_out[0..], .{});

        const result: i64 = @bitCast(handle_out[0]);

        // Check for guest error codes
        if (result < 0) {
            return switch (result) {
                GUEST_ERROR_INVALID_INPUT => error.GuestInvalidInput,
                GUEST_ERROR_ALLOC_FAILED => error.GuestAllocFailed,
                GUEST_ERROR_EXECUTION => error.GuestExecutionError,
                else => error.GuestUnknownError,
            };
        }

        // Unpack output: high 32 bits = ptr, low 32 bits = len
        const output_ptr: u32 = @truncate(@as(u64, @bitCast(result)) >> 32);
        const output_len: u32 = @truncate(@as(u64, @bitCast(result)));

        if (output_len == 0) {
            // Empty output is valid
            const empty = try self.allocator.alloc(u8, 0);
            return ExecutionResult{
                .output = empty,
                .allocator = self.allocator,
            };
        }

        // Validate output size
        if (output_len > wasm_module.config.max_output_size) {
            return error.OutputTooLarge;
        }

        // Read output from guest memory
        const output_end = @as(u64, output_ptr) + @as(u64, output_len);
        if (output_end > mem_data.len) return error.OutOfBoundsMemoryAccess;

        // Copy to host memory
        const output = try self.allocator.alloc(u8, output_len);
        @memcpy(output, mem_data[output_ptr .. output_ptr + output_len]);

        return ExecutionResult{
            .output = output,
            .allocator = self.allocator,
        };
    }

    /// Get module description (if the describe() export exists).
    pub fn describe(self: *Self, wasm_module: *ActionWasmModule) !?[]u8 {
        if (!wasm_module.has_describe) return null;

        var instance = Instance.init(
            self.allocator,
            &self.store,
            wasm_module.module,
        );
        errdefer instance.deinit();
        try instance.instantiate();
        defer instance.deinit();

        // Call init() if present (describe may depend on it)
        if (wasm_module.has_init) {
            var init_in = [0]u64{};
            var init_out = [1]u64{0};
            try instance.invoke("init", init_in[0..], init_out[0..], .{});
        }

        var desc_in = [0]u64{};
        var desc_out = [1]u64{0};
        try instance.invoke("describe", desc_in[0..], desc_out[0..], .{});

        const packed_result = desc_out[0];
        if (packed_result == 0) return null;

        const ptr: u32 = @truncate(packed_result >> 32);
        const len: u32 = @truncate(packed_result);
        if (len == 0) return null;

        const memory = try instance.getMemory(0);
        const data = memory.memory();
        const end = @as(u64, ptr) + @as(u64, len);
        if (end > data.len) return error.OutOfBoundsMemoryAccess;

        const result = try self.allocator.alloc(u8, len);
        @memcpy(result, data[ptr .. ptr + len]);
        return result;
    }

    /// Update the context pointer for KV host functions.
    /// This sets the usize context that gets passed to hostFnKvGet/Set/Delete.
    fn updateHostFunctionContexts(self: *Self, context: usize) void {
        for (self.store.functions.items) |*func| {
            if (func.subtype == .host_function) {
                const hf = &func.subtype.host_function;
                if (hf.func == hostFnKvGet or
                    hf.func == hostFnKvSet or
                    hf.func == hostFnKvDelete)
                {
                    hf.context = context;
                }
            }
        }
    }
};

// =============================================================================
// Host Function Implementations
// =============================================================================

const VirtualMachine = zware.VirtualMachine;
const WasmError = zware.WasmError;

/// flo.log(level, msg_ptr, msg_len)
fn hostFnLog(vm: *VirtualMachine, _: usize) WasmError!void {
    const msg_len = vm.popOperand(u32);
    const msg_ptr = vm.popOperand(u32);
    const level = vm.popOperand(u32);

    const memory = vm.inst.getMemory(0) catch return;
    const data = memory.memory();
    const end = @as(u64, msg_ptr) + @as(u64, msg_len);
    if (end > data.len) return;

    const msg = data[msg_ptr .. msg_ptr + msg_len];

    switch (level) {
        0 => std.log.debug("[wasm] {s}", .{msg}),
        1 => std.log.info("[wasm] {s}", .{msg}),
        2 => std.log.warn("[wasm] {s}", .{msg}),
        3 => std.log.err("[wasm] {s}", .{msg}),
        else => std.log.info("[wasm] {s}", .{msg}),
    }
}

// =============================================================================
// WASI Shims (minimal stubs for compatibility)
// =============================================================================

// Helper: read bytes from WASM linear memory
fn readGuestBytes(vm: *VirtualMachine, ptr: u32, len: u32) WasmError![]const u8 {
    const memory = try vm.inst.getMemory(0);
    const data = memory.memory();
    const end_offset = @as(u64, ptr) + @as(u64, len);
    if (end_offset > data.len) return error.OutOfBoundsMemoryAccess;
    return data[ptr .. ptr + len];
}

fn writeGuestBytes(vm: *VirtualMachine, ptr: u32, bytes: []const u8) WasmError!void {
    const memory = try vm.inst.getMemory(0);
    const data = memory.memory();
    const end_offset = @as(u64, ptr) + @as(u64, bytes.len);
    if (end_offset > data.len) return error.OutOfBoundsMemoryAccess;
    @memcpy(data[ptr .. ptr + @as(u32, @intCast(bytes.len))], bytes);
}

// =============================================================================
// KV Host Functions — flo.kv_get / flo.kv_set / flo.kv_delete
// =============================================================================

/// flo.kv_get(key_ptr, key_len, buf_ptr, buf_len) -> i32
/// Reads a value from the action's KV namespace.
/// Returns: bytes written (>= 0), RC_NOT_FOUND (-1), RC_BUFFER_TOO_SMALL (-3),
///          RC_STATE_ERROR (-5), or RC_INVALID_ARGS (-6).
fn hostFnKvGet(vm: *VirtualMachine, context: usize) WasmError!void {
    const buf_len = vm.popOperand(u32);
    const buf_ptr = vm.popOperand(u32);
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    // No context = KV disabled
    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.kv_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const kv_fn = ctx.kv_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const kv_ctx = ctx.kv_dispatch_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const key = readGuestBytes(vm, key_ptr, key_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const result = kv_fn(kv_ctx, .{ .get = .{
        .namespace = ctx.namespace,
        .key = key,
    } }, ctx.allocator);

    switch (result.status) {
        .not_found => {
            vm.pushOperandNoCheck(i32, RC_NOT_FOUND);
        },
        .err => {
            vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        },
        .success => {
            const value = result.value orelse {
                vm.pushOperandNoCheck(i32, RC_NOT_FOUND);
                return;
            };
            if (value.len > buf_len) {
                vm.pushOperandNoCheck(i32, RC_BUFFER_TOO_SMALL);
                return;
            }
            writeGuestBytes(vm, buf_ptr, value) catch {
                vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
                return;
            };
            vm.pushOperandNoCheck(i32, @as(i32, @intCast(value.len)));
        },
    }
}

/// flo.kv_set(key_ptr, key_len, val_ptr, val_len) -> i32
/// Writes a value to the action's KV namespace.
/// Returns: RC_SUCCESS (0) or RC_STATE_ERROR (-5).
fn hostFnKvSet(vm: *VirtualMachine, context: usize) WasmError!void {
    const val_len = vm.popOperand(u32);
    const val_ptr = vm.popOperand(u32);
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.kv_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const kv_fn = ctx.kv_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const kv_ctx = ctx.kv_dispatch_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const key = readGuestBytes(vm, key_ptr, key_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };
    const value = readGuestBytes(vm, val_ptr, val_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const result = kv_fn(kv_ctx, .{ .set = .{
        .namespace = ctx.namespace,
        .key = key,
        .value = value,
    } }, ctx.allocator);

    vm.pushOperandNoCheck(i32, switch (result.status) {
        .success => RC_SUCCESS,
        else => RC_STATE_ERROR,
    });
}

/// flo.kv_delete(key_ptr, key_len) -> i32
/// Deletes a key from the action's KV namespace.
/// Returns: RC_SUCCESS (0), RC_NOT_FOUND (-1), or RC_STATE_ERROR (-5).
fn hostFnKvDelete(vm: *VirtualMachine, context: usize) WasmError!void {
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.kv_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const kv_fn = ctx.kv_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const kv_ctx = ctx.kv_dispatch_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const key = readGuestBytes(vm, key_ptr, key_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const result = kv_fn(kv_ctx, .{ .delete = .{
        .namespace = ctx.namespace,
        .key = key,
    } }, ctx.allocator);

    vm.pushOperandNoCheck(i32, switch (result.status) {
        .success => RC_SUCCESS,
        .not_found => RC_NOT_FOUND,
        .err => RC_STATE_ERROR,
    });
}

// =============================================================================
// WASI Shims
// =============================================================================

fn hostFnWasiClockTimeGet(vm: *VirtualMachine, _: usize) WasmError!void {
    _ = vm.popOperand(u32); // time_ptr
    _ = vm.popOperand(i64); // precision
    _ = vm.popOperand(u32); // clock_id
    vm.pushOperandNoCheck(u32, 0); // errno = 0 (success)
}

fn hostFnWasiRandomGet(vm: *VirtualMachine, _: usize) WasmError!void {
    const buf_len = vm.popOperand(u32);
    const buf_ptr = vm.popOperand(u32);
    // Fill with pseudo-random bytes
    const memory = vm.inst.getMemory(0) catch {
        vm.pushOperandNoCheck(u32, 8); // EBADF
        return;
    };
    const data = memory.memory();
    const end = @as(u64, buf_ptr) + @as(u64, buf_len);
    if (end > data.len) {
        vm.pushOperandNoCheck(u32, 21); // EFAULT
        return;
    }
    // Simple fill (not cryptographically secure, fine for actions)
    var i: u32 = 0;
    while (i < buf_len) : (i += 1) {
        data[buf_ptr + i] = @truncate(i *% 17 +% 31);
    }
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiFdWrite(vm: *VirtualMachine, _: usize) WasmError!void {
    const nwritten_ptr = vm.popOperand(u32);
    const iovs_len = vm.popOperand(u32);
    _ = vm.popOperand(u32); // iovs_ptr
    _ = vm.popOperand(u32); // fd
    _ = iovs_len;
    // Write 0 bytes (stub)
    const memory = vm.inst.getMemory(0) catch {
        vm.pushOperandNoCheck(u32, 8);
        return;
    };
    const data = memory.memory();
    if (@as(u64, nwritten_ptr) + 4 > data.len) {
        vm.pushOperandNoCheck(u32, 21);
        return;
    }
    const dest: *align(1) u32 = @ptrCast(&data[nwritten_ptr]);
    dest.* = 0;
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiArgsSizesGet(vm: *VirtualMachine, _: usize) WasmError!void {
    const buf_size_ptr = vm.popOperand(u32);
    const argc_ptr = vm.popOperand(u32);
    const memory = vm.inst.getMemory(0) catch {
        vm.pushOperandNoCheck(u32, 8);
        return;
    };
    const data = memory.memory();
    if (@as(u64, argc_ptr) + 4 > data.len or @as(u64, buf_size_ptr) + 4 > data.len) {
        vm.pushOperandNoCheck(u32, 21);
        return;
    }
    const argc_dest: *align(1) u32 = @ptrCast(&data[argc_ptr]);
    argc_dest.* = 0;
    const buf_dest: *align(1) u32 = @ptrCast(&data[buf_size_ptr]);
    buf_dest.* = 0;
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiArgsGet(vm: *VirtualMachine, _: usize) WasmError!void {
    _ = vm.popOperand(u32);
    _ = vm.popOperand(u32);
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiEnvironSizesGet(vm: *VirtualMachine, _: usize) WasmError!void {
    const buf_size_ptr = vm.popOperand(u32);
    const argc_ptr = vm.popOperand(u32);
    const memory = vm.inst.getMemory(0) catch {
        vm.pushOperandNoCheck(u32, 8);
        return;
    };
    const data = memory.memory();
    if (@as(u64, argc_ptr) + 4 > data.len or @as(u64, buf_size_ptr) + 4 > data.len) {
        vm.pushOperandNoCheck(u32, 21);
        return;
    }
    const dest1: *align(1) u32 = @ptrCast(&data[argc_ptr]);
    dest1.* = 0;
    const dest2: *align(1) u32 = @ptrCast(&data[buf_size_ptr]);
    dest2.* = 0;
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiEnvironGet(vm: *VirtualMachine, _: usize) WasmError!void {
    _ = vm.popOperand(u32);
    _ = vm.popOperand(u32);
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiProcExit(vm: *VirtualMachine, _: usize) WasmError!void {
    _ = vm.popOperand(u32); // exit code
    // Don't actually exit — just return
}

// =============================================================================
// Tests
// =============================================================================

test "ActionWasmConfig defaults" {
    const config = ActionWasmConfig.default;
    try std.testing.expectEqual(@as(u32, 256), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 256 * 65536), config.maxMemoryBytes());
}

test "ActionWasmConfig testing minimal" {
    const config = ActionWasmConfig.testing_minimal;
    try std.testing.expectEqual(@as(u32, 32), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 32 * 65536), config.maxMemoryBytes());
}

test "ActionWasmRunner init and deinit" {
    var runner = try ActionWasmRunner.init(std.testing.allocator);
    defer runner.deinit();
    try std.testing.expect(runner.host_functions_registered);
}

test "ActionWasmRunner load and execute rules engine" {
    const allocator = std.testing.allocator;

    // Read the pre-built rules engine WASM module
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Verify required exports
    try std.testing.expect(module.hasExport("handle"));
    try std.testing.expect(module.hasExport("alloc"));
    try std.testing.expect(module.hasExport("dealloc"));

    // Verify optional exports
    try std.testing.expect(module.has_init);
    try std.testing.expect(module.has_describe);
}

test "ActionWasmRunner execute eligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age > 18, country == "US" → eligible
    var result = try runner.execute(&module, "{\"age\": 25, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute ineligible - age" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age <= 18 → not eligible
    var result = try runner.execute(&module, "{\"age\": 15, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute ineligible - country" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: country != "US" → not eligible
    var result = try runner.execute(&module, "{\"age\": 25, \"country\": \"UK\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}",
        result.outputStr(),
    );
}

test "ActionWasmRunner execute fully ineligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Test: age <= 18, country != "US"
    var result = try runner.execute(&module, "{\"age\": 10, \"country\": \"UK\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        result.outputStr(),
    );
}

test "ActionWasmRunner describe" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    const desc = try runner.describe(&module) orelse return error.NoDescription;
    defer allocator.free(desc);

    // Should contain the module name
    try std.testing.expect(std.mem.indexOf(u8, desc, "rules-engine") != null);
    try std.testing.expect(std.mem.indexOf(u8, desc, "1.0") != null);
}

test "ActionWasmRunner multiple executions on same module" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Execute multiple times (each gets fresh instance)
    const inputs = [_][]const u8{
        "{\"age\": 25, \"country\": \"US\"}",
        "{\"age\": 10, \"country\": \"UK\"}",
        "{\"age\": 30, \"country\": \"US\"}",
        "{\"age\": 18, \"country\": \"US\"}", // age == 18, NOT > 18
    };

    const expected = [_][]const u8{
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}", // 18 is not > 18
    };

    for (inputs, expected) |input, exp| {
        var result = try runner.execute(&module, input);
        defer result.deinit();
        try std.testing.expectEqualStrings(exp, result.outputStr());
    }
}

// =============================================================================
// Concurrency control tests
// =============================================================================

test "ActionWasmRunner concurrency semaphore" {
    var runner = try ActionWasmRunner.init(std.testing.allocator);
    defer runner.deinit();

    // Default max is 4
    try std.testing.expectEqual(@as(u32, 0), runner.activeCount());
    try std.testing.expectEqual(@as(u32, 4), runner.availableSlots());

    // Acquire 4 slots
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());

    // 5th should fail
    try std.testing.expect(!runner.tryAcquire());
    try std.testing.expectEqual(@as(u32, 4), runner.activeCount());
    try std.testing.expectEqual(@as(u32, 0), runner.availableSlots());

    // Release one and try again
    runner.release();
    try std.testing.expectEqual(@as(u32, 3), runner.activeCount());
    try std.testing.expect(runner.tryAcquire());

    // Release all
    runner.release();
    runner.release();
    runner.release();
    runner.release();
    try std.testing.expectEqual(@as(u32, 0), runner.activeCount());
}

test "ActionWasmRunner custom concurrency limit" {
    var runner = try ActionWasmRunner.initWithConfig(std.testing.allocator, .{
        .max_concurrent_executions = 2,
    });
    defer runner.deinit();

    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(runner.tryAcquire());
    try std.testing.expect(!runner.tryAcquire()); // 3rd fails with limit=2

    runner.release();
    runner.release();
}

test "WasmKvContext round-trip" {
    var ctx = WasmKvContext{
        .namespace = "test-ns",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = std.testing.allocator,
        .kv_enabled = true,
    };
    const as_usize = ctx.toContext();
    const recovered = WasmKvContext.fromContext(as_usize);
    try std.testing.expectEqualStrings("test-ns", recovered.namespace);
    try std.testing.expect(recovered.kv_enabled);
    try std.testing.expect(recovered.kv_dispatch_fn == null);
}

test "ActionWasmRunner execute with binary input" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("testdata/rules_engine.wasm");

    var runner = try ActionWasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    // Binary input that happens to be valid JSON — runner doesn't care about format
    const binary_input = "{\"age\": 25, \"country\": \"US\"}";
    var result = try runner.execute(&module, binary_input);
    defer result.deinit();

    // Output is also just bytes — we interpret it as JSON here because the guest does
    try std.testing.expect(result.output.len > 0);
}
