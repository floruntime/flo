//! Centralized WASM Runner
//!
//! Shared WASM runtime core used by both the Actions subsystem (stateless
//! request/response) and the Processing subsystem (per-record streaming).
//!
//! Owns the zware Store, registers host functions (flo.log, flo.kv_*,
//! WASI shims), loads/validates modules, and executes the guest ABI:
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
//!   flo.kv_get(key_ptr, key_len, buf_ptr, buf_len) -> i32
//!   flo.kv_set(key_ptr, key_len, val_ptr, val_len) -> i32
//!   flo.kv_delete(key_ptr, key_len) -> i32
//!   flo.set_tag(name_ptr: u32, name_len: u32) -> i32
//!   flo.emit(ptr: u32, len: u32) -> i32
//!   flo.state_get(key_ptr, key_len, buf_ptr, buf_len) -> i32
//!   flo.state_set(key_ptr, key_len, val_ptr, val_len) -> i32
//!   flo.state_delete(key_ptr, key_len) -> i32
//!
//! ## Filter Convention
//!
//!   handle() returning 0 means "drop this record" (filter).
//!   Negative values are errors. Positive packed ptr|len is output.
//!
//! ## Thread Safety
//!
//! One WasmRunner per shard (matches thread-per-shard model).
//! Not thread-safe — each shard has its own runner.

const std = @import("std");
const Allocator = std.mem.Allocator;
const zware = @import("zware");
const Store = zware.Store;
const Module = zware.Module;
const Instance = zware.Instance;
const ValType = zware.ValType;
const VirtualMachine = zware.VirtualMachine;
const WasmError = zware.WasmError;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the WASM execution sandbox.
pub const WasmConfig = struct {
    /// Maximum WASM linear memory in 64KB pages.
    /// Default: 256 pages = 16 MB.
    max_memory_pages: u32 = 256,

    /// Maximum output size in bytes.
    max_output_size: u32 = 1024 * 1024, // 1MB

    /// Module name (for logging)
    module_name: []const u8 = "wasm-module",

    /// Whether KV host functions are enabled.
    /// When true, WASM guest can call flo.kv_get/kv_set/kv_delete.
    enable_kv_access: bool = true,

    /// Default config
    pub const default: WasmConfig = .{};

    /// Minimal config for testing (must accommodate demo WASM module's 18-page minimum)
    pub const testing_minimal: WasmConfig = .{
        .max_memory_pages = 32, // 2MB (demo module needs at least 18 pages)
        .max_output_size = 64 * 1024, // 64KB
        .module_name = "wasm-test",
    };

    /// Compute maximum memory in bytes
    pub fn maxMemoryBytes(self: WasmConfig) u64 {
        return @as(u64, self.max_memory_pages) * 65536;
    }
};

// =============================================================================
// Error codes from guest handle() function
// =============================================================================

pub const GUEST_ERROR_INVALID_INPUT: i64 = -1;
pub const GUEST_ERROR_ALLOC_FAILED: i64 = -2;
pub const GUEST_ERROR_EXECUTION: i64 = -3;

/// handle() returning 0 means "filter / drop this record".
pub const GUEST_FILTER: i64 = 0;

// =============================================================================
// Host function ABI constants
// =============================================================================

const flo_module = "flo";
const wasi_module = "wasi_snapshot_preview1";

pub const RC_SUCCESS: i32 = 0;
pub const RC_NOT_FOUND: i32 = -1;
pub const RC_BUFFER_TOO_SMALL: i32 = -3;
pub const RC_STATE_ERROR: i32 = -5;
pub const RC_INVALID_ARGS: i32 = -6;

const log_params = &[_]ValType{ .I32, .I32, .I32 };
const log_results = &[_]ValType{};

const kv_get_params = &[_]ValType{ .I32, .I32, .I32, .I32 };
const kv_get_results = &[_]ValType{.I32};
const kv_set_params = &[_]ValType{ .I32, .I32, .I32, .I32 };
const kv_set_results = &[_]ValType{.I32};
const kv_delete_params = &[_]ValType{ .I32, .I32 };
const kv_delete_results = &[_]ValType{.I32};

const set_tag_params = &[_]ValType{ .I32, .I32 };
const set_tag_results = &[_]ValType{.I32};

const emit_params = &[_]ValType{ .I32, .I32 };
const emit_results = &[_]ValType{.I32};

const state_get_params = &[_]ValType{ .I32, .I32, .I32, .I32 };
const state_get_results = &[_]ValType{.I32};
const state_set_params = &[_]ValType{ .I32, .I32, .I32, .I32 };
const state_set_results = &[_]ValType{.I32};
const state_delete_params = &[_]ValType{ .I32, .I32 };
const state_delete_results = &[_]ValType{.I32};

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

/// Result of a WASM execution. Format-agnostic — may be JSON, binary, etc.
pub const ExecutionResult = struct {
    output: []u8,
    allocator: Allocator,
    /// True when handle() returned 0, meaning the record should be dropped.
    filtered: bool = false,

    pub fn deinit(self: *ExecutionResult) void {
        self.allocator.free(self.output);
    }

    pub fn outputStr(self: ExecutionResult) []const u8 {
        return self.output;
    }
};

// =============================================================================
// WasmModule — A decoded WASM module ready for execution
// =============================================================================

/// A loaded and decoded WASM module.
pub const WasmModule = struct {
    module: Module,
    config: WasmConfig,
    has_init: bool,
    has_describe: bool,

    pub fn deinit(self: *WasmModule) void {
        self.module.deinit();
    }

    pub fn hasExport(self: *const WasmModule, name: []const u8) bool {
        for (self.module.exports.list.items) |exp| {
            if (exp.tag == .Func and std.mem.eql(u8, exp.name, name)) {
                return true;
            }
        }
        return false;
    }
};

// =============================================================================
// KV dispatch types
// =============================================================================

pub const KvDispatchFn = *const fn (ctx: *anyopaque, op: KvOp, allocator: Allocator) KvResult;

pub const KvOp = union(enum) {
    get: struct { namespace: []const u8, key: []const u8 },
    set: struct { namespace: []const u8, key: []const u8, value: []const u8 },
    delete: struct { namespace: []const u8, key: []const u8 },
};

pub const KvResult = struct {
    status: enum { success, not_found, err },
    value: ?[]const u8 = null,
};

/// Callback type for tag resolution: maps tag name → bit position.
pub const TagResolveFn = *const fn (ctx: *anyopaque, tag_name: []const u8) ?u5;

/// Callback type for sandboxed operator state (processing only).
/// Mirrors KvDispatchFn but operates on the per-operator StateBackend.
pub const StateDispatchFn = *const fn (ctx: *anyopaque, op: StateOp, allocator: Allocator) StateResult;

pub const StateOp = union(enum) {
    get: struct { key: []const u8 },
    set: struct { key: []const u8, value: []const u8 },
    delete: struct { key: []const u8 },
};

pub const StateResult = struct {
    status: enum { success, not_found, err },
    value: ?[]const u8 = null,
};

/// A single emitted record from flo.emit() calls during handle().
pub const EmittedRecord = struct {
    data: []u8,
};

/// Context passed to host functions via the usize context parameter.
/// Lives for the duration of a single WASM execute() call.
pub const WasmKvContext = struct {
    namespace: []const u8,
    kv_dispatch_fn: ?KvDispatchFn,
    kv_dispatch_ctx: ?*anyopaque,
    allocator: Allocator,
    kv_enabled: bool,

    /// Tag resolution callback (optional — processing only).
    tag_resolve_fn: ?TagResolveFn = null,
    tag_resolve_ctx: ?*anyopaque = null,
    /// Accumulated tag bits set by the WASM guest via flo.set_tag().
    /// Read by the caller after execute() to apply to the output record.
    output_tags: u32 = 0,

    /// Sandboxed state dispatch (processing only). Separate from kv_dispatch.
    state_dispatch_fn: ?StateDispatchFn = null,
    state_dispatch_ctx: ?*anyopaque = null,
    state_enabled: bool = false,

    /// Records emitted via flo.emit() during handle(). Owned by caller.
    emitted_records: std.ArrayListUnmanaged(EmittedRecord) = .empty,
    /// Whether flo.emit() was called at least once during this invocation.
    emit_called: bool = false,

    pub fn fromContext(context: usize) *WasmKvContext {
        return @ptrFromInt(context);
    }

    pub fn toContext(self: *WasmKvContext) usize {
        return @intFromPtr(self);
    }
};

// =============================================================================
// WasmRunner — Core WASM execution engine
// =============================================================================

/// Core WASM runtime shared by Actions and Processing subsystems.
///
/// Owns the zware Store and registers minimal host functions.
/// Each shard has one runner instance.
pub const WasmRunner = struct {
    allocator: Allocator,
    store: Store,
    host_functions_registered: bool,

    const Self = @This();

    pub fn init(allocator: Allocator) !Self {
        var runner = Self{
            .allocator = allocator,
            .store = Store.init(allocator),
            .host_functions_registered = false,
        };

        try runner.registerHostFunctions();
        return runner;
    }

    pub fn deinit(self: *Self) void {
        self.store.deinit();
    }

    /// Register host functions (flo.log, flo.kv_*, WASI shims).
    fn registerHostFunctions(self: *Self) !void {
        if (self.host_functions_registered) return;

        try self.store.exposeHostFunction(flo_module, "log", hostFnLog, 0, log_params, log_results);

        try self.store.exposeHostFunction(flo_module, "kv_get", hostFnKvGet, 0, kv_get_params, kv_get_results);
        try self.store.exposeHostFunction(flo_module, "kv_set", hostFnKvSet, 0, kv_set_params, kv_set_results);
        try self.store.exposeHostFunction(flo_module, "kv_delete", hostFnKvDelete, 0, kv_delete_params, kv_delete_results);
        try self.store.exposeHostFunction(flo_module, "set_tag", hostFnSetTag, 0, set_tag_params, set_tag_results);
        try self.store.exposeHostFunction(flo_module, "emit", hostFnEmit, 0, emit_params, emit_results);
        try self.store.exposeHostFunction(flo_module, "state_get", hostFnStateGet, 0, state_get_params, state_get_results);
        try self.store.exposeHostFunction(flo_module, "state_set", hostFnStateSet, 0, state_set_params, state_set_results);
        try self.store.exposeHostFunction(flo_module, "state_delete", hostFnStateDelete, 0, state_delete_params, state_delete_results);

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
    /// Validates against config limits and checks for required exports.
    pub fn loadModule(self: *Self, wasm_bytes: []const u8, config: WasmConfig) !WasmModule {
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

        var wasm_module = WasmModule{
            .module = module,
            .config = config,
            .has_init = false,
            .has_describe = false,
        };

        wasm_module.has_init = wasm_module.hasExport("init");
        wasm_module.has_describe = wasm_module.hasExport("describe");

        if (!wasm_module.hasExport("handle")) {
            return error.MissingHandleExport;
        }
        if (!wasm_module.hasExport("alloc")) {
            return error.MissingAllocExport;
        }

        return wasm_module;
    }

    /// Execute a WASM module with the given input bytes.
    /// Input and output are opaque byte slices — format-agnostic.
    pub fn execute(self: *Self, wasm_module: *WasmModule, input: []const u8) !ExecutionResult {
        return self.executeWithKv(wasm_module, input, null);
    }

    /// Execute with optional KV context for host function access.
    pub fn executeWithKv(self: *Self, wasm_module: *WasmModule, input: []const u8, kv_ctx: ?*WasmKvContext) !ExecutionResult {
        const context_val: usize = if (kv_ctx) |ctx| ctx.toContext() else 0;
        self.updateHostFunctionContexts(context_val);
        defer self.updateHostFunctionContexts(0);

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

        if (result < 0) {
            return switch (result) {
                GUEST_ERROR_INVALID_INPUT => error.GuestInvalidInput,
                GUEST_ERROR_ALLOC_FAILED => error.GuestAllocFailed,
                GUEST_ERROR_EXECUTION => error.GuestExecutionError,
                else => error.GuestUnknownError,
            };
        }

        // If flo.emit() was called during handle(), those records are the output.
        // The handle() return value is ignored for output (only checked for errors above).
        if (kv_ctx) |ctx| {
            if (ctx.emit_called) {
                // Multi-emit mode: emitted_records owned by caller via kv_ctx.
                // Return empty output from executeWithKv — caller reads ctx.emitted_records.
                const empty = try self.allocator.alloc(u8, 0);
                return ExecutionResult{
                    .output = empty,
                    .allocator = self.allocator,
                };
            }
        }

        // Filter: handle() returning 0 means "drop this record"
        if (result == GUEST_FILTER) {
            const empty = try self.allocator.alloc(u8, 0);
            return ExecutionResult{
                .output = empty,
                .allocator = self.allocator,
                .filtered = true,
            };
        }

        // Standard map mode: unpack single output from handle() return
        const output_ptr: u32 = @truncate(@as(u64, @bitCast(result)) >> 32);
        const output_len: u32 = @truncate(@as(u64, @bitCast(result)));

        if (output_len == 0) {
            const empty = try self.allocator.alloc(u8, 0);
            return ExecutionResult{
                .output = empty,
                .allocator = self.allocator,
            };
        }

        if (output_len > wasm_module.config.max_output_size) {
            return error.OutputTooLarge;
        }

        const output_end = @as(u64, output_ptr) + @as(u64, output_len);
        if (output_end > mem_data.len) return error.OutOfBoundsMemoryAccess;

        const output = try self.allocator.alloc(u8, output_len);
        @memcpy(output, mem_data[output_ptr .. output_ptr + output_len]);

        return ExecutionResult{
            .output = output,
            .allocator = self.allocator,
        };
    }

    /// Get module description (if the describe() export exists).
    pub fn describe(self: *Self, wasm_module: *WasmModule) !?[]u8 {
        if (!wasm_module.has_describe) return null;

        var instance = Instance.init(
            self.allocator,
            &self.store,
            wasm_module.module,
        );
        errdefer instance.deinit();
        try instance.instantiate();
        defer instance.deinit();

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

    /// Update the context pointer for host functions (KV + tag).
    fn updateHostFunctionContexts(self: *Self, context: usize) void {
        for (self.store.functions.items) |*func| {
            if (func.subtype == .host_function) {
                const hf = &func.subtype.host_function;
                if (hf.func == hostFnKvGet or
                    hf.func == hostFnKvSet or
                    hf.func == hostFnKvDelete or
                    hf.func == hostFnSetTag or
                    hf.func == hostFnEmit or
                    hf.func == hostFnStateGet or
                    hf.func == hostFnStateSet or
                    hf.func == hostFnStateDelete)
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
// KV Host Functions
// =============================================================================

fn hostFnKvGet(vm: *VirtualMachine, context: usize) WasmError!void {
    const buf_len = vm.popOperand(u32);
    const buf_ptr = vm.popOperand(u32);
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
// Tag Host Function
// =============================================================================

/// flo.set_tag(name_ptr: u32, name_len: u32) -> i32
///
/// Resolve a tag name via the pipeline's TagRegistry and OR the
/// corresponding bit into `output_tags`.  Returns 0 on success,
/// RC_NOT_FOUND if the name is unknown, RC_STATE_ERROR if tagging
/// is not available (e.g. Actions context).
fn hostFnSetTag(vm: *VirtualMachine, context: usize) WasmError!void {
    const name_len = vm.popOperand(u32);
    const name_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);

    const resolve_fn = ctx.tag_resolve_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const resolve_ctx = ctx.tag_resolve_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const tag_name = readGuestBytes(vm, name_ptr, name_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const bit = resolve_fn(resolve_ctx, tag_name) orelse {
        vm.pushOperandNoCheck(i32, RC_NOT_FOUND);
        return;
    };

    ctx.output_tags |= @as(u32, 1) << bit;
    vm.pushOperandNoCheck(i32, RC_SUCCESS);
}

// =============================================================================
// Emit Host Function (FlatMap / Multi-emit)
// =============================================================================

/// flo.emit(ptr: u32, len: u32) -> i32
///
/// Called 0..N times during handle() to emit output records.
/// When emit is called at least once, handle()'s return value is
/// ignored for output purposes (only checked for errors).
/// Returns 0 on success, RC_STATE_ERROR if emit list is unavailable.
fn hostFnEmit(vm: *VirtualMachine, context: usize) WasmError!void {
    const len = vm.popOperand(u32);
    const ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    ctx.emit_called = true;

    const bytes = readGuestBytes(vm, ptr, len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    // Copy bytes out of guest memory into our allocator
    const copy = ctx.allocator.alloc(u8, len) catch {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    @memcpy(copy, bytes);

    ctx.emitted_records.append(ctx.allocator, .{ .data = copy }) catch {
        ctx.allocator.free(copy);
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    vm.pushOperandNoCheck(i32, RC_SUCCESS);
}

// =============================================================================
// State Host Functions (Sandboxed Per-Operator State)
// =============================================================================

/// flo.state_get(key_ptr, key_len, buf_ptr, buf_len) -> i32
///
/// Read from per-operator sandboxed state. Returns byte count on success,
/// RC_NOT_FOUND if key doesn't exist, RC_BUFFER_TOO_SMALL if value exceeds
/// buf_len, RC_STATE_ERROR if state is not available.
fn hostFnStateGet(vm: *VirtualMachine, context: usize) WasmError!void {
    const buf_len = vm.popOperand(u32);
    const buf_ptr = vm.popOperand(u32);
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.state_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const state_fn = ctx.state_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const state_ctx = ctx.state_dispatch_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const key = readGuestBytes(vm, key_ptr, key_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const result = state_fn(state_ctx, .{ .get = .{ .key = key } }, ctx.allocator);

    switch (result.status) {
        .not_found => vm.pushOperandNoCheck(i32, RC_NOT_FOUND),
        .err => vm.pushOperandNoCheck(i32, RC_STATE_ERROR),
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

/// flo.state_set(key_ptr, key_len, val_ptr, val_len) -> i32
fn hostFnStateSet(vm: *VirtualMachine, context: usize) WasmError!void {
    const val_len = vm.popOperand(u32);
    const val_ptr = vm.popOperand(u32);
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.state_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const state_fn = ctx.state_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const state_ctx = ctx.state_dispatch_ctx orelse {
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

    const result = state_fn(state_ctx, .{ .set = .{ .key = key, .value = value } }, ctx.allocator);
    vm.pushOperandNoCheck(i32, switch (result.status) {
        .success => RC_SUCCESS,
        else => RC_STATE_ERROR,
    });
}

/// flo.state_delete(key_ptr, key_len) -> i32
fn hostFnStateDelete(vm: *VirtualMachine, context: usize) WasmError!void {
    const key_len = vm.popOperand(u32);
    const key_ptr = vm.popOperand(u32);

    if (context == 0) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const ctx = WasmKvContext.fromContext(context);
    if (!ctx.state_enabled) {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    }

    const state_fn = ctx.state_dispatch_fn orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };
    const state_ctx = ctx.state_dispatch_ctx orelse {
        vm.pushOperandNoCheck(i32, RC_STATE_ERROR);
        return;
    };

    const key = readGuestBytes(vm, key_ptr, key_len) catch {
        vm.pushOperandNoCheck(i32, RC_INVALID_ARGS);
        return;
    };

    const result = state_fn(state_ctx, .{ .delete = .{ .key = key } }, ctx.allocator);
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
    vm.pushOperandNoCheck(u32, 0);
}

fn hostFnWasiRandomGet(vm: *VirtualMachine, _: usize) WasmError!void {
    const buf_len = vm.popOperand(u32);
    const buf_ptr = vm.popOperand(u32);
    const memory = vm.inst.getMemory(0) catch {
        vm.pushOperandNoCheck(u32, 8);
        return;
    };
    const data = memory.memory();
    const end = @as(u64, buf_ptr) + @as(u64, buf_len);
    if (end > data.len) {
        vm.pushOperandNoCheck(u32, 21);
        return;
    }
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
    _ = vm.popOperand(u32);
}

// =============================================================================
// Tests
// =============================================================================

test "WasmConfig defaults" {
    const config = WasmConfig.default;
    try std.testing.expectEqual(@as(u32, 256), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 256 * 65536), config.maxMemoryBytes());
}

test "WasmConfig testing minimal" {
    const config = WasmConfig.testing_minimal;
    try std.testing.expectEqual(@as(u32, 32), config.max_memory_pages);
    try std.testing.expectEqual(@as(u64, 32 * 65536), config.maxMemoryBytes());
}

test "WasmRunner init and deinit" {
    var runner = try WasmRunner.init(std.testing.allocator);
    defer runner.deinit();
    try std.testing.expect(runner.host_functions_registered);
}

test "WasmRunner load and execute rules engine" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../actions/testdata/rules_engine.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    try std.testing.expect(module.hasExport("handle"));
    try std.testing.expect(module.hasExport("alloc"));
    try std.testing.expect(module.hasExport("dealloc"));
    try std.testing.expect(module.has_init);
    try std.testing.expect(module.has_describe);
}

test "WasmRunner execute eligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../actions/testdata/rules_engine.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var result = try runner.execute(&module, "{\"age\": 25, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        result.outputStr(),
    );
}

test "WasmRunner execute ineligible" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../actions/testdata/rules_engine.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var result = try runner.execute(&module, "{\"age\": 15, \"country\": \"US\"}");
    defer result.deinit();

    try std.testing.expectEqualStrings(
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":1}",
        result.outputStr(),
    );
}

test "WasmRunner multiple executions" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../actions/testdata/rules_engine.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    const inputs = [_][]const u8{
        "{\"age\": 25, \"country\": \"US\"}",
        "{\"age\": 10, \"country\": \"UK\"}",
        "{\"age\": 30, \"country\": \"US\"}",
    };

    const expected = [_][]const u8{
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
        "{\"eligible\":false,\"rules_evaluated\":2,\"rules_passed\":0}",
        "{\"eligible\":true,\"rules_evaluated\":2,\"rules_passed\":2}",
    };

    for (inputs, expected) |input, exp| {
        var result = try runner.execute(&module, input);
        defer result.deinit();
        try std.testing.expectEqualStrings(exp, result.outputStr());
    }
}

test "WasmRunner describe" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../actions/testdata/rules_engine.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    const desc = try runner.describe(&module) orelse return error.NoDescription;
    defer allocator.free(desc);

    try std.testing.expect(std.mem.indexOf(u8, desc, "rules-engine") != null);
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

const TestTagRegistry = @import("../processing/definition.zig").TagRegistry;

fn testTagResolve(ctx_ptr: *anyopaque, tag_name: []const u8) ?u5 {
    const r: *const TestTagRegistry = @ptrCast(@alignCast(ctx_ptr));
    return r.resolve(tag_name);
}

test "WasmRunner set_tag via txn_classifier" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../processing/testdata/txn_classifier.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var reg: TestTagRegistry = .{};
    _ = reg.getOrCreate("high-value"); // bit 0
    _ = reg.getOrCreate("refund"); // bit 1
    _ = reg.getOrCreate("standard"); // bit 2

    var ctx = WasmKvContext{
        .namespace = "",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = allocator,
        .kv_enabled = false,
        .tag_resolve_fn = testTagResolve,
        .tag_resolve_ctx = @ptrCast(@constCast(&reg)),
        .output_tags = 0,
    };

    var result = try runner.executeWithKv(&module, "{\"txn_id\": \"T1\", \"amount\": 15000, \"merchant\": \"ACME\"}", &ctx);
    defer result.deinit();

    // Should have set "high-value" tag (bit 0)
    try std.testing.expectEqual(@as(u32, 0b001), ctx.output_tags);

    // Output should contain the classification
    try std.testing.expect(std.mem.indexOf(u8, result.outputStr(), "\"class\":\"high-value\"") != null);
}

test "WasmRunner set_tag refund classification" {
    const allocator = std.testing.allocator;
    const wasm_bytes = @embedFile("../processing/testdata/txn_classifier.wasm");

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var reg: TestTagRegistry = .{};
    _ = reg.getOrCreate("high-value"); // bit 0
    _ = reg.getOrCreate("refund"); // bit 1
    _ = reg.getOrCreate("standard"); // bit 2

    var ctx = WasmKvContext{
        .namespace = "",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = allocator,
        .kv_enabled = false,
        .tag_resolve_fn = testTagResolve,
        .tag_resolve_ctx = @ptrCast(@constCast(&reg)),
        .output_tags = 0,
    };

    var result = try runner.executeWithKv(&module, "{\"txn_id\": \"R1\", \"amount\": -500, \"merchant\": \"STORE\"}", &ctx);
    defer result.deinit();

    // Should have set "refund" tag (bit 1)
    try std.testing.expectEqual(@as(u32, 0b010), ctx.output_tags);
    try std.testing.expect(std.mem.indexOf(u8, result.outputStr(), "\"class\":\"refund\"") != null);
}

test "WasmRunner — filter: handle returns 0 sets filtered flag" {
    const allocator = std.testing.allocator;

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    const wasm_bytes = @embedFile("../processing/testdata/txn_enricher.wasm");
    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var ctx = WasmKvContext{
        .namespace = "",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = allocator,
        .kv_enabled = false,
    };

    // Missing "amount" field → handle returns 0 → filter
    var result = try runner.executeWithKv(&module, "{\"txn_id\": \"X1\", \"merchant\": \"NONE\"}", &ctx);
    defer result.deinit();

    try std.testing.expect(result.filtered);
    try std.testing.expectEqual(@as(usize, 0), result.output.len);
}

test "WasmRunner — emit: flo.emit() collects multiple records" {
    const allocator = std.testing.allocator;

    var runner = try WasmRunner.init(allocator);
    defer runner.deinit();

    const wasm_bytes = @embedFile("../processing/testdata/txn_enricher.wasm");
    var module = try runner.loadModule(wasm_bytes, .{});
    defer module.deinit();

    var reg: TestTagRegistry = .{};
    _ = reg.getOrCreate("high-value");
    _ = reg.getOrCreate("standard");

    var ctx = WasmKvContext{
        .namespace = "",
        .kv_dispatch_fn = null,
        .kv_dispatch_ctx = null,
        .allocator = allocator,
        .kv_enabled = false,
        .tag_resolve_fn = testTagResolve,
        .tag_resolve_ctx = @ptrCast(@constCast(&reg)),
        .output_tags = 0,
    };

    // High-value transaction → WASM calls flo.emit() twice
    var result = try runner.executeWithKv(&module, "{\"txn_id\": \"HV1\", \"amount\": 50000, \"merchant\": \"BIG\"}", &ctx);
    defer result.deinit();

    // emit_called should be true
    try std.testing.expect(ctx.emit_called);

    // Should have 2 emitted records
    try std.testing.expectEqual(@as(usize, 2), ctx.emitted_records.items.len);

    // Clean up emitted records
    defer {
        for (ctx.emitted_records.items) |er| {
            allocator.free(er.data);
        }
        ctx.emitted_records.deinit(allocator);
    }

    // First: enriched record with class
    try std.testing.expect(std.mem.indexOf(u8, ctx.emitted_records.items[0].data, "\"class\":\"high-value\"") != null);
    // Second: alert record
    try std.testing.expect(std.mem.indexOf(u8, ctx.emitted_records.items[1].data, "\"alert\":\"high-value-txn\"") != null);

    // Should have "high-value" tag set
    try std.testing.expectEqual(@as(u32, 0b01), ctx.output_tags);
}
