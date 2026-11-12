//! Workflow Core Types
//!
//! Fundamental types for workflow execution state management.
//!
//! # Key Types
//!
//! - `RunStatus`: Workflow run lifecycle states
//! - `Signal`: External event received by workflow
//! - `Timer`: Scheduled future event
//! - `SearchAttributes`: Custom queryable fields for business data
//!
//! # Wire Format
//!
//! All types support binary serialization for efficient storage and network transfer.
//! Format: [version:u8][field_count:u16][fields...]
//! Strings: [len:u16][bytes...]
//! Optional: [present:u8][value...]?

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

// =============================================================================
// Run Status
// =============================================================================

/// Status of a workflow run
pub const RunStatus = enum(u8) {
    /// Initial state, workflow created but not started
    pending = 0,
    /// Actively executing steps
    running = 1,
    /// Waiting for external input (signal, approval, human task)
    waiting = 2,
    /// Successfully completed (reached terminal with completed status)
    completed = 3,
    /// Failed (reached terminal with failed status)
    failed = 4,
    /// Cancelled by user
    cancelled = 5,
    /// Timed out (workflow-level or signal timeout)
    timed_out = 6,

    pub fn toString(self: RunStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .running => "running",
            .waiting => "waiting",
            .completed => "completed",
            .failed => "failed",
            .cancelled => "cancelled",
            .timed_out => "timed_out",
        };
    }

    pub fn fromString(s: []const u8) ?RunStatus {
        const map = std.StaticStringMap(RunStatus).initComptime(.{
            .{ "pending", .pending },
            .{ "running", .running },
            .{ "waiting", .waiting },
            .{ "completed", .completed },
            .{ "failed", .failed },
            .{ "cancelled", .cancelled },
            .{ "timed_out", .timed_out },
        });
        return map.get(s);
    }

    /// Returns true if this is a terminal status (no more transitions)
    pub fn isTerminal(self: RunStatus) bool {
        return switch (self) {
            .pending, .running, .waiting => false,
            .completed, .failed, .cancelled, .timed_out => true,
        };
    }

    /// Returns true if workflow is blocked waiting for external input
    pub fn isWaiting(self: RunStatus) bool {
        return self == .waiting;
    }

    /// Returns true if workflow can accept signals
    pub fn canReceiveSignal(self: RunStatus) bool {
        return self == .waiting or self == .running;
    }

    /// Returns true if workflow is in an active (non-terminal) state
    pub fn isActive(self: RunStatus) bool {
        return !self.isTerminal();
    }
};

// =============================================================================
// Wait Context
// =============================================================================

/// Type of wait operation
pub const WaitType = enum(u8) {
    /// Waiting for a signal (generic external event)
    signal = 0,
    /// Waiting for human approval
    approval = 1,
    /// Waiting for external system callback/webhook
    callback = 2,
    /// Waiting for a scheduled timer to fire
    timer = 3,
    /// Waiting for child workflow to complete
    child_workflow = 4,
    /// Waiting for an action to complete (async action execution)
    awaiting_action = 5,
    /// Polling an action with backoff (waiting for terminal outcome)
    polling = 6,

    pub fn toString(self: WaitType) []const u8 {
        return switch (self) {
            .signal => "signal",
            .approval => "approval",
            .callback => "callback",
            .timer => "timer",
            .child_workflow => "child_workflow",
            .awaiting_action => "awaiting_action",
            .polling => "polling",
        };
    }

    pub fn fromString(s: []const u8) ?WaitType {
        const map = std.StaticStringMap(WaitType).initComptime(.{
            .{ "signal", .signal },
            .{ "approval", .approval },
            .{ "callback", .callback },
            .{ "timer", .timer },
            .{ "child_workflow", .child_workflow },
            .{ "awaiting_action", .awaiting_action },
            .{ "polling", .polling },
        });
        return map.get(s);
    }
};

/// Context about what a workflow is waiting for
/// Stored when workflow enters waiting status
///
/// Wire format (version 1):
/// [version:u8]
/// [wait_type:u8]
/// [signal_type_len:u16][signal_type]
/// [wait_started_ms:i64]
/// [has_timeout_ms:u8][timeout_ms:i64]?
/// [has_timeout_at_ms:u8][timeout_at_ms:i64]?
/// [has_description:u8][description_len:u16]?[description]?
/// [has_approver_id:u8][approver_id_len:u16]?[approver_id]?
/// [has_callback_url:u8][callback_url_len:u16]?[callback_url]?
/// [has_child_run_id:u8][child_run_id_len:u16]?[child_run_id]?
pub const WaitContext = struct {
    /// Type of wait
    wait_type: WaitType,
    /// Signal type we're waiting for (e.g., "approval_decision")
    signal_type: []const u8,
    /// When the wait started (ms since epoch)
    wait_started_ms: i64,
    /// Timeout duration in milliseconds (from definition)
    timeout_ms: ?i64,
    /// Absolute time when wait will timeout (computed)
    timeout_at_ms: ?i64,
    /// Human-readable description of what we're waiting for
    description: ?[]const u8,
    /// For approval: who should approve
    approver_id: ?[]const u8,
    /// For callback: URL that will be called
    callback_url: ?[]const u8,
    /// For child workflow: the child's run ID
    child_run_id: ?[]const u8,
    /// For awaiting_action: the action run ID we're waiting for
    action_run_id: ?[]const u8 = null,
    /// For awaiting_action: the step name that invoked the action
    pending_step_name: ?[]const u8 = null,
    /// For polling: current poll attempt (0-indexed)
    poll_attempt: u32 = 0,
    /// For polling: max poll attempts configured
    poll_max_attempts: u32 = 0,
    /// For polling: next poll scheduled at (ms since epoch)
    poll_next_at_ms: ?i64 = null,

    const WIRE_VERSION: u8 = 1;

    pub fn deinit(self: *WaitContext, allocator: Allocator) void {
        allocator.free(self.signal_type);
        if (self.description) |d| allocator.free(d);
        if (self.approver_id) |a| allocator.free(a);
        if (self.callback_url) |u| allocator.free(u);
        if (self.child_run_id) |c| allocator.free(c);
        if (self.action_run_id) |a| allocator.free(a);
        if (self.pending_step_name) |s| allocator.free(s);
    }

    pub fn clone(self: WaitContext, allocator: Allocator) Allocator.Error!WaitContext {
        return .{
            .wait_type = self.wait_type,
            .signal_type = try allocator.dupe(u8, self.signal_type),
            .wait_started_ms = self.wait_started_ms,
            .timeout_ms = self.timeout_ms,
            .timeout_at_ms = self.timeout_at_ms,
            .description = if (self.description) |d| try allocator.dupe(u8, d) else null,
            .approver_id = if (self.approver_id) |a| try allocator.dupe(u8, a) else null,
            .callback_url = if (self.callback_url) |u| try allocator.dupe(u8, u) else null,
            .child_run_id = if (self.child_run_id) |c| try allocator.dupe(u8, c) else null,
            .action_run_id = if (self.action_run_id) |a| try allocator.dupe(u8, a) else null,
            .pending_step_name = if (self.pending_step_name) |s| try allocator.dupe(u8, s) else null,
            .poll_attempt = self.poll_attempt,
            .poll_max_attempts = self.poll_max_attempts,
            .poll_next_at_ms = self.poll_next_at_ms,
        };
    }

    /// Create a signal wait context
    pub fn forSignal(allocator: Allocator, signal_type: []const u8, timeout_ms: ?i64, now_ms: i64) Allocator.Error!WaitContext {
        return .{
            .wait_type = .signal,
            .signal_type = try allocator.dupe(u8, signal_type),
            .wait_started_ms = now_ms,
            .timeout_ms = timeout_ms,
            .timeout_at_ms = if (timeout_ms) |t| now_ms +| t else null,
            .description = null,
            .approver_id = null,
            .callback_url = null,
            .child_run_id = null,
        };
    }

    /// Create an approval wait context
    pub fn forApproval(allocator: Allocator, signal_type: []const u8, approver_id: ?[]const u8, description: ?[]const u8, timeout_ms: ?i64, now_ms: i64) Allocator.Error!WaitContext {
        return .{
            .wait_type = .approval,
            .signal_type = try allocator.dupe(u8, signal_type),
            .wait_started_ms = now_ms,
            .timeout_ms = timeout_ms,
            .timeout_at_ms = if (timeout_ms) |t| now_ms +| t else null,
            .description = if (description) |d| try allocator.dupe(u8, d) else null,
            .approver_id = if (approver_id) |a| try allocator.dupe(u8, a) else null,
            .callback_url = null,
            .child_run_id = null,
        };
    }

    /// Check if wait has timed out
    pub fn isTimedOut(self: WaitContext, now_ms: i64) bool {
        if (self.timeout_at_ms) |timeout_at| {
            return now_ms >= timeout_at;
        }
        return false;
    }

    /// Get remaining time until timeout (or null if no timeout)
    pub fn remainingMs(self: WaitContext, now_ms: i64) ?i64 {
        if (self.timeout_at_ms) |timeout_at| {
            const remaining = timeout_at - now_ms;
            return if (remaining > 0) remaining else 0;
        }
        return null;
    }

    /// Create an action wait context (for async action execution)
    pub fn forAction(allocator: Allocator, action_run_id: []const u8, step_name: []const u8, timeout_ms: ?i64, now_ms: i64) Allocator.Error!WaitContext {
        return .{
            .wait_type = .awaiting_action,
            .signal_type = try allocator.dupe(u8, "action_completed"), // synthetic signal type
            .wait_started_ms = now_ms,
            .timeout_ms = timeout_ms,
            .timeout_at_ms = if (timeout_ms) |t| now_ms +| t else null,
            .description = null,
            .approver_id = null,
            .callback_url = null,
            .child_run_id = null,
            .action_run_id = try allocator.dupe(u8, action_run_id),
            .pending_step_name = try allocator.dupe(u8, step_name),
        };
    }

    /// Create a polling wait context (for polling on pending outcome)
    pub fn forPolling(
        allocator: Allocator,
        step_name: []const u8,
        attempt: u32,
        max_attempts: u32,
        next_poll_at_ms: i64,
        now_ms: i64,
    ) Allocator.Error!WaitContext {
        return .{
            .wait_type = .polling,
            .signal_type = try allocator.dupe(u8, "poll_timer"), // synthetic signal type
            .wait_started_ms = now_ms,
            .timeout_ms = null,
            .timeout_at_ms = null,
            .description = null,
            .approver_id = null,
            .callback_url = null,
            .child_run_id = null,
            .action_run_id = null,
            .pending_step_name = try allocator.dupe(u8, step_name),
            .poll_attempt = attempt,
            .poll_max_attempts = max_attempts,
            .poll_next_at_ms = next_poll_at_ms,
        };
    }

    /// Create a child workflow wait context (for awaiting child workflow completion)
    pub fn forChildWorkflow(
        allocator: Allocator,
        child_run_id: []const u8,
        step_name: []const u8,
        timeout_ms: ?i64,
        now_ms: i64,
    ) Allocator.Error!WaitContext {
        return .{
            .wait_type = .child_workflow,
            .signal_type = try allocator.dupe(u8, "child_workflow_completed"), // synthetic signal type
            .wait_started_ms = now_ms,
            .timeout_ms = timeout_ms,
            .timeout_at_ms = if (timeout_ms) |t| now_ms +| t else null,
            .description = null,
            .approver_id = null,
            .callback_url = null,
            .child_run_id = try allocator.dupe(u8, child_run_id),
            .action_run_id = null,
            .pending_step_name = try allocator.dupe(u8, step_name),
        };
    }

    /// Encode to wire format
    pub fn encode(self: WaitContext, allocator: Allocator) Allocator.Error![]u8 {
        var aw: std.Io.Writer.Allocating = .init(allocator);
        errdefer aw.deinit();

        const writer = &aw.writer;

        // Version
        writer.writeByte(WIRE_VERSION) catch return error.OutOfMemory;

        // Wait type
        writer.writeByte(@backingInt(self.wait_type)) catch return error.OutOfMemory;

        // Signal type
        writer.writeInt(u16, @intCast(self.signal_type.len), .big) catch return error.OutOfMemory;
        writer.writeAll(self.signal_type) catch return error.OutOfMemory;

        // Wait started
        writer.writeInt(i64, self.wait_started_ms, .big) catch return error.OutOfMemory;

        // Timeout ms (optional)
        if (self.timeout_ms) |t| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(i64, t, .big) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Timeout at ms (optional)
        if (self.timeout_at_ms) |t| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(i64, t, .big) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Description (optional)
        if (self.description) |d| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(d.len), .big) catch return error.OutOfMemory;
            writer.writeAll(d) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Approver ID (optional)
        if (self.approver_id) |a| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(a.len), .big) catch return error.OutOfMemory;
            writer.writeAll(a) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Callback URL (optional)
        if (self.callback_url) |u| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(u.len), .big) catch return error.OutOfMemory;
            writer.writeAll(u) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Child run ID (optional)
        if (self.child_run_id) |c| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(c.len), .big) catch return error.OutOfMemory;
            writer.writeAll(c) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Action run ID (optional)
        if (self.action_run_id) |a| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(a.len), .big) catch return error.OutOfMemory;
            writer.writeAll(a) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        // Pending step name (optional)
        if (self.pending_step_name) |s| {
            writer.writeByte(1) catch return error.OutOfMemory;
            writer.writeInt(u16, @intCast(s.len), .big) catch return error.OutOfMemory;
            writer.writeAll(s) catch return error.OutOfMemory;
        } else {
            writer.writeByte(0) catch return error.OutOfMemory;
        }

        return aw.toOwnedSlice() catch return error.OutOfMemory;
    }

    /// Decode from wire format
    pub fn decode(allocator: Allocator, data: []const u8) !WaitContext {
        if (data.len < 1) return error.InvalidData;

        var pos: usize = 0;

        // Version
        const version = data[pos];
        pos += 1;
        if (version != WIRE_VERSION) return error.UnsupportedVersion;

        // Wait type
        if (pos >= data.len) return error.InvalidData;
        const wait_type: WaitType = @fromBackingInt(@intCast(data[pos]));
        pos += 1;

        // Signal type
        if (pos + 2 > data.len) return error.InvalidData;
        const signal_type_len = std.mem.readInt(u16, data[pos..][0..2], .big);
        pos += 2;
        if (pos + signal_type_len > data.len) return error.InvalidData;
        const signal_type = try allocator.dupe(u8, data[pos .. pos + signal_type_len]);
        errdefer allocator.free(signal_type);
        pos += signal_type_len;

        // Wait started
        if (pos + 8 > data.len) return error.InvalidData;
        const wait_started_ms = std.mem.readInt(i64, data[pos..][0..8], .big);
        pos += 8;

        // Timeout ms
        if (pos >= data.len) return error.InvalidData;
        const has_timeout_ms = data[pos] == 1;
        pos += 1;
        const timeout_ms: ?i64 = if (has_timeout_ms) blk: {
            if (pos + 8 > data.len) return error.InvalidData;
            const val = std.mem.readInt(i64, data[pos..][0..8], .big);
            pos += 8;
            break :blk val;
        } else null;

        // Timeout at ms
        if (pos >= data.len) return error.InvalidData;
        const has_timeout_at_ms = data[pos] == 1;
        pos += 1;
        const timeout_at_ms: ?i64 = if (has_timeout_at_ms) blk: {
            if (pos + 8 > data.len) return error.InvalidData;
            const val = std.mem.readInt(i64, data[pos..][0..8], .big);
            pos += 8;
            break :blk val;
        } else null;

        // Description
        if (pos >= data.len) return error.InvalidData;
        const has_description = data[pos] == 1;
        pos += 1;
        const description: ?[]const u8 = if (has_description) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;
        errdefer if (description) |d| allocator.free(d);

        // Approver ID
        if (pos >= data.len) return error.InvalidData;
        const has_approver_id = data[pos] == 1;
        pos += 1;
        const approver_id: ?[]const u8 = if (has_approver_id) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;
        errdefer if (approver_id) |a| allocator.free(a);

        // Callback URL
        if (pos >= data.len) return error.InvalidData;
        const has_callback_url = data[pos] == 1;
        pos += 1;
        const callback_url: ?[]const u8 = if (has_callback_url) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;
        errdefer if (callback_url) |u| allocator.free(u);

        // Child run ID
        if (pos >= data.len) return error.InvalidData;
        const has_child_run_id = data[pos] == 1;
        pos += 1;
        const child_run_id: ?[]const u8 = if (has_child_run_id) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;
        errdefer if (child_run_id) |c| allocator.free(c);

        // Action run ID
        if (pos >= data.len) return error.InvalidData;
        const has_action_run_id = data[pos] == 1;
        pos += 1;
        const action_run_id: ?[]const u8 = if (has_action_run_id) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;
        errdefer if (action_run_id) |a| allocator.free(a);

        // Pending step name
        if (pos >= data.len) return error.InvalidData;
        const has_pending_step_name = data[pos] == 1;
        pos += 1;
        const pending_step_name: ?[]const u8 = if (has_pending_step_name) blk: {
            if (pos + 2 > data.len) return error.InvalidData;
            const len = std.mem.readInt(u16, data[pos..][0..2], .big);
            pos += 2;
            if (pos + len > data.len) return error.InvalidData;
            const val = try allocator.dupe(u8, data[pos .. pos + len]);
            pos += len;
            break :blk val;
        } else null;

        return WaitContext{
            .wait_type = wait_type,
            .signal_type = signal_type,
            .wait_started_ms = wait_started_ms,
            .timeout_ms = timeout_ms,
            .timeout_at_ms = timeout_at_ms,
            .description = description,
            .approver_id = approver_id,
            .callback_url = callback_url,
            .child_run_id = child_run_id,
            .action_run_id = action_run_id,
            .pending_step_name = pending_step_name,
        };
    }
};

/// Approval status for human workflow tasks
pub const ApprovalStatus = enum(u8) {
    pending = 0,
    approved = 1,
    rejected = 2,
    escalated = 3,

    pub fn toString(self: ApprovalStatus) []const u8 {
        return switch (self) {
            .pending => "pending",
            .approved => "approved",
            .rejected => "rejected",
            .escalated => "escalated",
        };
    }

    pub fn fromString(s: []const u8) ?ApprovalStatus {
        const map = std.StaticStringMap(ApprovalStatus).initComptime(.{
            .{ "pending", .pending },
            .{ "approved", .approved },
            .{ "rejected", .rejected },
            .{ "escalated", .escalated },
        });
        return map.get(s);
    }
};

// =============================================================================
// Search Attributes
// =============================================================================

/// String attribute for exact match queries
pub const StringAttr = struct {
    key: []const u8,
    value: []const u8,

    pub fn deinit(self: *StringAttr, allocator: Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }

    pub fn clone(self: StringAttr, allocator: Allocator) !StringAttr {
        return .{
            .key = try allocator.dupe(u8, self.key),
            .value = try allocator.dupe(u8, self.value),
        };
    }
};

/// Numeric attribute for range queries
pub const NumericAttr = struct {
    key: []const u8,
    value: i64,

    pub fn deinit(self: *NumericAttr, allocator: Allocator) void {
        allocator.free(self.key);
    }

    pub fn clone(self: NumericAttr, allocator: Allocator) !NumericAttr {
        return .{
            .key = try allocator.dupe(u8, self.key),
            .value = self.value,
        };
    }
};

/// Timestamp attribute for time-based queries
pub const TimestampAttr = struct {
    key: []const u8,
    value: i64, // milliseconds since epoch

    pub fn deinit(self: *TimestampAttr, allocator: Allocator) void {
        allocator.free(self.key);
    }

    pub fn clone(self: TimestampAttr, allocator: Allocator) !TimestampAttr {
        return .{
            .key = try allocator.dupe(u8, self.key),
            .value = self.value,
        };
    }
};

/// Custom searchable fields set by workflow definition
/// Extracted from input JSON based on `search_attributes` config
pub const SearchAttributes = struct {
    /// String attributes (indexed for exact match)
    string_attrs: []StringAttr,
    /// Numeric attributes (indexed for range queries)
    numeric_attrs: []NumericAttr,
    /// Timestamp attributes (indexed for time-based queries)
    timestamp_attrs: []TimestampAttr,

    pub fn init() SearchAttributes {
        return .{
            .string_attrs = &[_]StringAttr{},
            .numeric_attrs = &[_]NumericAttr{},
            .timestamp_attrs = &[_]TimestampAttr{},
        };
    }

    pub fn deinit(self: *SearchAttributes, allocator: Allocator) void {
        for (self.string_attrs) |*attr| {
            attr.deinit(allocator);
        }
        if (self.string_attrs.len > 0) allocator.free(self.string_attrs);

        for (self.numeric_attrs) |*attr| {
            attr.deinit(allocator);
        }
        if (self.numeric_attrs.len > 0) allocator.free(self.numeric_attrs);

        for (self.timestamp_attrs) |*attr| {
            attr.deinit(allocator);
        }
        if (self.timestamp_attrs.len > 0) allocator.free(self.timestamp_attrs);
    }

    pub fn clone(self: SearchAttributes, allocator: Allocator) !SearchAttributes {
        var result: SearchAttributes = .{
            .string_attrs = &[_]StringAttr{},
            .numeric_attrs = &[_]NumericAttr{},
            .timestamp_attrs = &[_]TimestampAttr{},
        };

        if (self.string_attrs.len > 0) {
            result.string_attrs = try allocator.alloc(StringAttr, self.string_attrs.len);
            for (self.string_attrs, 0..) |attr, i| {
                result.string_attrs[i] = try attr.clone(allocator);
            }
        }

        if (self.numeric_attrs.len > 0) {
            result.numeric_attrs = try allocator.alloc(NumericAttr, self.numeric_attrs.len);
            for (self.numeric_attrs, 0..) |attr, i| {
                result.numeric_attrs[i] = try attr.clone(allocator);
            }
        }

        if (self.timestamp_attrs.len > 0) {
            result.timestamp_attrs = try allocator.alloc(TimestampAttr, self.timestamp_attrs.len);
            for (self.timestamp_attrs, 0..) |attr, i| {
                result.timestamp_attrs[i] = try attr.clone(allocator);
            }
        }

        return result;
    }

    /// Get string attribute by key
    pub fn getString(self: SearchAttributes, key: []const u8) ?[]const u8 {
        for (self.string_attrs) |attr| {
            if (mem.eql(u8, attr.key, key)) return attr.value;
        }
        return null;
    }

    /// Get numeric attribute by key
    pub fn getNumeric(self: SearchAttributes, key: []const u8) ?i64 {
        for (self.numeric_attrs) |attr| {
            if (mem.eql(u8, attr.key, key)) return attr.value;
        }
        return null;
    }

    /// Get timestamp attribute by key
    pub fn getTimestamp(self: SearchAttributes, key: []const u8) ?i64 {
        for (self.timestamp_attrs) |attr| {
            if (mem.eql(u8, attr.key, key)) return attr.value;
        }
        return null;
    }
};

// =============================================================================
// Step Output Map
// =============================================================================

/// Single step's output entry
pub const StepOutput = struct {
    /// Step name (e.g., "_start", "enrich_company")
    step_name: []const u8,
    /// Output JSON from the step
    output: []const u8,
    /// Outcome (e.g., "success", "failure")
    outcome: []const u8,

    pub fn deinit(self: *StepOutput, allocator: Allocator) void {
        allocator.free(self.step_name);
        allocator.free(self.output);
        allocator.free(self.outcome);
    }

    pub fn clone(self: StepOutput, allocator: Allocator) !StepOutput {
        return .{
            .step_name = try allocator.dupe(u8, self.step_name),
            .output = try allocator.dupe(u8, self.output),
            .outcome = try allocator.dupe(u8, self.outcome),
        };
    }
};

/// Map of step outputs for accessing previous step results via $.steps.*
/// Used for JSONPath resolution in workflow execution
pub const StepOutputMap = struct {
    /// Array of step outputs (ordered by execution)
    entries: []StepOutput,

    pub fn init() StepOutputMap {
        return .{
            .entries = &[_]StepOutput{},
        };
    }

    pub fn deinit(self: *StepOutputMap, allocator: Allocator) void {
        for (self.entries) |*entry| {
            entry.deinit(allocator);
        }
        if (self.entries.len > 0) allocator.free(self.entries);
    }

    pub fn clone(self: StepOutputMap, allocator: Allocator) !StepOutputMap {
        if (self.entries.len == 0) {
            return .{ .entries = &[_]StepOutput{} };
        }

        const entries = try allocator.alloc(StepOutput, self.entries.len);
        for (self.entries, 0..) |entry, i| {
            entries[i] = try entry.clone(allocator);
        }
        return .{ .entries = entries };
    }

    /// Get output for a step by name
    pub fn get(self: StepOutputMap, step_name: []const u8) ?*const StepOutput {
        for (self.entries) |*entry| {
            if (mem.eql(u8, entry.step_name, step_name)) return entry;
        }
        return null;
    }

    /// Add or update a step's output
    pub fn put(self: *StepOutputMap, allocator: Allocator, step_name: []const u8, output: []const u8, outcome: []const u8) !void {
        // Check if step already exists (update case)
        for (self.entries) |*entry| {
            if (mem.eql(u8, entry.step_name, step_name)) {
                // Update existing entry
                allocator.free(entry.output);
                allocator.free(entry.outcome);
                entry.output = try allocator.dupe(u8, output);
                entry.outcome = try allocator.dupe(u8, outcome);
                return;
            }
        }

        // Add new entry
        const new_len = self.entries.len + 1;
        const new_entries = try allocator.alloc(StepOutput, new_len);

        // Copy old entries
        for (self.entries, 0..) |entry, i| {
            new_entries[i] = entry;
        }

        // Add new entry
        new_entries[new_len - 1] = .{
            .step_name = try allocator.dupe(u8, step_name),
            .output = try allocator.dupe(u8, output),
            .outcome = try allocator.dupe(u8, outcome),
        };

        // Free old array (but not entries, they're moved)
        if (self.entries.len > 0) allocator.free(self.entries);
        self.entries = new_entries;
    }
};

// =============================================================================
// Signal
// =============================================================================

/// Signal received by a workflow
/// Stored at: _wf:run:{namespace}:{workflow}:{run_id}:sig:{signal_id}
pub const Signal = struct {
    /// Unique signal identifier
    signal_id: []const u8,
    /// Signal type (matches wait_for_signal.type in definition)
    signal_type: []const u8,
    /// Optional payload JSON
    payload: ?[]const u8,
    /// When the signal was received (ms since epoch)
    received_at_ms: i64,
    /// Whether the signal has been consumed by a step
    consumed: bool,

    const WIRE_VERSION: u8 = 1;

    pub fn deinit(self: *Signal, allocator: Allocator) void {
        allocator.free(self.signal_id);
        allocator.free(self.signal_type);
        if (self.payload) |p| allocator.free(p);
    }

    pub fn clone(self: Signal, allocator: Allocator) !Signal {
        return .{
            .signal_id = try allocator.dupe(u8, self.signal_id),
            .signal_type = try allocator.dupe(u8, self.signal_type),
            .payload = if (self.payload) |p| try allocator.dupe(u8, p) else null,
            .received_at_ms = self.received_at_ms,
            .consumed = self.consumed,
        };
    }

    pub fn encode(self: Signal, allocator: Allocator) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);

        try buf.append(allocator, WIRE_VERSION);
        try writeString(&buf, allocator, self.signal_id);
        try writeString(&buf, allocator, self.signal_type);
        try writeOptionalLargeString(&buf, allocator, self.payload);
        try writeI64(&buf, allocator, self.received_at_ms);
        try buf.append(allocator, if (self.consumed) 1 else 0);

        return buf.toOwnedSlice(allocator);
    }

    pub fn decode(allocator: Allocator, data: []const u8) !Signal {
        if (data.len < 1) return error.InvalidData;
        var pos: usize = 0;

        const version = data[pos];
        pos += 1;
        if (version != WIRE_VERSION) return error.UnsupportedVersion;

        const signal_id = try readString(allocator, data, &pos);
        errdefer allocator.free(signal_id);
        const signal_type = try readString(allocator, data, &pos);
        errdefer allocator.free(signal_type);
        const payload = try readOptionalLargeString(allocator, data, &pos);
        errdefer if (payload) |p| allocator.free(p);
        const received_at_ms = try readI64(data, &pos);

        if (pos >= data.len) return error.InvalidData;
        const consumed = data[pos] == 1;

        return .{
            .signal_id = signal_id,
            .signal_type = signal_type,
            .payload = payload,
            .received_at_ms = received_at_ms,
            .consumed = consumed,
        };
    }
};

// =============================================================================
// Timer
// =============================================================================

/// Type of timer
pub const TimerType = enum(u8) {
    /// Signal timeout (workflow waiting for signal)
    signal_timeout = 0,
    /// Retry backoff delay
    retry_backoff = 1,
    /// Overall workflow timeout
    workflow_timeout = 2,
    /// Async tracking timeout (waiting for webhook)
    async_tracking_timeout = 3,
    /// Poll backoff (waiting between poll attempts for pending outcome)
    poll_backoff = 4,

    pub fn toString(self: TimerType) []const u8 {
        return switch (self) {
            .signal_timeout => "signal_timeout",
            .retry_backoff => "retry_backoff",
            .workflow_timeout => "workflow_timeout",
            .async_tracking_timeout => "async_tracking_timeout",
            .poll_backoff => "poll_backoff",
        };
    }
};

/// Timer for delayed workflow events
/// Stored at: _wf:run:{namespace}:{workflow}:{run_id}:timer:{timer_id}
pub const Timer = struct {
    /// Unique timer identifier
    timer_id: []const u8,
    /// Type of timer
    timer_type: TimerType,
    /// When the timer should fire (ms since epoch)
    scheduled_for_ms: i64,
    /// Whether the timer has fired
    fired: bool,
    /// Optional payload (context for timer handler)
    payload: ?[]const u8,

    const WIRE_VERSION: u8 = 1;

    pub fn deinit(self: *Timer, allocator: Allocator) void {
        allocator.free(self.timer_id);
        if (self.payload) |p| allocator.free(p);
    }

    pub fn clone(self: Timer, allocator: Allocator) !Timer {
        return .{
            .timer_id = try allocator.dupe(u8, self.timer_id),
            .timer_type = self.timer_type,
            .scheduled_for_ms = self.scheduled_for_ms,
            .fired = self.fired,
            .payload = if (self.payload) |p| try allocator.dupe(u8, p) else null,
        };
    }

    pub fn encode(self: Timer, allocator: Allocator) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);

        try buf.append(allocator, WIRE_VERSION);
        try writeString(&buf, allocator, self.timer_id);
        try buf.append(allocator, @backingInt(self.timer_type));
        try writeI64(&buf, allocator, self.scheduled_for_ms);
        try buf.append(allocator, if (self.fired) 1 else 0);
        try writeOptionalLargeString(&buf, allocator, self.payload);

        return buf.toOwnedSlice(allocator);
    }

    pub fn decode(allocator: Allocator, data: []const u8) !Timer {
        if (data.len < 1) return error.InvalidData;
        var pos: usize = 0;

        const version = data[pos];
        pos += 1;
        if (version != WIRE_VERSION) return error.UnsupportedVersion;

        const timer_id = try readString(allocator, data, &pos);
        errdefer allocator.free(timer_id);

        if (pos >= data.len) return error.InvalidData;
        const timer_type: TimerType = @fromBackingInt(@intCast(data[pos]));
        pos += 1;

        const scheduled_for_ms = try readI64(data, &pos);

        if (pos >= data.len) return error.InvalidData;
        const fired = data[pos] == 1;
        pos += 1;

        const payload = try readOptionalLargeString(allocator, data, &pos);

        return .{
            .timer_id = timer_id,
            .timer_type = timer_type,
            .scheduled_for_ms = scheduled_for_ms,
            .fired = fired,
            .payload = payload,
        };
    }
};

// =============================================================================
// Wire Format Helpers
// =============================================================================

fn writeString(buf: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    const len: u16 = @intCast(@min(s.len, std.math.maxInt(u16)));
    var len_bytes: [2]u8 = undefined;
    mem.writeInt(u16, &len_bytes, len, .big);
    try buf.appendSlice(allocator, &len_bytes);
    try buf.appendSlice(allocator, s[0..len]);
}

fn writeOptionalLargeString(buf: *std.ArrayList(u8), allocator: Allocator, s: ?[]const u8) !void {
    if (s) |str| {
        try buf.append(allocator, 1);
        try writeLargeString(buf, allocator, str);
    } else {
        try buf.append(allocator, 0);
    }
}

fn writeLargeString(buf: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    const len: u32 = @intCast(@min(s.len, std.math.maxInt(u32)));
    var len_bytes: [4]u8 = undefined;
    mem.writeInt(u32, &len_bytes, len, .big);
    try buf.appendSlice(allocator, &len_bytes);
    try buf.appendSlice(allocator, s[0..len]);
}

fn writeI64(buf: *std.ArrayList(u8), allocator: Allocator, v: i64) !void {
    var bytes: [8]u8 = undefined;
    mem.writeInt(i64, &bytes, v, .big);
    try buf.appendSlice(allocator, &bytes);
}

fn readString(allocator: Allocator, data: []const u8, pos: *usize) ![]u8 {
    if (pos.* + 2 > data.len) return error.InvalidData;
    const len = mem.readInt(u16, data[pos.*..][0..2], .big);
    pos.* += 2;
    if (pos.* + len > data.len) return error.InvalidData;
    const result = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
    pos.* += len;
    return result;
}

fn readOptionalLargeString(allocator: Allocator, data: []const u8, pos: *usize) !?[]u8 {
    if (pos.* >= data.len) return error.InvalidData;
    const present = data[pos.*];
    pos.* += 1;
    if (present == 1) {
        if (pos.* + 4 > data.len) return error.InvalidData;
        const len = mem.readInt(u32, data[pos.*..][0..4], .big);
        pos.* += 4;
        if (pos.* + len > data.len) return error.InvalidData;
        const result = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
        pos.* += len;
        return result;
    }
    return null;
}

fn readI64(data: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > data.len) return error.InvalidData;
    const result = mem.readInt(i64, data[pos.*..][0..8], .big);
    pos.* += 8;
    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "RunStatus: toString and fromString" {
    const testing = std.testing;

    try testing.expectEqualStrings("pending", RunStatus.pending.toString());
    try testing.expectEqualStrings("running", RunStatus.running.toString());
    try testing.expectEqualStrings("completed", RunStatus.completed.toString());
    try testing.expectEqualStrings("failed", RunStatus.failed.toString());

    try testing.expectEqual(RunStatus.pending, RunStatus.fromString("pending").?);
    try testing.expectEqual(RunStatus.failed, RunStatus.fromString("failed").?);
    try testing.expectEqual(@as(?RunStatus, null), RunStatus.fromString("invalid"));
}

test "RunStatus: isTerminal" {
    const testing = std.testing;

    try testing.expect(!RunStatus.pending.isTerminal());
    try testing.expect(!RunStatus.running.isTerminal());
    try testing.expect(RunStatus.completed.isTerminal());
    try testing.expect(RunStatus.failed.isTerminal());
    try testing.expect(RunStatus.cancelled.isTerminal());
    try testing.expect(RunStatus.timed_out.isTerminal());
}

test "Signal: encode and decode roundtrip" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var signal = Signal{
        .signal_id = "sig-001",
        .signal_type = "payment.confirmed",
        .payload = "{\"txn_id\": \"xyz\"}",
        .received_at_ms = 1706140800000,
        .consumed = false,
    };

    const encoded = try signal.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try Signal.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try testing.expectEqualStrings("sig-001", decoded.signal_id);
    try testing.expectEqualStrings("payment.confirmed", decoded.signal_type);
    try testing.expectEqualStrings("{\"txn_id\": \"xyz\"}", decoded.payload.?);
    try testing.expectEqual(@as(i64, 1706140800000), decoded.received_at_ms);
    try testing.expect(!decoded.consumed);
}

test "Timer: encode and decode roundtrip" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var timer = Timer{
        .timer_id = "timer-001",
        .timer_type = .signal_timeout,
        .scheduled_for_ms = 1706140900000,
        .fired = false,
        .payload = "{\"step\": \"wait_approval\"}",
    };

    const encoded = try timer.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try Timer.decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try testing.expectEqualStrings("timer-001", decoded.timer_id);
    try testing.expectEqual(TimerType.signal_timeout, decoded.timer_type);
    try testing.expectEqual(@as(i64, 1706140900000), decoded.scheduled_for_ms);
    try testing.expect(!decoded.fired);
    try testing.expectEqualStrings("{\"step\": \"wait_approval\"}", decoded.payload.?);
}
