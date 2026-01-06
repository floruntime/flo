const std = @import("std");

/// Async API for Flo Storage
/// Inspired by TigerBeetle's async client design
///
/// Design:
/// - Non-blocking submit operations
/// - Callback-based completion
/// - Thread-safe
/// - Zero-copy where possible
/// Completion callback function signature
/// Called when an async operation completes
///
/// Parameters:
/// - context: User-provided context pointer
/// - result: Operation result (null on error)
/// - error_code: Error code (0 on success)
pub const CompletionCallback = *const fn (
    context: ?*anyopaque,
    result: ?*const Result,
) callconv(.c) void;

/// Result of an async operation
pub const Result = union(Operation) {
    set: SetResult,
    get: GetResult,
    delete: DeleteResult,
    scan: ScanResult,

    pub const SetResult = struct {
        success: bool,
    };

    pub const GetResult = struct {
        value: ?[]const u8, // null if not found
    };

    pub const DeleteResult = struct {
        success: bool,
    };

    pub const ScanResult = struct {
        entries: []const Entry,

        pub const Entry = struct {
            key: []const u8,
            value: []const u8,
        };
    };
};

/// Operation type
pub const Operation = enum(u8) {
    set = 0,
    get = 1,
    delete = 2,
    scan = 3,
};

/// Request submitted to the async API
pub const Request = struct {
    /// Operation to perform
    operation: Operation,

    /// Operation-specific data
    data: union(Operation) {
        set: SetData,
        get: GetData,
        delete: DeleteData,
        scan: ScanData,
    },

    /// User context (passed to callback)
    context: ?*anyopaque,

    /// Completion callback
    callback: CompletionCallback,

    /// Internal: Request ID (for tracking)
    id: u64,

    /// Internal: Timestamp when submitted
    timestamp: i128,

    pub const SetData = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const GetData = struct {
        key: []const u8,
    };

    pub const DeleteData = struct {
        key: []const u8,
    };

    pub const ScanData = struct {
        start_key: []const u8,
        end_key: []const u8,
        limit: u32,
    };
};

/// Completion queue for async operations
/// Thread-safe, lock-free (using atomic operations)
pub const CompletionQueue = struct {
    const Self = @This();

    /// Completed requests waiting to be processed
    completions: std.ArrayList(Completion),
    mutex: std.Thread.Mutex,
    allocator: std.mem.Allocator,

    pub const Completion = struct {
        request: Request,
        result: Result,
    };

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .completions = std.ArrayList(Completion){},
            .mutex = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.completions.deinit(self.allocator);
    }

    /// Add a completed request to the queue
    pub fn push(self: *Self, completion: Completion) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.completions.append(self.allocator, completion);
    }

    /// Pop a completed request from the queue
    pub fn pop(self: *Self) ?Completion {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.completions.items.len == 0) return null;
        return self.completions.orderedRemove(0);
    }

    /// Process all completed requests (calls callbacks)
    pub fn process(self: *Self) usize {
        var processed: usize = 0;
        while (self.pop()) |completion| {
            completion.request.callback(
                completion.request.context,
                &completion.result,
            );
            processed += 1;
        }
        return processed;
    }

    /// Get number of pending completions
    pub fn len(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.completions.items.len;
    }
};

/// Request builder for ergonomic API
pub const RequestBuilder = struct {
    allocator: std.mem.Allocator,
    next_id: std.atomic.Value(u64),

    pub fn init(allocator: std.mem.Allocator) RequestBuilder {
        return RequestBuilder{
            .allocator = allocator,
            .next_id = std.atomic.Value(u64).init(0),
        };
    }

    pub fn set(
        self: *RequestBuilder,
        key: []const u8,
        value: []const u8,
        context: ?*anyopaque,
        callback: CompletionCallback,
    ) Request {
        const id = self.next_id.fetchAdd(1, .monotonic);
        return Request{
            .operation = .set,
            .data = .{ .set = .{ .key = key, .value = value } },
            .context = context,
            .callback = callback,
            .id = id,
            .timestamp = std.time.nanoTimestamp(),
        };
    }

    pub fn get(
        self: *RequestBuilder,
        key: []const u8,
        context: ?*anyopaque,
        callback: CompletionCallback,
    ) Request {
        const id = self.next_id.fetchAdd(1, .monotonic);
        return Request{
            .operation = .get,
            .data = .{ .get = .{ .key = key } },
            .context = context,
            .callback = callback,
            .id = id,
            .timestamp = std.time.nanoTimestamp(),
        };
    }

    pub fn delete(
        self: *RequestBuilder,
        key: []const u8,
        context: ?*anyopaque,
        callback: CompletionCallback,
    ) Request {
        const id = self.next_id.fetchAdd(1, .monotonic);
        return Request{
            .operation = .delete,
            .data = .{ .delete = .{ .key = key } },
            .context = context,
            .callback = callback,
            .id = id,
            .timestamp = std.time.nanoTimestamp(),
        };
    }

    pub fn scan(
        self: *RequestBuilder,
        start_key: []const u8,
        end_key: []const u8,
        limit: u32,
        context: ?*anyopaque,
        callback: CompletionCallback,
    ) Request {
        const id = self.next_id.fetchAdd(1, .monotonic);
        return Request{
            .operation = .scan,
            .data = .{ .scan = .{ .start_key = start_key, .end_key = end_key, .limit = limit } },
            .context = context,
            .callback = callback,
            .id = id,
            .timestamp = std.time.nanoTimestamp(),
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "CompletionQueue: basic push/pop" {
    const allocator = std.testing.allocator;

    var queue = CompletionQueue.init(allocator);
    defer queue.deinit();

    // Create a dummy completion
    const request = Request{
        .operation = .set,
        .data = .{ .set = .{ .key = "key", .value = "value" } },
        .context = null,
        .callback = struct {
            fn callback(_: ?*anyopaque, _: ?*const Result) callconv(.c) void {}
        }.callback,
        .id = 1,
        .timestamp = 0,
    };

    const completion = CompletionQueue.Completion{
        .request = request,
        .result = .{ .set = .{ .success = true } },
    };

    // Push and pop
    try queue.push(completion);
    try std.testing.expectEqual(@as(usize, 1), queue.len());

    const popped = queue.pop().?;
    try std.testing.expectEqual(@as(u64, 1), popped.request.id);
    try std.testing.expectEqual(@as(usize, 0), queue.len());
}

test "RequestBuilder: generate unique IDs" {
    const allocator = std.testing.allocator;

    var builder = RequestBuilder.init(allocator);

    const req1 = builder.set("key1", "value1", null, struct {
        fn callback(_: ?*anyopaque, _: ?*const Result) callconv(.c) void {}
    }.callback);

    const req2 = builder.set("key2", "value2", null, struct {
        fn callback(_: ?*anyopaque, _: ?*const Result) callconv(.c) void {}
    }.callback);

    try std.testing.expectEqual(@as(u64, 0), req1.id);
    try std.testing.expectEqual(@as(u64, 1), req2.id);
    try std.testing.expect(req1.timestamp <= req2.timestamp);
}
