//! YAML Builder - Helper for constructing workflow YAML in tests
//!
//! Provides a fluent builder API to construct workflow YAML programmatically
//! for E2E and integration tests.
//!
//! ## Example Usage
//!
//! ```zig
//! const yaml = try YamlBuilder.init(allocator)
//!     .workflow("process-order", "1.0.0")
//!     .idempotency("required")
//!     .plans()
//!         .plan("payment", "health-weighted")
//!         .executor("stripe", "@actions/charge-stripe", 100)
//!         .executor("paypal", "@actions/charge-paypal", 90)
//!         .done()
//!     .done()
//!     .start("@actions/validate")
//!         .onSuccess("charge")
//!         .onFailure("flo.Failed")
//!     .step("charge")
//!         .run("@plan/payment")
//!         .onSuccess("flo.Completed")
//!         .onFailure("flo.Failed")
//!     .build();
//! defer allocator.free(yaml);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Helper to build workflow YAML programmatically for E2E tests
pub const YamlBuilder = struct {
    const Self = @This();

    allocator: Allocator,
    lines: std.ArrayList([]const u8),
    indent: usize,

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .lines = .empty,
            .indent = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.lines.items) |line| {
            self.allocator.free(line);
        }
        self.lines.deinit(self.allocator);
    }

    /// Start a new workflow definition
    pub fn workflow(self: *Self, name: []const u8, version: []const u8) *Self {
        self.appendLine("kind: Workflow") catch {};
        self.appendFmt("name: {s}", .{name}) catch {};
        self.appendFmt("version: \"{s}\"", .{version}) catch {};
        return self;
    }

    /// Set idempotency mode
    pub fn idempotency(self: *Self, mode: []const u8) *Self {
        self.appendFmt("idempotency: {s}", .{mode}) catch {};
        return self;
    }

    /// Start the plans section
    pub fn plans(self: *Self) *PlanSectionBuilder {
        self.appendLine("") catch {};
        self.appendLine("plans:") catch {};
        self.indent = 2;
        return @ptrCast(self);
    }

    /// Define the start step
    pub fn start(self: *Self, target: []const u8) *StepBuilder {
        self.indent = 0;
        self.appendLine("") catch {};
        self.appendLine("start:") catch {};
        self.indent = 2;
        self.appendFmt("run: \"{s}\"", .{target}) catch {};
        self.appendLine("transitions:") catch {};
        self.indent = 4;
        return @ptrCast(self);
    }

    /// Start the steps section
    pub fn steps(self: *Self) *Self {
        self.indent = 0;
        self.appendLine("") catch {};
        self.appendLine("steps:") catch {};
        return self;
    }

    /// Add a named step
    pub fn step(self: *Self, name: []const u8) *StepBuilder {
        self.indent = 2;
        self.appendFmt("{s}:", .{name}) catch {};
        self.indent = 4;
        return @ptrCast(self);
    }

    /// Define custom terminals
    pub fn terminals(self: *Self) *Self {
        self.indent = 0;
        self.appendLine("") catch {};
        self.appendLine("terminals:") catch {};
        self.indent = 2;
        return self;
    }

    /// Add a custom terminal
    pub fn terminal(self: *Self, name: []const u8, status: []const u8) *Self {
        self.appendFmt("{s}:", .{name}) catch {};
        self.indent = 4;
        self.appendFmt("status: {s}", .{status}) catch {};
        self.indent = 2;
        return self;
    }

    /// Build the final YAML string
    pub fn build(self: *Self) ![]const u8 {
        var total_len: usize = 0;
        for (self.lines.items) |line| {
            total_len += line.len + 1; // +1 for newline
        }

        const result = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        for (self.lines.items) |line| {
            @memcpy(result[pos .. pos + line.len], line);
            pos += line.len;
            result[pos] = '\n';
            pos += 1;
        }
        return result;
    }

    // Internal helpers
    fn appendLine(self: *Self, line: []const u8) !void {
        const indented = try self.indentedLine(line);
        try self.lines.append(self.allocator, indented);
    }

    fn appendFmt(self: *Self, comptime fmt: []const u8, args: anytype) !void {
        const formatted = try std.fmt.allocPrint(self.allocator, fmt, args);
        const indented = try self.indentedLine(formatted);
        self.allocator.free(formatted);
        try self.lines.append(self.allocator, indented);
    }

    fn indentedLine(self: *Self, line: []const u8) ![]const u8 {
        if (self.indent == 0) {
            return try self.allocator.dupe(u8, line);
        }
        const spaces = try self.allocator.alloc(u8, self.indent);
        @memset(spaces, ' ');
        defer self.allocator.free(spaces);

        return try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ spaces, line });
    }
};

/// Builder for plan section
pub const PlanSectionBuilder = struct {
    const Self = @This();

    /// Get the underlying YamlBuilder
    fn yaml(self: *Self) *YamlBuilder {
        return @ptrCast(@alignCast(self));
    }

    /// Add an inline plan
    pub fn plan(self: *Self, name: []const u8, selection: []const u8) *PlanBuilder {
        const y = self.yaml();
        y.appendFmt("{s}:", .{name}) catch {};
        y.indent = 4;
        y.appendFmt("selection: {s}", .{selection}) catch {};
        y.appendLine("executors:") catch {};
        y.indent = 6;
        return @ptrCast(y);
    }

    /// Finish plans section, return to main builder
    pub fn done(self: *Self) *YamlBuilder {
        return self.yaml();
    }
};

/// Builder for individual plan
pub const PlanBuilder = struct {
    const Self = @This();

    fn yaml(self: *Self) *YamlBuilder {
        return @ptrCast(@alignCast(self));
    }

    /// Add an executor to the plan
    pub fn executor(self: *Self, name: []const u8, action: []const u8, priority: i32) *Self {
        const y = self.yaml();
        y.appendFmt("- name: {s}", .{name}) catch {};
        y.indent = 8;
        y.appendFmt("run: \"{s}\"", .{action}) catch {};
        y.appendFmt("priority: {d}", .{priority}) catch {};
        y.indent = 6;
        return self;
    }

    /// Add an executor with retry config
    pub fn executorWithRetry(
        self: *Self,
        name: []const u8,
        action: []const u8,
        priority: i32,
        max_retries: u32,
        backoff: []const u8,
    ) *Self {
        const y = self.yaml();
        y.appendFmt("- name: {s}", .{name}) catch {};
        y.indent = 8;
        y.appendFmt("run: \"{s}\"", .{action}) catch {};
        y.appendFmt("priority: {d}", .{priority}) catch {};
        y.appendLine("retry:") catch {};
        y.indent = 10;
        y.appendFmt("max_attempts: {d}", .{max_retries}) catch {};
        y.appendFmt("backoff: {s}", .{backoff}) catch {};
        y.indent = 6;
        return self;
    }

    /// Add an executor with circuit breaker
    pub fn executorWithBreaker(
        self: *Self,
        name: []const u8,
        action: []const u8,
        priority: i32,
        failure_threshold: u32,
        cooldown_ms: u64,
    ) *Self {
        const y = self.yaml();
        y.appendFmt("- name: {s}", .{name}) catch {};
        y.indent = 8;
        y.appendFmt("run: \"{s}\"", .{action}) catch {};
        y.appendFmt("priority: {d}", .{priority}) catch {};
        y.appendLine("breaker:") catch {};
        y.indent = 10;
        y.appendFmt("failure_threshold: {d}", .{failure_threshold}) catch {};
        y.appendFmt("cooldown_ms: {d}", .{cooldown_ms}) catch {};
        y.indent = 6;
        return self;
    }

    /// Finish this plan, return to plan section
    pub fn done(self: *Self) *PlanSectionBuilder {
        const y = self.yaml();
        y.indent = 2;
        return @ptrCast(y);
    }
};

/// Builder for steps
pub const StepBuilder = struct {
    const Self = @This();

    fn yaml(self: *Self) *YamlBuilder {
        return @ptrCast(@alignCast(self));
    }

    /// Set the run target
    pub fn run(self: *Self, target: []const u8) *Self {
        const y = self.yaml();
        y.appendFmt("run: \"{s}\"", .{target}) catch {};
        y.appendLine("transitions:") catch {};
        y.indent += 2;
        return self;
    }

    /// Add success transition
    pub fn onSuccess(self: *Self, target: []const u8) *Self {
        const y = self.yaml();
        y.appendFmt("success: {s}", .{target}) catch {};
        return self;
    }

    /// Add failure transition
    pub fn onFailure(self: *Self, target: []const u8) *Self {
        const y = self.yaml();
        y.appendFmt("failure: {s}", .{target}) catch {};
        return self;
    }

    /// Add timeout transition
    pub fn onTimeout(self: *Self, target: []const u8) *Self {
        const y = self.yaml();
        y.appendFmt("timeout: {s}", .{target}) catch {};
        return self;
    }

    /// Add custom outcome transition
    pub fn on(self: *Self, outcome: []const u8, target: []const u8) *Self {
        const y = self.yaml();
        y.appendFmt("{s}: {s}", .{ outcome, target }) catch {};
        return self;
    }

    /// Finish this step, return to main builder
    pub fn done(self: *Self) *YamlBuilder {
        const y = self.yaml();
        y.indent = 0;
        return y;
    }
};

// =============================================================================
// Temp File Helpers
// =============================================================================

/// Write YAML content to a temporary file and return the path
pub fn writeTempYaml(allocator: Allocator, yaml: []const u8, filename: []const u8) ![]const u8 {
    const tmp_dir = "/tmp/flo-e2e-tests";

    // Create directory if needed (best-effort — ignore if it exists)
    var path_buf: [256]u8 = undefined;
    const path0 = try std.fmt.bufPrintZ(&path_buf, "{s}", .{tmp_dir});
    _ = std.c.mkdir(path0.ptr, 0o755);

    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ tmp_dir, filename });

    const file = try @import("../../fs.zig").createFileAbsolute(path, .{});
    defer @import("../../fs.zig").closeFile(file);
    try @import("../../fs.zig").writeAll(file, yaml);

    return path;
}

/// Clean up temp file
pub fn cleanupTempFile(allocator: Allocator, path: []const u8) void {
    @import("../../fs.zig").deleteFileAbsolute(path) catch {};
    allocator.free(path);
}

// =============================================================================
// Dotted-Key → Nested YAML Converter
// =============================================================================

/// Convert a flat dotted-key format (e.g. "source.stream: pipe-input")
/// into proper nested YAML (e.g. "source:\n  stream: pipe-input").
///
/// Input lines like:
///   name: e2e-passthrough
///   source.stream: pipe-input
///   sink.stream: pipe-output
///   parallelism: 1
///
/// Become:
///   name: e2e-passthrough
///   source:
///     stream: pipe-input
///   sink:
///     stream: pipe-output
///   parallelism: 1
///
/// Caller owns the returned slice.
pub fn dottedToYaml(allocator: Allocator, flat: []const u8) ![]const u8 {
    const Node = struct {
        key: []const u8,
        value: ?[]const u8, // null = parent-only node
        depth: usize,
        children: std.ArrayList(usize), // indices into nodes list

        fn deinit(self: *@This(), alloc: Allocator) void {
            self.children.deinit(alloc);
        }
    };

    // Root node (virtual)
    var nodes: std.ArrayList(Node) = .empty;
    defer {
        for (nodes.items) |*n| n.deinit(allocator);
        nodes.deinit(allocator);
    }
    try nodes.append(allocator, .{
        .key = "",
        .value = null,
        .depth = 0,
        .children = .empty,
    });

    // Parse each line
    var lines = std.mem.splitScalar(u8, flat, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, &std.ascii.whitespace);
        if (line.len == 0) continue;

        // Split on first ':'
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const full_key = std.mem.trim(u8, line[0..colon], &std.ascii.whitespace);
        const value = std.mem.trim(u8, line[colon + 1 ..], &std.ascii.whitespace);

        // Split key on dots
        var parts: std.ArrayList([]const u8) = .empty;
        defer parts.deinit(allocator);
        var key_iter = std.mem.splitScalar(u8, full_key, '.');
        while (key_iter.next()) |part| {
            try parts.append(allocator, part);
        }

        // Walk/create the tree
        var parent_idx: usize = 0; // root
        for (parts.items, 0..) |part, i| {
            const is_leaf = (i == parts.items.len - 1);

            // Find existing child with this key
            var found: ?usize = null;
            for (nodes.items[parent_idx].children.items) |child_idx| {
                if (std.mem.eql(u8, nodes.items[child_idx].key, part)) {
                    found = child_idx;
                    break;
                }
            }

            if (found) |idx| {
                if (is_leaf) {
                    nodes.items[idx].value = value;
                }
                parent_idx = idx;
            } else {
                const new_idx = nodes.items.len;
                try nodes.append(allocator, .{
                    .key = part,
                    .value = if (is_leaf) value else null,
                    .depth = i + 1,
                    .children = .empty,
                });
                try nodes.items[parent_idx].children.append(allocator, new_idx);
                parent_idx = new_idx;
            }
        }
    }

    // Render to YAML string
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);

    const WriteCtx = struct {
        output: *std.ArrayList(u8),
        all_nodes: []const Node,
        alloc: Allocator,

        /// Check whether a key segment is an array index like "[0]", "[1]", etc.
        fn isArrayIndex(key: []const u8) bool {
            if (key.len < 3) return false; // minimum "[0]"
            if (key[0] != '[' or key[key.len - 1] != ']') return false;
            for (key[1 .. key.len - 1]) |c| {
                if (!std.ascii.isDigit(c)) return false;
            }
            return true;
        }

        const WriteError = Allocator.Error;

        fn writeNode(self: *@This(), idx: usize, indent: usize) WriteError!void {
            const node = &self.all_nodes[idx];

            if (isArrayIndex(node.key)) {
                // YAML array item
                if (node.children.items.len == 0) {
                    // Scalar array item: "- value"
                    try self.writeIndent(indent);
                    try self.output.appendSlice(self.alloc, "- ");
                    try self.output.appendSlice(self.alloc, node.value orelse "");
                    try self.output.append(self.alloc, '\n');
                } else {
                    // Array item with children: "- first_child_inline\n  rest..."
                    for (node.children.items, 0..) |child_idx, i| {
                        if (i == 0) {
                            try self.writeIndent(indent);
                            try self.output.appendSlice(self.alloc, "- ");
                            try self.writeNodeInline(child_idx, indent + 2);
                        } else {
                            try self.writeNode(child_idx, indent + 2);
                        }
                    }
                }
            } else if (node.children.items.len == 0) {
                // Leaf: "key: value"
                try self.writeIndent(indent);
                try self.output.appendSlice(self.alloc, node.key);
                try self.output.appendSlice(self.alloc, ": ");
                try self.output.appendSlice(self.alloc, node.value orelse "");
                try self.output.append(self.alloc, '\n');
            } else {
                // Parent: "key:" then children indented
                try self.writeIndent(indent);
                try self.output.appendSlice(self.alloc, node.key);
                try self.output.appendSlice(self.alloc, ":\n");
                for (node.children.items) |child_idx| {
                    try self.writeNode(child_idx, indent + 2);
                }
            }
        }

        /// Render a node inline (no leading indent), used after "- " prefix.
        fn writeNodeInline(self: *@This(), idx: usize, indent: usize) WriteError!void {
            const node = &self.all_nodes[idx];
            if (node.children.items.len == 0) {
                // Leaf: "key: value\n"
                try self.output.appendSlice(self.alloc, node.key);
                try self.output.appendSlice(self.alloc, ": ");
                try self.output.appendSlice(self.alloc, node.value orelse "");
                try self.output.append(self.alloc, '\n');
            } else {
                // Parent: "key:\n  children..."
                try self.output.appendSlice(self.alloc, node.key);
                try self.output.appendSlice(self.alloc, ":\n");
                for (node.children.items) |child_idx| {
                    try self.writeNode(child_idx, indent + 2);
                }
            }
        }

        fn writeIndent(self: *@This(), n: usize) WriteError!void {
            for (0..n) |_| {
                try self.output.append(self.alloc, ' ');
            }
        }
    };

    var ctx = WriteCtx{
        .output = &out,
        .all_nodes = nodes.items,
        .alloc = allocator,
    };

    // Render root's children (skip the virtual root itself)
    for (nodes.items[0].children.items) |child_idx| {
        try ctx.writeNode(child_idx, 0);
    }

    return try allocator.dupe(u8, out.items);
}

/// Write a flat dotted-key definition as proper nested YAML to a temp file.
/// Converts "source.stream: x" → "source:\n  stream: x" style YAML.
/// Also supports array index notation: "sources.[0].stream: x" produces:
///   sources:
///     - stream: x
/// Caller owns the returned path (free via cleanupTempFile).
pub fn writeDottedToTempYaml(allocator: Allocator, flat_def: []const u8, filename: []const u8) ![]const u8 {
    const yaml = try dottedToYaml(allocator, flat_def);
    defer allocator.free(yaml);
    return writeTempYaml(allocator, yaml, filename);
}

// =============================================================================
// dottedToYaml tests
// =============================================================================

test "dottedToYaml: flat keys stay flat" {
    const input = "name: my-job\nparallelism: 4\nbatch_size: 100\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "name: my-job") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "parallelism: 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "batch_size: 100") != null);
}

test "dottedToYaml: dotted keys become nested" {
    const input = "name: test\nsource.stream: in\nsink.stream: out\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "source:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  stream: in") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "sink:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  stream: out") != null);
}

test "dottedToYaml: deeply nested keys" {
    const input = "a.b.c: deep\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "a:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  b:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "    c: deep") != null);
}

test "dottedToYaml: shared parent grouping" {
    const input = "source.stream: in\nsource.group: g1\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    // Should produce a single "source:" parent with two children
    // Count occurrences of "source:"
    var count: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, result, pos, "source:")) |idx| {
        count += 1;
        pos = idx + 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expect(std.mem.indexOf(u8, result, "  stream: in") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  group: g1") != null);
}

test "dottedToYaml: single array item" {
    const input = "sources.[0].stream: pipe-input\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    // Expected:
    //   sources:
    //     - stream: pipe-input
    try std.testing.expect(std.mem.indexOf(u8, result, "sources:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: pipe-input") != null);
}

test "dottedToYaml: multiple array items" {
    const input = "sources.[0].stream: input-a\nsources.[1].stream: input-b\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    // Expected:
    //   sources:
    //     - stream: input-a
    //     - stream: input-b
    try std.testing.expect(std.mem.indexOf(u8, result, "sources:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: input-a") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: input-b") != null);
}

test "dottedToYaml: array item with multiple properties" {
    const input = "sources.[0].stream: events\nsources.[0].group: my-group\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    // Expected:
    //   sources:
    //     - stream: events
    //       group: my-group
    try std.testing.expect(std.mem.indexOf(u8, result, "sources:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: events") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "    group: my-group") != null);
}

test "dottedToYaml: scalar array items" {
    const input = "tags.[0]: fast\ntags.[1]: prod\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    // Expected:
    //   tags:
    //     - fast
    //     - prod
    try std.testing.expect(std.mem.indexOf(u8, result, "tags:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - fast") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - prod") != null);
}

test "dottedToYaml: mixed flat, dotted, and array keys" {
    const input = "kind: Processing\nname: multi-src\nsources.[0].stream: in1\nsources.[1].stream: in2\nsink.stream: out\n";
    const result = try dottedToYaml(std.testing.allocator, input);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "kind: Processing") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "name: multi-src") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "sources:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: in1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  - stream: in2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "sink:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "  stream: out") != null);
}

// =============================================================================
// Unit Tests
// =============================================================================

test "YamlBuilder: simple workflow" {
    const testing = std.testing;
    var builder = YamlBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = builder.workflow("test", "1.0.0");
    _ = builder.start("@actions/test")
        .onSuccess("flo.Completed")
        .onFailure("flo.Failed");

    const yaml = try builder.build();
    defer testing.allocator.free(yaml);

    // Verify key parts are present
    try testing.expect(std.mem.indexOf(u8, yaml, "kind: Workflow") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "name: test") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "version: \"1.0.0\"") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "run: \"@actions/test\"") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "success: flo.Completed") != null);
}

test "YamlBuilder: workflow with plan" {
    const testing = std.testing;
    var builder = YamlBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = builder.workflow("test", "1.0.0");
    _ = builder.plans()
        .plan("my-plan", "health-weighted")
        .executor("exec1", "@actions/action1", 100)
        .executor("exec2", "@actions/action2", 50)
        .done()
        .done();
    _ = builder.start("@plan/my-plan")
        .onSuccess("flo.Completed")
        .onFailure("flo.Failed");

    const yaml = try builder.build();
    defer testing.allocator.free(yaml);

    try testing.expect(std.mem.indexOf(u8, yaml, "plans:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "my-plan:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "selection: health-weighted") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "run: \"@actions/action1\"") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "priority: 100") != null);
}

test "YamlBuilder: workflow with retry config" {
    const testing = std.testing;
    var builder = YamlBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = builder.workflow("retry-test", "1.0.0");
    _ = builder.plans()
        .plan("with-retry", "static-order")
        .executorWithRetry("flaky", "@actions/flaky-service", 100, 3, "exponential")
        .done()
        .done();

    const yaml = try builder.build();
    defer testing.allocator.free(yaml);

    try testing.expect(std.mem.indexOf(u8, yaml, "retry:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "max_attempts: 3") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "backoff: exponential") != null);
}

test "YamlBuilder: workflow with circuit breaker" {
    const testing = std.testing;
    var builder = YamlBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = builder.workflow("breaker-test", "1.0.0");
    _ = builder.plans()
        .plan("resilient", "static-order")
        .executorWithBreaker("primary", "@actions/primary", 100, 5, 60000)
        .done()
        .done();

    const yaml = try builder.build();
    defer testing.allocator.free(yaml);

    try testing.expect(std.mem.indexOf(u8, yaml, "breaker:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "failure_threshold: 5") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "cooldown_ms: 60000") != null);
}

test "YamlBuilder: multi-step workflow with terminals" {
    const testing = std.testing;
    var builder = YamlBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = builder.workflow("multi-step", "1.0.0");

    _ = builder.start("@actions/validate")
        .onSuccess("process")
        .on("invalid", "ValidationFailed");

    _ = builder.steps();

    _ = builder.step("process")
        .run("@actions/process")
        .onSuccess("notify")
        .onFailure("ProcessingFailed");

    _ = builder.step("notify")
        .run("@actions/notify")
        .onSuccess("flo.Completed")
        .onFailure("flo.Completed");

    _ = builder.terminals()
        .terminal("ValidationFailed", "failed")
        .terminal("ProcessingFailed", "failed");

    const yaml = try builder.build();
    defer testing.allocator.free(yaml);

    try testing.expect(std.mem.indexOf(u8, yaml, "steps:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "process:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "notify:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "terminals:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "ValidationFailed:") != null);
    try testing.expect(std.mem.indexOf(u8, yaml, "status: failed") != null);
}
