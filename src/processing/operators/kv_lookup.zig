//! KV Lookup Operator
//!
//! Enriches or filters streaming records by looking up keys in KV storage.
//! The lookup key is constructed via a **template** string that gives the
//! developer full control — no hidden separators or conventions.
//!
//! ## Modes
//!
//!   - **filter** (default): Drop the record if the constructed key is
//!     not found in KV storage.
//!   - **enrich**: Attach the KV value to the record as a JSON field.
//!     Records with no matching key pass through unchanged.
//!
//! ## lookup_key template syntax
//!
//! Templates mix literal text with `${<jsonpath>}` placeholders that are
//! resolved against the record's JSON value at runtime:
//!
//!   - `"account:${$.account_id}"`     — colon separator
//!   - `"accounts/${$.account_id}"`    — slash separator
//!   - `"${$.account_id}"`            — no prefix at all
//!   - `"user.${$.org}.${$.user_id}"` — composite multi-field key
//!
//! If any placeholder cannot be resolved (record is not valid JSON, or
//! the field path does not exist), the operator applies its **miss policy**:
//! in `filter` mode the record is dropped; in `enrich` mode it passes
//! through unchanged.
//!
//! ## YAML example
//!
//! ```yaml
//! operators:
//!   - type: kv_lookup
//!     name: check-account
//!     lookup_key: "account:${$.account_id}"
//!     namespace: default
//!     mode: filter       # filter | enrich (default: filter)
//!     enrich_field: _kv  # JSON field name for enriched value (enrich mode only)
//! ```
//!
//! ## Runtime behaviour
//!
//! The operator does **not** call into KV storage directly — it has no
//! reference to a Shard or Partition. Instead, the processing handler
//! wires a `KvLookupFn` callback at job startup. During unit tests a
//! simple HashMap stub is injected.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Operator = @import("../operator.zig").Operator;
const noOpSnapshot = @import("../operator.zig").noOpSnapshot;
const noOpRestore = @import("../operator.zig").noOpRestore;
const OperatorContext = @import("../context.zig").OperatorContext;
const record_mod = @import("../record.zig");
const ProcessingRecord = record_mod.ProcessingRecord;
const Watermark = record_mod.Watermark;

pub const Mode = enum {
    filter,
    enrich,

    pub fn fromString(s: []const u8) ?Mode {
        if (std.mem.eql(u8, s, "filter")) return .filter;
        if (std.mem.eql(u8, s, "enrich")) return .enrich;
        return null;
    }
};

/// A segment of the compiled lookup_key template.
const TemplatePart = union(enum) {
    /// Literal text copied verbatim.
    literal: []const u8,
    /// JSONPath segments to resolve against the record value.
    json_path: []const []const u8,
};

/// Callback signature used to perform the actual KV get.
/// Accepts namespace + key, returns value or null.
pub const KvLookupFn = *const fn (ctx: *anyopaque, namespace: []const u8, key: []const u8) ?[]const u8;

pub const KvLookupOperator = struct {
    name: []const u8,
    mode: Mode,
    namespace: []const u8,
    enrich_field: []const u8,

    /// Compiled template parts (owned).
    parts: []const TemplatePart,
    /// Flat storage for all json_path segment slices.
    segment_storage: []const []const u8,
    /// Owned copy of the template string (literal parts reference this).
    template_owned: []const u8,
    /// Owned copies of json_path segment strings.
    segment_strings_owned: []const u8,

    /// KV lookup callback — wired by the processing handler at job startup.
    lookup_fn: ?KvLookupFn = null,
    lookup_ctx: ?*anyopaque = null,

    /// Scratch buffer for building the lookup key at runtime (avoids alloc per record).
    key_buf: [1024]u8 = undefined,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        lookup_key_template: []const u8,
        namespace: []const u8,
        mode: Mode,
        enrich_field: []const u8,
    ) !Self {
        // Own all string inputs — they may be freed after init returns.
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);
        const owned_namespace = try allocator.dupe(u8, namespace);
        errdefer allocator.free(owned_namespace);
        const owned_enrich = try allocator.dupe(u8, enrich_field);
        errdefer allocator.free(owned_enrich);
        const owned_template = try allocator.dupe(u8, lookup_key_template);
        errdefer allocator.free(owned_template);

        // Store segment index ranges temporarily (start, end) for each json_path part.
        // We'll rebase them against the final segment_storage slice after toOwnedSlice.
        const IndexRange = struct { start: usize, end: usize };
        var parts_list: std.ArrayListUnmanaged(TemplatePart) = .{};
        errdefer parts_list.deinit(allocator);
        var ranges_list: std.ArrayListUnmanaged(IndexRange) = .{};
        errdefer ranges_list.deinit(allocator);
        var all_segments: std.ArrayListUnmanaged([]const u8) = .{};
        errdefer all_segments.deinit(allocator);

        // Accumulate all json_path segment characters for bulk duplication
        var seg_chars: std.ArrayListUnmanaged(u8) = .{};
        errdefer seg_chars.deinit(allocator);

        // Parse the template: split on ${...} placeholders
        // Use owned_template so literal slices reference owned memory.
        var i: usize = 0;
        while (i < owned_template.len) {
            // Look for next "${" starting from i
            if (std.mem.indexOfPos(u8, owned_template, i, "${")) |start| {
                // Emit literal before the placeholder
                if (start > i) {
                    try parts_list.append(allocator, .{ .literal = owned_template[i..start] });
                    try ranges_list.append(allocator, .{ .start = 0, .end = 0 });
                }
                // Find closing "}"
                const end = std.mem.indexOfScalarPos(u8, owned_template, start + 2, '}') orelse {
                    // Malformed — treat rest as literal
                    try parts_list.append(allocator, .{ .literal = owned_template[i..] });
                    try ranges_list.append(allocator, .{ .start = 0, .end = 0 });
                    break;
                };
                const expr = owned_template[start + 2 .. end];
                // Parse JSONPath expression (strip "$." prefix, split on ".")
                const field_path = if (std.mem.startsWith(u8, expr, "$.")) expr[2..] else expr;
                const seg_start = all_segments.items.len;
                var iter = std.mem.splitScalar(u8, field_path, '.');
                while (iter.next()) |seg| {
                    if (seg.len > 0) {
                        // Record the segment's position in seg_chars for later rebasing
                        const char_start = seg_chars.items.len;
                        try seg_chars.appendSlice(allocator, seg);
                        // Temporarily store with offset info (will be rebased)
                        try all_segments.append(allocator, seg_chars.items[char_start..]);
                    }
                }
                const seg_end = all_segments.items.len;
                // Temporarily store a placeholder — will be rebased below
                try parts_list.append(allocator, .{ .json_path = &.{} });
                try ranges_list.append(allocator, .{ .start = seg_start, .end = seg_end });
                i = end + 1;
            } else {
                // No more placeholders — rest is literal
                if (i < owned_template.len) {
                    try parts_list.append(allocator, .{ .literal = owned_template[i..] });
                    try ranges_list.append(allocator, .{ .start = 0, .end = 0 });
                }
                break;
            }
        }

        // Finalize owned segment character storage
        const seg_strings_owned = try seg_chars.toOwnedSlice(allocator);
        errdefer allocator.free(seg_strings_owned);

        // Rebase segment slices to point into the stable owned storage.
        // We only need the .len from each entry (stored by value in all_segments),
        // then rebuild slices at the correct offset into seg_strings_owned.
        var seg_offset: usize = 0;
        for (all_segments.items) |*seg| {
            const len = seg.len;
            seg.* = seg_strings_owned[seg_offset..][0..len];
            seg_offset += len;
        }

        const segment_storage = try all_segments.toOwnedSlice(allocator);
        const parts = try parts_list.toOwnedSlice(allocator);
        const ranges = try ranges_list.toOwnedSlice(allocator);
        defer allocator.free(ranges);

        // Rebase json_path slices against the final segment_storage
        for (parts, 0..) |*part, idx| {
            if (part.* == .json_path) {
                const r = ranges[idx];
                part.* = .{ .json_path = segment_storage[r.start..r.end] };
            }
        }

        return .{
            .name = owned_name,
            .mode = mode,
            .namespace = owned_namespace,
            .enrich_field = owned_enrich,
            .parts = parts,
            .segment_storage = segment_storage,
            .template_owned = owned_template,
            .segment_strings_owned = seg_strings_owned,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.parts);
        allocator.free(self.segment_storage);
        allocator.free(self.template_owned);
        allocator.free(self.segment_strings_owned);
        allocator.free(self.name);
        allocator.free(self.namespace);
        allocator.free(self.enrich_field);
    }

    /// Wire the KV lookup callback. Called by the processing handler at job startup.
    pub fn setLookupFn(self: *Self, func: KvLookupFn, ctx: *anyopaque) void {
        self.lookup_fn = func;
        self.lookup_ctx = ctx;
    }

    pub fn operator(self: *Self) Operator {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Operator.VTable{
        .processElement = processElement,
        .processWatermark = processWatermark,
        .getName = getName,
        .close = close,
        .snapshotState = noOpSnapshot,
        .restoreState = noOpRestore,
    };

    fn processElement(ptr: *anyopaque, rec: ProcessingRecord, ctx: *OperatorContext) !void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Build the lookup key from the template
        const lookup_key = self.buildKey(rec.value) orelse {
            // Could not resolve template — filter drops, enrich passes through
            if (self.mode == .enrich) try ctx.emit(rec);
            return;
        };

        // Perform the KV lookup
        const lookup_fn = self.lookup_fn orelse {
            // No lookup function wired — pass through or drop
            if (self.mode == .enrich) try ctx.emit(rec);
            return;
        };
        const kv_value = lookup_fn(self.lookup_ctx.?, self.namespace, lookup_key);

        switch (self.mode) {
            .filter => {
                if (kv_value != null) {
                    try ctx.emit(rec);
                }
                // else: drop
            },
            .enrich => {
                if (kv_value) |val| {
                    // Build enriched JSON: add the KV value as a field
                    const enriched = self.enrichRecord(ctx.allocator, rec.value, val) catch {
                        // Enrichment failed — pass through unchanged
                        try ctx.emit(rec);
                        return;
                    };
                    try ctx.emitWithKey(rec.key, enriched, rec.event_time_ms);
                } else {
                    // Key not found — pass through unchanged
                    try ctx.emit(rec);
                }
            },
        }
    }

    fn processWatermark(_: *anyopaque, _: Watermark, _: *OperatorContext) !void {}

    fn getName(ptr: *anyopaque) []const u8 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.name;
    }

    fn close(_: *anyopaque) void {}

    // =========================================================================
    // Template resolution
    // =========================================================================

    /// Build the lookup key by resolving the template against the record value.
    /// Returns null if any JSONPath placeholder cannot be resolved.
    fn buildKey(self: *Self, value: []const u8) ?[]const u8 {
        if (self.parts.len == 0) return null;

        // Fast path: single literal part (no JSONPath resolution needed)
        if (self.parts.len == 1) {
            switch (self.parts[0]) {
                .literal => |lit| return lit,
                .json_path => {},
            }
        }

        // Parse JSON once for all placeholder resolutions
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            std.heap.page_allocator,
            value,
            .{},
        ) catch return null;
        defer parsed.deinit();

        var pos: usize = 0;
        for (self.parts) |part| {
            switch (part) {
                .literal => |lit| {
                    if (pos + lit.len > self.key_buf.len) return null;
                    @memcpy(self.key_buf[pos..][0..lit.len], lit);
                    pos += lit.len;
                },
                .json_path => |segments| {
                    const resolved = resolveJsonPath(parsed.value, segments) orelse return null;
                    if (pos + resolved.len > self.key_buf.len) return null;
                    @memcpy(self.key_buf[pos..][0..resolved.len], resolved);
                    pos += resolved.len;
                },
            }
        }

        return self.key_buf[0..pos];
    }

    /// Walk JSON object by segment path and return string value.
    fn resolveJsonPath(root: std.json.Value, segments: []const []const u8) ?[]const u8 {
        var current = root;
        for (segments) |seg| {
            if (current != .object) return null;
            current = current.object.get(seg) orelse return null;
        }
        return switch (current) {
            .string => |s| s,
            else => null,
        };
    }

    /// Create enriched JSON by merging the KV value into the record.
    fn enrichRecord(self: *const Self, allocator: Allocator, record_value: []const u8, kv_value: []const u8) ![]const u8 {
        // Build: {"original_field": ..., "<enrich_field>": "<kv_value>"}
        // We do simple string concatenation to avoid full JSON parse/rebuild.
        // The record_value should be a JSON object — we insert before the closing '}'.
        if (record_value.len < 2) return record_value;

        // Find last '}'
        const last_brace = std.mem.lastIndexOfScalar(u8, record_value, '}') orelse return record_value;

        // Build: <everything before }>, "<enrich_field>":"<kv_value>"}
        const needs_comma = blk: {
            // Check if there's content between '{' and '}'
            var j: usize = last_brace;
            while (j > 0) {
                j -= 1;
                if (record_value[j] == '{') break :blk false; // empty object
                if (record_value[j] != ' ' and record_value[j] != '\n' and record_value[j] != '\t' and record_value[j] != '\r') break :blk true;
            }
            break :blk false;
        };

        const prefix = record_value[0..last_brace];
        const comma = if (needs_comma) "," else "";
        return std.fmt.allocPrint(allocator, "{s}{s}\"{s}\":\"{s}\"}}", .{ prefix, comma, self.enrich_field, kv_value });
    }

    // =========================================================================
    // Test helpers
    // =========================================================================

    /// Build the lookup key for a given record value (test helper).
    pub fn buildKeyForTest(self: *Self, value: []const u8) ?[]const u8 {
        return self.buildKey(value);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "KvLookupOperator — template parsing: simple prefix" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "account:${$.account_id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"account_id\":\"abc123\",\"amount\":100}");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("account:abc123", key.?);
}

test "KvLookupOperator — template parsing: no prefix" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "${$.id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"id\":\"xyz\"}");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("xyz", key.?);
}

test "KvLookupOperator — template parsing: composite key" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "user.${$.org}.${$.user_id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"org\":\"acme\",\"user_id\":\"42\"}");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("user.acme.42", key.?);
}

test "KvLookupOperator — template parsing: nested path" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "tx:${$.meta.txn_id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"meta\":{\"txn_id\":\"t-99\"}}");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("tx:t-99", key.?);
}

test "KvLookupOperator — missing field returns null" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "account:${$.missing}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"account_id\":\"abc\"}");
    try std.testing.expect(key == null);
}

test "KvLookupOperator — invalid JSON returns null" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "${$.id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("not-json");
    try std.testing.expect(key == null);
}

test "KvLookupOperator — filter mode: drops when no lookup fn" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var op = try KvLookupOperator.init(allocator, "filter-op", "${$.id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = @import("../context.zig").OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    const iface = op.operator();
    try iface.processElement(ProcessingRecord.init("k", "{\"id\":\"abc\"}", 100), &ctx);
    // No lookup_fn → filter drops
    try std.testing.expectEqual(@as(usize, 0), collector.count());
}

test "KvLookupOperator — enrich mode: passes through when no lookup fn" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    var op = try KvLookupOperator.init(allocator, "enrich-op", "${$.id}", "default", .enrich, "_kv");
    defer op.deinit(allocator);

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = @import("../context.zig").OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    const iface = op.operator();
    try iface.processElement(ProcessingRecord.init("k", "{\"id\":\"abc\"}", 100), &ctx);
    // No lookup_fn → enrich passes through
    try std.testing.expectEqual(@as(usize, 1), collector.count());
}

test "KvLookupOperator — filter mode with stub lookup fn" {
    const allocator = std.testing.allocator;
    const OutputCollector = @import("../collector.zig").OutputCollector;
    const OperatorMetrics = @import("../context.zig").OperatorMetrics;

    // Stub KV store using a simple HashMap
    const StubKv = struct {
        map: std.StringHashMapUnmanaged([]const u8) = .{},
        fn lookup(ctx_ptr: *anyopaque, _: []const u8, key: []const u8) ?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            return self.map.get(key);
        }
    };

    var stub = StubKv{};
    stub.map.put(allocator, "account:alice", "active") catch unreachable;
    defer stub.map.deinit(allocator);

    var op = try KvLookupOperator.init(allocator, "filter-kv", "account:${$.account_id}", "default", .filter, "_kv");
    defer op.deinit(allocator);
    op.setLookupFn(StubKv.lookup, @ptrCast(&stub));

    var collector = OutputCollector.init(allocator);
    defer collector.deinit();
    var metrics = OperatorMetrics{};
    var ctx = @import("../context.zig").OperatorContext{
        .collector = &collector,
        .metrics = &metrics,
        .allocator = allocator,
        .current_processing_time_ms = 1000,
        .current_watermark_ms = 0,
        .operator_name = "test",
    };

    const iface = op.operator();

    // Record with known account → passes filter
    try iface.processElement(ProcessingRecord.init("k1", "{\"account_id\":\"alice\"}", 100), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());

    // Record with unknown account → dropped
    try iface.processElement(ProcessingRecord.init("k2", "{\"account_id\":\"bob\"}", 200), &ctx);
    try std.testing.expectEqual(@as(usize, 1), collector.count());
}

test "KvLookupOperator — getName" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "my-lookup", "account:${$.id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const iface = op.operator();
    try std.testing.expectEqualStrings("my-lookup", iface.getName());
}

test "KvLookupOperator — Mode.fromString" {
    try std.testing.expect(Mode.fromString("filter") == .filter);
    try std.testing.expect(Mode.fromString("enrich") == .enrich);
    try std.testing.expect(Mode.fromString("invalid") == null);
}

test "KvLookupOperator — literal-only template" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "static-key", "default", .filter, "_kv");
    defer op.deinit(allocator);

    // Literal template doesn't need valid JSON
    const key = op.buildKeyForTest("anything");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("static-key", key.?);
}

test "KvLookupOperator — slash separator" {
    const allocator = std.testing.allocator;
    var op = try KvLookupOperator.init(allocator, "test", "accounts/${$.id}", "default", .filter, "_kv");
    defer op.deinit(allocator);

    const key = op.buildKeyForTest("{\"id\":\"u-001\"}");
    try std.testing.expect(key != null);
    try std.testing.expectEqualStrings("accounts/u-001", key.?);
}
