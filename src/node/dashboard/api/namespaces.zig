//! Dashboard API — Namespace Endpoints
//!
//! Namespaces are the cross-cutting organizational primitive.
//!
//! - GET /namespaces             — List all namespaces with resource counts
//! - GET /namespaces/:name       — Namespace detail
//! - GET /namespaces/:ns/streams — Streams in namespace
//! - GET /namespaces/:ns/queues  — Queues in namespace
//! - GET /namespaces/:ns/kv      — KV stats for namespace

const std = @import("std");
const Allocator = std.mem.Allocator;
const h = @import("helpers.zig");
const json = h.json;
const DashboardContext = h.DashboardContext;

/// GET /namespaces - List all namespaces with resource counts
pub fn getNamespaces(allocator: Allocator, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    // Collect namespace names from metrics registry
    var ns_set = std.StringHashMap(void).init(allocator);
    defer ns_set.deinit();

    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        // From KV namespaces
        var kv_it = ctx.metrics.kv_namespaces.iterator();
        while (kv_it.next()) |entry| {
            try ns_set.put(entry.value_ptr.namespace, {});
        }

        // From queues
        var q_it = ctx.metrics.queues.iterator();
        while (q_it.next()) |entry| {
            try ns_set.put(entry.value_ptr.namespace, {});
        }

        // From streams
        var s_it = ctx.metrics.streams.iterator();
        while (s_it.next()) |entry| {
            try ns_set.put(entry.value_ptr.namespace, {});
        }
    }

    // Always include "default"
    try ns_set.put("default", {});

    var ns_it = ns_set.iterator();
    while (ns_it.next()) |entry| {
        try arr.next();
        const ns_name = entry.key_ptr.*;

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", ns_name);
        try obj.intField("stream_count", countStreamsInNamespace(ctx, ns_name));
        try obj.intField("queue_count", countQueuesInNamespace(ctx, ns_name));
        try obj.intField("kv_count", countKVInNamespace(ctx, ns_name));
        try obj.intField("created_at", 0);
        try obj.boolField("is_system", std.mem.startsWith(u8, ns_name, "_"));
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:name - Namespace detail
pub fn getNamespaceDetail(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);
    try obj.intField("stream_count", countStreamsInNamespace(ctx, name));
    try obj.intField("queue_count", countQueuesInNamespace(ctx, name));
    try obj.intField("kv_count", countKVInNamespace(ctx, name));
    try obj.intField("created_at", 0);
    try obj.boolField("is_system", std.mem.startsWith(u8, name, "_"));
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/streams - Streams in namespace
pub fn getNamespaceStreams(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (!std.mem.eql(u8, s.namespace, namespace)) continue;

            const gop = try seen.getOrPut(s.topic);
            if (!gop.found_existing) {
                try arr.next();
                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("name", s.topic);
                try obj.stringField("namespace", s.namespace);
                try obj.intField("partitions", 1);
                try obj.end();
            }
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/queues - Queues in namespace
pub fn getNamespaceQueues(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var it = ctx.metrics.queues.iterator();
        while (it.next()) |entry| {
            const q = entry.value_ptr.*;
            if (!std.mem.eql(u8, q.namespace, namespace)) continue;

            try arr.next();
            const snap = q.metrics.snapshot();
            var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try obj.begin();
            try obj.stringField("name", q.queue);
            try obj.stringField("namespace", q.namespace);
            try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
            try obj.end();
        }
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/kv - KV stats for namespace
pub fn getNamespaceKV(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("namespace", namespace);
    try obj.intField("key_count", countKVInNamespace(ctx, namespace));
    try obj.intField("bytes_stored", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}

// =============================================================================
// Helpers
// =============================================================================

fn countStreamsInNamespace(ctx: *DashboardContext, namespace: []const u8) u32 {
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var count: u32 = 0;
    var it = ctx.metrics.streams.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.value_ptr.namespace, namespace)) count += 1;
    }
    return count;
}

fn countQueuesInNamespace(ctx: *DashboardContext, namespace: []const u8) u32 {
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var count: u32 = 0;
    var it = ctx.metrics.queues.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.value_ptr.namespace, namespace)) count += 1;
    }
    return count;
}

fn countKVInNamespace(ctx: *DashboardContext, namespace: []const u8) u64 {
    ctx.metrics.mutex.lock();
    defer ctx.metrics.mutex.unlock();

    var it = ctx.metrics.kv_namespaces.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.value_ptr.namespace, namespace)) {
            return entry.value_ptr.metrics.key_count.load(.monotonic);
        }
    }
    return 0;
}

// =============================================================================
// Tests
// =============================================================================

test "getNamespaces includes default namespace" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getNamespaces(allocator, &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"default\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"stream_count\"") != null);
}

test "getNamespaceDetail returns detail object" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getNamespaceDetail(allocator, "default", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"default\"") != null);
}
