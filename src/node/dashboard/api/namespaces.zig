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
const Shard = @import("../../shard.zig").Shard;
const KVProjection = @import("../../../projection/kv.zig").KVProjection;

// ── Helpers ──

fn getKVProjection(ctx: *DashboardContext, idx: usize) ?*KVProjection {
    const ptrs = ctx.shard_ptrs orelse return null;
    if (idx >= ptrs.len) return null;
    const shard: *Shard = @ptrCast(@alignCast(ptrs[idx]));
    return &shard.defaultPartition().kv;
}

fn shardCount(ctx: *DashboardContext) usize {
    return if (ctx.shard_ptrs) |p| p.len else 0;
}

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

/// GET /namespaces/:name - Namespace detail with resource arrays
pub fn getNamespaceDetail(allocator: Allocator, name: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", name);

    // streams array
    {
        var streams_arr = try obj.arrayField("streams");
        try streams_arr.begin();

        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var seen = std.StringHashMap(void).init(allocator);
        defer seen.deinit();

        var it = ctx.metrics.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (!std.mem.eql(u8, s.namespace, name)) continue;
            const gop = try seen.getOrPut(s.topic);
            if (!gop.found_existing) {
                try streams_arr.next();
                var sobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try sobj.begin();
                try sobj.stringField("name", s.topic);
                try sobj.stringField("namespace", s.namespace);
                try sobj.intField("partitions", 1);
                try sobj.intField("ingest_rate", 0);
                try sobj.intField("reads", 0);
                try sobj.stringField("retention", "7d");
                try sobj.end();
            }
        }

        try streams_arr.end();
    }

    // queues array
    {
        var queues_arr = try obj.arrayField("queues");
        try queues_arr.begin();

        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var it = ctx.metrics.queues.iterator();
        while (it.next()) |entry| {
            const q = entry.value_ptr.*;
            if (!std.mem.eql(u8, q.namespace, name)) continue;
            try queues_arr.next();
            const snap = q.metrics.snapshot();
            var qobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
            try qobj.begin();
            try qobj.stringField("name", q.queue);
            try qobj.stringField("namespace", q.namespace);
            try qobj.intField("pending", snap.queue_available_current + snap.leases_active_current);
            try qobj.intField("available", snap.queue_available_current);
            try qobj.intField("enqueued", snap.enqueue_ops_total);
            try qobj.intField("dequeued", snap.dequeue_ops_total);
            try qobj.intField("acked", snap.leases_completed_total);
            try qobj.intField("nacked", snap.leases_failed_total);
            try qobj.intField("dlq_count", snap.dlq_messages_total);
            try qobj.intField("bytes_total", snap.enqueue_bytes_total);
            try qobj.end();
        }

        try queues_arr.end();
    }

    // kv_keys array — scan from shard KV projections
    {
        var kv_arr = try obj.arrayField("kv_keys");
        try kv_arr.begin();

        var kv_seen = std.StringHashMap(void).init(allocator);
        defer kv_seen.deinit();

        const n = shardCount(ctx);
        for (0..n) |i| {
            if (getKVProjection(ctx, i)) |kv| {
                var kit = kv.map.iterator();
                while (kit.next()) |kv_entry| {
                    const key = kv_entry.key_ptr.*;
                    // Check if key belongs to this namespace (prefix "namespace:")
                    if (name.len > 0 and !std.mem.eql(u8, name, "default")) {
                        const prefix_len = name.len + 1; // "namespace:"
                        if (key.len <= prefix_len) continue;
                        if (!std.mem.startsWith(u8, key, name)) continue;
                        if (key[name.len] != ':') continue;
                    }
                    const gop = try kv_seen.getOrPut(key);
                    if (!gop.found_existing) {
                        try kv_arr.next();
                        var kobj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                        try kobj.begin();
                        try kobj.stringField("key", key);
                        try kobj.end();
                    }
                }
            }
        }

        try kv_arr.end();
    }

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

/// GET /namespaces/:ns/kv - KV stats for namespace (also used for /kv/namespaces/:ns)
pub fn getNamespaceKV(allocator: Allocator, namespace: []const u8, ctx: *DashboardContext) ![]const u8 {
    var json_buf: std.ArrayList(u8) = .empty;
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("namespace", namespace);

    // Look for namespace in metrics
    {
        ctx.metrics.mutex.lock();
        defer ctx.metrics.mutex.unlock();

        var found = false;
        var it = ctx.metrics.kv_namespaces.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.namespace, namespace)) {
                const snap = entry.value_ptr.metrics.snapshot();
                try obj.intField("key_count", snap.key_count);
                try obj.intField("bytes_stored", snap.bytes_stored);
                try obj.intField("get_ops", snap.get_ops_total);
                try obj.intField("set_ops", snap.set_ops_total);
                try obj.intField("delete_ops", snap.delete_ops_total);
                found = true;
                break;
            }
        }

        if (!found) {
            try obj.intField("key_count", 0);
            try obj.intField("bytes_stored", 0);
            try obj.intField("get_ops", 0);
            try obj.intField("set_ops", 0);
            try obj.intField("delete_ops", 0);
        }
    }

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

test "getNamespaceDetail returns detail object with arrays" {
    const allocator = std.testing.allocator;
    var metrics = h.MetricsRegistry.init(allocator);
    defer metrics.deinit();
    var ctx = DashboardContext.init(allocator, &metrics, 1);

    const result = try getNamespaceDetail(allocator, "default", &ctx);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"default\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"streams\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"queues\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"kv_keys\":[]") != null);
}
