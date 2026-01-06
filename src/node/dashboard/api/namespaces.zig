//! Dashboard API — Namespace Endpoints
//!
//! Namespaces are the cross-cutting organizational primitive.
//! Source of truth: MetadataCache on Controller (cores[0])
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
const Dispatcher = h.Dispatcher;
const Core = h.Core;
const MetricsRegistry = h.MetricsRegistry;

/// Namespace info with resource counts
const NamespaceInfo = struct {
    name: []const u8,
    stream_count: u32,
    queue_count: u32,
    kv_count: u64,
    created_at: i64,
    is_system: bool,
};

/// Get the Controller core (shard 0) from the cores slice.
/// Returns null if cores is unavailable or empty.
fn getController(cores: ?[]*Core) ?*Core {
    const c = cores orelse return null;
    if (c.len == 0) return null;
    const controller = c[0];
    // Sanity check: controller must be shard 0
    std.debug.assert(controller.core_id == 0);
    return controller;
}

/// GET /namespaces - List all namespaces with resource counts
pub fn getNamespaces(allocator: Allocator, dispatchers: []*Dispatcher, cores: ?[]*Core, metrics: *MetricsRegistry) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const controller = getController(cores) orelse
        return h.jsonError(allocator, "No cores available");
    const ns_list = try controller.metadata_cache.listNamespacesSafe(allocator, false);
    defer allocator.free(ns_list);

    var namespace_info = std.StringHashMap(NamespaceInfo).init(allocator);
    defer namespace_info.deinit();

    for (ns_list) |ns_meta| {
        try namespace_info.put(ns_meta.name, .{
            .name = ns_meta.name,
            .stream_count = h.countStreamsInNamespace(cores, ns_meta.name),
            .queue_count = 0,
            .kv_count = 0,
            .created_at = ns_meta.created_at,
            .is_system = ns_meta.config.is_system,
        });
    }

    // Aggregate resource counts from MetricsRegistry (when populated)
    {
        metrics.mutex.lock();
        defer metrics.mutex.unlock();

        {
            var it = metrics.queues.iterator();
            while (it.next()) |entry| {
                const queue_entry = entry.value_ptr.*;
                if (namespace_info.getPtr(queue_entry.namespace)) |info| {
                    info.queue_count += 1;
                }
            }
        }

        {
            var it = metrics.kv_namespaces.iterator();
            while (it.next()) |entry| {
                const kv_entry = entry.value_ptr.*;
                if (namespace_info.getPtr(kv_entry.namespace)) |info| {
                    info.kv_count = kv_entry.metrics.key_count.load(.monotonic);
                }
            }
        }
    }

    // When metrics registry has no KV data (registerKVNamespace not yet called),
    // fall back to counting via dispatcher local scans.
    // NOTE(cluster): countLocalKVKeys only sees this node's shards.
    if (dispatchers.len > 0) {
        var ns_it2 = namespace_info.iterator();
        while (ns_it2.next()) |entry| {
            const info = entry.value_ptr;
            if (info.kv_count == 0) {
                var count: u64 = 0;
                for (dispatchers) |d| {
                    count += d.countLocalKVKeys(info.name);
                }
                info.kv_count = count;
            }
        }
    }

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    var ns_it = namespace_info.iterator();
    while (ns_it.next()) |entry| {
        try arr.next();
        const info = entry.value_ptr.*;

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", info.name);
        try obj.intField("stream_count", info.stream_count);
        try obj.intField("queue_count", info.queue_count);
        try obj.intField("kv_count", info.kv_count);
        try obj.intField("created_at", info.created_at);
        try obj.boolField("is_system", info.is_system);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:name - Namespace details
pub fn getNamespaceDetail(allocator: Allocator, namespace: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core, metrics: *MetricsRegistry) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    const controller = getController(cores) orelse
        return h.jsonError(allocator, "No cores available");

    const ns_meta = controller.metadata_cache.getNamespaceSafe(namespace) orelse {
        return h.jsonError(allocator, "Namespace not found");
    };

    const stream_count: u32 = h.countStreamsInNamespace(cores, namespace);
    var queue_count: u32 = 0;
    var kv_count: u64 = 0;

    {
        metrics.mutex.lock();
        defer metrics.mutex.unlock();

        var it = metrics.queues.iterator();
        while (it.next()) |entry| {
            const queue_entry = entry.value_ptr.*;
            if (std.mem.eql(u8, queue_entry.namespace, namespace)) {
                queue_count += 1;
            }
        }

        var kv_it = metrics.kv_namespaces.iterator();
        while (kv_it.next()) |entry| {
            const kv_entry = entry.value_ptr.*;
            if (std.mem.eql(u8, kv_entry.namespace, namespace)) {
                kv_count = kv_entry.metrics.key_count.load(.monotonic);
                break;
            }
        }
    }

    // Fallback: count via dispatcher local scans when metrics not populated
    if (kv_count == 0 and dispatchers.len > 0) {
        for (dispatchers) |d| {
            kv_count += d.countLocalKVKeys(namespace);
        }
    }

    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("name", namespace);
    try obj.intField("stream_count", stream_count);
    try obj.intField("queue_count", queue_count);
    try obj.intField("kv_count", kv_count);
    try obj.intField("created_at", ns_meta.created_at);
    try obj.boolField("is_system", ns_meta.config.is_system);
    try obj.end();

    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/streams - Streams in namespace
pub fn getNamespaceStreams(allocator: Allocator, namespace: []const u8, dispatchers: []*Dispatcher, cores: ?[]*Core, metrics: *MetricsRegistry) ![]const u8 {
    _ = dispatchers;
    _ = metrics;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    const discovered = try h.scanStreamsFromCores(allocator, cores, namespace);
    defer h.freeDiscoveredStreams(allocator, discovered);

    for (discovered) |stream| {
        try arr.next();

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", stream.name);
        try obj.stringField("namespace", stream.namespace);
        try obj.intField("partitions", stream.partition_count);
        try obj.intField("ingest_rate", 0);
        try obj.intField("reads", 0);
        try obj.stringField("retention", "7d");
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/queues - Queues in namespace
pub fn getNamespaceQueues(allocator: Allocator, namespace: []const u8, dispatchers: []*Dispatcher, metrics: *MetricsRegistry) ![]const u8 {
    _ = dispatchers;

    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    var arr = json.ArrayBuilder(@TypeOf(writer)).init(writer);
    try arr.begin();

    metrics.mutex.lock();
    defer metrics.mutex.unlock();

    var it = metrics.queues.iterator();
    while (it.next()) |entry| {
        const queue_entry = entry.value_ptr.*;
        if (!std.mem.eql(u8, queue_entry.namespace, namespace)) continue;

        try arr.next();

        const snap = queue_entry.metrics.snapshot();

        var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
        try obj.begin();
        try obj.stringField("name", queue_entry.queue);
        try obj.stringField("namespace", queue_entry.namespace);
        try obj.intField("pending", snap.queue_available_current + snap.leases_active_current);
        try obj.intField("available", snap.queue_available_current);
        try obj.intField("enqueued", snap.enqueue_ops_total);
        try obj.intField("dequeued", snap.dequeue_ops_total);
        try obj.intField("dlq_count", snap.dlq_messages_total);
        try obj.end();
    }

    try arr.end();
    return try json_buf.toOwnedSlice(allocator);
}

/// GET /namespaces/:ns/kv - KV stats for namespace
pub fn getNamespaceKV(allocator: Allocator, namespace: []const u8, dispatchers: []*Dispatcher, metrics: *MetricsRegistry) ![]const u8 {
    var json_buf = std.ArrayList(u8){};
    errdefer json_buf.deinit(allocator);
    const writer = json_buf.writer(allocator);

    // Try metrics registry first (has live op counters)
    {
        metrics.mutex.lock();
        defer metrics.mutex.unlock();

        var it = metrics.kv_namespaces.iterator();
        while (it.next()) |entry| {
            const kv_entry = entry.value_ptr.*;

            if (std.mem.eql(u8, kv_entry.namespace, namespace)) {
                const snap = kv_entry.metrics.snapshot();

                var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
                try obj.begin();
                try obj.stringField("namespace", namespace);
                try obj.intField("key_count", snap.key_count);
                try obj.intField("bytes_stored", snap.bytes_stored);
                try obj.intField("get_ops", snap.get_ops_total);
                try obj.intField("set_ops", snap.set_ops_total);
                try obj.intField("delete_ops", snap.delete_ops_total);
                try obj.end();

                return try json_buf.toOwnedSlice(allocator);
            }
        }
    }

    // Fallback: count via dispatcher local scans
    var key_count: u64 = 0;
    for (dispatchers) |d| {
        key_count += d.countLocalKVKeys(namespace);
    }
    var obj = json.ObjectBuilder(@TypeOf(writer)).init(writer);
    try obj.begin();
    try obj.stringField("namespace", namespace);
    try obj.intField("key_count", key_count);
    try obj.intField("bytes_stored", 0);
    try obj.intField("get_ops", 0);
    try obj.intField("set_ops", 0);
    try obj.intField("delete_ops", 0);
    try obj.end();
    return try json_buf.toOwnedSlice(allocator);
}
