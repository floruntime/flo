//! Job Definition — Data Types
//!
//! Defines the `JobDefinition` struct that represents a parsed stream
//! processing job configuration. Parsing logic lives in `parser.zig`.
//!
//! ## YAML Format (multi-source / multi-sink)
//!
//! ```yaml
//! kind: Processing
//! name: my-pipeline
//! parallelism: 4
//! batch_size: 200
//!
//! sources:
//!   - name: events-source
//!     stream:
//!       name: input-events
//!       namespace: production
//!       partitions: 0              # single partition (integer)
//!       batch_size: 500            # per-source override
//!     # omitting `partitions:` defaults to all partitions
//!
//!   # Partition range — expands into one SourceSpec per partition
//!   - name: all-partitions
//!     stream:
//!       name: click-events
//!       partitions: "0-63"         # range: creates 64 internal sources
//!
//!   # Partition list — comma-separated
//!   - name: selected
//!     stream:
//!       name: impressions
//!       partitions: "0,3,7"        # explicit list
//!
//!   # All partitions — resolved at runtime from stream metadata
//!   - name: full-stream
//!     stream:
//!       name: transactions
//!       partitions: "all"          # sentinel, handler resolves partition count
//!
//! sinks:
//!   - name: output
//!     stream:                     # stream sink (receives main output)
//!       name: output-events
//!       namespace: production
//!
//!   - name: late-events
//!     stream:                     # receives only records tagged "late"
//!       name: late-data
//!     tags: [late]
//!
//!   - name: errors
//!     queue:                      # receives only records tagged "errors"
//!       name: error-queue
//!       namespace: default
//!     tags: [errors]
//!
//!   - name: profiles
//!     kv:                         # KV sink
//!       namespace: profiles
//!       key_prefix: user
//!       write_mode: upsert
//!
//!   - name: tasks
//!     queue:                      # queue sink
//!       name: task-queue
//!       namespace: default
//!       priority: 5
//!
//! operators:
//!   - type: filter
//!     name: positive
//!     condition: "value_contains:success"
//!
//!   - type: keyby
//!     name: by-user
//!     key_expression: "$.user.id"
//!
//!   - type: passthrough
//!     name: debug-tap
//!
//!   # Declarative map — project/rename fields (1:1)
//!   - type: map
//!     name: extract-user
//!     user_name: "$.user.name"
//!     user_email: "$.user.email"
//!     source: "signup-service"     # constant value
//!
//!   # Declarative flatmap — explode array field (1:N)
//!   - type: flatmap
//!     name: explode-items
//!     array_field: "$.order.items"
//!     element_key: "$.sku"         # optional — extract key
//!
//!   # Declarative aggregate (sum/count/avg/min/max + optional windowing)
//!   - type: aggregate
//!     name: hourly-total
//!     function: sum
//!     field: "$.amount"
//!     window: tumbling
//!     window_size: 3600           # seconds
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

// =============================================================================
// TagRegistry — maps tag names to bit positions (0–31)
// =============================================================================

/// Maps symbolic tag names to bit positions within a 32-bit bitfield.
///
/// Built at pipeline init time from:
///   - All sinks' `tags:` lists
///   - All classify operators' tag_N name values
///
/// Used to:
///   1. Resolve classify operator tag names → bit positions
///   2. Compute each sink's `required_tags` bitmask
///
/// Capacity: 32 tags per pipeline (u5 bit positions, u32 bitfield).
pub const TagRegistry = struct {
    names: [MAX_TAGS][]const u8 = undefined,
    count: u6 = 0,

    pub const MAX_TAGS: usize = 32;

    /// Look up or create a bit position for a tag name.
    /// Returns null if the registry is full (32 tags).
    /// Names are compared by value — caller must ensure strings outlive the registry.
    pub fn getOrCreate(self: *TagRegistry, name: []const u8) ?u5 {
        // Check existing entries
        for (0..@as(usize, self.count)) |i| {
            if (std.mem.eql(u8, self.names[i], name)) return @intCast(i);
        }
        // Add new entry
        if (self.count >= MAX_TAGS) return null;
        const bit: u5 = @intCast(self.count);
        self.names[self.count] = name;
        self.count += 1;
        return bit;
    }

    /// Look up a tag name's bit position. Returns null if not found.
    pub fn resolve(self: *const TagRegistry, name: []const u8) ?u5 {
        for (0..@as(usize, self.count)) |i| {
            if (std.mem.eql(u8, self.names[i], name)) return @intCast(i);
        }
        return null;
    }

    /// Build a bitmask from a list of tag names.
    /// Unknown names are silently ignored (resolve returns null).
    pub fn buildMask(self: *const TagRegistry, names: []const []const u8) u32 {
        var mask: u32 = 0;
        for (names) |name| {
            if (self.resolve(name)) |bit| {
                mask |= @as(u32, 1) << bit;
            }
        }
        return mask;
    }
};

// =============================================================================
// SourceKind — which Flo primitive the source reads from
// =============================================================================

/// Determines which Flo primitive the source reads from.
pub const SourceKind = enum(u8) {
    stream = 0,
    ts = 1,

    pub fn toStr(self: SourceKind) []const u8 {
        return switch (self) {
            .stream => "stream",
            .ts => "ts",
        };
    }
};

// =============================================================================
// SourceSpec — one logical source (Flo-Stream or Flo-TS)
// =============================================================================

/// Sentinel value for `partition` indicating "all partitions".
/// The handler resolves the actual partition count from stream metadata at runtime.
pub const PARTITION_ALL: u32 = std.math.maxInt(u32);

/// Specification for a source endpoint in the processing topology.
///
/// For **stream** sources the parser expands `partitions: "0-63"` or
/// `partitions: "0,3,7"` into multiple SourceSpec entries (one per
/// partition). The `partitions: "all"` form sets `partition = PARTITION_ALL`
/// for runtime resolution.
///
/// For **ts** sources the spec identifies a measurement + optional tags and
/// field to read. The handler periodically polls `ts_read` to feed the
/// pipeline with time-series data points.
pub const SourceSpec = struct {
    /// Source kind: stream (default) or ts
    kind: SourceKind = .stream,

    /// Logical name within the DAG (e.g. "events-source")
    name: []const u8,
    /// Flo-Stream name (stream sources) — empty for TS sources
    stream: []const u8,
    /// Namespace (default: "default")
    namespace: []const u8,
    /// Partition to read from (stream sources only, default: 0).
    /// Set to `PARTITION_ALL` when `partitions: "all"` is specified;
    /// the handler resolves the actual count from stream metadata.
    partition: u32,
    /// Batch size for this source (default: inherited from top-level batch_size)
    batch_size: u32,

    // -- TS-specific fields (only used when kind = .ts) --

    /// Measurement name (ts sources only)
    ts_measurement: []const u8 = "",
    /// Tag filter as flat pairs: [key1, val1, key2, val2, …]
    /// Empty = read all series for the measurement.
    ts_tags: []const []const u8 = &.{},
    /// Field name to read (ts sources only, default: "value")
    ts_field: []const u8 = "",
    /// Polling interval in milliseconds for TS source (default: 1000)
    ts_poll_interval_ms: u32 = 1000,
};

// =============================================================================
// SinkSpec — one logical sink (stream, KV, or queue)
// =============================================================================

/// Determines which Flo primitive the sink writes to.
pub const SinkKind = enum(u8) {
    stream = 0,
    kv = 1,
    queue = 2,
    ts = 3,

    pub fn toStr(self: SinkKind) []const u8 {
        return switch (self) {
            .stream => "stream",
            .kv => "kv",
            .queue => "queue",
            .ts => "ts",
        };
    }
};

/// Specification for a sink endpoint in the processing topology.
///
/// The `kind` field determines which Flo primitive is targeted.
/// Only fields relevant to the chosen kind need to be set — the parser
/// fills defaults for unused fields.
pub const SinkSpec = struct {
    /// Logical name within the DAG (e.g. "output")
    name: []const u8,
    /// Sink type: stream, kv, or queue
    kind: SinkKind,

    // -- Target identification --

    /// Stream name (stream sinks) or queue name (queue sinks).
    /// Empty for KV sinks.
    target: []const u8,
    /// Namespace of the target resource
    namespace: []const u8,

    // -- Tag-based routing --

    /// When set, this sink receives only records whose tag bitfield
    /// matches ALL listed tag names (AND logic). When null, the sink
    /// receives all records from the main operator chain output.
    match: ?[]const []const u8 = null,

    /// Pre-computed bitmask from tag names (resolved at pipeline init).
    /// `record.tags & required_tags == required_tags` → deliver.
    required_tags: u32 = 0,

    // -- KV-specific options --

    /// Key prefix prepended to every record key (default: "")
    key_prefix: []const u8,
    /// Delimiter between prefix and record key (default: ":")
    separator: []const u8,
    /// Write mode: "upsert" | "if_absent" | "versioned" (default: "upsert")
    write_mode: []const u8,
    /// TTL for KV entries in milliseconds (null = no expiration)
    ttl_ms: ?u64,

    // -- Queue-specific options --

    /// Message priority (0 = default)
    priority: u8,
    /// Delay before message becomes visible, in ms (null = immediate)
    delay_ms: ?u64,
    /// Use record key as dedup_key for idempotent delivery (default: true)
    use_key_as_dedup: bool,

    // -- TS-specific options --

    /// Measurement name for time-series writes (ts sinks only)
    ts_measurement: []const u8 = "",
    /// Tag extractors: pairs of (tag_key, json_key) for extracting tags from record JSON.
    /// Stored as flat pairs: [tag_key_1, json_key_1, tag_key_2, json_key_2, ...]
    ts_tag_keys: []const []const u8 = &.{},
    /// Field extractors: pairs of (field_name, json_key) for extracting numeric fields.
    /// Stored as flat pairs: [field_name_1, json_key_1, field_name_2, json_key_2, ...]
    ts_field_keys: []const []const u8 = &.{},
    /// Shorthand: field name for treating entire value as a single numeric field.
    /// If non-empty, ts_field_keys are ignored. Defaults to "value" for TS sinks.
    ts_value_field: []const u8 = "",
};

// =============================================================================
// OperatorSpec — one operator in the pipeline
// =============================================================================

/// Specification for an operator in the pipeline.
pub const OperatorSpec = struct {
    /// Operator type (e.g., "filter", "map", "flatmap", "keyby", "passthrough")
    type_name: []const u8,
    /// Operator name (e.g., "positive-filter", "transform")
    name: []const u8,
    /// Module path (reserved for future use). Null for built-in operators.
    module: ?[]const u8 = null,
    /// Declarative configuration for native operators.
    /// Keys are operator-specific (e.g., "condition", "key_expression").
    /// Stored as string→string pairs parsed from YAML operator config block.
    config: ?[]const ConfigEntry = null,

    /// A key-value config entry for declarative operator configuration.
    pub const ConfigEntry = struct {
        key: []const u8,
        value: []const u8,
    };

    /// Look up a config value by key. Returns null if not found or no config.
    pub fn getConfig(self: *const OperatorSpec, key: []const u8) ?[]const u8 {
        const entries = self.config orelse return null;
        for (entries) |entry| {
            if (std.mem.eql(u8, entry.key, key)) return entry.value;
        }
        return null;
    }
};

// =============================================================================
// JobDefinition — parsed configuration for a processing job
// =============================================================================

/// Parsed job definition extracted from YAML/JSON.
///
/// Specifies which Flo-Streams to read from (sources), which
/// primitives to write to (sinks), and what operators to chain
/// in between.
pub const JobDefinition = struct {
    /// Job name (defaults to "unnamed-job" if not specified)
    name: []const u8,
    /// Optional human-readable description (empty string if not provided)
    description: []const u8,
    /// Namespace for this job (inherited by sources/sinks when not overridden)
    namespace: []const u8,
    /// Parallelism level (defaults to 1)
    parallelism: u32,
    /// Default batch size for sources (defaults to 100)
    batch_size: u32,

    /// Source endpoints (at least one required)
    sources: std.ArrayList(SourceSpec),
    /// Sink endpoints (at least one required)
    sinks: std.ArrayList(SinkSpec),
    /// Ordered list of operator specifications
    operators: std.ArrayList(OperatorSpec),

    /// Checkpoint interval in milliseconds (null = use default, 0 = disabled)
    checkpoint_interval_ms: ?u64 = null,

    // =========================================================================
    // Convenience accessors
    // =========================================================================

    /// Primary source (sources[0]) — used by handler until multi-source is wired.
    pub fn primarySource(self: *const JobDefinition) ?*const SourceSpec {
        if (self.sources.items.len == 0) return null;
        return &self.sources.items[0];
    }

    /// Primary sink (sinks[0]) — used by handler until multi-sink is wired.
    pub fn primarySink(self: *const JobDefinition) ?*const SinkSpec {
        if (self.sinks.items.len == 0) return null;
        return &self.sinks.items[0];
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /// Free all owned memory.
    pub fn deinit(self: *JobDefinition, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.description);
        allocator.free(self.namespace);

        for (self.sources.items) |src| {
            allocator.free(src.name);
            allocator.free(src.stream);
            allocator.free(src.namespace);
            // TS-specific fields
            if (src.ts_measurement.len > 0) allocator.free(src.ts_measurement);
            if (src.ts_field.len > 0) allocator.free(src.ts_field);
            for (src.ts_tags) |t| allocator.free(t);
            if (src.ts_tags.len > 0) allocator.free(src.ts_tags);
        }
        self.sources.deinit(allocator);

        for (self.sinks.items) |snk| {
            allocator.free(snk.name);
            allocator.free(snk.target);
            allocator.free(snk.namespace);
            allocator.free(snk.key_prefix);
            allocator.free(snk.separator);
            allocator.free(snk.write_mode);
            if (snk.match) |match_list| {
                for (match_list) |t| allocator.free(t);
                allocator.free(match_list);
            }
            // TS-specific fields
            if (snk.ts_measurement.len > 0) allocator.free(snk.ts_measurement);
            if (snk.ts_value_field.len > 0) allocator.free(snk.ts_value_field);
            for (snk.ts_tag_keys) |k| allocator.free(k);
            if (snk.ts_tag_keys.len > 0) allocator.free(snk.ts_tag_keys);
            for (snk.ts_field_keys) |k| allocator.free(k);
            if (snk.ts_field_keys.len > 0) allocator.free(snk.ts_field_keys);
        }
        self.sinks.deinit(allocator);

        for (self.operators.items) |op| {
            allocator.free(op.type_name);
            allocator.free(op.name);
            if (op.module) |mp| allocator.free(mp);
            if (op.config) |entries| {
                for (entries) |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                }
                allocator.free(entries);
            }
        }
        self.operators.deinit(allocator);
    }
};

// =============================================================================
// TagRegistry tests
// =============================================================================

test "TagRegistry: getOrCreate adds and returns bit positions" {
    var reg: TagRegistry = .{};
    const bit0 = reg.getOrCreate("errors").?;
    const bit1 = reg.getOrCreate("warnings").?;
    try std.testing.expectEqual(@as(u5, 0), bit0);
    try std.testing.expectEqual(@as(u5, 1), bit1);
    try std.testing.expectEqual(@as(u5, 2), reg.count);
}

test "TagRegistry: getOrCreate returns existing bit position" {
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("errors");
    _ = reg.getOrCreate("warnings");
    const again = reg.getOrCreate("errors").?;
    try std.testing.expectEqual(@as(u5, 0), again);
    try std.testing.expectEqual(@as(u5, 2), reg.count);
}

test "TagRegistry: resolve finds existing tag" {
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("errors");
    _ = reg.getOrCreate("warnings");
    try std.testing.expectEqual(@as(u5, 1), reg.resolve("warnings").?);
}

test "TagRegistry: resolve returns null for unknown tag" {
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("errors");
    try std.testing.expect(reg.resolve("unknown") == null);
}

test "TagRegistry: buildMask computes bitmask from names" {
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("errors"); // bit 0
    _ = reg.getOrCreate("warnings"); // bit 1
    _ = reg.getOrCreate("info"); // bit 2

    const mask = reg.buildMask(&.{ "errors", "info" });
    try std.testing.expectEqual(@as(u32, 0b101), mask);
}

test "TagRegistry: buildMask ignores unknown names" {
    var reg: TagRegistry = .{};
    _ = reg.getOrCreate("errors"); // bit 0

    const mask = reg.buildMask(&.{ "errors", "nonexistent" });
    try std.testing.expectEqual(@as(u32, 0b1), mask);
}

test "TagRegistry: capacity limit" {
    var reg: TagRegistry = .{};
    var buf: [32][10]u8 = undefined;
    for (0..32) |i| {
        const name = std.fmt.bufPrint(&buf[i], "tag{d}", .{i}) catch unreachable;
        try std.testing.expect(reg.getOrCreate(name) != null);
    }
    // 33rd tag should fail
    try std.testing.expect(reg.getOrCreate("overflow") == null);
}
