//! Workflow Plan Types
//!
//! Types for inline plans - composed actions with resilience features.
//! Plans are defined inline within Workflow YAML, not as separate entities.
//!
//! # Key Types
//!
//! - `ExecutorConfig`: Individual executor configuration
//! - `ExecutorHealth`: Runtime health metrics (stored in KV)
//! - `PlanFeatures`: Auto-detected features from config
//! - `SelectionStrategy`: How to select among executors
//!
//! # Features
//!
//! Plans can have various features auto-detected from config:
//! - Circuit breaker (from `breaker:` config)
//! - Health-weighted routing (from `selection: health-weighted`)
//! - Rate limiting (from `rate_limit:` config)
//! - Caching (from `cache:` config)
//! - Async tracking (from `tracking: { mode: async }`)
//! - Fallback values (from `fallback:` config)

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const types = @import("types.zig");

// Re-export from types for convenience
pub const RunStatus = types.RunStatus;

// =============================================================================
// Selection Strategy
// =============================================================================

/// Strategy for selecting executors in a plan
pub const SelectionStrategy = enum(u8) {
    /// Always try in definition order (by priority)
    static_order = 0,
    /// Rotate starting executor across requests
    round_robin = 1,
    /// Random selection for load distribution
    random = 2,
    /// Prefer healthier executors based on success rate
    health_weighted = 3,

    pub fn toString(self: SelectionStrategy) []const u8 {
        return switch (self) {
            .static_order => "static-order",
            .round_robin => "round-robin",
            .random => "random",
            .health_weighted => "health-weighted",
        };
    }

    pub fn fromString(s: []const u8) ?SelectionStrategy {
        const map = std.StaticStringMap(SelectionStrategy).initComptime(.{
            .{ "static-order", .static_order },
            .{ "static_order", .static_order },
            .{ "round-robin", .round_robin },
            .{ "round_robin", .round_robin },
            .{ "random", .random },
            .{ "health-weighted", .health_weighted },
            .{ "health_weighted", .health_weighted },
        });
        return map.get(s);
    }
};

// =============================================================================
// Plan Features (Auto-detected)
// =============================================================================

/// Features auto-detected from plan configuration
/// Not stored - computed at parse time
pub const PlanFeatures = struct {
    /// true if any executor has breaker config
    circuit_breaker: bool = false,
    /// true if selection is health-weighted
    health_tracking: bool = false,
    /// true if any executor has async tracking
    async_tracking: bool = false,
    /// true if any executor has rate_limit config
    rate_limiting: bool = false,
    /// true if cache config exists
    caching: bool = false,
    /// true if fallback config exists
    fallback_value: bool = false,

    /// Compute features from inline plan configuration
    pub fn fromInlinePlan(
        selection: SelectionStrategy,
        executors: []const ExecutorConfig,
        cache_config: ?CacheConfig,
        fallback_config: ?FallbackConfig,
    ) PlanFeatures {
        var features = PlanFeatures{};

        // Check selection strategy
        features.health_tracking = selection == .health_weighted;

        // Check executors
        for (executors) |exec| {
            if (exec.breaker != null) features.circuit_breaker = true;
            if (exec.rate_limit != null) features.rate_limiting = true;
            if (exec.tracking) |t| {
                if (t.mode == .async_mode) features.async_tracking = true;
            }
        }

        // Check plan-level configs
        features.caching = cache_config != null;
        features.fallback_value = fallback_config != null;

        return features;
    }
};

// =============================================================================
// Circuit Breaker
// =============================================================================

/// Circuit breaker state
pub const CircuitBreakerState = enum(u8) {
    /// Normal operation, requests flow through
    closed = 0,
    /// Blocking requests, executor is skipped
    open = 1,
    /// Testing recovery with probe requests
    half_open = 2,

    pub fn toString(self: CircuitBreakerState) []const u8 {
        return switch (self) {
            .closed => "closed",
            .open => "open",
            .half_open => "half_open",
        };
    }
};

/// Circuit breaker configuration
pub const CircuitBreakerConfig = struct {
    /// Number of consecutive failures before opening
    failure_threshold: u32 = 5,
    /// How long to stay open before half-open (ms)
    cooldown_ms: i64 = 30000,
    /// Number of probe requests allowed in half-open state
    half_open_max_calls: u32 = 3,
};

// =============================================================================
// Tracking Configuration
// =============================================================================

/// Tracking mode for outcome reporting
pub const TrackingMode = enum(u8) {
    /// Synchronous - outcome known immediately from API call
    sync = 0,
    /// Asynchronous - outcome reported later via webhook
    async_mode = 1,

    pub fn toString(self: TrackingMode) []const u8 {
        return switch (self) {
            .sync => "sync",
            .async_mode => "async",
        };
    }

    pub fn fromString(s: []const u8) ?TrackingMode {
        const map = std.StaticStringMap(TrackingMode).initComptime(.{
            .{ "sync", .sync },
            .{ "async", .async_mode },
        });
        return map.get(s);
    }
};

/// Tracking configuration for async outcomes
pub const TrackingConfig = struct {
    /// Tracking mode
    mode: TrackingMode,
    /// Timeout for async outcome (ms), null for sync
    timeout_ms: ?i64,
};

// =============================================================================
// Rate Limiting
// =============================================================================

/// Rate limit configuration
pub const RateLimitConfig = struct {
    /// Max requests per second (null = unlimited)
    max_per_second: ?u32,
    /// Max requests per minute (null = unlimited)
    max_per_minute: ?u32,
    /// Max requests per hour (null = unlimited)
    max_per_hour: ?u32,
};

// =============================================================================
// Caching
// =============================================================================

/// Cache configuration for result reuse
pub const CacheConfig = struct {
    /// Time-to-live for cached results (ms)
    ttl_ms: i64,
    /// Template for cache key (e.g., "user:{input.user_id}")
    key_template: []const u8,
    /// Event types that invalidate cache
    invalidate_on: [][]const u8,

    pub fn deinit(self: *CacheConfig, allocator: Allocator) void {
        allocator.free(self.key_template);
        for (self.invalidate_on) |event| {
            allocator.free(event);
        }
        if (self.invalidate_on.len > 0) allocator.free(self.invalidate_on);
    }

    pub fn clone(self: CacheConfig, allocator: Allocator) !CacheConfig {
        const invalidate_on = try allocator.alloc([]const u8, self.invalidate_on.len);
        for (self.invalidate_on, 0..) |event, i| {
            invalidate_on[i] = try allocator.dupe(u8, event);
        }
        return .{
            .ttl_ms = self.ttl_ms,
            .key_template = try allocator.dupe(u8, self.key_template),
            .invalidate_on = invalidate_on,
        };
    }
};

// =============================================================================
// Health Configuration
// =============================================================================

/// Health tracking configuration
pub const HealthConfig = struct {
    /// Rolling window for health calculation (ms)
    window_ms: i64,
    /// Exponential decay factor for old samples
    decay: f64,
    /// Minimum samples before health-weighted kicks in
    min_samples: u32,
};

// =============================================================================
// Fallback Configuration
// =============================================================================

/// Condition for using fallback value
pub const FallbackCondition = enum(u8) {
    /// Only when all executors exhausted
    exhausted = 0,
    /// On any error (including fatal)
    any_error = 1,

    pub fn toString(self: FallbackCondition) []const u8 {
        return switch (self) {
            .exhausted => "exhausted",
            .any_error => "any_error",
        };
    }
};

/// Fallback configuration
pub const FallbackConfig = struct {
    /// Default value to return (JSON)
    value: []const u8,
    /// When to use fallback
    condition: FallbackCondition,

    pub fn deinit(self: *FallbackConfig, allocator: Allocator) void {
        allocator.free(self.value);
    }

    pub fn clone(self: FallbackConfig, allocator: Allocator) !FallbackConfig {
        return .{
            .value = try allocator.dupe(u8, self.value),
            .condition = self.condition,
        };
    }
};

// =============================================================================
// Error Classification
// =============================================================================

/// Error classification for retry decisions
pub const ErrorClassification = struct {
    /// Errors that should trigger retry
    retryable: [][]const u8,
    /// Errors that should not retry (immediate failure)
    fatal: [][]const u8,

    pub fn deinit(self: *ErrorClassification, allocator: Allocator) void {
        for (self.retryable) |err| allocator.free(err);
        if (self.retryable.len > 0) allocator.free(self.retryable);
        for (self.fatal) |err| allocator.free(err);
        if (self.fatal.len > 0) allocator.free(self.fatal);
    }

    pub fn clone(self: ErrorClassification, allocator: Allocator) !ErrorClassification {
        const retryable = try allocator.alloc([]const u8, self.retryable.len);
        for (self.retryable, 0..) |err, i| {
            retryable[i] = try allocator.dupe(u8, err);
        }

        const fatal = try allocator.alloc([]const u8, self.fatal.len);
        for (self.fatal, 0..) |err, i| {
            fatal[i] = try allocator.dupe(u8, err);
        }

        return .{
            .retryable = retryable,
            .fatal = fatal,
        };
    }

    /// Check if an error code is retryable
    pub fn isRetryable(self: ErrorClassification, error_code: []const u8) bool {
        for (self.retryable) |code| {
            if (mem.eql(u8, code, error_code)) return true;
        }
        return false;
    }

    /// Check if an error code is fatal
    pub fn isFatal(self: ErrorClassification, error_code: []const u8) bool {
        for (self.fatal) |code| {
            if (mem.eql(u8, code, error_code)) return true;
        }
        return false;
    }
};

// =============================================================================
// Retry Policy
// =============================================================================

/// Backoff type for retry delays
pub const BackoffType = enum(u8) {
    /// Fixed delay between retries
    constant = 0,
    /// Linearly increasing delay
    linear = 1,
    /// Exponentially increasing delay
    exponential = 2,
    /// Exponential with random jitter
    exponential_jitter = 3,

    pub fn toString(self: BackoffType) []const u8 {
        return switch (self) {
            .constant => "constant",
            .linear => "linear",
            .exponential => "exponential",
            .exponential_jitter => "exp-jitter",
        };
    }

    pub fn fromString(s: []const u8) ?BackoffType {
        // Handle various YAML formats
        if (mem.startsWith(u8, s, "constant")) return .constant;
        if (mem.startsWith(u8, s, "linear")) return .linear;
        if (mem.startsWith(u8, s, "exp-jitter") or mem.startsWith(u8, s, "exponential-jitter") or mem.startsWith(u8, s, "exponential_jitter")) return .exponential_jitter;
        if (mem.startsWith(u8, s, "exp") or mem.startsWith(u8, s, "exponential")) return .exponential;
        return null;
    }
};

/// Retry policy for executor attempts
pub const RetryPolicy = struct {
    /// Maximum number of attempts
    max_attempts: u32 = 3,
    /// Type of backoff
    backoff: BackoffType = .exponential,
    /// Initial delay (ms)
    initial_delay_ms: u32 = 1000,
    /// Maximum delay cap (ms)
    max_delay_ms: u32 = 30000,
    /// Total time budget for all retries (ms), null = unlimited
    within_ms: ?u64 = null,
    /// Calculate delay for a given attempt (0-indexed)
    pub fn calculateDelay(self: RetryPolicy, attempt: u32) u32 {
        const delay: u32 = switch (self.backoff) {
            .constant => self.initial_delay_ms,
            .linear => self.initial_delay_ms *| (attempt +| 1),
            .exponential => blk: {
                const multiplier = std.math.powi(u32, 2, attempt) catch std.math.maxInt(u32);
                break :blk self.initial_delay_ms *| multiplier;
            },
            .exponential_jitter => blk: {
                const base_multiplier = std.math.powi(u32, 2, attempt) catch std.math.maxInt(u32);
                const base_delay = self.initial_delay_ms *| base_multiplier;
                // Add up to 25% jitter
                var rng = std.Random.DefaultPrng.init(@intCast(@import("stdx").time.milliTimestamp()));
                const jitter = rng.random().intRangeAtMost(u32, 0, base_delay / 4);
                break :blk base_delay +| jitter;
            },
        };
        return @min(delay, self.max_delay_ms);
    }
};

// =============================================================================
// Executor Configuration
// =============================================================================

/// Configuration for a single executor in a plan
pub const ExecutorConfig = struct {
    /// Executor name (unique within plan)
    name: []const u8,
    /// Action to invoke (e.g., "@actions/charge-stripe")
    action_name: []const u8,
    /// Priority (higher = tried first)
    priority: i32,
    /// Retry policy for this executor
    retry: ?RetryPolicy,
    /// Circuit breaker configuration
    breaker: ?CircuitBreakerConfig,
    /// Tracking configuration (sync/async)
    tracking: ?TrackingConfig,
    /// Rate limiting configuration
    rate_limit: ?RateLimitConfig,

    pub fn deinit(self: *ExecutorConfig, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.action_name);
    }

    pub fn clone(self: ExecutorConfig, allocator: Allocator) !ExecutorConfig {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .action_name = try allocator.dupe(u8, self.action_name),
            .priority = self.priority,
            .retry = self.retry,
            .breaker = self.breaker,
            .tracking = self.tracking,
            .rate_limit = self.rate_limit,
        };
    }
};

// =============================================================================
// Idempotency Mode
// =============================================================================

/// Idempotency mode for plan execution
pub const IdempotencyMode = enum(u8) {
    /// No idempotency check
    none = 0,
    /// Idempotency key optional
    optional = 1,
    /// Idempotency key required
    required = 2,

    pub fn toString(self: IdempotencyMode) []const u8 {
        return switch (self) {
            .none => "none",
            .optional => "optional",
            .required => "required",
        };
    }

    pub fn fromString(s: []const u8) ?IdempotencyMode {
        const map = std.StaticStringMap(IdempotencyMode).initComptime(.{
            .{ "none", .none },
            .{ "optional", .optional },
            .{ "required", .required },
        });
        return map.get(s);
    }
};

// =============================================================================
// Executor Health (Runtime State)
// =============================================================================

/// Runtime health metrics for an executor
/// Stored at: _wf:health:{namespace}:{plan}:{executor}
pub const ExecutorHealth = struct {
    /// Executor name
    executor_name: []const u8,

    // API health (for circuit breaker)
    api_total_attempts: i64 = 0,
    api_successes: i64 = 0,
    api_failures: i64 = 0,
    api_success_rate: f64 = 1.0,
    api_consecutive_failures: i32 = 0,

    // Business health (for routing)
    business_total_attempts: i64 = 0,
    business_successes: i64 = 0,
    business_failures: i64 = 0,
    business_success_rate: f64 = 1.0,
    pending_attempts: i64 = 0,

    // Circuit breaker state
    breaker_state: CircuitBreakerState = .closed,
    breaker_opened_at_ms: i64 = 0,

    // Rate limiting counters
    requests_this_second: u32 = 0,
    requests_this_minute: u32 = 0,
    requests_this_hour: u32 = 0,

    // Latency percentiles (ms)
    latency_p50_ms: i64 = 0,
    latency_p95_ms: i64 = 0,
    latency_p99_ms: i64 = 0,

    // Timestamps
    last_attempt_ms: i64 = 0,
    last_success_ms: i64 = 0,
    last_failure_ms: i64 = 0,
    updated_at_ms: i64 = 0,

    const WIRE_VERSION: u8 = 1;

    /// Create a new ExecutorHealth with default values
    pub fn init() ExecutorHealth {
        return .{
            .executor_name = "",
        };
    }

    /// Create a new ExecutorHealth with a name
    pub fn initWithName(name: []const u8) ExecutorHealth {
        return .{
            .executor_name = name,
        };
    }

    pub fn deinit(self: *ExecutorHealth, allocator: Allocator) void {
        allocator.free(self.executor_name);
    }

    pub fn clone(self: ExecutorHealth, allocator: Allocator) !ExecutorHealth {
        var result = self;
        result.executor_name = try allocator.dupe(u8, self.executor_name);
        return result;
    }

    /// Calculate overall health score (0.0 - 1.0)
    /// Higher score = healthier executor
    pub fn healthScore(self: ExecutorHealth) f64 {
        // Weighted combination of API and business success rates
        // API success is more important for circuit breaker
        const api_weight: f64 = 0.7;
        const business_weight: f64 = 0.3;

        var score: f64 = 0.0;

        // API health contribution (70%)
        if (self.api_total_attempts > 0) {
            score += self.api_success_rate * api_weight;
        } else {
            // No data = assume healthy
            score += api_weight;
        }

        // Business health contribution (30%)
        if (self.business_total_attempts > 0) {
            score += self.business_success_rate * business_weight;
        } else {
            // No data = assume healthy
            score += business_weight;
        }

        // Penalize if circuit breaker is open
        if (self.breaker_state == .open) {
            score *= 0.1; // Heavy penalty for open breaker
        } else if (self.breaker_state == .half_open) {
            score *= 0.5; // Moderate penalty for half-open
        }

        return score;
    }

    /// Record an API call attempt
    pub fn recordApiAttempt(self: *ExecutorHealth, success: bool, latency_ms: i64, now_ms: i64) void {
        self.api_total_attempts += 1;
        self.last_attempt_ms = now_ms;
        self.updated_at_ms = now_ms;

        if (success) {
            self.api_successes += 1;
            self.api_consecutive_failures = 0;
            self.last_success_ms = now_ms;
        } else {
            self.api_failures += 1;
            self.api_consecutive_failures += 1;
            self.last_failure_ms = now_ms;
        }

        // Update success rate
        if (self.api_total_attempts > 0) {
            self.api_success_rate = @as(f64, @floatFromInt(self.api_successes)) / @as(f64, @floatFromInt(self.api_total_attempts));
        }

        // Update latency (simplified - in production would use histogram)
        _ = latency_ms;
    }

    /// Record a business outcome (for async tracking)
    pub fn recordBusinessOutcome(self: *ExecutorHealth, success: bool, now_ms: i64) void {
        self.business_total_attempts += 1;
        self.pending_attempts = @max(0, self.pending_attempts - 1);
        self.updated_at_ms = now_ms;

        if (success) {
            self.business_successes += 1;
        } else {
            self.business_failures += 1;
        }

        if (self.business_total_attempts > 0) {
            self.business_success_rate = @as(f64, @floatFromInt(self.business_successes)) / @as(f64, @floatFromInt(self.business_total_attempts));
        }
    }

    /// Check if circuit breaker should allow request
    pub fn shouldAllowRequest(self: *ExecutorHealth, config: CircuitBreakerConfig, now_ms: i64) bool {
        switch (self.breaker_state) {
            .closed => return true,
            .open => {
                // Check if cooldown has passed
                if (now_ms - self.breaker_opened_at_ms >= config.cooldown_ms) {
                    self.breaker_state = .half_open;
                    return true;
                }
                return false;
            },
            .half_open => {
                // Allow limited probe requests
                return true;
            },
        }
    }

    /// Update circuit breaker state after attempt
    pub fn updateBreakerState(self: *ExecutorHealth, config: CircuitBreakerConfig, success: bool, now_ms: i64) void {
        switch (self.breaker_state) {
            .closed => {
                if (!success and @as(i64, self.api_consecutive_failures) >= config.failure_threshold) {
                    self.breaker_state = .open;
                    self.breaker_opened_at_ms = now_ms;
                }
            },
            .half_open => {
                if (success) {
                    self.breaker_state = .closed;
                    self.api_consecutive_failures = 0;
                } else {
                    self.breaker_state = .open;
                    self.breaker_opened_at_ms = now_ms;
                }
            },
            .open => {
                // Should not reach here during normal flow
            },
        }
    }
};

// =============================================================================
// Wire Format Helpers
// =============================================================================
// =============================================================================
// Tests
// =============================================================================

test "SelectionStrategy: toString and fromString" {
    const testing = std.testing;

    try testing.expectEqualStrings("static-order", SelectionStrategy.static_order.toString());
    try testing.expectEqualStrings("health-weighted", SelectionStrategy.health_weighted.toString());

    try testing.expectEqual(SelectionStrategy.static_order, SelectionStrategy.fromString("static-order").?);
    try testing.expectEqual(SelectionStrategy.static_order, SelectionStrategy.fromString("static_order").?);
    try testing.expectEqual(SelectionStrategy.health_weighted, SelectionStrategy.fromString("health-weighted").?);
}

test "RetryPolicy: calculateDelay" {
    const testing = std.testing;

    const policy = RetryPolicy{
        .max_attempts = 3,
        .backoff = .exponential,
        .initial_delay_ms = 1000,
        .max_delay_ms = 30000,
    };

    // Exponential: 1000 * 2^attempt
    try testing.expectEqual(@as(u32, 1000), policy.calculateDelay(0)); // 1000 * 1
    try testing.expectEqual(@as(u32, 2000), policy.calculateDelay(1)); // 1000 * 2
    try testing.expectEqual(@as(u32, 4000), policy.calculateDelay(2)); // 1000 * 4

    // Constant backoff
    const constant_policy = RetryPolicy{
        .backoff = .constant,
        .initial_delay_ms = 500,
        .max_delay_ms = 30000,
    };
    try testing.expectEqual(@as(u32, 500), constant_policy.calculateDelay(0));
    try testing.expectEqual(@as(u32, 500), constant_policy.calculateDelay(5));
}

test "ExecutorHealth: circuit breaker state machine" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var health = ExecutorHealth{
        .executor_name = try allocator.dupe(u8, "stripe"),
    };
    defer health.deinit(allocator);

    const config = CircuitBreakerConfig{
        .failure_threshold = 3,
        .cooldown_ms = 60000,
        .half_open_max_calls = 2,
    };

    var now_ms: i64 = 1000000;

    // Initially closed
    try testing.expectEqual(CircuitBreakerState.closed, health.breaker_state);
    try testing.expect(health.shouldAllowRequest(config, now_ms));

    // Record failures up to threshold
    health.recordApiAttempt(false, 100, now_ms);
    health.updateBreakerState(config, false, now_ms);
    try testing.expectEqual(CircuitBreakerState.closed, health.breaker_state);

    health.recordApiAttempt(false, 100, now_ms);
    health.updateBreakerState(config, false, now_ms);
    try testing.expectEqual(CircuitBreakerState.closed, health.breaker_state);

    health.recordApiAttempt(false, 100, now_ms);
    health.updateBreakerState(config, false, now_ms);
    // Now should be open
    try testing.expectEqual(CircuitBreakerState.open, health.breaker_state);

    // Should not allow request while open
    try testing.expect(!health.shouldAllowRequest(config, now_ms));

    // After cooldown, transitions to half-open
    now_ms += config.cooldown_ms + 1;
    try testing.expect(health.shouldAllowRequest(config, now_ms));
    try testing.expectEqual(CircuitBreakerState.half_open, health.breaker_state);

    // Success in half-open closes breaker
    health.recordApiAttempt(true, 50, now_ms);
    health.updateBreakerState(config, true, now_ms);
    try testing.expectEqual(CircuitBreakerState.closed, health.breaker_state);
}
