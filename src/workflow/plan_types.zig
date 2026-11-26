//! Workflow Plan Types
//!
//! Types for inline plans: one step tried through a list of executors.
//! Plans are defined inline within Workflow YAML, not as separate entities.
//!
//! - `ExecutorConfig`: one executor, its retries and its circuit breaker
//! - `ExecutorHealth`: an executor's runtime health, leader memory
//! - `SelectionStrategy`: how a plan picks among its executors
//!
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
    /// How long to stay open before half-open (ms); half-open takes the next
    /// call as its probe.
    cooldown_ms: i64 = 30000,
};

// =============================================================================
// Retry Policy
// =============================================================================

/// Backoff type for poll delays
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

/// Retry policy for executor attempts. A retry runs at once after a failure.
pub const RetryPolicy = struct {
    /// Maximum number of attempts
    max_attempts: u32 = 3,
};

// =============================================================================
// Executor Configuration
// =============================================================================

/// Most executors one plan may list.
pub const MAX_PLAN_EXECUTORS = 32;

/// Configuration for a single executor in a plan
pub const ExecutorConfig = struct {
    /// Executor name (unique within plan)
    name: []const u8,
    /// Action to invoke (e.g., "@actions/charge-stripe")
    action_name: []const u8,
    /// Retry policy for this executor
    retry: ?RetryPolicy,
    /// Circuit breaker configuration
    breaker: ?CircuitBreakerConfig,

    pub fn deinit(self: *ExecutorConfig, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.action_name);
    }

    pub fn clone(self: ExecutorConfig, allocator: Allocator) !ExecutorConfig {
        return .{
            .name = try allocator.dupe(u8, self.name),
            .action_name = try allocator.dupe(u8, self.action_name),
            .retry = self.retry,
            .breaker = self.breaker,
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
    /// What `shouldAllowRequest` would answer, changing nothing.
    pub fn wouldAllowRequest(self: *const ExecutorHealth, config: CircuitBreakerConfig, now_ms: i64) bool {
        return self.breaker_state != .open or now_ms - self.breaker_opened_at_ms >= config.cooldown_ms;
    }

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
