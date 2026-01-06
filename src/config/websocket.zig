//! WebSocket Configuration
//!
//! Configuration types for WebSocket server behavior

/// WebSocket configuration
pub const WebSocketConfig = struct {
    /// Rate limit: max requests per window (0 = unlimited)
    rate_limit_requests: u32 = 1000,
    /// Rate limit: window size in milliseconds
    rate_limit_window_ms: i64 = 1000,
    /// Ping interval in milliseconds (0 = disabled)
    ping_interval_ms: i64 = 30_000,
    /// Pong timeout in milliseconds (connection closed if no pong received)
    pong_timeout_ms: i64 = 10_000,
};
