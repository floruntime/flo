//! Metrics Configuration
//!
//! Configuration types for Prometheus metrics endpoint

/// Metrics configuration
pub const MetricsConfig = struct {
    /// Enable HTTP metrics endpoint for Prometheus scraping
    enabled: bool = true,
    /// Port for metrics HTTP server (0 = derive from listen_port + 1)
    port: u16 = 0,
    /// Bind address for metrics server
    bind: []const u8 = "0.0.0.0",
};
