//! Metrics Configuration
//!
//! Configuration types for Prometheus metrics endpoint

/// Metrics configuration
pub const MetricsConfig = struct {
    /// Enable HTTP metrics endpoint for Prometheus scraping
    enabled: bool = true,
    /// Port for metrics HTTP server (0 = derive from listen_port + 1)
    port: u16 = 0,
    /// Bind address for the metrics server.
    ///
    /// Defaults to loopback: /metrics and /health are unauthenticated (unlike
    /// the dashboard, which is API-key gated), so the scrape endpoint is not
    /// exposed off-box unless an operator opts in. Set "0.0.0.0" to allow
    /// remote scraping.
    bind: []const u8 = "127.0.0.1",
};
