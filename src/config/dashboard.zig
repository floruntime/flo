//! Dashboard Configuration
//!
//! Configuration types for the web dashboard.
//!
//! Security Model: Network-level (like Redis/Nomad)
//! - Default bind to localhost for safe out-of-box experience
//! - Optional admin_token for basic protection behind VPN/proxy
//! - Operators firewall/VPN to secure access

/// Dashboard configuration
pub const DashboardConfig = struct {
    /// Enable embedded web dashboard
    enabled: bool = true,
    /// Port for dashboard HTTP server (0 = derive from listen_port + 2)
    port: u16 = 0,
    /// Bind address for dashboard server
    /// Default: "127.0.0.1" (localhost only - safe by default)
    /// Set to "0.0.0.0" to expose externally (use with firewall/VPN)
    bind: []const u8 = "127.0.0.1",
    /// CORS origins (comma-separated, or "*" for all)
    cors_origins: []const u8 = "*",
    /// Optional admin token for basic authentication
    /// If set, requires ?token=xxx query param or X-Admin-Token header
    /// Empty string means no authentication required
    admin_token: []const u8 = "",
};
