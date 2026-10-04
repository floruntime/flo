//! Dashboard Configuration
//!
//! Configuration types for the web dashboard.
//!
//! Security Model: API key + session token (M.8 auth)
//! - Default bind to localhost for safe out-of-box experience
//! - Requires `flo server bootstrap` to generate root API key
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
    /// Other origins whose pages may call the API, exactly as a browser
    /// names them (`https://ops.example.com:8443`), comma-separated. Empty:
    /// only the dashboard's own pages. There is no wildcard.
    cors_origins: []const u8 = "",
    /// Host names the dashboard answers to besides localhost, 127.0.0.1 and
    /// [::1], comma-separated (`flo.internal,10.0.1.5`).
    hosts: []const u8 = "",
};
