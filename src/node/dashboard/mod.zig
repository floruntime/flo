//! Dashboard Module
//!
//! Web dashboard for monitoring and managing Flo primitives.
//! Provides REST API and static file serving for the React UI.

pub const DashboardServer = @import("http_server.zig").DashboardServer;
pub const DashboardServerConfig = @import("http_server.zig").DashboardServerConfig;
pub const api = @import("api.zig");
