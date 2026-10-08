//! Central Configuration Module
//!
//! This module provides shared configuration types used across the codebase.
//! It ensures a single source of truth for configuration structs like
//! ColdStorageConfig, ClusterConfig, etc.
//!
//! Usage:
//!   const config = @import("config/mod.zig");
//!   const ColdStorageConfig = config.ColdStorageConfig;

pub const metrics = @import("metrics.zig");
pub const dashboard = @import("dashboard.zig");
pub const server = @import("server.zig");
pub const cluster = @import("cluster.zig");
pub const tiered_log = @import("tiered_log.zig");

// Re-export commonly used types at top level

pub const TieredLogConfig = tiered_log.TieredLogConfig;

pub const MetricsConfig = metrics.MetricsConfig;
pub const DashboardConfig = dashboard.DashboardConfig;

// Cluster config
pub const ClusterConfig = cluster.ClusterConfig;
pub const parseClusterConfig = cluster.parseClusterConfig;

// Server config
pub const ServerConfig = server.ServerConfig;
pub const MAX_SHARDS = server.MAX_SHARDS;
pub const load = server.load;
pub const loadWithOverrides = server.loadWithOverrides;
pub const generateDefaultConfig = server.generateDefaultConfig;
