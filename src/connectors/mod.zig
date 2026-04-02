//! Connectors — External system integrations
//!
//! Each sub-module implements wire-protocol clients for an external system.
//! Connectors are consumed by processing pipelines (sources/sinks) and
//! potentially by other subsystems (replication, streaming, etc.).
//!
//! ## Adding a new connector
//! 1. Create a sub-directory (e.g. `redis/`)
//! 2. Implement the wire protocol codec + connection pool
//! 3. Implement Source/Sink vtable adapters for processing
//! 4. Re-export from this module

pub const kafka = @import("kafka/mod.zig");
