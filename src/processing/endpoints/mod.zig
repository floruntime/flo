//! Endpoints Module
//!
//! Source and Sink vtable interfaces for stream processing pipelines.
//! Sources produce records; sinks consume them.

pub const source = @import("source.zig");
pub const Source = source.Source;
pub const SliceSource = source.SliceSource;

pub const sink = @import("sink.zig");
pub const Sink = sink.Sink;
pub const CollectingSink = sink.CollectingSink;

test {
    _ = source;
    _ = sink;
}
