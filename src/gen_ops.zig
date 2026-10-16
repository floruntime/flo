//! `zig build gen`: writes ops.json (protocol/ops_json.zig) to stdout.

const std = @import("std");
const stdx = @import("stdx");
const ops_json = @import("protocol/ops_json.zig");

pub fn main() !void {
    var out: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
    defer out.deinit();
    try ops_json.write(&out.writer);
    try out.writer.writeByte('\n');
    if (stdx.io.writeFd(std.posix.STDOUT_FILENO, out.written()) != out.written().len) return error.WriteFailed;
}
