//! stdx.fs — Compatibility shim for `std.fs` APIs that became `std.Io.Dir`/`File`
//! in Zig 0.16. These wrappers fetch the process-wide `Io` from `stdx.io.instance()`
//! so call sites don't need to thread an `Io` parameter through their signatures.
//!
//! This is **only** for boundary code (CLI, config loading, snapshots, manifests,
//! cold storage, dashboard helpers). Hot-path code should not use these — the
//! reactor stays on `std.posix.system.*` directly.

const std = @import("std");
const io_mod = @import("io.zig");

pub const Dir = std.Io.Dir;
pub const File = std.Io.File;
pub const path = std.fs.path;
pub const max_path_bytes = std.fs.max_path_bytes;
pub const max_name_bytes = std.fs.max_name_bytes;

/// Get the process-wide `Io` used by all shim helpers.
fn io() std.Io {
    return io_mod.instance();
}

/// Current working directory.
pub fn cwd() Dir {
    return Dir.cwd();
}

/// Open a directory relative to cwd.
pub fn openDir(sub_path: []const u8, options: Dir.OpenOptions) Dir.OpenError!Dir {
    return cwd().openDir(io(), sub_path, options);
}

/// Open a file relative to cwd.
pub fn openFile(sub_path: []const u8, options: Dir.OpenFileOptions) File.OpenError!File {
    return cwd().openFile(io(), sub_path, options);
}

/// Create (or truncate) a file relative to cwd.
pub fn createFile(sub_path: []const u8, flags: Dir.CreateFileOptions) File.OpenError!File {
    return cwd().createFile(io(), sub_path, flags);
}

/// Make a directory tree relative to cwd (succeeds if it already exists).
pub fn makePath(sub_path: []const u8) Dir.CreateDirPathError!void {
    return cwd().createDirPath(io(), sub_path);
}

/// Delete a file relative to cwd.
pub fn deleteFile(sub_path: []const u8) Dir.DeleteFileError!void {
    return cwd().deleteFile(io(), sub_path);
}

/// Delete a directory tree relative to cwd.
pub fn deleteTree(sub_path: []const u8) Dir.DeleteTreeError!void {
    return cwd().deleteTree(io(), sub_path);
}

/// Read entire file relative to cwd.
pub fn readFile(sub_path: []const u8, buffer: []u8) Dir.ReadFileError![]u8 {
    return cwd().readFile(io(), sub_path, buffer);
}

/// Read entire file with allocator.
pub fn readFileAlloc(allocator: std.mem.Allocator, sub_path: []const u8, max_bytes: usize) ![]u8 {
    return cwd().readFileAlloc(io(), sub_path, allocator, .limited(max_bytes));
}

/// Stat a file relative to cwd.
pub fn statFile(sub_path: []const u8, options: Dir.StatFileOptions) Dir.StatFileError!Dir.Stat {
    return cwd().statFile(io(), sub_path, options);
}

/// Get the canonical absolute path for `pathname`.
pub fn realpathAlloc(allocator: std.mem.Allocator, pathname: []const u8) ![]u8 {
    var buf: [max_path_bytes]u8 = undefined;
    const len = try cwd().realPathFile(io(), pathname, &buf);
    return allocator.dupe(u8, buf[0..len]);
}

/// Get the canonical absolute path for `pathname` resolved against `dir`.
pub fn dirRealpathAlloc(dir: Dir, allocator: std.mem.Allocator, pathname: []const u8) ![]u8 {
    var buf: [max_path_bytes]u8 = undefined;
    const len = try dir.realPathFile(io(), pathname, &buf);
    return allocator.dupe(u8, buf[0..len]);
}

/// Close helpers — old `std.fs.Dir.close()` is now `Dir.close(io)`.
pub fn closeDir(dir: Dir) void {
    dir.close(io());
}

pub fn closeFile(file: File) void {
    file.close(io());
}

/// Open a file by absolute path.
pub fn openFileAbsolute(absolute_path: []const u8, options: Dir.OpenFileOptions) File.OpenError!File {
    return Dir.openFileAbsolute(io(), absolute_path, options);
}

/// Create (or truncate) a file by absolute path.
pub fn createFileAbsolute(absolute_path: []const u8, flags: Dir.CreateFileOptions) File.OpenError!File {
    return Dir.createFileAbsolute(io(), absolute_path, flags);
}

/// Delete a file by absolute path.
pub fn deleteFileAbsolute(absolute_path: []const u8) Dir.DeleteFileError!void {
    return Dir.deleteFileAbsolute(io(), absolute_path);
}

/// Check if `sub_path` exists / is accessible relative to cwd.
pub fn access(sub_path: []const u8, options: Dir.AccessOptions) Dir.AccessError!void {
    return cwd().access(io(), sub_path, options);
}

/// Rename `old_path` to `new_path` (both relative to cwd).
pub fn rename(old_path: []const u8, new_path: []const u8) Dir.RenameError!void {
    const c = cwd();
    return c.rename(old_path, c, new_path, io());
}

/// Read all bytes from an opened file with allocator.
/// Replacement for `std.fs.File.readToEndAlloc`.
pub fn readToEndAlloc(file: File, allocator: std.mem.Allocator, max_bytes: usize) ![]u8 {
    const file_size = try file.length(io());
    if (file_size > max_bytes) return error.FileTooBig;
    const buf = try allocator.alloc(u8, @intCast(file_size));
    errdefer allocator.free(buf);
    var read_total: usize = 0;
    while (read_total < buf.len) {
        const n = try file.readPositional(io(), &.{buf[read_total..]}, read_total);
        if (n == 0) break;
        read_total += n;
    }
    return buf[0..read_total];
}

/// Read up to `buffer.len` bytes from an opened file at the current position.
/// Advances the file's position (sequential read). Returns total bytes read,
/// which may be less than `buffer.len` on EOF.
pub fn readAll(file: File, buffer: []u8) !usize {
    var read_total: usize = 0;
    while (read_total < buffer.len) {
        const n = file.readStreaming(io(), &.{buffer[read_total..]}) catch |err| switch (err) {
            error.EndOfStream => break,
            else => |e| return e,
        };
        if (n == 0) break;
        read_total += n;
    }
    return read_total;
}

/// Write all bytes to an opened file.
pub fn writeAll(file: File, bytes: []const u8) !void {
    return file.writeStreamingAll(io(), bytes);
}

/// Get file size (replacement for `file.stat().size`). Uses the process-wide Io.
pub fn fileLength(file: File) !u64 {
    return file.length(@import("io.zig").instance());
}

/// Sync file contents to disk.
pub fn sync(file: File) !void {
    return file.sync(@import("io.zig").instance());
}

/// Stat a file via its handle.
pub fn statHandle(file: File) !File.Stat {
    return file.stat(@import("io.zig").instance());
}

/// Read up to `buf.len` bytes from current file position.
pub fn readBytes(file: File, buf: []u8) !usize {
    return readAll(file, buf);
}
