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

/// Expand a leading `~` to `$HOME` so paths are never created literally.
///
/// `~` alone → `$HOME`; `~/x` → `$HOME/x`. Any other form (including a `~`
/// that isn't the first byte) is returned unchanged. The result is always a
/// freshly allocated copy the caller owns and must free.
///
/// This is the single choke point for tilde handling — every code path that
/// turns a configured data dir into real directories (`makePath`) must route
/// through here, otherwise an unexpanded `~/.flo/data` silently creates a
/// literal `~` directory under the current working directory.
pub fn expandTilde(allocator: std.mem.Allocator, sub_path: []const u8) ![]u8 {
    if (sub_path.len > 0 and sub_path[0] == '~') {
        const home = io_mod.getenv("HOME") orelse return error.NoHomeDirectory;
        if (sub_path.len == 1) {
            return allocator.dupe(u8, home);
        }
        if (sub_path[1] == '/') {
            return std.fmt.allocPrint(allocator, "{s}{s}", .{ home, sub_path[1..] });
        }
    }
    return allocator.dupe(u8, sub_path);
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

/// Directory fsyncs performed by this process. A dir fsync has no effect a
/// test can observe, so tests read this to prove a durable write made one.
pub var dir_syncs: std.atomic.Value(u64) = .init(0);

pub const SyncDirError = Dir.OpenError || File.SyncError || error{DirSyncUnsupported};

/// fsync the directory at `dir_path`. A file's own fsync covers its bytes,
/// not the directory entry that names it: after a create, rename or unlink
/// the change is atomic but can still vanish on power loss until the
/// directory itself is synced.
pub fn syncDir(dir_path: []const u8) SyncDirError!void {
    return syncDirAt(cwd(), if (dir_path.len == 0) "." else dir_path);
}

/// `syncDir` for a directory already open as `dir`.
pub fn syncDirHandle(dir: Dir) SyncDirError!void {
    return syncDirAt(dir, ".");
}

fn syncDirAt(base: Dir, sub_path: []const u8) SyncDirError!void {
    // `.iterate` because without it Linux opens the directory O_PATH, and
    // fsync on an O_PATH descriptor fails with EBADF.
    const dir = try base.openDir(io(), sub_path, .{ .iterate = true });
    defer closeDir(dir);
    // Called directly rather than through `File.sync`: std treats EINVAL as a
    // programmer bug and panics in debug builds, but some filesystems refuse
    // to sync a directory, and that must surface as an error, not a crash.
    while (true) {
        switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
            .SUCCESS => {
                _ = dir_syncs.fetchAdd(1, .monotonic);
                return;
            },
            .INTR => continue,
            .INVAL => return error.DirSyncUnsupported,
            .BADF => unreachable, // opened above, and not O_PATH
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .DQUOT => return error.DiskQuota,
            .ACCES, .PERM => return error.AccessDenied,
            else => |e| return std.posix.unexpectedErrno(e),
        }
    }
}

/// `makePath`, then fsync the parent so the new directory's own entry
/// survives power loss. Covers one level: create nested durable directories
/// one call per level, outermost first.
pub fn makePathDurable(sub_path: []const u8) (Dir.CreateDirPathError || SyncDirError)!void {
    try makePath(sub_path);
    try syncDir(path.dirname(sub_path) orelse ".");
}

/// Rename `old_path` to `new_path`, then fsync the directory holding
/// `new_path` so the new name survives power loss, not just a crash.
pub fn renameDurable(old_path: []const u8, new_path: []const u8) (Dir.RenameError || SyncDirError)!void {
    try rename(old_path, new_path);
    try syncDir(path.dirname(new_path) orelse ".");
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

test "syncDir, renameDurable and makePathDurable sync a directory, and a missing one is an error" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [max_path_bytes]u8 = undefined;
    const len = try tmp.dir.realPathFile(io(), ".", &buf);
    const dir_path = buf[0..len];

    const before = dir_syncs.load(.monotonic);
    try syncDir(dir_path);
    try std.testing.expectEqual(before + 1, dir_syncs.load(.monotonic));

    var a_buf: [max_path_bytes]u8 = undefined;
    var b_buf: [max_path_bytes]u8 = undefined;
    const a = try std.fmt.bufPrint(&a_buf, "{s}/a", .{dir_path});
    const b = try std.fmt.bufPrint(&b_buf, "{s}/b", .{dir_path});
    closeFile(try createFile(a, .{}));
    try renameDurable(a, b);
    try std.testing.expectEqual(before + 2, dir_syncs.load(.monotonic));
    try access(b, .{});

    var d_buf: [max_path_bytes]u8 = undefined;
    const d = try std.fmt.bufPrint(&d_buf, "{s}/d", .{dir_path});
    try makePathDurable(d);
    try std.testing.expectEqual(before + 3, dir_syncs.load(.monotonic));

    var gone_buf: [max_path_bytes]u8 = undefined;
    const gone = try std.fmt.bufPrint(&gone_buf, "{s}/missing", .{dir_path});
    try std.testing.expectError(error.FileNotFound, syncDir(gone));
    try std.testing.expectEqual(before + 3, dir_syncs.load(.monotonic));
}
