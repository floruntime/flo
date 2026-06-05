//! Local-node host telemetry — real process CPU / memory / IO for THIS node.
//!
//! Each Flo node only observes itself here (the dashboard has no cluster-membership
//! view), so this reports the live process, not peers. Signals:
//!   - cpu: process CPU-time delta (getrusage user+sys) / wall delta, normalised
//!          by core count → 0..100% of one machine's compute.
//!   - mem: whole-node memory utilisation (used / total physical) → 0..100%.
//!          (Process RSS would round to ~0% of a host's RAM, so the meter tracks
//!          the node, matching how a "MEM" gauge reads elsewhere.)
//!   - io:  disk-IO throughput relative to this process's observed peak → 0..100%.
//!          (Disk IO has no natural ceiling, so the meter is self-scaling against
//!          the busiest interval seen since start.)
//!
//! CPU is portable via getrusage. Linux uses /proc for real disk IO and memory
//! (MemTotal/MemAvailable); macOS uses sysctl (hw.memsize, vm.page_free_count)
//! and approximates IO from getrusage block-IO counters.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const time = @import("stdx").time;

const is_linux = builtin.os.tag == .linux;
const is_darwin = switch (builtin.os.tag) {
    .macos, .ios, .tvos, .watchos, .visionos => true,
    else => false,
};

/// RUSAGE_SELF — the `SELF` decl only exists on linux's rusage, so use the
/// portable literal (0 on every supported platform).
const RUSAGE_SELF: i32 = 0;

/// Per-node sampler. Holds the previous reading so rates can be derived between
/// polls. Lives on DashboardContext. Not thread-safe: the dashboard serves from a
/// single thread, and a torn read only skews one meter for one interval.
pub const Sampler = struct {
    initialized: bool = false,
    last_cpu_us: u64 = 0,
    last_io_bytes: u64 = 0,
    last_wall_ms: i64 = 0,
    peak_io_rate: f64 = 0, // bytes/sec — denominator for io%

    pub const Usage = struct { cpu: u8, mem: u8, io: u8 };

    /// Take a reading and return 0..100 meters. The first call seeds the baseline
    /// and returns zero for the rate-based meters (no delta yet).
    pub fn sample(self: *Sampler) Usage {
        const now_ms = time.milliTimestamp();
        const cpu_us = processCpuMicros();
        const io_bytes = processIoBytes();
        const mem_pct = memPercent();

        if (!self.initialized) {
            self.initialized = true;
            self.last_cpu_us = cpu_us;
            self.last_io_bytes = io_bytes;
            self.last_wall_ms = now_ms;
            return .{ .cpu = 0, .mem = mem_pct, .io = 0 };
        }

        var cpu_pct: u8 = 0;
        var io_pct: u8 = 0;
        const wall_ms = now_ms - self.last_wall_ms;
        if (wall_ms > 0) {
            const wall_s = @as(f64, @floatFromInt(wall_ms)) / 1000.0;
            const cpu_dt_s = @as(f64, @floatFromInt(cpu_us -| self.last_cpu_us)) / 1_000_000.0;
            const cores: f64 = @floatFromInt(@max(@as(usize, 1), cpuCount()));
            cpu_pct = clampPct(cpu_dt_s / wall_s / cores * 100.0);

            const io_rate = @as(f64, @floatFromInt(io_bytes -| self.last_io_bytes)) / wall_s;
            if (io_rate > self.peak_io_rate) self.peak_io_rate = io_rate;
            if (self.peak_io_rate > 0) io_pct = clampPct(io_rate / self.peak_io_rate * 100.0);
        }

        self.last_cpu_us = cpu_us;
        self.last_io_bytes = io_bytes;
        self.last_wall_ms = now_ms;
        return .{ .cpu = cpu_pct, .mem = mem_pct, .io = io_pct };
    }
};

fn clampPct(v: f64) u8 {
    if (v <= 0) return 0;
    if (v >= 100) return 100;
    return @intFromFloat(@round(v));
}

fn cpuCount() usize {
    return std.Thread.getCpuCount() catch 1;
}

/// Total process CPU time (user + system) in microseconds — portable via getrusage.
fn processCpuMicros() u64 {
    const ru = posix.getrusage(RUSAGE_SELF);
    return tvMicros(ru.utime) + tvMicros(ru.stime);
}

fn tvMicros(tv: posix.timeval) u64 {
    const sec: i64 = @intCast(tv.sec);
    const usec: i64 = @intCast(tv.usec);
    const total = sec * 1_000_000 + usec;
    return if (total > 0) @intCast(total) else 0;
}

fn memPercent() u8 {
    const total = totalRamBytes();
    if (total == 0) return 0;
    const used = usedRamBytes(total);
    if (used == 0) return 0;
    return clampPct(@as(f64, @floatFromInt(used)) / @as(f64, @floatFromInt(total)) * 100.0);
}

/// Bytes of physical memory in use across the node (total − available/free).
fn usedRamBytes(total: u64) u64 {
    if (is_linux) {
        const avail_kb = readProcField("/proc/meminfo", "MemAvailable:") orelse return 0;
        const avail = avail_kb * 1024;
        return if (total > avail) total - avail else 0;
    }
    if (is_darwin) {
        const avail = darwinAvailableBytes();
        return if (avail > 0 and total > avail) total - avail else 0;
    }
    return 0;
}

/// macOS has no single "available" sysctl, so sum the page classes the kernel can
/// reclaim without swapping: free + speculative + purgeable + reclaimable
/// file-backed (external) cache. (Activity-Monitor's "available" in spirit.)
fn darwinAvailableBytes() u64 {
    const ps = darwinSysctlU32("hw.pagesize") orelse return 0;
    var pages: u64 = 0;
    inline for (.{
        "vm.page_free_count",
        "vm.page_speculative_count",
        "vm.page_purgeable_count",
        "vm.page_pageable_external_count",
    }) |name| {
        pages += darwinSysctlU32(name) orelse 0;
    }
    return pages * @as(u64, ps);
}

fn darwinSysctlU32(comptime name: [:0]const u8) ?u32 {
    var v: u32 = 0;
    var len: usize = @sizeOf(u32);
    if (std.c.sysctlbyname(name, @ptrCast(&v), &len, null, 0) != 0) return null;
    return v;
}

fn totalRamBytes() u64 {
    if (is_linux) {
        return if (readProcField("/proc/meminfo", "MemTotal:")) |kb| kb * 1024 else 0;
    }
    if (is_darwin) {
        var val: u64 = 0;
        var len: usize = @sizeOf(u64);
        if (std.c.sysctlbyname("hw.memsize", @ptrCast(&val), &len, null, 0) == 0) return val;
    }
    return 0;
}

fn processIoBytes() u64 {
    if (is_linux) {
        const r = readProcField("/proc/self/io", "read_bytes:") orelse 0;
        const w = readProcField("/proc/self/io", "write_bytes:") orelse 0;
        return r + w;
    }
    // Fallback: block-IO operations × 512-byte blocks (often 0 on macOS).
    const ru = posix.getrusage(RUSAGE_SELF);
    const ib: u64 = if (ru.inblock > 0) @intCast(ru.inblock) else 0;
    const ob: u64 = if (ru.oublock > 0) @intCast(ru.oublock) else 0;
    return (ib + ob) * 512;
}

/// Read a `key: <integer>` field from a small /proc file. Best-effort: returns
/// null on any error or if the key/number isn't in the first 4 KiB.
fn readProcField(path: []const u8, key: []const u8) ?u64 {
    var buf: [4096]u8 = undefined;
    const file = std.fs.openFileAbsolute(path, .{}) catch return null;
    defer file.close();
    const n = file.readAll(&buf) catch return null;
    const content = buf[0..n];
    const at = std.mem.indexOf(u8, content, key) orelse return null;
    var i = at + key.len;
    while (i < content.len and (content[i] < '0' or content[i] > '9')) : (i += 1) {}
    var val: u64 = 0;
    var found = false;
    while (i < content.len and content[i] >= '0' and content[i] <= '9') : (i += 1) {
        val = val * 10 + (content[i] - '0');
        found = true;
    }
    return if (found) val else null;
}

test "sampler returns in-range meters and seeds on first call" {
    var s = Sampler{};
    const first = s.sample();
    try std.testing.expect(first.cpu == 0 and first.io == 0); // no delta yet
    try std.testing.expect(first.mem <= 100);
    try std.testing.expect(s.initialized);

    // burn a little CPU so the second sample has a delta
    var acc: u64 = 0;
    var k: u64 = 0;
    while (k < 2_000_000) : (k += 1) acc +%= k;
    std.mem.doNotOptimizeAway(acc);

    const second = s.sample();
    try std.testing.expect(second.cpu <= 100);
    try std.testing.expect(second.mem <= 100);
    try std.testing.expect(second.io <= 100);
}
