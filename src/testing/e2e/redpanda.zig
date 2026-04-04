//! RedpandaProcess — Manages a Redpanda Docker container for E2E testing.
//!
//! Follows the same lifecycle pattern as `ServerProcess`:
//!   init() → start() → {test} → stop() → deinit()
//!
//! Uses dynamic ports via `findFreePort()` so tests can run in parallel.
//! If Docker is not available, `start()` returns an error that callers
//! should catch to skip the test: `rp.start() catch return error.SkipZigTest;`

const std = @import("std");
const Allocator = std.mem.Allocator;
const server = @import("server.zig");
const findFreePort = server.findFreePort;

pub const RedpandaProcess = struct {
    const Self = @This();

    allocator: Allocator,
    container_id: ?[]const u8,
    kafka_port: u16,
    admin_port: u16,
    started: bool,
    config: RedpandaConfig,

    pub const RedpandaConfig = struct {
        image: []const u8 = "redpandadata/redpanda:v24.3.1",
        sasl_enabled: bool = false,
        sasl_user: []const u8 = "admin",
        sasl_password: []const u8 = "admin",
        memory: []const u8 = "128M",
        smp: u8 = 1,
    };

    pub fn init(allocator: Allocator, config: RedpandaConfig) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .container_id = null,
            .kafka_port = 0,
            .admin_port = 0,
            .started = false,
            .config = config,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.started) self.stop();
        if (self.container_id) |id| self.allocator.free(id);
        self.allocator.destroy(self);
    }

    /// Start the Redpanda container. Returns error on failure (e.g. Docker not available).
    /// Callers should catch to skip: `rp.start() catch return error.SkipZigTest;`
    pub fn start(self: *Self) !void {
        // Check Docker is available first
        try self.checkDocker();

        self.kafka_port = try findFreePort();
        self.admin_port = try findFreePort();

        // Build port format strings
        var kafka_map_buf: [32]u8 = undefined;
        const kafka_map = std.fmt.bufPrint(&kafka_map_buf, "{d}:19092", .{self.kafka_port}) catch unreachable;

        var admin_map_buf: [32]u8 = undefined;
        const admin_map = std.fmt.bufPrint(&admin_map_buf, "{d}:9644", .{self.admin_port}) catch unreachable;

        var external_advertise_buf: [64]u8 = undefined;
        const external_advertise = std.fmt.bufPrint(&external_advertise_buf, "localhost:{d}", .{self.kafka_port}) catch unreachable;

        var smp_buf: [4]u8 = undefined;
        const smp_str = std.fmt.bufPrint(&smp_buf, "{d}", .{self.config.smp}) catch unreachable;

        // Build argv
        // Two listeners: "internal" on 9092 (for rpk inside container) and
        // "external" on 19092 (mapped to host dynamic port). This avoids the
        // classic Docker problem where rpk can't reach the advertised host port.
        var argv_list: std.ArrayList([]const u8) = .empty;
        defer argv_list.deinit(self.allocator);

        try argv_list.appendSlice(self.allocator, &.{
            "docker",                  "run",
            "-d",                      "--rm",
            "-p",                      kafka_map,
            "-p",                      admin_map,
            self.config.image,
            "redpanda",                "start",
            "--mode",                  "dev-container",
            "--smp",                   smp_str,
            "--memory",                self.config.memory,
            "--kafka-addr",            "internal://0.0.0.0:9092,external://0.0.0.0:19092",
            "--advertise-kafka-addr",
        });

        // Advertise: internal=localhost:9092 (for rpk in container),
        // external=localhost:{host_port} (for Flo on host)
        var advertise_buf: [128]u8 = undefined;
        const advertise_str = std.fmt.bufPrint(
            &advertise_buf,
            "internal://localhost:9092,external://{s}",
            .{external_advertise},
        ) catch unreachable;
        try argv_list.append(self.allocator, advertise_str);

        if (self.config.sasl_enabled) {
            try argv_list.appendSlice(self.allocator, &.{
                "--set", "redpanda.enable_sasl=true",
            });
        }

        // Run docker
        var child = std.process.Child.init(argv_list.items, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        try child.spawn();

        var stdout_buf: std.ArrayList(u8) = .empty;
        var stderr_buf: std.ArrayList(u8) = .empty;
        try child.collectOutput(self.allocator, &stdout_buf, &stderr_buf, 64 * 1024);
        defer stderr_buf.deinit(self.allocator);

        const result = try child.wait();
        const exit_code: u8 = switch (result) {
            .Exited => |code| code,
            else => 255,
        };

        if (exit_code != 0) {
            stdout_buf.deinit(self.allocator);
            return error.DockerRunFailed;
        }

        // Container ID is on stdout (trimmed)
        const raw = try stdout_buf.toOwnedSlice(self.allocator);
        self.container_id = std.mem.trim(u8, raw, &std.ascii.whitespace);
        // Free the excess allocation if trimming shrunk it
        if (self.container_id.?.ptr != raw.ptr or self.container_id.?.len != raw.len) {
            const duped = try self.allocator.dupe(u8, self.container_id.?);
            self.allocator.free(raw);
            self.container_id = duped;
        }

        self.started = true;

        // Wait for readiness (TCP connect to Kafka port)
        self.waitForReady() catch |err| {
            self.stop();
            return err;
        };

        // If SASL enabled, create the superuser
        if (self.config.sasl_enabled) {
            try self.createSaslUser(self.config.sasl_user, self.config.sasl_password);
        }
    }

    pub fn stop(self: *Self) void {
        if (!self.started) return;
        const cid = self.container_id orelse return;

        var child = std.process.Child.init(&.{ "docker", "rm", "-f", cid }, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Ignore;
        child.spawn() catch return;
        _ = child.wait() catch {};

        self.started = false;
    }

    /// Returns "localhost:{kafka_port}"
    pub fn brokerAddr(self: *Self) ![]const u8 {
        var buf: [64]u8 = undefined;
        const addr = std.fmt.bufPrint(&buf, "localhost:{d}", .{self.kafka_port}) catch unreachable;
        return try self.allocator.dupe(u8, addr);
    }

    /// Create a topic with given number of partitions.
    /// Retries a few times if the Kafka API isn't fully settled.
    pub fn createTopic(self: *Self, name: []const u8, partitions: u32) !void {
        const cid = self.container_id orelse return error.NotStarted;

        var part_buf: [16]u8 = undefined;
        const part_str = std.fmt.bufPrint(&part_buf, "{d}", .{partitions}) catch unreachable;

        const max_retries: u32 = 5;
        const retry_sleep: u64 = 2 * std.time.ns_per_s;

        for (0..max_retries) |attempt| {
            self.dockerExec(&.{ "rpk", "topic", "create", name, "-p", part_str }, cid) catch |err| {
                if (attempt + 1 < max_retries) {
                    std.debug.print("[RedpandaProcess] createTopic attempt {d}/{d} failed, retrying...\n", .{ attempt + 1, max_retries });
                    std.Thread.sleep(retry_sleep);
                    continue;
                }
                return err;
            };
            return; // success
        }
    }

    /// Produce a single record to a topic
    pub fn produce(self: *Self, topic: []const u8, key: ?[]const u8, value: []const u8) !void {
        const cid = self.container_id orelse return error.NotStarted;

        if (key) |k| {
            // Use rpk format flag to parse key:value from stdin
            var full_argv: [10][]const u8 = undefined;
            full_argv[0] = "docker";
            full_argv[1] = "exec";
            full_argv[2] = "-i";
            full_argv[3] = cid;
            full_argv[4] = "rpk";
            full_argv[5] = "topic";
            full_argv[6] = "produce";
            full_argv[7] = topic;
            full_argv[8] = "-f";
            full_argv[9] = "%k:%v\\n";

            var child = std.process.Child.init(&full_argv, self.allocator);
            child.stdin_behavior = .Pipe;
            child.stdout_behavior = .Ignore;
            child.stderr_behavior = .Ignore;
            try child.spawn();

            // Write key:value\n then close stdin
            const stdin = child.stdin.?;
            stdin.writeAll(k) catch {};
            stdin.writeAll(":") catch {};
            stdin.writeAll(value) catch {};
            stdin.writeAll("\n") catch {};
            stdin.close();
            child.stdin = null;

            _ = try child.wait();
        } else {
            // No key — just pipe value
            var full_argv: [8][]const u8 = undefined;
            full_argv[0] = "docker";
            full_argv[1] = "exec";
            full_argv[2] = "-i";
            full_argv[3] = cid;
            full_argv[4] = "rpk";
            full_argv[5] = "topic";
            full_argv[6] = "produce";
            full_argv[7] = topic;

            var child = std.process.Child.init(&full_argv, self.allocator);
            child.stdin_behavior = .Pipe;
            child.stdout_behavior = .Ignore;
            child.stderr_behavior = .Ignore;
            try child.spawn();

            const stdin = child.stdin.?;
            stdin.writeAll(value) catch {};
            stdin.writeAll("\n") catch {};
            stdin.close();
            child.stdin = null;

            _ = try child.wait();
        }
    }

    /// Produce a batch of records to a topic via a single rpk invocation
    pub fn produceBatch(self: *Self, topic: []const u8, records: []const Record) !void {
        const cid = self.container_id orelse return error.NotStarted;

        const has_keys = for (records) |r| {
            if (r.key != null) break true;
        } else false;

        var argv_list: std.ArrayList([]const u8) = .empty;
        defer argv_list.deinit(self.allocator);

        try argv_list.appendSlice(self.allocator, &.{
            "docker", "exec", "-i", cid,
            "rpk",    "topic", "produce", topic,
        });
        if (has_keys) {
            try argv_list.appendSlice(self.allocator, &.{ "-f", "%k:%v\\n" });
        }

        var child = std.process.Child.init(argv_list.items, self.allocator);
        child.stdin_behavior = .Pipe;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Ignore;
        try child.spawn();

        const stdin = child.stdin.?;
        for (records) |rec| {
            if (rec.key) |k| {
                stdin.writeAll(k) catch {};
                stdin.writeAll(":") catch {};
            }
            stdin.writeAll(rec.value) catch {};
            stdin.writeAll("\n") catch {};
        }
        stdin.close();
        child.stdin = null;

        _ = try child.wait();
    }

    /// Delete a topic
    pub fn deleteTopic(self: *Self, name: []const u8) !void {
        const cid = self.container_id orelse return error.NotStarted;
        try self.dockerExec(&.{ "rpk", "topic", "delete", name }, cid);
    }

    /// List topics (returns owned slice — caller must free)
    pub fn listTopics(self: *Self) ![][]const u8 {
        const cid = self.container_id orelse return error.NotStarted;

        const output = try self.dockerExecCapture(&.{ "rpk", "topic", "list", "--format", "%n" }, cid);
        defer self.allocator.free(output);

        var topics: std.ArrayList([]const u8) = .empty;
        var iter = std.mem.splitScalar(u8, output, '\n');
        while (iter.next()) |line| {
            const trimmed = std.mem.trim(u8, line, &std.ascii.whitespace);
            if (trimmed.len == 0) continue;
            // Skip internal topics
            if (std.mem.startsWith(u8, trimmed, "_")) continue;
            try topics.append(self.allocator, try self.allocator.dupe(u8, trimmed));
        }
        return try topics.toOwnedSlice(self.allocator);
    }

    pub const Record = struct {
        key: ?[]const u8 = null,
        value: []const u8,
    };

    // =========================================================================
    // Internal helpers
    // =========================================================================

    fn checkDocker(self: *Self) !void {
        _ = self;
        var child = std.process.Child.init(&.{ "docker", "info" }, std.heap.page_allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Ignore;
        child.spawn() catch return error.DockerNotAvailable;
        const result = child.wait() catch return error.DockerNotAvailable;
        switch (result) {
            .Exited => |code| if (code != 0) return error.DockerNotAvailable,
            else => return error.DockerNotAvailable,
        }
    }

    fn waitForReady(self: *Self) !void {
        const cid = self.container_id orelse return error.NotStarted;

        // Phase 1: Wait for TCP port to accept connections (fast)
        const tcp_max: u32 = 60; // 30s at 500ms intervals
        const sleep_ns: u64 = 500 * std.time.ns_per_ms;
        var tcp_ok = false;
        for (0..tcp_max) |_| {
            if (self.canConnect()) {
                tcp_ok = true;
                break;
            }
            std.Thread.sleep(sleep_ns);
        }
        if (!tcp_ok) return error.RedpandaNotReady;

        // Phase 2: Wait for Kafka API to be operational via `rpk cluster health`
        // TCP accept != API ready; Redpanda needs time to initialize its Raft groups.
        const api_max: u32 = 60; // 30s at 500ms intervals
        for (0..api_max) |_| {
            if (self.isKafkaApiReady(cid)) return;
            std.Thread.sleep(sleep_ns);
        }

        return error.RedpandaNotReady;
    }

    fn canConnect(self: *Self) bool {
        const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, self.kafka_port);
        const sock = std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0) catch return false;
        defer std.posix.close(sock);
        std.posix.connect(sock, &addr.any, @sizeOf(std.posix.sockaddr.in)) catch return false;
        return true;
    }

    /// Check if Kafka API is actually ready (not just TCP).
    fn isKafkaApiReady(self: *Self, cid: []const u8) bool {
        const argv = &[_][]const u8{ "docker", "exec", cid, "rpk", "cluster", "health" };
        var child = std.process.Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Ignore;
        child.stderr_behavior = .Ignore;
        child.spawn() catch return false;
        const result = child.wait() catch return false;
        return switch (result) {
            .Exited => |code| code == 0,
            else => false,
        };
    }

    fn dockerExec(self: *Self, rpk_args: []const []const u8, cid: []const u8) !void {
        const total = 3 + rpk_args.len;

        var argv = try self.allocator.alloc([]const u8, total);
        defer self.allocator.free(argv);

        argv[0] = "docker";
        argv[1] = "exec";
        argv[2] = cid;
        for (rpk_args, 0..) |arg, i| {
            argv[3 + i] = arg;
        }

        var child = std.process.Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        try child.spawn();

        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        try child.collectOutput(self.allocator, &stdout_list, &stderr_list, 256 * 1024);
        stdout_list.deinit(self.allocator);
        defer stderr_list.deinit(self.allocator);

        const result = try child.wait();
        switch (result) {
            .Exited => |code| if (code != 0) {
                const stderr_out = stderr_list.items;
                if (stderr_out.len > 0) {
                    std.debug.print("\n[RedpandaProcess] rpk failed (exit {d}): {s}\n", .{ code, stderr_out });
                } else {
                    std.debug.print("\n[RedpandaProcess] rpk failed with exit code {d}\n", .{code});
                }
                return error.RpkCommandFailed;
            },
            else => return error.RpkCommandFailed,
        }
    }

    fn dockerExecCapture(self: *Self, rpk_args: []const []const u8, cid: []const u8) ![]const u8 {
        const total = 3 + rpk_args.len;

        var argv = try self.allocator.alloc([]const u8, total);
        defer self.allocator.free(argv);

        argv[0] = "docker";
        argv[1] = "exec";
        argv[2] = cid;
        for (rpk_args, 0..) |arg, i| {
            argv[3 + i] = arg;
        }

        var child = std.process.Child.init(argv, self.allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;
        try child.spawn();

        var stdout_list: std.ArrayList(u8) = .empty;
        var stderr_list: std.ArrayList(u8) = .empty;
        try child.collectOutput(self.allocator, &stdout_list, &stderr_list, 256 * 1024);
        stderr_list.deinit(self.allocator);

        const result = try child.wait();
        switch (result) {
            .Exited => |code| if (code != 0) {
                stdout_list.deinit(self.allocator);
                return error.RpkCommandFailed;
            },
            else => {
                stdout_list.deinit(self.allocator);
                return error.RpkCommandFailed;
            },
        }

        return try stdout_list.toOwnedSlice(self.allocator);
    }

    fn createSaslUser(self: *Self, user: []const u8, password: []const u8) !void {
        const cid = self.container_id orelse return error.NotStarted;

        // Build: rpk acl user create <user> -p <password> --mechanism SCRAM-SHA-256
        try self.dockerExec(&.{
            "rpk", "acl", "user", "create", user, "-p", password, "--mechanism", "SCRAM-SHA-256",
        }, cid);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RedpandaProcess: init and deinit without start" {
    const rp = try RedpandaProcess.init(std.testing.allocator, .{});
    rp.deinit();
}

test "RedpandaProcess: default config values" {
    const rp = try RedpandaProcess.init(std.testing.allocator, .{});
    defer rp.deinit();

    try std.testing.expectEqualStrings("redpandadata/redpanda:v24.3.1", rp.config.image);
    try std.testing.expect(!rp.config.sasl_enabled);
    try std.testing.expectEqualStrings("128M", rp.config.memory);
    try std.testing.expectEqual(@as(u8, 1), rp.config.smp);
}

test "RedpandaProcess: custom config" {
    const rp = try RedpandaProcess.init(std.testing.allocator, .{
        .sasl_enabled = true,
        .memory = "256M",
        .smp = 2,
    });
    defer rp.deinit();

    try std.testing.expect(rp.config.sasl_enabled);
    try std.testing.expectEqualStrings("256M", rp.config.memory);
    try std.testing.expectEqual(@as(u8, 2), rp.config.smp);
}
