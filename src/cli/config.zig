//! Client configuration management for Flo CLI
//! Manages contexts for connecting to different Flo servers (like kubectl contexts)
//! Config stored in ~/.flo/config.json

const std = @import("std");
const Allocator = std.mem.Allocator;
const commander = @import("commander/mod.zig");

/// Default endpoint for CLI commands.
/// Precedence: FLO_ENDPOINT env → "127.0.0.1:9000"
pub fn getDefaultEndpoint() []const u8 {
    return std.posix.getenv("FLO_ENDPOINT") orelse "127.0.0.1:9000";
}

/// Default namespace for CLI commands.
/// Precedence: FLO_NAMESPACE env → "default"
pub fn getDefaultNamespace() []const u8 {
    return std.posix.getenv("FLO_NAMESPACE") orelse "default";
}

/// Static buffer for --port constructed endpoint (CLI is single-threaded).
var port_endpoint_buf: [21]u8 = undefined; // "127.0.0.1:" (10) + max u16 digits (5) + margin

/// Get endpoint from command flags, falling back to env/default.
/// Precedence: --endpoint > --port > FLO_ENDPOINT > FLO_PORT > "127.0.0.1:9000"
pub fn getEndpoint(ctx: *commander.Context) []const u8 {
    // --endpoint flag takes highest precedence
    if (ctx.getString("endpoint")) |ep| {
        if (ep.len > 0) return ep;
    }
    // --port flag: construct 127.0.0.1:{port}
    if (ctx.getChangedUint("port")) |port_val| {
        if (port_val > 0 and port_val <= 65535) {
            return std.fmt.bufPrint(&port_endpoint_buf, "127.0.0.1:{d}", .{port_val}) catch
                return getDefaultEndpoint();
        }
    }
    // FLO_ENDPOINT env
    if (std.posix.getenv("FLO_ENDPOINT")) |ep| return ep;
    // FLO_PORT env
    if (std.posix.getenv("FLO_PORT")) |port_str| {
        if (std.fmt.parseInt(u16, port_str, 10)) |_| {
            return std.fmt.bufPrint(&port_endpoint_buf, "127.0.0.1:{s}", .{port_str}) catch
                return "127.0.0.1:9000";
        } else |_| {}
    }
    return "127.0.0.1:9000";
}

/// Get namespace from command flags, falling back to env/default.
pub fn getNamespace(ctx: *commander.Context) []const u8 {
    if (ctx.getString("namespace")) |ns| {
        if (ns.len > 0 and !std.mem.eql(u8, ns, "default")) return ns;
    }
    return getDefaultNamespace();
}

/// A context represents a connection to a Flo server
pub const Context = struct {
    endpoint: []const u8,
    namespace: ?[]const u8 = null,
};

/// Client configuration with multiple contexts
pub const Config = struct {
    allocator: Allocator,
    current_context: []const u8,
    contexts: std.StringHashMap(Context),

    // Track owned strings for cleanup
    _owned_strings: std.ArrayListUnmanaged([]const u8) = .{},

    const config_dir = ".flo";
    const config_file = "config.json";

    pub fn init(allocator: Allocator) Config {
        return .{
            .allocator = allocator,
            .current_context = "local",
            .contexts = std.StringHashMap(Context).init(allocator),
        };
    }

    pub fn deinit(self: *Config) void {
        // Free all owned strings
        for (self._owned_strings.items) |s| {
            self.allocator.free(s);
        }
        self._owned_strings.deinit(self.allocator);

        // Free context keys and values
        var it = self.contexts.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.endpoint);
            if (entry.value_ptr.namespace) |ns| {
                self.allocator.free(ns);
            }
        }
        self.contexts.deinit();
    }

    fn dupeString(self: *Config, s: []const u8) ![]const u8 {
        const owned = try self.allocator.dupe(u8, s);
        try self._owned_strings.append(self.allocator, owned);
        return owned;
    }

    /// Get the endpoint for the current context
    pub fn getCurrentEndpoint(self: *const Config) ![]const u8 {
        const ctx = self.contexts.get(self.current_context) orelse return error.ContextNotFound;
        return ctx.endpoint;
    }

    /// Get the current context
    pub fn getCurrentContext(self: *const Config) ?Context {
        return self.contexts.get(self.current_context);
    }

    /// Set a context (creates or updates)
    pub fn setContext(self: *Config, name: []const u8, endpoint: []const u8, namespace: ?[]const u8) !void {
        // Remove old context if exists
        if (self.contexts.fetchRemove(name)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value.endpoint);
            if (old.value.namespace) |ns| {
                self.allocator.free(ns);
            }
        }

        // Add new context
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        const owned_endpoint = try self.allocator.dupe(u8, endpoint);
        errdefer self.allocator.free(owned_endpoint);

        const owned_namespace = if (namespace) |ns| try self.allocator.dupe(u8, ns) else null;
        errdefer if (owned_namespace) |ns| self.allocator.free(ns);

        try self.contexts.put(owned_name, .{
            .endpoint = owned_endpoint,
            .namespace = owned_namespace,
        });
    }

    /// Switch to a different context
    pub fn useContext(self: *Config, name: []const u8) !void {
        if (!self.contexts.contains(name)) {
            return error.ContextNotFound;
        }
        self.current_context = try self.dupeString(name);
    }

    /// Load config from ~/.flo/config.json
    pub fn load(allocator: Allocator) !Config {
        var config = Config.init(allocator);
        errdefer config.deinit();

        const home = std.posix.getenv("HOME") orelse return error.NoHomeDirectory;
        const config_path = try std.fs.path.join(allocator, &.{ home, config_dir, config_file });
        defer allocator.free(config_path);

        const file = std.fs.openFileAbsolute(config_path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                // Return defaults with local context
                try config.setContext("local", "localhost:9000", null);
                return config;
            }
            return err;
        };
        defer file.close();

        const content = try file.readToEndAlloc(allocator, 1024 * 1024);
        defer allocator.free(content);

        // Parse JSON
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
        defer parsed.deinit();

        const root = parsed.value.object;

        // Get current_context
        if (root.get("current_context")) |cc| {
            if (cc == .string) {
                config.current_context = try config.dupeString(cc.string);
            }
        }

        // Get contexts
        if (root.get("contexts")) |ctxs| {
            if (ctxs == .object) {
                var ctx_it = ctxs.object.iterator();
                while (ctx_it.next()) |entry| {
                    const name = entry.key_ptr.*;
                    const ctx_obj = entry.value_ptr.*;
                    if (ctx_obj == .object) {
                        var endpoint: []const u8 = "localhost:9000";
                        var namespace: ?[]const u8 = null;

                        if (ctx_obj.object.get("endpoint")) |ep| {
                            if (ep == .string) {
                                endpoint = ep.string;
                            }
                        }
                        if (ctx_obj.object.get("namespace")) |ns| {
                            if (ns == .string) {
                                namespace = ns.string;
                            }
                        }

                        try config.setContext(name, endpoint, namespace);
                    }
                }
            }
        }

        // Ensure we have at least a local context
        if (config.contexts.count() == 0) {
            try config.setContext("local", "localhost:9000", null);
        }

        return config;
    }

    /// Save config to ~/.flo/config.json
    pub fn save(self: *const Config) !void {
        const home = std.posix.getenv("HOME") orelse return error.NoHomeDirectory;

        // Create ~/.flo directory if it doesn't exist
        const dir_path = try std.fs.path.join(self.allocator, &.{ home, config_dir });
        defer self.allocator.free(dir_path);

        std.fs.makeDirAbsolute(dir_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        // Build JSON
        var json_buf: std.ArrayListUnmanaged(u8) = .{};
        defer json_buf.deinit(self.allocator);

        var writer = json_buf.writer(self.allocator);
        try writer.writeAll("{\n");
        try writer.print("  \"current_context\": \"{s}\",\n", .{self.current_context});
        try writer.writeAll("  \"contexts\": {\n");

        var first = true;
        var it = self.contexts.iterator();
        while (it.next()) |entry| {
            if (!first) {
                try writer.writeAll(",\n");
            }
            first = false;

            try writer.print("    \"{s}\": {{\n", .{entry.key_ptr.*});
            try writer.print("      \"endpoint\": \"{s}\"", .{entry.value_ptr.endpoint});

            if (entry.value_ptr.namespace) |ns| {
                try writer.writeAll(",\n");
                try writer.print("      \"namespace\": \"{s}\"\n", .{ns});
            } else {
                try writer.writeAll("\n");
            }
            try writer.writeAll("    }");
        }

        try writer.writeAll("\n  }\n}\n");

        // Write to file
        const config_path = try std.fs.path.join(self.allocator, &.{ home, config_dir, config_file });
        defer self.allocator.free(config_path);

        const file = try std.fs.createFileAbsolute(config_path, .{});
        defer file.close();

        try file.writeAll(json_buf.items);
    }

    /// Print config summary
    pub fn show(self: *const Config, writer: anytype) !void {
        try writer.print("Current context: {s}\n\n", .{self.current_context});
        try writer.writeAll("Contexts:\n");

        var it = self.contexts.iterator();
        while (it.next()) |entry| {
            const marker: []const u8 = if (std.mem.eql(u8, entry.key_ptr.*, self.current_context)) "*" else " ";
            try writer.print("  {s} {s}: {s}", .{ marker, entry.key_ptr.*, entry.value_ptr.endpoint });
            if (entry.value_ptr.namespace) |ns| {
                try writer.print(" (namespace: {s})", .{ns});
            }
            try writer.writeAll("\n");
        }
    }
};

/// Get default config (creates local context if needed)
pub fn defaults(allocator: Allocator) !Config {
    var config = Config.init(allocator);
    try config.setContext("local", "localhost:9000", null);
    return config;
}

// Tests
test "config defaults" {
    const allocator = std.testing.allocator;
    var config = try defaults(allocator);
    defer config.deinit();

    try std.testing.expectEqualStrings("local", config.current_context);
    try std.testing.expectEqualStrings("localhost:9000", try config.getCurrentEndpoint());
}

test "set and use context" {
    const allocator = std.testing.allocator;
    var config = try defaults(allocator);
    defer config.deinit();

    try config.setContext("staging", "staging.example.com:9000", null);
    try config.useContext("staging");

    try std.testing.expectEqualStrings("staging", config.current_context);
    try std.testing.expectEqualStrings("staging.example.com:9000", try config.getCurrentEndpoint());
}

test "context not found" {
    const allocator = std.testing.allocator;
    var config = try defaults(allocator);
    defer config.deinit();

    const result = config.useContext("nonexistent");
    try std.testing.expectError(error.ContextNotFound, result);
}
