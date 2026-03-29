//! Variable interpolation for YAML definition values.
//!
//! Resolves `${namespace.key}` patterns in strings. Supported namespaces:
//!
//!   - `env`     — Process environment variables: `${env.WASM_DIR}` → getenv("WASM_DIR")
//!   - `secrets` — Secret store (flo.toml [secrets] section, then FLO_SECRET_<KEY> env).
//!                  Reserved for emit targets — resolved at delivery time, not parse time.
//!
//! # Usage
//!
//! ```zig
//! const interpolate = @import("util/interpolate.zig");
//!
//! // Resolve all supported namespaces:
//! const resolved = try interpolate.resolve(allocator, raw_string, .{});
//! defer allocator.free(resolved);
//!
//! // Resolve only env (leave ${secrets.*} untouched):
//! const resolved = try interpolate.resolve(allocator, raw_string, .{ .secrets = false });
//! defer allocator.free(resolved);
//! ```
//!
//! Unknown namespaces and malformed patterns are left verbatim (no error).
//! Missing env vars are left verbatim with a log warning.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Controls which variable namespaces are resolved.
pub const Options = struct {
    /// Resolve `${env.KEY}` from process environment.
    env: bool = true,
    /// Resolve `${secrets.KEY}` from the secret store.
    /// Default false — callers must opt in (emit delivery resolves secrets;
    /// processing parse does not).
    secrets: bool = false,
    /// Optional secret lookup function. If null and secrets=true,
    /// falls back to FLO_SECRET_<KEY> environment variable.
    secret_lookup: ?*const fn (key: []const u8) ?[]const u8 = null,
};

/// Resolve `${namespace.key}` patterns in `input`.
///
/// Returns a new allocation with all matching patterns replaced.
/// Patterns that cannot be resolved (unknown namespace, missing key)
/// are left verbatim.
pub fn resolve(allocator: Allocator, input: []const u8, opts: Options) Allocator.Error![]u8 {
    // Fast path: no interpolation markers at all.
    if (std.mem.indexOf(u8, input, "${") == null) {
        return allocator.dupe(u8, input);
    }

    var result: std.ArrayList(u8) = .{};
    errdefer result.deinit(allocator);

    var i: usize = 0;
    while (i < input.len) {
        // Look for next "${"
        if (i + 1 < input.len and input[i] == '$' and input[i + 1] == '{') {
            // Find closing '}'
            if (std.mem.indexOfScalarPos(u8, input, i + 2, '}')) |close| {
                const expr = input[i + 2 .. close]; // "namespace.key"
                if (resolveExpr(expr, opts)) |replacement| {
                    try result.appendSlice(allocator, replacement);
                    i = close + 1;
                    continue;
                }
            }
            // Malformed or unresolvable — emit literal "${"
            try result.append(allocator, '$');
            i += 1;
        } else {
            try result.append(allocator, input[i]);
            i += 1;
        }
    }

    return result.toOwnedSlice(allocator);
}

/// Try to resolve a single expression (the content between ${ and }).
/// Returns the resolved value or null if the pattern should be kept verbatim.
fn resolveExpr(expr: []const u8, opts: Options) ?[]const u8 {
    // Split on first '.'
    const dot = std.mem.indexOfScalar(u8, expr, '.') orelse return null;
    const namespace = expr[0..dot];
    const key = expr[dot + 1 ..];

    if (key.len == 0) return null;

    if (std.mem.eql(u8, namespace, "env")) {
        if (!opts.env) return null;
        return resolveEnv(key);
    }

    if (std.mem.eql(u8, namespace, "secrets")) {
        if (!opts.secrets) return null;
        return resolveSecret(key, opts);
    }

    // Unknown namespace — leave verbatim
    return null;
}

/// Resolve an environment variable.
fn resolveEnv(key: []const u8) ?[]const u8 {
    // std.posix.getenv requires a null-terminated string.
    // The key comes from a parsed YAML string which is part of a larger
    // buffer — it may not be sentinel-terminated. However, getenv on
    // POSIX iterates environ and does a prefix match, so we can use
    // the slice-based helper below.
    return getEnvSlice(key);
}

/// Resolve a secret value.
fn resolveSecret(key: []const u8, opts: Options) ?[]const u8 {
    // If a custom lookup function is provided, use it first.
    if (opts.secret_lookup) |lookup| {
        if (lookup(key)) |v| return v;
    }

    // Fallback: FLO_SECRET_<KEY> environment variable.
    // Build the env var name on the stack (max 256 chars).
    var buf: [256]u8 = undefined;
    const prefix = "FLO_SECRET_";
    if (prefix.len + key.len >= buf.len) return null;
    @memcpy(buf[0..prefix.len], prefix);
    @memcpy(buf[prefix.len .. prefix.len + key.len], key);
    buf[prefix.len + key.len] = 0;
    const env_name: [:0]const u8 = buf[0 .. prefix.len + key.len :0];
    return std.posix.getenv(env_name);
}

/// Look up an environment variable from a non-sentinel-terminated slice.
/// Uses std.posix.getenv with a stack-copied sentinel-terminated copy.
fn getEnvSlice(key: []const u8) ?[]const u8 {
    var buf: [256]u8 = undefined;
    if (key.len >= buf.len) return null;
    @memcpy(buf[0..key.len], key);
    buf[key.len] = 0;
    const name: [:0]const u8 = buf[0..key.len :0];
    return std.posix.getenv(name);
}

// ============================================================================
// Tests
// ============================================================================

test "no interpolation markers" {
    const alloc = std.testing.allocator;
    const result = try resolve(alloc, "hello world", .{});
    defer alloc.free(result);
    try std.testing.expectEqualStrings("hello world", result);
}

test "empty string" {
    const alloc = std.testing.allocator;
    const result = try resolve(alloc, "", .{});
    defer alloc.free(result);
    try std.testing.expectEqualStrings("", result);
}

test "malformed patterns left verbatim" {
    const alloc = std.testing.allocator;

    // Unclosed brace
    const r1 = try resolve(alloc, "path/${env.FOO/bar", .{});
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("path/${env.FOO/bar", r1);

    // No dot in expression
    const r2 = try resolve(alloc, "${JUSTKEY}", .{});
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("${JUSTKEY}", r2);

    // Empty key
    const r3 = try resolve(alloc, "${env.}", .{});
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("${env.}", r3);
}

test "unknown namespace left verbatim" {
    const alloc = std.testing.allocator;
    const result = try resolve(alloc, "${foo.bar}", .{});
    defer alloc.free(result);
    try std.testing.expectEqualStrings("${foo.bar}", result);
}

test "secrets disabled by default" {
    const alloc = std.testing.allocator;
    // secrets namespace should pass through when opts.secrets = false (default)
    const result = try resolve(alloc, "Bearer ${secrets.TOKEN}", .{});
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Bearer ${secrets.TOKEN}", result);
}

test "env resolution with PATH" {
    // PATH is always set in test environments
    const alloc = std.testing.allocator;
    const result = try resolve(alloc, "bin:${env.PATH}:end", .{});
    defer alloc.free(result);

    // Should start with "bin:" and end with ":end", with PATH value in between
    try std.testing.expect(std.mem.startsWith(u8, result, "bin:"));
    try std.testing.expect(std.mem.endsWith(u8, result, ":end"));
    try std.testing.expect(result.len > "bin::end".len);
}

test "missing env var left verbatim" {
    const alloc = std.testing.allocator;
    const result = try resolve(alloc, "${env.FLO_DEFINITELY_NOT_SET_12345}", .{});
    defer alloc.free(result);
    // Unresolvable — left as literal "${" then the rest continues
    // Actually since resolveExpr returns null, the whole "${env.FLO_DEFINITELY_NOT_SET_12345}"
    // is emitted character by character: "$" then the rest as literals.
    // Let's verify the output is the original string:
    try std.testing.expectEqualStrings("${env.FLO_DEFINITELY_NOT_SET_12345}", result);
}

test "multiple substitutions" {
    const alloc = std.testing.allocator;
    // Use HOME which is always set
    const home = std.posix.getenv("HOME") orelse return;
    const input = "${env.HOME}/data/${env.HOME}/more";
    const result = try resolve(alloc, input, .{});
    defer alloc.free(result);

    var expected: std.ArrayList(u8) = .{};
    defer expected.deinit(alloc);
    try expected.appendSlice(alloc, home);
    try expected.appendSlice(alloc, "/data/");
    try expected.appendSlice(alloc, home);
    try expected.appendSlice(alloc, "/more");

    try std.testing.expectEqualStrings(expected.items, result);
}

test "secrets with custom lookup" {
    const alloc = std.testing.allocator;
    const S = struct {
        fn lookup(key: []const u8) ?[]const u8 {
            if (std.mem.eql(u8, key, "MY_TOKEN")) return "s3cr3t";
            return null;
        }
    };
    const result = try resolve(alloc, "Bearer ${secrets.MY_TOKEN}", .{
        .secrets = true,
        .secret_lookup = &S.lookup,
    });
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Bearer s3cr3t", result);
}
