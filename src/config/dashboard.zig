//! Dashboard Configuration
//!
//! Configuration types for the web dashboard.
//!
//! Security: the dashboard authenticates no one yet. It binds to localhost by
//! default and checks Origin, Host and content type on what it accepts; keep
//! it on a private interface.

/// Dashboard configuration
pub const DashboardConfig = struct {
    /// Enable embedded web dashboard
    enabled: bool = true,
    /// Port for dashboard HTTP server (0 = derive from listen_port + 2)
    port: u16 = 0,
    /// Bind address for dashboard server
    /// Default: "127.0.0.1" (localhost only - safe by default)
    /// Set to "0.0.0.0" to expose externally (use with firewall/VPN)
    bind: []const u8 = "127.0.0.1",
    /// Other origins whose pages may call the API, exactly as a browser
    /// names them (`https://ops.example.com:8443`), comma-separated. Empty:
    /// only the dashboard's own pages. There is no wildcard.
    cors_origins: []const u8 = "",
    /// Host names the dashboard answers to besides localhost, 127.0.0.1 and
    /// [::1], comma-separated (`flo.internal,10.0.1.5`).
    hosts: []const u8 = "",
};

const std = @import("std");

/// Why `list` is not a valid `cors_origins`, or null. Each entry must be an
/// origin as a browser sends it — lowercase `http://` or `https://`, a host,
/// an optional port, nothing after — because the match is exact and an
/// entry written any other way would silently never match.
pub fn originsRefusal(list: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |raw| {
        const o = std.mem.trim(u8, raw, " \t");
        if (o.len == 0) continue;
        if (std.mem.eql(u8, o, "*")) return "\"*\" is not accepted; list exact origins";
        if (std.mem.eql(u8, o, "null")) return "\"null\" is every sandboxed frame and local file, never an allowed origin";
        for (o) |c| if (std.ascii.isUpper(c)) return "origins are lowercase, as browsers send them";
        const rest = if (std.mem.startsWith(u8, o, "https://"))
            o["https://".len..]
        else if (std.mem.startsWith(u8, o, "http://"))
            o["http://".len..]
        else
            return "an origin starts with http:// or https://";
        if (hostPortRefusal(rest)) |why| return why;
        // A browser leaves the scheme's own port out of Origin.
        if (std.mem.endsWith(u8, o, if (o[4] == 's') ":443" else ":80")) return "leave out the default port (:443 for https, :80 for http); browsers do";
    }
    return null;
}

/// `host[:port]` or `[v6][:port]`, with nothing after.
fn hostPortRefusal(s: []const u8) ?[]const u8 {
    const bad = "an origin is scheme://host[:port], with no path, query or trailing '/'";
    if (std.mem.indexOfAny(u8, s, "/?#@ ") != null) return bad;
    var host = s;
    var port: ?[]const u8 = null;
    if (std.mem.startsWith(u8, s, "[")) {
        const close = std.mem.indexOfScalar(u8, s, ']') orelse return bad;
        host = s[0 .. close + 1];
        if (close + 1 < s.len) {
            if (s[close + 1] != ':') return bad;
            port = s[close + 2 ..];
        }
    } else if (std.mem.indexOfScalar(u8, s, ':')) |colon| {
        host = s[0..colon];
        port = s[colon + 1 ..];
    }
    if (host.len == 0) return bad;
    if (port) |p| _ = std.fmt.parseInt(u16, p, 10) catch return bad;
    return null;
}

/// Why `list` is not a valid `hosts`, or null. Each entry is a name or
/// address alone (`flo.internal`, `10.0.1.5`, `[fd00::5]`): the port of a
/// request's Host is not compared, so an entry with one would never match.
pub fn hostsRefusal(list: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |raw| {
        const h = std.mem.trim(u8, raw, " \t");
        if (h.len == 0) continue;
        if (std.mem.indexOfAny(u8, h, "/?#@ ") != null) return "a host is a name or address alone, no scheme or path";
        if (std.mem.indexOfScalar(u8, h, '*') != null) return "hosts are exact; no wildcards";
        if (std.mem.endsWith(u8, h, ".")) return "write the name without its trailing '.'";
        if (std.mem.startsWith(u8, h, "[")) {
            if (!std.mem.endsWith(u8, h, "]")) return "a host is a name or address alone, no port";
        } else if (std.mem.indexOfScalar(u8, h, ':') != null) {
            return "a host is a name or address alone, no port (write an IPv6 address in brackets)";
        }
    }
    return null;
}

test "dashboard config: origins and hosts that could never match are refused" {
    for ([_][]const u8{ "", "https://ops.example.com:8443", "http://localhost:5173, https://a.b", "http://[::1]:9002", "https://x.y", "http://a.b:8080", "https://a.b:4430" }) |ok| {
        try std.testing.expectEqual(@as(?[]const u8, null), originsRefusal(ok));
    }
    for ([_][]const u8{ "*", "null", "https://ops.example.com/", "https://a.b/console", "HTTPS://a.b", "https://A.b", "a.b", "ftp://a.b", "https://a.b:port", "https://:80", "https://a.b:443", "http://a.b:80" }) |bad| {
        if (originsRefusal(bad) == null) {
            std.debug.print("accepted origin: {s}\n", .{bad});
            return error.TestUnexpectedResult;
        }
    }
    for ([_][]const u8{ "", "flo.internal", "Flo.Internal, 10.0.1.5", "[fd00::5]" }) |ok| {
        try std.testing.expectEqual(@as(?[]const u8, null), hostsRefusal(ok));
    }
    for ([_][]const u8{ "flo.internal:9002", "http://flo.internal", "[fd00::5]:9002", "fd00::5", "a/b", "*", "*.example.com", "localhost." }) |bad| {
        if (hostsRefusal(bad) == null) {
            std.debug.print("accepted host: {s}\n", .{bad});
            return error.TestUnexpectedResult;
        }
    }
}
