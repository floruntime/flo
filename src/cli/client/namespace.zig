//! Namespace Client Operations
//!
//! Namespace management operations for the Flo CLI client.
//! All functions take a *Client as parameter.

const base = @import("base.zig");
const Client = base.Client;
const Response = base.Response;

/// Create a new namespace
pub fn create(client: *Client, name: []const u8) !Response {
    // namespace_create uses the namespace name in the key field
    return client.sendRequest(.namespace_create, "", name, "");
}

/// Delete an existing namespace
pub fn delete(client: *Client, name: []const u8, force: bool) !Response {
    // namespace_delete uses the namespace name in the key field and force flag in value
    const value: [1]u8 = .{if (force) 1 else 0};
    return client.sendRequest(.namespace_delete, "", name, &value);
}

/// List all namespaces; value [limit:u32][cursor] like every list op.
pub fn list(client: *Client) !Response {
    var value_buf: [base.WALK_VALUE_MAX]u8 = undefined;
    return client.sendRequest(.namespace_list, "", "", try base.walkValue(&value_buf, null, null));
}

/// Get namespace info
pub fn info(client: *Client, name: []const u8) !Response {
    // namespace_info uses the namespace name in the key field
    return client.sendRequest(.namespace_info, "", name, "");
}
