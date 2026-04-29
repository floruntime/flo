//! Simple YAML to JSON Converter
//!
//! Converts a subset of YAML to JSON for workflow definition parsing.
//! This is a lightweight converter that handles the YAML features used
//! by workflow definitions:
//!
//! - Key-value pairs
//! - Nested objects (indentation-based)
//! - Arrays (with - prefix)
//! - Quoted and unquoted strings
//! - Numbers and booleans
//!
//! Limitations:
//! - No multi-line strings (|, >)
//! - No anchors/aliases
//! - No complex YAML features

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

pub const ConvertError = error{
    InvalidIndentation,
    UnexpectedToken,
    OutOfMemory,
    InvalidFormat,
};

/// Convert YAML content to JSON
pub fn convert(allocator: Allocator, yaml: []const u8) ConvertError![]const u8 {
    var converter = Converter.init(allocator);
    defer converter.deinit();
    return converter.convert(yaml);
}

const Line = struct {
    indent: usize,
    content: []const u8,
    raw: []const u8,
};

const Converter = struct {
    allocator: Allocator,
    output: std.ArrayList(u8),
    lines: std.ArrayList(Line),

    fn init(allocator: Allocator) Converter {
        return .{
            .allocator = allocator,
            .output = .empty,
            .lines = .empty,
        };
    }

    fn deinit(self: *Converter) void {
        self.output.deinit(self.allocator);
        self.lines.deinit(self.allocator);
    }

    fn convert(self: *Converter, yaml: []const u8) ConvertError![]const u8 {
        // Parse lines
        var iter = mem.splitScalar(u8, yaml, '\n');
        while (iter.next()) |raw_line| {
            const line = mem.trimEnd(u8, raw_line, " \t\r");
            const content = mem.trimStart(u8, line, " \t");

            // Skip empty lines and comments
            if (content.len == 0) continue;
            if (content[0] == '#') continue;

            const parsed = Line{
                .indent = countIndent(line),
                .content = content,
                .raw = line,
            };
            self.lines.append(self.allocator, parsed) catch return ConvertError.OutOfMemory;
        }

        if (self.lines.items.len == 0) {
            self.output.appendSlice(self.allocator, "{}") catch return ConvertError.OutOfMemory;
            return self.output.toOwnedSlice(self.allocator) catch return ConvertError.OutOfMemory;
        }

        // Determine if root is array or object
        const first = self.lines.items[0];
        if (first.content[0] == '-') {
            _ = try self.processArray(0, 0);
        } else {
            _ = try self.processObject(0, 0);
        }

        return self.output.toOwnedSlice(self.allocator) catch return ConvertError.OutOfMemory;
    }

    fn processObject(self: *Converter, start: usize, base_indent: usize) ConvertError!usize {
        self.output.append(self.allocator, '{') catch return ConvertError.OutOfMemory;

        var i = start;
        var first = true;

        while (i < self.lines.items.len) {
            const line = self.lines.items[i];

            // Exit if indent is less than base (for nested objects)
            if (i > start and line.indent < base_indent) break;
            // Exit if this is an array item at same or lower indent
            if (line.content[0] == '-' and line.indent <= base_indent) break;

            // Must be a key: value line
            if (mem.indexOf(u8, line.content, ":")) |colon_pos| {
                if (!first) {
                    self.output.append(self.allocator, ',') catch return ConvertError.OutOfMemory;
                }
                first = false;

                const key = mem.trim(u8, line.content[0..colon_pos], " \t");
                try self.writeString(key);
                self.output.append(self.allocator, ':') catch return ConvertError.OutOfMemory;

                const after_colon = if (colon_pos + 1 < line.content.len)
                    mem.trimStart(u8, line.content[colon_pos + 1 ..], " \t")
                else
                    "";

                if (after_colon.len > 0) {
                    // Value on same line
                    try self.writeValue(after_colon);
                    i += 1;
                } else {
                    // Value on next line(s) - nested structure
                    i += 1;
                    if (i < self.lines.items.len) {
                        const next = self.lines.items[i];
                        if (next.indent > line.indent) {
                            if (next.content[0] == '-') {
                                // Nested array
                                i = try self.processArray(i, next.indent);
                            } else {
                                // Nested object
                                i = try self.processObject(i, next.indent);
                            }
                        } else {
                            self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                        }
                    } else {
                        self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                    }
                }
            } else {
                // Not a key:value, skip
                i += 1;
            }
        }

        self.output.append(self.allocator, '}') catch return ConvertError.OutOfMemory;
        return i;
    }

    fn processArray(self: *Converter, start: usize, base_indent: usize) ConvertError!usize {
        self.output.append(self.allocator, '[') catch return ConvertError.OutOfMemory;

        var i = start;
        var first = true;

        while (i < self.lines.items.len) {
            const line = self.lines.items[i];

            // Exit if indent is less than base
            if (line.indent < base_indent) break;
            // Exit if not an array item at base indent and not a continuation
            if (line.content[0] != '-' and line.indent <= base_indent) break;

            if (line.content[0] == '-') {
                // Skip array items at different indent level
                if (line.indent != base_indent) {
                    i += 1;
                    continue;
                }

                if (!first) {
                    self.output.append(self.allocator, ',') catch return ConvertError.OutOfMemory;
                }
                first = false;

                const item_content = mem.trimStart(u8, line.content[1..], " \t");

                if (item_content.len == 0) {
                    // Array item with value on next line
                    i += 1;
                    if (i < self.lines.items.len) {
                        const next = self.lines.items[i];
                        if (next.indent > line.indent) {
                            if (next.content[0] == '-') {
                                i = try self.processArray(i, next.indent);
                            } else {
                                i = try self.processObject(i, next.indent);
                            }
                        } else {
                            self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                        }
                    } else {
                        self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                    }
                } else if (mem.indexOf(u8, item_content, ":") != null) {
                    // Array item is an inline object: "- key: value"
                    i = try self.processArrayItemObject(i, line.indent);
                } else {
                    // Simple value
                    try self.writeValue(item_content);
                    i += 1;
                }
            } else {
                // Not an array item, should have exited already
                break;
            }
        }

        self.output.append(self.allocator, ']') catch return ConvertError.OutOfMemory;
        return i;
    }

    /// Process an array item that starts an object on the same line
    /// e.g., "- name: value" followed by more properties
    fn processArrayItemObject(self: *Converter, start: usize, array_indent: usize) ConvertError!usize {
        self.output.append(self.allocator, '{') catch return ConvertError.OutOfMemory;

        const line = self.lines.items[start];
        const item_content = mem.trimStart(u8, line.content[1..], " \t");
        const item_indent = line.indent;

        var i = start;
        var first_property = true;

        // Write the first key:value from the "- key: value" line
        if (mem.indexOf(u8, item_content, ":")) |colon_pos| {
            const key = mem.trim(u8, item_content[0..colon_pos], " \t");
            try self.writeString(key);
            self.output.append(self.allocator, ':') catch return ConvertError.OutOfMemory;
            first_property = false;

            const after_colon = if (colon_pos + 1 < item_content.len)
                mem.trimStart(u8, item_content[colon_pos + 1 ..], " \t")
            else
                "";

            if (after_colon.len > 0) {
                try self.writeValue(after_colon);
                i += 1;
            } else {
                // Value on next lines - nested structure
                i += 1;
                if (i < self.lines.items.len) {
                    const next = self.lines.items[i];
                    if (next.indent > item_indent) {
                        if (next.content[0] == '-') {
                            i = try self.processArray(i, next.indent);
                        } else {
                            i = try self.processObject(i, next.indent);
                        }
                    } else {
                        self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                    }
                } else {
                    self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                }
            }
        } else {
            i += 1;
        }

        // Continue with more properties at deeper indent
        const expected_indent = item_indent + 2; // Properties should be indented more than the "- " line

        while (i < self.lines.items.len) {
            const prop_line = self.lines.items[i];

            // Exit if we hit the array indent or lower
            if (prop_line.indent <= array_indent) break;
            // Exit if we hit another array item at the same level
            if (prop_line.content[0] == '-' and prop_line.indent <= item_indent) break;
            // Skip if not at expected property indent
            if (prop_line.indent < expected_indent) break;

            // Process as key:value
            if (mem.indexOf(u8, prop_line.content, ":")) |colon_pos| {
                if (!first_property) {
                    self.output.append(self.allocator, ',') catch return ConvertError.OutOfMemory;
                }
                first_property = false;

                const key = mem.trim(u8, prop_line.content[0..colon_pos], " \t");
                try self.writeString(key);
                self.output.append(self.allocator, ':') catch return ConvertError.OutOfMemory;

                const after_colon = if (colon_pos + 1 < prop_line.content.len)
                    mem.trimStart(u8, prop_line.content[colon_pos + 1 ..], " \t")
                else
                    "";

                if (after_colon.len > 0) {
                    try self.writeValue(after_colon);
                    i += 1;
                } else {
                    // Nested structure
                    i += 1;
                    if (i < self.lines.items.len) {
                        const next = self.lines.items[i];
                        if (next.indent > prop_line.indent) {
                            if (next.content[0] == '-') {
                                i = try self.processArray(i, next.indent);
                            } else {
                                i = try self.processObject(i, next.indent);
                            }
                        } else {
                            self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                        }
                    } else {
                        self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
                    }
                }
            } else {
                i += 1;
            }
        }

        self.output.append(self.allocator, '}') catch return ConvertError.OutOfMemory;
        return i;
    }

    fn writeValue(self: *Converter, value: []const u8) ConvertError!void {
        const trimmed = mem.trim(u8, value, " \t");

        // Already quoted string — don't strip comments inside quotes
        if (trimmed.len >= 2) {
            if ((trimmed[0] == '"' and trimmed[trimmed.len - 1] == '"') or
                (trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\''))
            {
                if (trimmed[0] == '\'') {
                    const inner = trimmed[1 .. trimmed.len - 1];
                    try self.writeString(inner);
                } else {
                    self.output.appendSlice(self.allocator, trimmed) catch return ConvertError.OutOfMemory;
                }
                return;
            }
        }

        // YAML flow sequence: [item1, item2, ...] → JSON array
        if (trimmed.len >= 2 and trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']') {
            try self.writeFlowSequence(trimmed);
            return;
        }

        // Strip inline comments from unquoted values (YAML spec: " #" starts a comment)
        const clean = stripInlineComment(trimmed);

        // Boolean
        if (mem.eql(u8, clean, "true") or mem.eql(u8, clean, "false")) {
            self.output.appendSlice(self.allocator, clean) catch return ConvertError.OutOfMemory;
            return;
        }

        // Null
        if (mem.eql(u8, clean, "null") or mem.eql(u8, clean, "~")) {
            self.output.appendSlice(self.allocator, "null") catch return ConvertError.OutOfMemory;
            return;
        }

        // Number
        if (isNumber(clean)) {
            self.output.appendSlice(self.allocator, clean) catch return ConvertError.OutOfMemory;
            return;
        }

        // Unquoted string - needs to be quoted
        try self.writeString(clean);
    }

    /// Convert a YAML flow sequence `[item1, item2, ...]` to a JSON array.
    fn writeFlowSequence(self: *Converter, value: []const u8) ConvertError!void {
        const inner = mem.trim(u8, value[1 .. value.len - 1], " \t");
        self.output.append(self.allocator, '[') catch return ConvertError.OutOfMemory;

        if (inner.len > 0) {
            var first = true;
            var rest: []const u8 = inner;
            while (rest.len > 0) {
                if (!first) {
                    self.output.append(self.allocator, ',') catch return ConvertError.OutOfMemory;
                }
                first = false;

                if (mem.indexOf(u8, rest, ",")) |comma_pos| {
                    const item = mem.trim(u8, rest[0..comma_pos], " \t");
                    try self.writeValue(item);
                    rest = if (comma_pos + 1 < rest.len) rest[comma_pos + 1 ..] else "";
                } else {
                    const item = mem.trim(u8, rest, " \t");
                    try self.writeValue(item);
                    break;
                }
            }
        }

        self.output.append(self.allocator, ']') catch return ConvertError.OutOfMemory;
    }

    /// Strip inline YAML comments: " # ..." (space-hash) outside quoted strings.
    fn stripInlineComment(value: []const u8) []const u8 {
        // Find " #" which starts an inline comment per YAML spec
        var i: usize = 1;
        while (i < value.len) : (i += 1) {
            if (value[i] == '#' and value[i - 1] == ' ') {
                return mem.trimEnd(u8, value[0 .. i - 1], " \t");
            }
        }
        return value;
    }

    fn writeString(self: *Converter, s: []const u8) ConvertError!void {
        self.output.append(self.allocator, '"') catch return ConvertError.OutOfMemory;
        for (s) |c| {
            switch (c) {
                '"' => {
                    self.output.appendSlice(self.allocator, "\\\"") catch return ConvertError.OutOfMemory;
                },
                '\\' => {
                    self.output.appendSlice(self.allocator, "\\\\") catch return ConvertError.OutOfMemory;
                },
                '\n' => {
                    self.output.appendSlice(self.allocator, "\\n") catch return ConvertError.OutOfMemory;
                },
                '\r' => {
                    self.output.appendSlice(self.allocator, "\\r") catch return ConvertError.OutOfMemory;
                },
                '\t' => {
                    self.output.appendSlice(self.allocator, "\\t") catch return ConvertError.OutOfMemory;
                },
                else => {
                    self.output.append(self.allocator, c) catch return ConvertError.OutOfMemory;
                },
            }
        }
        self.output.append(self.allocator, '"') catch return ConvertError.OutOfMemory;
    }

    fn countIndent(line: []const u8) usize {
        var count: usize = 0;
        for (line) |c| {
            if (c == ' ') {
                count += 1;
            } else if (c == '\t') {
                count += 2;
            } else {
                break;
            }
        }
        return count;
    }

    fn isNumber(s: []const u8) bool {
        if (s.len == 0) return false;
        var i: usize = 0;
        if (s[0] == '-' or s[0] == '+') i = 1;
        if (i >= s.len) return false;

        var has_digit = false;
        var has_dot = false;

        while (i < s.len) : (i += 1) {
            const c = s[i];
            if (c >= '0' and c <= '9') {
                has_digit = true;
            } else if (c == '.' and !has_dot) {
                has_dot = true;
            } else {
                return false;
            }
        }
        return has_digit;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "yaml_to_json: simple key-value" {
    const yaml =
        \\kind: Workflow
        \\name: test
        \\version: "1.0.0"
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    try std.testing.expect(mem.indexOf(u8, json, "\"kind\":\"Workflow\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"name\":\"test\"") != null);
}

test "yaml_to_json: nested object" {
    const yaml =
        \\kind: Workflow
        \\name: test
        \\start:
        \\  run: "@actions/validate"
        \\  onSuccess: flo.Completed
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    try std.testing.expect(mem.indexOf(u8, json, "\"start\":{") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"run\":\"@actions/validate\"") != null);
}

test "yaml_to_json: array" {
    const yaml =
        \\items:
        \\  - name: first
        \\    value: 1
        \\  - name: second
        \\    value: 2
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    try std.testing.expect(mem.indexOf(u8, json, "\"items\":[") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"name\":\"first\"") != null);
}

test "yaml_to_json: boolean and number" {
    const yaml =
        \\enabled: true
        \\count: 42
        \\ratio: 3.14
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    try std.testing.expect(mem.indexOf(u8, json, "\"enabled\":true") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"count\":42") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"ratio\":3.14") != null);
}

test "yaml_to_json: workflow definition with plans" {
    // This tests array-based plans (like what parser docs show)
    const yaml =
        \\kind: Workflow
        \\name: order-process
        \\version: "1.0.0"
        \\idempotency: required
        \\
        \\plans:
        \\  - name: payment
        \\    selection: health-weighted
        \\    executors:
        \\      - name: stripe
        \\        run: "@actions/charge-stripe"
        \\        weight: 100
        \\      - name: paypal
        \\        run: "@actions/charge-paypal"
        \\        weight: 80
        \\
        \\start:
        \\  run: "@plan/payment"
        \\  transitions:
        \\    success: flo.Completed
        \\    failure: flo.Failed
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    // Verify the JSON contains expected workflow fields
    try std.testing.expect(mem.indexOf(u8, json, "\"kind\":\"Workflow\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"name\":\"order-process\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"plans\":[") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"name\":\"payment\"") != null);
    // executors should be an array in the nested object
    try std.testing.expect(mem.indexOf(u8, json, "\"executors\":[") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"start\":{") != null);
}

test "yaml_to_json: workflow with object-based plans (YamlBuilder format)" {
    // This tests the object-based plans format that YamlBuilder produces
    // plans:
    //   plan-name:
    //     selection: ...
    const yaml =
        \\kind: Workflow
        \\name: order-process
        \\version: "1.0.0"
        \\
        \\plans:
        \\  payment:
        \\    selection: health-weighted
        \\    executors:
        \\      - name: stripe
        \\        run: "@actions/charge-stripe"
        \\        priority: 100
        \\      - name: paypal
        \\        run: "@actions/charge-paypal"
        \\        priority: 80
        \\
        \\start:
        \\  run: "@plan/payment"
        \\  transitions:
        \\    success: flo.Completed
        \\    failure: flo.Failed
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    // Verify the JSON contains expected workflow fields
    try std.testing.expect(mem.indexOf(u8, json, "\"kind\":\"Workflow\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"name\":\"order-process\"") != null);
    // plans should be an object
    try std.testing.expect(mem.indexOf(u8, json, "\"plans\":{") != null);
    // payment should be a key in the plans object
    try std.testing.expect(mem.indexOf(u8, json, "\"payment\":{") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"selection\":\"health-weighted\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"executors\":[") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"start\":{") != null);
}

test "yaml_to_json: single-quoted value with embedded double quotes" {
    const yaml =
        \\schedule:
        \\  cron: "0 */6 * * *"
        \\  max_concurrent: 1
        \\  input: '{"mode":"full"}'
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    // Verify valid JSON by parsing it
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();

    // Verify schedule fields
    try std.testing.expect(mem.indexOf(u8, json, "\"cron\":\"0 */6 * * *\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"max_concurrent\":1") != null);
    // The input value should have escaped double quotes inside
    try std.testing.expect(mem.indexOf(u8, json, "\"input\":\"{\\\"mode\\\":\\\"full\\\"}\"") != null);
}

test "yaml_to_json: inline comments stripped from values" {
    const yaml =
        \\transitions:
        \\  success: settle     # mark as done
        \\  failure: escalate   # alert ops
        \\  timeout: flo.Failed
        \\count: 42 # the answer
        \\quoted: "has # inside"
    ;
    const json = try convert(std.testing.allocator, yaml);
    defer std.testing.allocator.free(json);

    // Comments should be stripped from unquoted values
    try std.testing.expect(mem.indexOf(u8, json, "\"success\":\"settle\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"failure\":\"escalate\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"timeout\":\"flo.Failed\"") != null);
    try std.testing.expect(mem.indexOf(u8, json, "\"count\":42") != null);
    // Quoted value should preserve the # character
    try std.testing.expect(mem.indexOf(u8, json, "\"quoted\":\"has # inside\"") != null);
}
