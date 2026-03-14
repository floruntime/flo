//! Flo Actions WASM Demo: Rules Engine
//!
//! A minimal decision engine compiled to WebAssembly.
//! Evaluates rules against JSON input and returns a decision.
//!
//! ## ABI (Actions WASM Contract)
//!
//! Guest exports:
//!   - `handle(input_ptr: u32, input_len: u32) -> i64`
//!     Returns packed (output_ptr << 32) | output_len, or negative error code.
//!   - `alloc(size: u32) -> u32`
//!   - `dealloc(ptr: u32, size: u32) -> void`
//!
//! ## Rules
//!
//!   1. age > 18
//!   2. country == "US"
//!
//! ## Example
//!
//! Input:  {"age": 25, "country": "US"}
//! Output: {"eligible": true, "rules_evaluated": 2, "rules_passed": 2}
//!
//! ## Build
//!
//!   zig build-lib -target wasm32-freestanding -O ReleaseSmall rules_engine.zig
//!
//! Or use the build.zig:
//!   cd wasm/actions-demo && zig build

// =============================================================================
// Heap — Bump allocator for guest memory
// =============================================================================

var heap: [65536]u8 = undefined;
var heap_offset: usize = 0;

/// Allocate `size` bytes from the bump allocator.
/// Returns a WASM pointer (u32) or 0 on failure.
export fn alloc(size: u32) u32 {
    const s: usize = @intCast(size);
    // Align to 8 bytes
    const aligned = (heap_offset + 7) & ~@as(usize, 7);
    if (aligned + s > heap.len) return 0;
    const ptr: [*]u8 = @ptrCast(&heap[aligned]);
    heap_offset = aligned + s;
    return @intFromPtr(ptr);
}

/// Dealloc is a no-op for bump allocator.
/// The host resets by calling handle() which resets the heap.
export fn dealloc(_: u32, _: u32) void {}

fn resetHeap() void {
    heap_offset = 0;
}

// =============================================================================
// Output buffer
// =============================================================================

var output_buf: [4096]u8 = undefined;

// =============================================================================
// handle — Main entry point
// =============================================================================

/// Evaluate rules against JSON input.
///
/// Input JSON: {"age": <number>, "country": "<string>"}
/// Output JSON: {"eligible": <bool>, "rules_evaluated": 2, "rules_passed": <n>}
///
/// Returns: packed i64 = (output_ptr << 32) | output_len
///          Negative on error: -1 = invalid input, -2 = alloc failed
export fn handle(input_ptr: [*]const u8, input_len: u32) i64 {
    resetHeap();

    const input = input_ptr[0..input_len];

    // Parse age
    const age = parseIntField(input, "age") orelse return -1;

    // Parse country
    const country = parseStringField(input, "country") orelse return -1;

    // Evaluate rules
    var rules_passed: u32 = 0;
    const rules_evaluated: u32 = 2;

    // Rule 1: age > 18
    if (age > 18) rules_passed += 1;

    // Rule 2: country == "US"
    if (streql(country, "US")) rules_passed += 1;

    const eligible = (rules_passed == rules_evaluated);

    // Build output JSON
    const output_len = formatOutput(&output_buf, eligible, rules_evaluated, rules_passed);
    if (output_len == 0) return -2;

    // Copy output to allocated guest memory
    const out_ptr = alloc(@intCast(output_len));
    if (out_ptr == 0) return -2;

    const dest: [*]u8 = @ptrFromInt(out_ptr);
    for (0..output_len) |i| {
        dest[i] = output_buf[i];
    }

    // Pack output_ptr and output_len into i64
    const ptr_part: i64 = @as(i64, out_ptr) << 32;
    const len_part: i64 = @intCast(output_len);
    return ptr_part | len_part;
}

// =============================================================================
// init — Optional initialization (called once after instantiation)
// =============================================================================

export fn init() i32 {
    resetHeap();
    return 0; // success
}

// =============================================================================
// describe — Optional introspection
// =============================================================================

const description = "{\"name\":\"rules-engine\",\"version\":\"1.0\",\"rules\":[\"age > 18\",\"country == US\"]}";

export fn describe() i64 {
    const ptr = alloc(@intCast(description.len));
    if (ptr == 0) return 0;
    const dest: [*]u8 = @ptrFromInt(ptr);
    for (0..description.len) |i| {
        dest[i] = description[i];
    }
    return (@as(i64, ptr) << 32) | @as(i64, @intCast(description.len));
}

// =============================================================================
// Minimal JSON parsing (no std.json — pure freestanding)
// =============================================================================

/// Parse an integer field from JSON: {"...", "field": 123, "..."}
fn parseIntField(json: []const u8, field_name: []const u8) ?i64 {
    // Find "field_name":
    const key_start = findKey(json, field_name) orelse return null;

    // Skip to value (past colon and whitespace)
    var i = key_start;
    while (i < json.len and json[i] != ':') : (i += 1) {}
    if (i >= json.len) return null;
    i += 1; // skip ':'
    while (i < json.len and isWhitespace(json[i])) : (i += 1) {}

    // Parse integer (possibly negative)
    var negative = false;
    if (i < json.len and json[i] == '-') {
        negative = true;
        i += 1;
    }

    var value: i64 = 0;
    var found_digit = false;
    while (i < json.len and json[i] >= '0' and json[i] <= '9') {
        value = value * 10 + @as(i64, json[i] - '0');
        found_digit = true;
        i += 1;
    }

    if (!found_digit) return null;
    return if (negative) -value else value;
}

/// Parse a string field from JSON: {"...", "field": "value", "..."}
fn parseStringField(json: []const u8, field_name: []const u8) ?[]const u8 {
    // Find "field_name":
    const key_start = findKey(json, field_name) orelse return null;

    // Skip to value
    var i = key_start;
    while (i < json.len and json[i] != ':') : (i += 1) {}
    if (i >= json.len) return null;
    i += 1; // skip ':'
    while (i < json.len and isWhitespace(json[i])) : (i += 1) {}

    // Expect opening quote
    if (i >= json.len or json[i] != '"') return null;
    i += 1;

    // Find closing quote
    const start = i;
    while (i < json.len and json[i] != '"') : (i += 1) {}
    if (i >= json.len) return null;

    return json[start..i];
}

/// Find the position just after a key name in JSON.
/// Searches for `"key_name"` and returns the index after the closing quote.
fn findKey(json: []const u8, key: []const u8) ?usize {
    // Search for "key"
    var i: usize = 0;
    while (i + key.len + 2 <= json.len) {
        if (json[i] == '"') {
            // Check if this is our key
            const name_start = i + 1;
            const name_end = name_start + key.len;
            if (name_end < json.len and
                json[name_end] == '"' and
                sliceEql(json[name_start..name_end], key))
            {
                return name_end + 1;
            }
        }
        i += 1;
    }
    return null;
}

fn sliceEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (ca != cb) return false;
    }
    return true;
}

fn streql(a: []const u8, b: []const u8) bool {
    return sliceEql(a, b);
}

fn isWhitespace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

// =============================================================================
// Output formatting (no std.fmt — pure freestanding)
// =============================================================================

/// Format the output JSON into the provided buffer.
/// Returns the number of bytes written, or 0 on failure.
fn formatOutput(buf: []u8, eligible: bool, rules_evaluated: u32, rules_passed: u32) usize {
    var pos: usize = 0;

    const prefix = "{\"eligible\":";
    if (pos + prefix.len > buf.len) return 0;
    copySlice(buf[pos..], prefix);
    pos += prefix.len;

    const bool_str = if (eligible) "true" else "false";
    if (pos + bool_str.len > buf.len) return 0;
    copySlice(buf[pos..], bool_str);
    pos += bool_str.len;

    const mid1 = ",\"rules_evaluated\":";
    if (pos + mid1.len > buf.len) return 0;
    copySlice(buf[pos..], mid1);
    pos += mid1.len;

    const eval_len = writeU32(buf[pos..], rules_evaluated);
    pos += eval_len;

    const mid2 = ",\"rules_passed\":";
    if (pos + mid2.len > buf.len) return 0;
    copySlice(buf[pos..], mid2);
    pos += mid2.len;

    const pass_len = writeU32(buf[pos..], rules_passed);
    pos += pass_len;

    if (pos + 1 > buf.len) return 0;
    buf[pos] = '}';
    pos += 1;

    return pos;
}

fn copySlice(dest: []u8, src: []const u8) void {
    for (0..src.len) |i| {
        dest[i] = src[i];
    }
}

/// Write a u32 as decimal ASCII. Returns bytes written.
fn writeU32(buf: []u8, value: u32) usize {
    if (value == 0) {
        buf[0] = '0';
        return 1;
    }
    var tmp: [10]u8 = undefined;
    var len: usize = 0;
    var v = value;
    while (v > 0) {
        tmp[len] = @intCast((v % 10) + '0');
        v /= 10;
        len += 1;
    }
    // Reverse into buf
    for (0..len) |i| {
        buf[i] = tmp[len - 1 - i];
    }
    return len;
}
