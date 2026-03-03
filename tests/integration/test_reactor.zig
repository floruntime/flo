const std = @import("std");
const testing = std.testing;
const posix = std.posix;
const src = @import("src");

const Reactor = src.node.reactor.Reactor;
const EventSource = src.node.reactor.EventSource;
const Tag = src.node.reactor.Tag;

test "integration: reactor init and deinit" {
    // Verify that init + deinit works cleanly with no leaks (GPA catches leaks)
    var reactor = try Reactor.init(testing.allocator);
    defer reactor.deinit();

    // Verify the kqueue fd is valid by doing a zero-timeout poll
    const events = try reactor.poll(0);
    // No sources registered, so no events expected
    try testing.expectEqual(@as(usize, 0), events.len);
}

test "integration: reactor add and remove sources" {
    var reactor = try Reactor.init(testing.allocator);
    defer reactor.deinit();

    // Create 10 pipes and register the read ends with the reactor
    var pipes: [10][2]posix.fd_t = undefined;
    for (0..10) |i| {
        pipes[i] = try posix.pipe();
    }

    // Register all 10 read-end fds
    for (0..10) |i| {
        try reactor.addSource(.{
            .fd = pipes[i][0],
            .tag = .client_read,
            .interests = .{ .readable = true },
            .user_data = i,
        });
    }

    // Verify all are registered
    for (0..10) |i| {
        try testing.expect(reactor.sources.contains(pipes[i][0]));
    }

    // Write to a few pipes and verify poll picks them up
    _ = try posix.write(pipes[0][1], "A");
    _ = try posix.write(pipes[5][1], "B");
    _ = try posix.write(pipes[9][1], "C");

    const events = try reactor.poll(100);
    try testing.expect(events.len >= 3);

    // Verify the correct fds fired
    var found_0 = false;
    var found_5 = false;
    var found_9 = false;
    for (events) |ev| {
        if (ev.fd == pipes[0][0]) found_0 = true;
        if (ev.fd == pipes[5][0]) found_5 = true;
        if (ev.fd == pipes[9][0]) found_9 = true;
        try testing.expect(ev.readable);
        try testing.expectEqual(Tag.client_read, ev.tag);
    }
    try testing.expect(found_0);
    try testing.expect(found_5);
    try testing.expect(found_9);

    // Remove all sources from the reactor
    for (0..10) |i| {
        reactor.removeSource(pipes[i][0]);
    }

    // Verify all removed
    for (0..10) |i| {
        try testing.expect(!reactor.sources.contains(pipes[i][0]));
    }

    // Clean up pipes
    for (0..10) |i| {
        posix.close(pipes[i][0]);
        posix.close(pipes[i][1]);
    }

    // A poll after removal should return 0 events (no sources)
    const events2 = try reactor.poll(0);
    try testing.expectEqual(@as(usize, 0), events2.len);
}
