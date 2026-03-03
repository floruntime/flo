const std = @import("std");
const testing = std.testing;
const src = @import("src");

const Inbox = src.node.inbox.Inbox;
const Message = src.node.inbox.Message;
const Tag = src.node.inbox.Tag;

fn makeMsg(tag: Tag, shard: u8, seq: u64) Message {
    return .{
        .tag = tag,
        .src_shard = shard,
        .partition_id = 0,
        .payload_len = 0,
        .sequence = seq,
        .payload_ptr = null,
        ._padding = .{0} ** 8,
    };
}

test "integration: inbox single producer single consumer" {
    var inbox = try Inbox.init(testing.allocator, 2048);
    defer inbox.deinit();

    // Send 1000 messages
    for (0..1000) |i| {
        const ok = inbox.send(makeMsg(.forward_request, 0, @intCast(i)));
        try testing.expect(ok);
    }

    try testing.expectEqual(@as(usize, 1000), inbox.pending());

    // Receive all 1000 in batches
    var batch: [128]Message = undefined;
    var total_received: usize = 0;
    var next_expected_seq: u64 = 0;

    while (total_received < 1000) {
        const n = inbox.drain(&batch);
        try testing.expect(n > 0);
        for (0..n) |j| {
            try testing.expectEqual(next_expected_seq, batch[j].sequence);
            try testing.expectEqual(Tag.forward_request, batch[j].tag);
            next_expected_seq += 1;
        }
        total_received += n;
    }

    try testing.expectEqual(@as(usize, 1000), total_received);
    try testing.expectEqual(@as(usize, 0), inbox.pending());
}

test "integration: inbox fill to capacity" {
    // Use minimum capacity (16) for easy testing
    var inbox = try Inbox.init(testing.allocator, 16);
    defer inbox.deinit();

    // Fill the ring completely
    for (0..16) |i| {
        const ok = inbox.send(makeMsg(.forward_request, 0, @intCast(i)));
        try testing.expect(ok);
    }

    // Verify backpressure — send should return false when full
    const overflow = inbox.send(makeMsg(.forward_request, 0, 999));
    try testing.expect(!overflow);
    try testing.expectEqual(@as(usize, 16), inbox.pending());

    // Drain some to free space
    var batch: [4]Message = undefined;
    const drained = inbox.drain(&batch);
    try testing.expectEqual(@as(usize, 4), drained);
    try testing.expectEqual(@as(usize, 12), inbox.pending());

    // Now sends should work again
    for (0..4) |i| {
        const ok = inbox.send(makeMsg(.forward_response, 1, @as(u64, @intCast(i)) + 100));
        try testing.expect(ok);
    }
    try testing.expectEqual(@as(usize, 16), inbox.pending());

    // One more should fail again
    const overflow2 = inbox.send(makeMsg(.forward_request, 0, 888));
    try testing.expect(!overflow2);
}

test "integration: inbox tag filtering" {
    var inbox = try Inbox.init(testing.allocator, 256);
    defer inbox.deinit();

    // Send mixed tags
    _ = inbox.send(makeMsg(.forward_request, 0, 1));
    _ = inbox.send(makeMsg(.forward_response, 1, 2));
    _ = inbox.send(makeMsg(.connection_handoff, 2, 3));
    _ = inbox.send(makeMsg(.raft_message, 3, 4));
    _ = inbox.send(makeMsg(.metadata_update, 0, 5));
    _ = inbox.send(makeMsg(.shutdown, 0, 6));
    _ = inbox.send(makeMsg(.forward_request, 1, 7));
    _ = inbox.send(makeMsg(.forward_response, 2, 8));

    try testing.expectEqual(@as(usize, 8), inbox.pending());

    // Drain all and verify each tag delivered correctly
    var batch: [16]Message = undefined;
    const n = inbox.drain(&batch);
    try testing.expectEqual(@as(usize, 8), n);

    // Verify order and tags preserved
    try testing.expectEqual(Tag.forward_request, batch[0].tag);
    try testing.expectEqual(@as(u64, 1), batch[0].sequence);
    try testing.expectEqual(@as(u8, 0), batch[0].src_shard);

    try testing.expectEqual(Tag.forward_response, batch[1].tag);
    try testing.expectEqual(@as(u64, 2), batch[1].sequence);
    try testing.expectEqual(@as(u8, 1), batch[1].src_shard);

    try testing.expectEqual(Tag.connection_handoff, batch[2].tag);
    try testing.expectEqual(@as(u64, 3), batch[2].sequence);
    try testing.expectEqual(@as(u8, 2), batch[2].src_shard);

    try testing.expectEqual(Tag.raft_message, batch[3].tag);
    try testing.expectEqual(@as(u64, 4), batch[3].sequence);
    try testing.expectEqual(@as(u8, 3), batch[3].src_shard);

    try testing.expectEqual(Tag.metadata_update, batch[4].tag);
    try testing.expectEqual(@as(u64, 5), batch[4].sequence);

    try testing.expectEqual(Tag.shutdown, batch[5].tag);
    try testing.expectEqual(@as(u64, 6), batch[5].sequence);

    try testing.expectEqual(Tag.forward_request, batch[6].tag);
    try testing.expectEqual(@as(u64, 7), batch[6].sequence);
    try testing.expectEqual(@as(u8, 1), batch[6].src_shard);

    try testing.expectEqual(Tag.forward_response, batch[7].tag);
    try testing.expectEqual(@as(u64, 8), batch[7].sequence);
    try testing.expectEqual(@as(u8, 2), batch[7].src_shard);

    try testing.expectEqual(@as(usize, 0), inbox.pending());
}
