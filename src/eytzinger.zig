const std = @import("std");

fn parent(idx: u32) u32 {
    std.debug.assert(idx > 0);
    return (idx - 1) / 2;
}

fn left(idx: u32) u32 {
    return 2 * idx + 1;
}

fn right(idx: u32) u32 {
    return 2 * idx + 2;
}

const ChildType = enum {
    root,
    left,
    right,
};

fn idxChildType(idx: u32) ChildType {
    if (idx == 0) return .root;
    switch (idx % 2) {
        1 => return .left,
        0 => return .right,
        else => unreachable,
    }
}

fn next(idx: u32, len: u32) ?u32 {
    if (right(idx) < len) {
        var current_idx = right(idx);
        while (left(current_idx) < len) : (current_idx = left(current_idx)) {}
        return current_idx;
    } else {
        var current_idx = idx;
        while (true) {
            switch (idxChildType(current_idx)) {
                .root => return null,
                .left => break,
                .right => current_idx = parent(current_idx),
            }
        }
        return parent(current_idx);
    }
}

fn fromLinear(indexLinear: u32, len: u32) u32 {
    std.debug.assert(len > 0);
    std.debug.assert(indexLinear < len);

    if (len == 1) return 0;

    const height = std.math.log2_int_ceil(u32, len);
    const layer = height - @ctz(u32, indexLinear + 1);

    const layer_start_index = (@as(u32, 1) << @intCast(u5, layer - 1)) - 1;

    const pos_in_layer = (indexLinear >> @intCast(u5, height - layer + 1));

    return layer_start_index + pos_in_layer;
}

fn toLinear(indexEytzinger: u32, len: u32) u32 {
    // TODO: Account for incomplete layers
    std.debug.assert(len > 0);
    std.debug.assert(indexEytzinger < len);

    if (len == 1) return 0;

    const l = std.math.log2_int_ceil(u32, indexEytzinger + 2);
    const h = std.math.log2_int_ceil(u32, len);

    const half_stride = (@as(u32, 1) << @intCast(u5, h - l));
    const layer_start_index = (@as(u32, 1) << @intCast(u5, l - 1)) - 1;

    const pos_in_layer = indexEytzinger - layer_start_index;
    const stride_of_layer = half_stride << 1;

    return half_stride - 1 + pos_in_layer * stride_of_layer;
}

test "linear/eytzinger index conversion" {
    try std.testing.expectEqual(@as(u32, 15), fromLinear(0, 31));
    try std.testing.expectEqual(@as(u32, 21), fromLinear(12, 31));
    try std.testing.expectEqual(@as(u32, 0), fromLinear(15, 31));
    try std.testing.expectEqual(@as(u32, 19), fromLinear(8, 31));
    try std.testing.expectEqual(@as(u32, 5), fromLinear(19, 31));
    try std.testing.expectEqual(@as(u32, 9), fromLinear(9, 31));

    try std.testing.expectEqual(@as(u32, 95), toLinear(8, 511));
    try std.testing.expectEqual(@as(u32, 3965), toLinear(2014, 4095));
}

test "fuzz linear/eytzinger index conversion" {
    const iterations = 100;

    var prng = std.rand.DefaultPrng.init(std.crypto.random.int(u64));

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const len = (@as(u32, 1) << prng.random.int(u5)) - 1;
        if (len == 0) continue;
        const linear_idx = prng.random.uintLessThan(u32, len);
        try testLinearEytzingerConversion(linear_idx, len);
    }
}

fn testLinearEytzingerConversion(linearIdx: u32, len: u32) !void {
    errdefer std.log.err("linear index = {}, len = {}", .{ linearIdx, len });

    const eytzinger_idx = fromLinear(linearIdx, len);
    errdefer std.log.err("eytzinger index = {}", .{eytzinger_idx});

    try std.testing.expect(eytzinger_idx < len);

    try std.testing.expectEqual(linearIdx, toLinear(eytzinger_idx, len));
}
