const std = @import("std");

pub const Entry = struct {
    key: i64,
    val: i64,
};

pub fn makeEntries(allocator: *std.mem.Allocator, random: *std.rand.Random, count: usize) ![]Entry {
    const entries = try allocator.alloc(Entry, count);
    errdefer allocator.free(entries);

    for (entries) |*entry| {
        entry.* = .{
            .key = random.int(i64),
            .val = random.int(i64),
        };
    }

    return entries;
}

