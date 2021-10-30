const std = @import("std");
const tracy = @import("tracy");

pub fn main() !void {
    const t = tracy.trace(@src());
    defer t.end();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(!gpa.deinit());

    var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
    defer arena.deinit();

    // Get argument values
    const seed = std.crypto.random.int(u64);
    const count = 1_000_000;
    const queries = 100_000_000;

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();

    try writer.print("running sort_array comparison benchmark\n", .{});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});
    try writer.print("\tqueries = {}\n", .{queries});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    var timer = try std.time.Timer.start();

    // Construct tree
    var array = std.ArrayList(Entry).init(&gpa.allocator);
    defer array.deinit();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const new_entry = Entry{
            .key = random.int(i64),
            .val = random.int(i64),
        };
        const search_result = binarySearch(Entry, new_entry, array.items, {}, entryOrder);
        if (search_result.foundExisting) {
            array.items[search_result.index] = new_entry;
        } else {
            try array.insert(search_result.index, new_entry);
        }
    }
    std.debug.assert(std.sort.isSorted(Entry, array.items, {}, entryLessThan));

    // Write time
    const ns_to_construct = timer.read();
    try writer.print("array constructed in {}\n", .{std.fmt.fmtDuration(ns_to_construct)});

    // TODO: Query tree x times
    timer.reset();
    _ = queries;

    try writer.print("done\n", .{});
}

fn entryLessThan(_: void, a: Entry, b: Entry) bool {
    return a.key < b.key;
}

fn entryOrder(_: void, a: Entry, b: Entry) math.Order {
    return std.math.order(a.key, b.key);
}

const Entry = struct {
    key: i64,
    val: i64,
};

const math = std.math;

const SearchResult = struct {
    index: usize,
    foundExisting: bool,
};

pub fn binarySearch(
    comptime T: type,
    key: T,
    items: []const T,
    context: anytype,
    comptime compareFn: fn (context: @TypeOf(context), lhs: T, rhs: T) math.Order,
) SearchResult {
    var left: usize = 0;
    var right: usize = items.len;

    while (left < right) {
        // Avoid overflowing in the midpoint calculation
        const mid = left + (right - left) / 2;
        // Compare the key with the midpoint element
        switch (compareFn(context, key, items[mid])) {
            .eq => return .{ .index = mid, .foundExisting = true },
            .gt => left = mid + 1,
            .lt => right = mid,
        }
    }

    return .{ .index = left, .foundExisting = false };
}
