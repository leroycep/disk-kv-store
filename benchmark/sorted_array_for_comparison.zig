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
    const array = try gpa.allocator.alloc(Entry, count);
    defer gpa.allocator.free(array);

    for (array) |*entry| {
        entry.* = .{
            .key = random.int(i64),
            .val = random.int(i64),
        };
    }

    std.sort.sort(Entry, array, {}, entryLessThan);

    // Write time
    const ns_to_construct = timer.read();
    try writer.print("tree constructed in {}\n", .{std.fmt.fmtDuration(ns_to_construct)});

    // TODO: Query tree x times
    timer.reset();
    _ = queries;

    try writer.print("done\n", .{});
}

fn entryLessThan(_: void, a: Entry, b: Entry) bool {
    return a.key < b.key;
}

const Entry = struct {
    key: i64,
    val: i64,
};
