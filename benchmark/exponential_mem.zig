const std = @import("std");
const Tree = @import("disk-kv-store").mem_cow_exponential_generic.Tree;
const tracy = @import("tracy");

pub fn main() !void {
    const t = tracy.trace(@src());
    defer t.end();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit());

    var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
    defer arena.deinit();

    // Get argument values
    const seed = std.crypto.random.int(u64);
    const count = 1_000_000;
    const queries = 100_000_000;

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();

    try writer.print("running exponential_mem benchmark\n", .{});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});
    try writer.print("\tqueries = {}\n", .{queries});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    var timer = try std.time.Timer.start();

    // Construct tree
    var tree = try construct(&gpa.allocator, random, count);
    defer {
        const t_free = tracy.trace(@src());
        defer t_free.end();

        tree.deinit();
    }

    const ns_to_construct = timer.read();

    // Write time
    try writer.print("tree constructed in {}ns\n", .{ns_to_construct});

    // TODO: Query tree x times
    timer.reset();
    _ = queries;

    try writer.print("done\n", .{});
}

const Entry = struct {
    key: i64,
    val: i64,
};

pub fn construct(allocator: *std.mem.Allocator, random: *std.rand.Random, count: usize) !Tree(i64, i64) {
    const t = tracy.trace(@src());
    defer t.end();

    var tree = Tree(i64, i64).init(allocator);
    errdefer tree.deinit();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        _ = try tree.put(
            random.int(i64),
            random.int(i64),
        );
    }

    return tree;
}
