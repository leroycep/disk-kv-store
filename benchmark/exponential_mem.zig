const std = @import("std");
const Tree = @import("disk-kv-store").mem_cow_exponential_generic.Tree;
const tracy = @import("tracy");
const utils = @import("./benchmark_utils.zig");
const clap = @import("clap");

pub fn main() !void {
    const t = tracy.trace(@src());
    defer t.end();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(!gpa.deinit());

    var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
    defer arena.deinit();

    // Set default argument values
    var seed = std.crypto.random.int(u64);
    var count: u64 = 1_000_000;
    var random_queries: u64 = 5_000_000;
    var from_entries_queries: u64 = 5_000_000;

    // Parse command line arguments
    {
        const params = comptime [_]clap.Param(clap.Help){
            clap.parseParam("-h, --help                    Display this help and exit") catch unreachable,
            clap.parseParam("-s, --seed <NUM>              Set the psuedo-random seed") catch unreachable,
            clap.parseParam("-c, --count <NUM>             Set the number of values to insert") catch unreachable,
            clap.parseParam("-r, --queries-random <NUM>    Set the number of random values to query the tree with") catch unreachable,
            clap.parseParam("-e, --queries-existing <NUM>  Set the number of existing values to query the tree with") catch unreachable,
        };

        var args = try clap.parse(clap.Help, &params, .{});
        defer args.deinit();

        if (args.flag("--help"))
            return try clap.help(std.io.getStdErr().writer(), &params);
        if (args.option("--seed")) |n|
            seed = try std.fmt.parseInt(u64, n, 10);
        if (args.option("--count")) |n|
            count = try std.fmt.parseInt(u64, n, 10);
        if (args.option("--queries-random")) |n|
            random_queries = try std.fmt.parseInt(u64, n, 10);
        if (args.option("--queries-existing")) |n|
            from_entries_queries = try std.fmt.parseInt(u64, n, 10);
    }

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();

    try writer.print("running exponential_mem benchmark\n", .{});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});
    try writer.print("\trandom queries = {}\n", .{random_queries});
    try writer.print("\tqueries from list of entries = {}\n", .{from_entries_queries});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    const entries = try utils.makeEntries(&arena.allocator, random, count);

    var entries_hashmap = std.AutoHashMap(i64, i64).init(&arena.allocator);
    try entries_hashmap.ensureTotalCapacity(@intCast(u32, entries.len));
    for (entries) |entry| {
        entries_hashmap.putAssumeCapacity(entry.key, entry.val);
    }

    // Construct tree
    var timer = try std.time.Timer.start();
    var tree = try construct(&gpa.allocator, entries);
    defer tree.deinit();

    const ns_to_construct = timer.read();

    // Write time
    try writer.print("tree constructed in {}\n", .{std.fmt.fmtDuration(ns_to_construct)});

    // Build hashmap to compare against

    // Query tree with random keys
    timer.reset();
    try randomQueries(tree, entries_hashmap, random, random_queries);
    const ns_to_query_random = timer.read();

    try writer.print("tree answered random queries in {}\n", .{std.fmt.fmtDuration(ns_to_query_random)});

    // Query tree with existing keys
    timer.reset();
    try fromEntriesQueries(tree, entries, random, from_entries_queries);
    const ns_to_query_existing = timer.read();

    try writer.print("tree answered existing queries in {}\n", .{std.fmt.fmtDuration(ns_to_query_existing)});
    try writer.print("done\n", .{});
}

pub fn construct(allocator: *std.mem.Allocator, entries: []const utils.Entry) !Tree(i64, i64) {
    const t = tracy.trace(@src());
    defer t.end();

    var tree = Tree(i64, i64).init(allocator);
    errdefer tree.deinit();

    for (entries) |entry| {
        _ = try tree.put(
            entry.key,
            entry.val,
        );
    }

    return tree;
}

pub fn randomQueries(tree: Tree(i64, i64), truth: std.AutoHashMap(i64, i64), random: *std.rand.Random, count: usize) !void {
    const t = tracy.trace(@src());
    defer t.end();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const key = random.int(i64);

        const tree_result = tree.get(key);
        const truth_result = truth.get(key);

        try std.testing.expectEqual(tree_result, truth_result);
    }
}

pub fn fromEntriesQueries(tree: Tree(i64, i64), entries: []const utils.Entry, random: *std.rand.Random, count: usize) !void {
    const t = tracy.trace(@src());
    defer t.end();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const entry_idx = random.uintLessThan(usize, entries.len);
        const key = entries[entry_idx].key;

        try std.testing.expect(tree.get(key) != null);
    }
}
