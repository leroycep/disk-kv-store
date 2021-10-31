const std = @import("std");
const Tree = @import("disk-kv-store").mem_cow_exponential_generic.Tree;
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
    const random_queries = 5_000_000;
    const from_entries_queries = 5_000_000;

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();

    try writer.print("running exponential_mem benchmark\n", .{});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});
    try writer.print("\trandom queries = {}\n", .{random_queries});
    try writer.print("\tqueries from list of entries = {}\n", .{from_entries_queries});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    const entries = try makeEntries(&arena.allocator, random, count);

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

const Entry = struct {
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

pub fn construct(allocator: *std.mem.Allocator, entries: []const Entry) !Tree(i64, i64) {
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

pub fn fromEntriesQueries(tree: Tree(i64, i64), entries: []const Entry, random: *std.rand.Random, count: usize) !void {
    const t = tracy.trace(@src());
    defer t.end();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const entry_idx = random.uintLessThan(usize, entries.len);
        const key = entries[entry_idx].key;

        try std.testing.expect(tree.get(key) != null);
    }
}
