const std = @import("std");
const Tree = @import("disk-kv-store").mem_cow_exponential_generic.Tree;
const tracy = @import("tracy");

pub fn main() !void {
    const t = tracy.trace(@src());
    defer t.end();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // Get argument values
    const seed = std.crypto.random.int(u64);
    const count = 1_000_000;
    const queries = 100_000_000;
    const print_every_x_keys = 100_000;

    const stdout = std.io.getStdOut();
    defer stdout.close();
    const writer = stdout.writer();

    try writer.print("running space_used benchmark\n", .{});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});
    try writer.print("\tqueries = {}\n", .{queries});
    try writer.print("\tprinting space used every {} keys\n", .{print_every_x_keys});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

    var unique_keys: usize = 0;

    // Construct tree
    var tree = construct: {
        const t1 = tracy.trace(@src());
        defer t1.end();

        var tree = Tree(i64, i64).init(&gpa.allocator);
        errdefer tree.deinit();

        try writer.print("idx\t# keys\t# bytes\n", .{});

        var i: usize = 0;
        while (i < count) : (i += 1) {
            if (i % print_every_x_keys == 0) {
                const bytes_used = tree.countBytesUsed();
                const bytes_in_allocation_cache = tree.countBytesInAllocationCache();

                try writer.print("{}\t{}\t{}\t{}\t{}\t{}\n", .{ i, unique_keys, bytes_used, bytes_in_allocation_cache, bytes_in_allocation_cache, bytes_used + bytes_in_allocation_cache });
            }

            const was_duplicate_key = try tree.put(
                random.int(i64),
                random.int(i64),
            );
            if (!was_duplicate_key) unique_keys += 1;
        }

        break :construct tree;
    };

    defer {
        const t_free = tracy.trace(@src());
        defer t_free.end();

        tree.deinit();
    }

    const bytes_used = tree.countBytesUsed();
    const bytes_in_allocation_cache = tree.countBytesInAllocationCache();
    const total_bytes_allocated = bytes_used + bytes_in_allocation_cache;

    const bytes_used_f = @intToFloat(f32, bytes_used);
    const bytes_in_allocation_cache_f = @intToFloat(f32, bytes_in_allocation_cache);
    const total_bytes_allocated_f = @intToFloat(f32, total_bytes_allocated);

    const key_size = @sizeOf(i64);
    const val_size = @sizeOf(i64);
    const entry_size = key_size + val_size;

    const bytes_used_per_entry = bytes_used_f / @intToFloat(f32, unique_keys);
    const overhead = bytes_used_per_entry - @intToFloat(f32, entry_size);

    try writer.print("\n", .{});
    try writer.print("unique_keys = {}\n", .{unique_keys});
    try writer.print("bytes_used = {}/{} ({d:2.1}%)\n", .{ bytes_used, total_bytes_allocated, bytes_used_f / total_bytes_allocated_f * 100.0 });
    try writer.print("bytes_in_allocation_cache = {}/{} ({d:2.1}%)\n", .{ bytes_in_allocation_cache, total_bytes_allocated, bytes_in_allocation_cache_f / total_bytes_allocated_f * 100.0 });
    try writer.print("size_of_key = {}, size_of_value = {}, entry_size = {}\n", .{ key_size, val_size, entry_size });
    try writer.print("bytes_used/entry = {d:2.1}, overhead = {d:2.1} ({d:2.1}%)\n", .{ bytes_used_per_entry, overhead, overhead / @intToFloat(f32, entry_size) * 100.0 });

    try writer.print("done\n", .{});
}
