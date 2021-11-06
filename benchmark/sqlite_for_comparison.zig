const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});
const utils = @import("./benchmark_utils.zig");

pub fn main() !u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(!gpa.deinit());

    var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
    defer arena.deinit();

    const db_path_z: [:0]const u8 = ":memory:";

    const seed = std.crypto.random.int(u64);
    const count = 1_000_000;

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();
    const random_queries = 5_000_000;
    const from_entries_queries = 5_000_000;

    try writer.print("running sqlite for comparison benchmark\n", .{});
    try writer.print("\tdb path = {s}\n", .{db_path_z});
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

    var timer = try std.time.Timer.start();

    var db: ?*c.sqlite3 = null;
    defer _ = c.sqlite3_close(db);
    if (c.sqlite3_open(db_path_z, &db) != 0) {
        std.log.err("Can't open database: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const INIT_SQL: [:0]const u8 =
        \\CREATE TABLE key_vals (
        \\  key int primary key not null,
        \\  val int not null
        \\);
    ;
    if (c.sqlite3_exec(db, INIT_SQL, null, null, null) != 0) {
        std.log.err("Can't execute init SQL: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const INSERT_SQL =
        \\INSERT INTO key_vals(key, val) VALUES (?, ?);
    ;

    var stmt: ?*c.sqlite3_stmt = null;
    defer _ = c.sqlite3_finalize(stmt);
    if (c.sqlite3_prepare_v2(db, INSERT_SQL, INSERT_SQL.len, &stmt, null) != c.SQLITE_OK) {
        std.log.err("Can't prepare insert statment: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const SELECT_SQL =
        \\SELECT val FROM key_vals WHERE key = ?;
    ;
    var select_stmt: ?*c.sqlite3_stmt = null;
    defer _ = c.sqlite3_finalize(select_stmt);
    if (c.sqlite3_prepare_v2(db, SELECT_SQL, SELECT_SQL.len, &select_stmt, null) != c.SQLITE_OK) {
        std.log.err("Can't prepare select statment: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const ns_to_prepare = timer.read();
    try writer.print("table and statement prepared in {}\n", .{std.fmt.fmtDuration(ns_to_prepare)});
    timer.reset();

    if (false and c.sqlite3_exec(db, "BEGIN TRANSACTION;", null, null, null) != c.SQLITE_OK) {
        std.log.err("Can't execute init SQL: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    {
        for (entries) |entry| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_clear_bindings(stmt);

            std.debug.assert(c.sqlite3_bind_int64(stmt, 1, entry.key) == c.SQLITE_OK);
            std.debug.assert(c.sqlite3_bind_int64(stmt, 2, entry.val) == c.SQLITE_OK);

            std.debug.assert(c.sqlite3_step(stmt) == c.SQLITE_DONE);
        }
    }

    if (false and c.sqlite3_exec(db, "END TRANSACTION;", null, null, null) != c.SQLITE_OK) {
        std.log.err("Can't execute init SQL: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const ns_to_construct = timer.read();

    // Write time
    try writer.print("keys and values inserted in {}\n", .{std.fmt.fmtDuration(ns_to_construct)});

    // Query table with random keys
    timer.reset();
    {
        var i: usize = 0;
        while (i < count) : (i += 1) {
            _ = c.sqlite3_reset(select_stmt);
            _ = c.sqlite3_clear_bindings(select_stmt);

            const key = random.int(i64);
            std.debug.assert(c.sqlite3_bind_int64(select_stmt, 1, key) == c.SQLITE_OK);

            switch (c.sqlite3_step(select_stmt)) {
                c.SQLITE_DONE => {},
                c.SQLITE_ROW => {
                    const result = c.sqlite3_column_int64(select_stmt, 0);
                    try std.testing.expectEqual(entries_hashmap.get(key), result);
                },
                else => {
                    std.log.err("Error returned while select key {}: {s}", .{ key, c.sqlite3_errmsg(db) });
                    return 1;
                },
            }
        }
    }
    const ns_to_query_random = timer.read();

    try writer.print("tree answered random queries in {}\n", .{std.fmt.fmtDuration(ns_to_query_random)});

    timer.reset();
    {
        var i: usize = 0;
        while (i < count) : (i += 1) {
            errdefer std.log.err("In query {}", .{i});
            _ = c.sqlite3_reset(select_stmt);
            _ = c.sqlite3_clear_bindings(select_stmt);

            const entry_idx = random.uintLessThan(usize, entries.len);
            const key = entries[entry_idx].key;
            std.debug.assert(c.sqlite3_bind_int64(select_stmt, 1, key) == c.SQLITE_OK);

            const expected = entries_hashmap.get(key);
            const result: ?i64 = switch (c.sqlite3_step(select_stmt)) {
                c.SQLITE_DONE => null,
                c.SQLITE_ROW => c.sqlite3_column_int64(select_stmt, 0),
                else => {
                    std.log.err("Error returned while select key {}: {s}", .{ key, c.sqlite3_errmsg(db) });
                    return 1;
                },
            };

            try std.testing.expectEqual(expected, result);
        }
    }
    const ns_to_query_existing = timer.read();
    try writer.print("tree answered existing queries in {}\n", .{std.fmt.fmtDuration(ns_to_query_existing)});

    return 0;
}
