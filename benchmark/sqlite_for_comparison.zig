const std = @import("std");
const c = @cImport({
    @cInclude("sqlite3.h");
});

pub fn main() !u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(!gpa.deinit());

    const db_path_z: [:0]const u8 = ":memory:";

    const seed = std.crypto.random.int(u64);
    const count = 1_000_000;

    const stdout = std.io.getStdOut();
    const writer = stdout.writer();

    try writer.print("running exponential_mem benchmark\n", .{});
    try writer.print("\tdb path = {s}\n", .{db_path_z});
    try writer.print("\tseed = {}\n", .{seed});
    try writer.print("\tcount = {}\n", .{count});

    var prng = std.rand.DefaultPrng.init(seed);
    const random = &prng.random;

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

    const ns_to_prepare = timer.read();
    try writer.print("table and statement prepared in {}\n", .{std.fmt.fmtDuration(ns_to_prepare)});
    timer.reset();

    if (c.sqlite3_exec(db, "BEGIN;", null, null, null) != 0) {
        std.log.err("Can't execute init SQL: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    var i: usize = 0;
    while (i < count) : (i += 1) {
        _ = c.sqlite3_reset(stmt);
        _ = c.sqlite3_clear_bindings(stmt);

        std.debug.assert(c.sqlite3_bind_int64(stmt, 1, random.int(i64)) == c.SQLITE_OK);
        std.debug.assert(c.sqlite3_bind_int64(stmt, 2, random.int(i64)) == c.SQLITE_OK);

        std.debug.assert(c.sqlite3_step(stmt) == c.SQLITE_DONE);
    }

    if (c.sqlite3_exec(db, "COMMIT;", null, null, null) != 0) {
        std.log.err("Can't execute init SQL: {s}", .{c.sqlite3_errmsg(db)});
        return 1;
    }

    const ns_to_construct = timer.read();

    // Write time
    try writer.print("keys and values inserted in {}\n", .{std.fmt.fmtDuration(ns_to_construct)});

    return 0;
}
