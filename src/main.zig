const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const btree = @import("btree.zig");

pub const Database = struct {
    file: fs.File,

    pub const Options = struct {
        create_if_not_exists: bool = true,
    };

    pub fn initFile(dir: fs.Dir, filename: []const u8, options: Options) !@This() {
        _ = options;

        var file = dir.openFile(filename, .{ .read = true }) catch |open_error| switch (open_error) {
            error.FileNotFound => if (options.create_if_not_exists) attempt_create_file: {
                var file = try dir.createFile(filename, .{ .read = true });

                try file.writeAll("DiskBtree 1\x00");
                try file.writer();
                try file.seekTo(0);

                break :attempt_create_file file;
            } else {
                return open_error;
            },
            else => |e| return e,
        };
        errdefer file.close();

        return @This(){
            .file = file,
        };
    }

    pub fn deinit(this: *@This()) void {
        // ??? Anything to do here?
        this.file.close();
    }

    pub fn begin(this: *@This(), allocator: *std.mem.Allocator, options: Transaction.Options) !Transaction {
        return Transaction.init(this, allocator, options);
    }
};

pub const Transaction = struct {
    database: *Database,
    key_buf: std.ArrayList(u8),
    val_buf: std.ArrayList(u8),

    pub const Options = struct {};

    pub fn init(database: *Database, allocator: *std.mem.Allocator, options: Options) @This() {
        _ = options;
        return @This(){
            .database = database,
            .key_buf = std.ArrayList(u8).init(allocator),
            .val_buf = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(this: *@This()) void {
        this.key_buf.deinit();
        this.val_buf.deinit();
    }

    pub fn commit(this: *@This()) !void {
        _ = this;
    }

    pub fn store(this: *@This(), name: []const u8) !Store {
        _ = name;
        return Store{
            .txn = this,
        };
    }
};

pub const Store = struct {
    txn: *Transaction,

    pub fn set(this: *@This(), key: []const u8, value: []const u8) !void {
        _ = this;
        _ = key;
        _ = value;
    }

    pub fn get(this: *@This(), key: []const u8) !?[]const u8 {
        _ = key;
        return this.txn.val_buf.items;
    }
};

test {
    testing.refAllDecls(@This());
}

test "inserting data and then retriving it in one store" {
    if (true) return;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var db = try Database.initFile(tmp.dir, "library.db", .{});
    defer db.deinit();

    {
        var txn = try db.begin(testing.allocator, .{});
        defer txn.deinit();

        var books = try txn.store("books");
        try books.set("Pride and Prejudice", "Jane Austen");
        try books.set("Worth the Candle", "Alexander Wales");
        try books.set("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
        try books.set("Mother of Learning", "nobody103");

        try txn.commit();
    }

    {
        var txn = try db.begin(testing.allocator, .{});
        defer txn.deinit();

        var books = try txn.store("books");
        try testing.expectEqualSlices(u8, "Jane Austen", (try books.get("Pride and Prejudice")).?);
        try testing.expectEqualSlices(u8, "Alexander Wales", (try books.get("Worth the Candle")).?);
        try testing.expectEqualSlices(u8, "香月美夜 (Miya Kazuki)", (try books.get("Ascendance of a Bookworm")).?);
        try testing.expectEqualSlices(u8, "nobody103", (try books.get("Mother of Learning")).?);

        try testing.expectEqual(@as(?[]const u8, null), try books.get("The Winds of Winter"));
        try testing.expectEqual(@as(?[]const u8, null), try books.get("Doors of Stone"));
    }
}
