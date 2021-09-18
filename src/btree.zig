const std = @import("std");
const testing = std.testing;

const PAGE_SIZE = 2048;

pub const BTree = struct {
    source: std.io.StreamSource,
    offset: u64,
    order: u64,

    pub const Options = struct {
        offset: u64 = 0,
        order: u64 = 15,
    };

    pub fn init(source: std.io.StreamSource, options: Options) @This() {
        return @This(){
            .source = source,
            .offset = options.offset,
            .order = options.order,
        };
    }

    pub fn create(source: std.io.StreamSource, options: Options) !@This() {
        var btree_source = source;
        try btree_source.seekTo(options.offset);

        const writer = btree_source.writer();
        try writer.writeIntLittle(u32, 0);
        try writer.writeIntLittle(u32, 0);
        try writer.writeByte(@enumToInt(BTreeTag.leaf));
        try writer.writeIntLittle(u8, 0);
        try writer.writeIntLittle(u8, 0);

        try btree_source.seekTo(options.offset + PAGE_SIZE - 1);
        try writer.writeByte(0);

        return @This(){
            .source = btree_source,
            .offset = options.offset,
            .order = options.order,
        };
    }

    pub const BTreeTag = enum {
        internal,
        leaf,
    };

    const LeafCell = packed struct {
        key_ptr: u8,
        key_len: u8,
        val_ptr: u8,
        val_len: u8,
    };

    const LEAF_CELL_SIZE = @sizeOf(LeafCell);

    pub fn deinit(this: *@This()) void {
        this.valBuf.deinit();
    }

    pub fn put(this: *@This(), key: []const u8, val: []const u8) !void {
        std.debug.assert(key.len > 0); // key length must be greater than zero

        std.debug.assert(key.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page
        std.debug.assert(val.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page

        const writer = this.source.writer();
        const reader = this.source.reader();
        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type

        const current_number_of_keys = try reader.readByte();
        const current_kv_offset: u16 = try reader.readByte();

        if (current_number_of_keys >= 169) {
            return error.BTreeFull;
        }
        if (current_kv_offset + std.math.min((key.len - 1) / 4 + 1, 64) + std.math.min((val.len - 1) / 4 + 1, 64) >= 255) {
            return error.BTreeFull;
        }

        const cell_to_write_to = try this.findCellPos(key);

        const cell_offset = this.offset + 8 + 8 + LEAF_CELL_SIZE * switch (cell_to_write_to) {
            .cellEmpty, .cellExists, .cellReplace => |o| o,
        };
        const key_offset = current_kv_offset + ((key.len - 1) / 4) + 1;
        const val_offset = key_offset + ((val.len - 1) / 4) + 1;

        if (key_offset > 255 or val_offset > 255) return error.BTreeFull;

        const cell = LeafCell{
            .key_ptr = @intCast(u8, key_offset),
            .key_len = @intCast(u8, key.len),
            .val_ptr = @intCast(u8, val_offset),
            .val_len = @intCast(u8, val.len),
        };

        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type
        try writer.writeByte(if (cell_to_write_to == .cellExists) current_number_of_keys else current_number_of_keys + 1);
        try writer.writeByte(@intCast(u8, val_offset));

        switch (cell_to_write_to) {
            .cellExists => unreachable, // TODO: Implement overwriting key and value
            .cellEmpty => {},
            .cellReplace => |cell_idx| {
                // Move all cells after cell index
                const num_cells_to_move = current_number_of_keys - cell_idx;

                // Read cells
                var cells_buf: [255 * LEAF_CELL_SIZE]u8 = undefined;
                try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * cell_idx);
                std.debug.assert((try reader.readAll(cells_buf[0 .. num_cells_to_move * LEAF_CELL_SIZE])) == num_cells_to_move * LEAF_CELL_SIZE);

                // Write cells to their new location, 1 cell down
                try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * (cell_idx + 1));
                try writer.writeAll(cells_buf[0 .. num_cells_to_move * LEAF_CELL_SIZE]);
            },
        }

        try this.source.seekTo(cell_offset);
        try writer.writeAll(std.mem.asBytes(&cell));

        try this.source.seekTo(this.offset + PAGE_SIZE - ((key_offset) * 4));
        try writer.writeAll(key);

        try this.source.seekTo(this.offset + PAGE_SIZE - ((val_offset) * 4));
        try writer.writeAll(val);
    }

    const FindResult = union(enum) {
        cellExists: u8, // Cell already exists, overwrite it with the new value
        cellEmpty: u8, // Cell does not exist, but here is an empty spot for it
        cellReplace: u8, // Cell does not exist, this cell must be moved to make room
    };

    fn findCellPos(this: *@This(), key: []const u8) !FindResult {
        const reader = this.source.reader();

        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type
        const number_of_cells = try reader.readByte();

        var cell_idx: u8 = 0;
        while (cell_idx < number_of_cells) : (cell_idx += 1) {
            try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * cell_idx);

            const key_offset = try reader.readIntLittle(u8);
            const key_len = try reader.readIntLittle(u8);

            std.debug.assert(key_len != 0xFF);

            try this.source.seekTo(this.offset + PAGE_SIZE - (key_offset) * 4);

            var bytes: [256]u8 = undefined;
            const bytes_read = try reader.readAll(bytes[0..key_len]);
            if (bytes_read < bytes.len and bytes_read < key_len) {
                return error.UnexpectedEOF;
            }

            switch (std.mem.order(u8, key, bytes[0..bytes_read])) {
                .gt => continue,
                .eq => return FindResult{ .cellExists = cell_idx },
                .lt => return FindResult{ .cellReplace = cell_idx },
            }
        }
        return FindResult{ .cellEmpty = cell_idx };
    }

    pub fn get(this: *@This(), allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        std.debug.assert(key.len > 0);

        const reader = this.source.reader();

        switch (try this.findCellPos(key)) {
            .cellEmpty, .cellReplace => return null,
            .cellExists => |cell_pos| {
                try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * cell_pos);

                var cell: LeafCell = undefined;
                std.debug.assert((try reader.readAll(std.mem.asBytes(&cell))) == @sizeOf(LeafCell));

                const value = try allocator.alloc(u8, cell.val_len);
                errdefer allocator.free(value);

                try this.source.seekTo(this.offset + PAGE_SIZE - (cell.val_ptr) * 4);
                const val_bytes_read = try reader.readAll(value);
                if (val_bytes_read < cell.val_len) {
                    return error.UnexpectedEOF;
                }

                return value;
            },
        }
    }

    pub const RangeIterator = struct {
        btree: *BTree,
        start: Limit,
        end: Limit,
        pos: u8,
        dir: Direction,
        cellsInPage: u8,

        pub fn nextEntry(this: *@This(), allocator: *std.mem.Allocator) !?Entry {
            if (this.dir == .forwards and this.pos >= this.cellsInPage) return null;
            if (this.dir == .backwards and this.pos == 0) return null;

            const pos = if (this.dir == .backwards) this.pos - 1 else this.pos;

            switch (this.dir) {
                .forwards => this.pos += 1,
                .backwards => this.pos -= 1,
            }

            try this.btree.source.seekTo(this.btree.offset + 8 + 8 + LEAF_CELL_SIZE * pos);

            const reader = this.btree.source.reader();

            var cell: LeafCell = undefined;
            std.debug.assert((try reader.readAll(std.mem.asBytes(&cell))) == @sizeOf(LeafCell));

            const key = try allocator.alloc(u8, cell.key_len);
            errdefer allocator.free(key);

            try this.btree.source.seekTo(this.btree.offset + PAGE_SIZE - cell.key_ptr * 4);
            const key_bytes_read = try reader.readAll(key);
            if (key_bytes_read < key.len) {
                return error.UnexpectedEOF;
            }

            const value = try allocator.alloc(u8, cell.val_len);
            errdefer allocator.free(value);

            try this.btree.source.seekTo(this.btree.offset + PAGE_SIZE - cell.val_ptr * 4);
            const val_bytes_read = try reader.readAll(value);
            if (val_bytes_read < value.len) {
                return error.UnexpectedEOF;
            }

            return Entry{
                .allocator = allocator,
                .key = key,
                .val = value,
            };
        }
    };

    pub const Entry = struct {
        allocator: *std.mem.Allocator,
        key: []const u8,
        val: []const u8,

        pub fn deinit(this: @This()) void {
            this.allocator.free(this.key);
            this.allocator.free(this.val);
        }
    };

    pub const Limit = union(enum) {
        first,
        last,
    };

    pub const Direction = enum {
        forwards,
        backwards,
    };

    pub fn range(this: *@This(), start: Limit, end: Limit) !RangeIterator {
        std.debug.assert(!std.meta.eql(start, end));

        try this.source.seekTo(this.offset + 8 + 1);
        const cells_in_page = try this.source.reader().readByte();

        return RangeIterator{
            .btree = this,
            .start = start,
            .end = end,
            .pos = switch (start) {
                .first => 0,
                .last => cells_in_page,
            },
            .dir = switch (start) {
                .first => switch (end) {
                    .first => unreachable,
                    .last => .forwards,
                },
                .last => switch (end) {
                    .first => .backwards,
                    .last => unreachable,
                },
            },
            .cellsInPage = cells_in_page,
        };
    }
};

test "putting data and retriving it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try BTree.create(.{ .file = file }, .{});

        try library.put("Pride and Prejudice", "Jane Austen");
        try library.put("Worth the Candle", "Alexander Wales");
        try library.put("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
        try library.put("Mother of Learning", "nobody103");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = BTree.init(.{ .file = file }, .{});

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = &arena.allocator;

        try testing.expectEqualStrings("Jane Austen", (try library.get(a, "Pride and Prejudice")).?);
        try testing.expectEqualStrings("Alexander Wales", (try library.get(a, "Worth the Candle")).?);
        try testing.expectEqualStrings("香月美夜 (Miya Kazuki)", (try library.get(a, "Ascendance of a Bookworm")).?);
        try testing.expectEqualStrings("nobody103", (try library.get(a, "Mother of Learning")).?);

        try testing.expectEqual(@as(?[]u8, null), try library.get(a, "The Winds of Winter"));
        try testing.expectEqual(@as(?[]u8, null), try library.get(a, "Doors of Stone"));
    }
}

test "put a key a retrieve it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try BTree.create(.{ .file = file }, .{});

        try library.put("Worth the Candle", "Alexander Wales");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = BTree.init(.{ .file = file }, .{});

        const value = (try library.get(std.testing.allocator, "Worth the Candle")).?;
        defer std.testing.allocator.free(value);
        try testing.expectEqualStrings("Alexander Wales", value);

        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Wirth the Candle"));
        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Doors of Stone"));
    }
}

test "iterating in lexographic order" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const Book = struct {
        title: []const u8,
        author: []const u8,
    };

    const data = [_]Book{
        .{ .title = "Ascendance of a Bookworm", .author = "香月美夜 (Miya Kazuki)" },
        .{ .title = "Mother of Learning", .author = "nobody103" },
        .{ .title = "Pride and Prejudice", .author = "Jane Austen" },
        .{ .title = "Worth the Candle", .author = "Alexander Wales" },
    };
    var data_shuffled = data;

    var random = std.rand.DefaultPrng.init(1337);
    random.random.shuffle(Book, &data_shuffled);

    for (data_shuffled) |_, idx| {
        std.debug.assert(!std.meta.eql(data[idx], data_shuffled[idx]));
    }

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try BTree.create(.{ .file = file }, .{});

        for (data_shuffled) |book| {
            try library.put(book.title, book.author);
        }
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = BTree.init(.{ .file = file }, .{});

        var i: usize = 0;
        var iter = try library.range(.first, .last);
        while (try iter.nextEntry(std.testing.allocator)) |entry| {
            defer entry.deinit();
            defer i += 1;

            if (i < data.len) {
                try testing.expectEqualStrings(data[i].title, entry.key);
                try testing.expectEqualStrings(data[i].author, entry.val);
            } else {
                std.log.err("Didn't expect any more entries, found {}, {}", .{
                    std.zig.fmtEscapes(entry.key),
                    std.zig.fmtEscapes(entry.val),
                });
                return error.UnexpectedEntry;
            }
        }
    }
}
