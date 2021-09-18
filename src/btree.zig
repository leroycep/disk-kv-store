const std = @import("std");
const testing = std.testing;
const io = std.io;

const PAGE_SIZE = 2048;

pub const LeafNode = struct {
    pageNumber: u32,
    overflowPageNumber: u32,
    endOfData: u8,
    numberOfCells: u8,

    pub fn readHeader(reader: anytype) !@This() {
        const page_number = try reader.readIntLittle(u32);
        const overflow_page_number = try reader.readIntLittle(u32);
        const end_of_data = try reader.readIntLittle(u8);
        const number_of_cells = try reader.readIntLittle(u8);

        return @This(){
            .pageNumber = page_number,
            .overflowPageNumber = overflow_page_number,
            .endOfData = end_of_data,
            .numberOfCells = number_of_cells,
        };
    }

    pub fn writeHeader(this: @This(), writer: anytype) !void {
        try writer.writeIntLittle(u32, this.pageNumber);
        try writer.writeIntLittle(u32, this.overflowPageNumber);
        try writer.writeIntLittle(u8, this.endOfData);
        try writer.writeIntLittle(u8, this.numberOfCells);
    }

    const LeafCell = packed struct {
        key_ptr: u8,
        key_len: u8,
        val_ptr: u8,
        val_len: u8,
    };

    const LEAF_HEADER_SIZE = 16;
    const LEAF_CELL_SIZE = @sizeOf(LeafCell);

    pub fn put(this: @This(), key: []const u8, val: []const u8) !void {
        std.debug.assert(key.len > 0); // key length must be greater than zero

        std.debug.assert(key.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page
        std.debug.assert(val.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page

        const writer = this.source.writer();
        const reader = this.source.reader();
        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type

        const current_number_of_keys = try reader.readByte();
        const current_kv_offset: u16 = try reader.readByte();

        if (current_number_of_keys >= 169) {
            return error.NodeFull;
        }
        if (current_kv_offset + std.math.min((key.len - 1) / 4 + 1, 64) + std.math.min((val.len - 1) / 4 + 1, 64) >= 255) {
            return error.NodeFull;
        }

        const cell_to_write_to = try this.findCellPos(key);

        const cell_offset = this.offset + 8 + 8 + LEAF_CELL_SIZE * switch (cell_to_write_to) {
            .cellEmpty, .cellExists, .cellReplace => |o| @intCast(u16, o),
        };
        const key_offset = current_kv_offset + ((key.len - 1) / 4) + 1;
        const val_offset = key_offset + ((val.len - 1) / 4) + 1;

        if (key_offset > 255 or val_offset > 255) return error.NodeFull;

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
                var cells_buf: [255]LeafCell = undefined;
                try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * @as(u16, cell_idx));
                try reader.readNoEof(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));

                // Write cells to their new location, 1 cell down
                try this.source.seekTo(this.offset + 8 + 8 + LEAF_CELL_SIZE * (@as(u16, cell_idx) + 1));
                try writer.writeAll(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));
            },
        }

        try this.source.seekTo(cell_offset);
        try writer.writeAll(std.mem.asBytes(&cell));

        try this.source.seekTo(this.offset + PAGE_SIZE - ((key_offset) * 4));
        try writer.writeAll(key);

        try this.source.seekTo(this.offset + PAGE_SIZE - ((val_offset) * 4));
        try writer.writeAll(val);
    }

    const FindResult = struct {
        index: u8,
        cell: ?LeafCell,
        isKey: bool, // Existing cell is the correct key
    };

    fn findCellPos(this: @This(), reader: anytype, seeker: anytype, key: []const u8) !FindResult {
        var cell_idx: usize = 0;
        while (cell_idx < this.numberOfCells) : (cell_idx += 1) {
            const cell = this.readCell(reader, seeker, cell_idx);

            // TODO: Handle keys that go into the overflow
            var bytes: [255]u8 = undefined;
            const cell_key = bytes[0..cell.key_len];
            try this.readData(reader, seeker, cell.key_ptr, cell_key);

            switch (std.mem.order(u8, key, cell_key)) {
                .gt => continue,
                .eq => return FindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = true },
                .lt => return FindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = false },
            }
        }
        return FindResult{ .index = @intCast(u8, cell_idx), .cell = null, .isKey = false };
    }

    fn readCell(this: @This(), reader: anytype, seeker: anytype, cell_index: u8) !LeafCell {
        std.debug.assert(cell_index < 252);
        if (cell_index >= this.numberOfCells) return error.OutOfBounds; // The requested cell index is beyond the end of the cell array

        try seeker.seekTo(this.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * @as(u64, cell_index));

        var cell: LeafCell = undefined;
        try reader.readNoEof(std.mem.asBytes(&cell));

        return cell;
    }

    /// Does not read data longer than 0xFE bytes
    fn readData(this: @This(), reader: anytype, seeker: anytype, ptr: u8, buffer: []u8) !void {
        std.debug.assert(this.endOfData <= ptr);
        std.debug.assert(buffer.len < 0xFF);

        try seeker.seekTo(this.pageNumber * PAGE_SIZE + PAGE_SIZE - @as(u64, ptr) * 4);
        try reader.readNoEof(buffer);
    }

    pub fn get(this: *@This(), reader: anytype, seeker: anytype, allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        std.debug.assert(key.len > 0);

        const pos = try this.findCellPos(key);
        if (pos.cell == null or !pos.isKey) return null;

        const value = try allocator.alloc(u8, pos.cell.val_len);
        errdefer allocator.free(value);
        try this.readData(reader, seeker, pos.cell.val_ptr, value);

        return value;
    }

    pub const RangeIterator = struct {
        btree: *LeafNode,
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

        var library = try LeafNode.create(.{ .file = file }, .{});

        try library.put("Pride and Prejudice", "Jane Austen");
        try library.put("Worth the Candle", "Alexander Wales");
        try library.put("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
        try library.put("Mother of Learning", "nobody103");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = LeafNode.init(.{ .file = file }, .{});

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

        var library = try LeafNode.create(.{ .file = file }, .{});

        try library.put("Worth the Candle", "Alexander Wales");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = LeafNode.init(.{ .file = file }, .{});

        const value = (try library.get(std.testing.allocator, "Worth the Candle")).?;
        defer std.testing.allocator.free(value);
        try testing.expectEqualStrings("Alexander Wales", value);

        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Wirth the Candle"));
        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Doors of Stone"));
    }
}

test "iterating in lexographic order" {
    if (true) return;

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

        var library = try LeafNode.create(.{ .file = file }, .{});

        for (data_shuffled) |book| {
            try library.put(book.title, book.author);
        }
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = LeafNode.init(.{ .file = file }, .{});

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

test "storing 10000 keys" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("many_keys.btree", .{ .read = true });
        defer file.close();

        var library = try LeafNode.create(.{ .file = file }, .{});

        var i: usize = 0;
        while (i < 10000) : (i += 1) {
            var key_buf: [20]u8 = undefined;
            var val_buf: [20]u8 = undefined;

            const key = try std.fmt.bufPrint(&key_buf, "{x}", .{i});
            const val = try std.fmt.bufPrint(&val_buf, "{}", .{i});

            try library.put(key, val);
        }
    }
}
