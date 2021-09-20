const std = @import("std");
const testing = std.testing;
const io = std.io;

const PAGE_SIZE = 2048;
const MAX_CELLS_LEN = 252;
const LEAF_HEADER_SIZE = 16;
const LEAF_CELL_SIZE = @sizeOf(LeafCell);

pub const FileBTree = struct {
    file: std.fs.File,

    pub fn init(file: std.fs.File) !@This() {
        return @This(){ .file = file };
    }

    pub fn create(file: std.fs.File) !@This() {
        try file.seekTo(PAGE_SIZE - 1);
        try file.writer().writeByte(0);

        return @This(){ .file = file };
    }

    pub fn get(this: *@This(), allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        return try raw.get(FileContext{ .file = &this.file }, allocator, key);
    }

    pub fn put(this: *@This(), key: []const u8, val: []const u8) !void {
        return try raw.put(FileContext{ .file = &this.file }, key, val);
    }

    const FileContext = struct {
        file: *std.fs.File,

        pub fn seekTo(this: @This(), pos: u64) !void {
            try this.file.seekTo(pos);
        }

        pub fn readIntLittle(this: @This(), comptime T: type) !T {
            return try this.file.reader().readIntLittle(T);
        }

        pub fn readNoEof(this: @This(), buffer: []u8) !void {
            return try this.file.reader().readNoEof(buffer);
        }

        pub fn writeIntLittle(this: @This(), comptime T: type, val: T) !void {
            return try this.file.writer().writeIntLittle(T, val);
        }

        pub fn writeAll(this: @This(), buffer: []const u8) !void {
            return try this.file.writer().writeAll(buffer);
        }
    };
};

pub const MemoryBTree = struct {
    memory: std.ArrayList(u8),
    cursor: u64,

    pub fn init(allocator: *std.mem.Allocator) !@This() {
        var mem = std.ArrayList(u8).init(allocator);
        errdefer mem.deinit();

        try mem.appendNTimes(0, PAGE_SIZE);

        return @This(){
            .memory = mem,
            .cursor = 0,
        };
    }

    pub fn deinit(this: *@This()) void {
        this.memory.deinit();
    }

    pub fn get(this: *@This(), allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        return try raw.get(MemContext{ .btree = this }, allocator, key);
    }

    pub fn put(this: *@This(), key: []const u8, val: []const u8) !void {
        return try raw.put(MemContext{ .btree = this }, key, val);
    }

    const MemContext = struct {
        btree: *MemoryBTree,

        pub fn seekTo(this: @This(), pos: u64) !void {
            std.debug.assert(pos < PAGE_SIZE);
            this.btree.cursor = pos;
        }

        pub fn readIntLittle(this: @This(), comptime T: type) !T {
            const size = @sizeOf(T);
            const bytes = this.btree.memory.items[this.btree.cursor..][0..size];
            this.btree.cursor += size;
            return std.mem.readIntLittle(T, bytes);
        }

        pub fn readNoEof(this: @This(), buffer: []u8) !void {
            const bytes = this.btree.memory.items[this.btree.cursor..];
            if (bytes.len < buffer.len) {
                return error.UnexpectedEOF;
            }
            std.mem.copy(u8, buffer, bytes[0..buffer.len]);
            this.btree.cursor += std.math.min(bytes.len, buffer.len);
        }

        pub fn writeIntLittle(this: @This(), comptime T: type, val: T) !void {
            if (this.btree.cursor > this.btree.memory.items.len) {
                try this.btree.memory.resize(this.btree.cursor);
            }
            if (this.btree.cursor < this.btree.memory.items.len) {
                const size = @sizeOf(T);
                const bytes = this.btree.memory.items[this.btree.cursor..][0..size];
                std.mem.writeIntLittle(T, bytes, val);
                this.btree.cursor += size;
            } else {
                try this.btree.memory.writer().writeIntLittle(T, val);
            }
        }

        pub fn writeAll(this: @This(), buffer: []const u8) !void {
            if (this.btree.cursor > this.btree.memory.items.len) {
                try this.btree.memory.resize(this.btree.cursor);
            }
            if (this.btree.cursor < this.btree.memory.items.len) {
                std.mem.copy(u8, this.btree.memory.items[this.btree.cursor..][0..buffer.len], buffer);
                this.btree.cursor += buffer.len;
            } else {
                try this.btree.memory.writer().writeAll(buffer);
            }
        }
    };
};

const isContext = std.meta.trait.multiTrait(.{
    std.meta.trait.hasFn("seekTo"),
    std.meta.trait.hasFn("readNoEof"),
    std.meta.trait.hasFn("readIntLittle"),
    std.meta.trait.hasFn("writeAll"),
    std.meta.trait.hasFn("writeIntLittle"),
});

const raw = struct {
    fn get(context: anytype, allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(key.len > 0);

        try context.seekTo(0);
        const page_header = try NodeHeader.read(context);

        const pos = try findLeafCellPos(context, page_header, key);
        if (pos.cell == null or !pos.isKey) return null;

        const value = try allocator.alloc(u8, pos.cell.?.val_len);
        errdefer allocator.free(value);
        try readData(context, page_header, pos.cell.?.val_ptr, value);

        return value;
    }

    fn put(context: anytype, key: []const u8, val: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        try context.seekTo(0);
        const page_header = try NodeHeader.read(context);

        try putLeaf(context, page_header, key, val);
    }

    const LeafFindResult = struct {
        index: u8,
        cell: ?LeafCell,
        isKey: bool, // Existing cell is the correct key
    };

    fn findLeafCellPos(context: anytype, header: NodeHeader, key: []const u8) !LeafFindResult {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .leaf);

        var cell_idx: usize = 0;
        while (cell_idx < header.numberOfCells) : (cell_idx += 1) {
            const cell = try readLeafCell(context, header, @intCast(u8, cell_idx));

            // TODO: Handle keys that go into the overflow
            var bytes: [255]u8 = undefined;
            const cell_key = bytes[0..cell.key_len];
            try readData(context, header, cell.key_ptr, cell_key);

            switch (std.mem.order(u8, key, cell_key)) {
                .gt => continue,
                .eq => return LeafFindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = true },
                .lt => return LeafFindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = false },
            }
        }
        return LeafFindResult{ .index = @intCast(u8, cell_idx), .cell = null, .isKey = false };
    }

    fn readLeafCell(context: anytype, header: NodeHeader, cell_index: u8) !LeafCell {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .leaf);
        std.debug.assert(cell_index < MAX_CELLS_LEN);
        if (cell_index >= header.numberOfCells) return error.OutOfBounds; // The requested cell index is beyond the end of the cell array

        try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * @as(u64, cell_index));
        const cell = try LeafCell.read(context);

        return cell;
    }

    /// Does not read data longer than 0xFE bytes
    fn readData(context: anytype, header: NodeHeader, ptr: u8, buffer: []u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.endOfData >= ptr);
        std.debug.assert(buffer.len < 0xFF);

        try context.seekTo(header.pageNumber * PAGE_SIZE + PAGE_SIZE - @as(u64, ptr) * 4);
        try context.readNoEof(buffer);
    }

    fn putLeaf(context: anytype, oldHeader: NodeHeader, key: []const u8, val: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        var header = oldHeader;
        std.debug.assert(header.pageType == .leaf);
        std.debug.assert(key.len > 0); // key length must be greater than zero

        if (key.len > 0xFF) unreachable; // TODO: Remove this restriction by placing data in an overflow page

        if (header.numberOfCells >= MAX_CELLS_LEN) {
            return error.NodeFull;
        }

        const key_align_len: usize = (key.len - 1) / 4 + 1;
        const val_align_len: usize = (val.len - 1) / 4 + 1;
        const new_end_of_data: usize = header.endOfData + std.math.min(key_align_len, 64) + std.math.min(val_align_len, 64);

        if (new_end_of_data >= 255) {
            return error.NodeFull;
        }

        const key_ptr = header.endOfData + ((key.len - 1) / 4) + 1;
        const val_ptr = key_ptr + ((val.len - 1) / 4) + 1;

        const find_res = try findLeafCellPos(context, header, key);

        header.numberOfCells += if (!find_res.isKey) @as(u8, 1) else 0;
        header.endOfData = @intCast(u8, val_ptr);
        try context.seekTo(header.pageNumber * PAGE_SIZE);
        try header.write(context);

        if (find_res.cell != null and !find_res.isKey) {
            // Move all cells after cell index
            const num_cells_to_move = header.numberOfCells - 1 - find_res.index;

            // Read cells
            var cells_buf: [255]LeafCell = undefined;
            try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * @as(u64, find_res.index));
            try context.readNoEof(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));

            // Write cells to their new location, 1 cell down
            try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * (@as(u64, find_res.index) + 1));
            try context.writeAll(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));
        }

        try writeLeafCell(context, header, find_res.index, .{
            .key_ptr = @intCast(u8, key_ptr),
            .key_len = @intCast(u8, key.len),
            .val_ptr = @intCast(u8, val_ptr),
            .val_len = @intCast(u8, val.len),
        });
        try writeData(context, header, @intCast(u8, val_ptr), val);
        try writeData(context, header, @intCast(u8, key_ptr), key);
    }

    fn writeLeafCell(context: anytype, header: NodeHeader, cell_index: u8, cell: LeafCell) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .leaf);
        std.debug.assert(cell_index < MAX_CELLS_LEN);

        try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * @as(u64, cell_index));

        try context.writeAll(std.mem.asBytes(&cell));
    }

    fn writeData(context: anytype, header: NodeHeader, ptr: u8, buffer: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(buffer.len < 0xFF);

        try context.seekTo(header.pageNumber * PAGE_SIZE + PAGE_SIZE - @as(u64, ptr) * 4);
        try context.writeAll(buffer);
    }
};

pub const NodeType = enum(u8) {
    leaf = 0,
    internal = 1,
};

pub const NodeHeader = struct {
    pageNumber: u32,
    overflowPageNumber: u32,
    pageType: NodeType,
    endOfData: u8,
    numberOfCells: u8,

    pub fn read(context: anytype) !@This() {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        const page_number = try context.readIntLittle(u32);
        const overflow_page_number = try context.readIntLittle(u32);
        const page_type_num = try context.readIntLittle(u8);
        const end_of_data = try context.readIntLittle(u8);
        const number_of_cells = try context.readIntLittle(u8);

        return NodeHeader{
            .pageNumber = page_number,
            .overflowPageNumber = overflow_page_number,
            .pageType = try std.meta.intToEnum(NodeType, page_type_num),
            .endOfData = end_of_data,
            .numberOfCells = number_of_cells,
        };
    }

    pub fn write(this: @This(), context: anytype) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        try context.writeIntLittle(u32, this.pageNumber);
        try context.writeIntLittle(u32, this.overflowPageNumber);
        try context.writeIntLittle(u8, @enumToInt(this.pageType));
        try context.writeIntLittle(u8, this.endOfData);
        try context.writeIntLittle(u8, this.numberOfCells);
    }
};

const LeafCell = packed struct {
    key_ptr: u8,
    key_len: u8,
    val_ptr: u8,
    val_len: u8,

    pub fn read(context: anytype) !LeafCell {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        var cell: LeafCell = undefined;
        try context.readNoEof(std.mem.asBytes(&cell));

        return cell;
    }
};

pub const LeafNode = struct {
    pageNumber: u32,
    overflowPageNumber: u32,
    endOfData: u8,
    numberOfCells: u8,

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

        var library = try FileBTree.create(file);

        try library.put("Pride and Prejudice", "Jane Austen");
        try library.put("Worth the Candle", "Alexander Wales");
        try library.put("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
        try library.put("Mother of Learning", "nobody103");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = try FileBTree.init(file);

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

test "putting data and retriving it in memory" {
    var library = try MemoryBTree.init(std.testing.allocator);
    defer library.deinit();

    try library.put("Pride and Prejudice", "Jane Austen");
    try library.put("Worth the Candle", "Alexander Wales");
    try library.put("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
    try library.put("Mother of Learning", "nobody103");

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

test "put a key and retrieve it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try FileBTree.create(file);

        try library.put("Worth the Candle", "Alexander Wales");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = try FileBTree.init(file);

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
    if (true) return;

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
