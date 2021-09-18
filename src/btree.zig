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
        try writer.writeByte(@enumToInt(NodeTag.leaf));
        try writer.writeIntLittle(u8, 0);
        try writer.writeIntLittle(u8, 0);

        return @This(){
            .source = btree_source,
            .offset = options.offset,
            .order = options.order,
        };
    }

    pub const NodeTag = enum {
        internal,
        leaf,
    };

    pub fn deinit(this: *@This()) void {
        this.valBuf.deinit();
    }

    pub fn insert(this: *@This(), key: []const u8, val: []const u8) !void {
        std.debug.assert(key.len > 0); // key length must be greater than zero

        std.debug.assert(key.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page
        std.debug.assert(val.len < 0xFE); // TODO: Remove this restriction by placing data in an overflow page

        const writer = this.source.writer();
        const reader = this.source.reader();
        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type

        const current_number_of_keys = try reader.readByte();
        const current_kv_offset: u16 = try reader.readByte();

        if (current_number_of_keys >= 169) {
            return error.OutOfSpace;
        }
        if (current_kv_offset >= 255) {
            return error.OutOfSpace;
        }

        const cell_offset = this.offset + 8 + 8 + 6 * current_number_of_keys;
        const key_offset = current_kv_offset + ((key.len - 1) / 4) + 1;
        const val_offset = key_offset + ((val.len - 1) / 4) + 1;

        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type
        try writer.writeByte(current_number_of_keys + 1);
        try writer.writeByte(@intCast(u8, val_offset));

        if (key_offset > 255 or val_offset > 255) return error.OutOfSpace;

        try this.source.seekTo(cell_offset);
        try writer.writeIntLittle(u8, @intCast(u8, key_offset));
        try writer.writeIntLittle(u8, @intCast(u8, key.len));
        try writer.writeIntLittle(u16, @intCast(u16, val_offset));
        try writer.writeIntLittle(u16, @intCast(u16, val.len));

        try this.source.seekTo(this.offset + PAGE_SIZE - ((key_offset) * 4));
        try writer.writeAll(key);

        try this.source.seekTo(this.offset + PAGE_SIZE - ((val_offset) * 4));
        try writer.writeAll(val);
    }

    pub fn get(this: *@This(), allocator: *std.mem.Allocator, key: []const u8) !?[]u8 {
        std.debug.assert(key.len > 0);

        const reader = this.source.reader();

        try this.source.seekTo(this.offset + 4 + 4 + 1); // Offset, page num, overflow page ptr, page type
        const number_of_cells = try reader.readByte();

        var cell_idx: usize = 0;
        while (cell_idx < number_of_cells) : (cell_idx += 1) {
            try this.source.seekTo(this.offset + 8 + 8 + 6 * cell_idx);

            const key_offset = try reader.readIntLittle(u8);
            const key_len = try reader.readIntLittle(u8);
            const val_offset = try reader.readIntLittle(u16);
            const val_len = try reader.readIntLittle(u16);

            if (key_len != key.len) {
                continue;
            }

            std.debug.assert(key_len != 0xFF);

            try this.source.seekTo(this.offset + PAGE_SIZE - (key_offset) * 4);

            var bytes: [256]u8 = undefined;
            const bytes_read = try reader.readAll(bytes[0..key_len]);
            if (bytes_read < bytes.len and bytes_read < key_len) {
                return error.UnexpectedEOF;
            }

            if (!std.mem.eql(u8, key, bytes[0..bytes_read])) {
                continue;
            }

            const value = try allocator.alloc(u8, val_len);
            errdefer allocator.free(value);

            try this.source.seekTo(this.offset + PAGE_SIZE - (val_offset) * 4);
            const val_bytes_read = try reader.readAll(value);
            if (val_bytes_read < value.len) {
                return error.UnexpectedEOF;
            }

            return value;
        }

        return null;
    }
};

test "inserting data and retriving it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try BTree.create(.{ .file = file }, .{});

        try library.insert("Pride and Prejudice", "Jane Austen");
        try library.insert("Worth the Candle", "Alexander Wales");
        try library.insert("Ascendance of a Bookworm", "香月美夜 (Miya Kazuki)");
        try library.insert("Mother of Learning", "nobody103");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = BTree.init(.{ .file = file }, .{});

        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = &arena.allocator;

        try testing.expectEqualSlices(u8, "Jane Austen", (try library.get(a, "Pride and Prejudice")).?);
        try testing.expectEqualSlices(u8, "Alexander Wales", (try library.get(a, "Worth the Candle")).?);
        try testing.expectEqualSlices(u8, "香月美夜 (Miya Kazuki)", (try library.get(a, "Ascendance of a Bookworm")).?);
        try testing.expectEqualSlices(u8, "nobody103", (try library.get(a, "Mother of Learning")).?);

        try testing.expectEqual(@as(?[]u8, null), try library.get(a, "The Winds of Winter"));
        try testing.expectEqual(@as(?[]u8, null), try library.get(a, "Doors of Stone"));
    }
}

test "insert a key a retrieve it" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("library.btree", .{ .read = true });
        defer file.close();

        var library = try BTree.create(.{ .file = file }, .{});

        try library.insert("Worth the Candle", "Alexander Wales");
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = BTree.init(.{ .file = file }, .{});

        const value = (try library.get(std.testing.allocator, "Worth the Candle")).?;
        defer std.testing.allocator.free(value);
        try testing.expectEqualSlices(u8, "Alexander Wales", value);

        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Wirth the Candle"));
        try testing.expectEqual(@as(?[]u8, null), try library.get(std.testing.allocator, "Doors of Stone"));
    }
}
