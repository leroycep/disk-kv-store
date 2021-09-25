const std = @import("std");
const testing = std.testing;
const io = std.io;

const PAGE_SIZE = 2048;
const MAX_CELLS_LEN = 252;
const LEAF_HEADER_SIZE = 16;
const LEAF_CELL_SIZE = @sizeOf(LeafCell);
const INTERNAL_CELL_SIZE = @sizeOf(InternalCell);
const MAX_INTERNAL_CELLS_LEN = @divFloor(1024 - LEAF_HEADER_SIZE, INTERNAL_CELL_SIZE);
const MAX_FREE_LIST_LEN = @divFloor(2048 - LEAF_HEADER_SIZE, 4);

pub const FileBTree = struct {
    file: std.fs.File,

    pub fn init(file: std.fs.File) !@This() {
        return @This(){ .file = file };
    }

    pub fn create(file: std.fs.File) !@This() {
        var f = file;

        try file.seekTo(2 * PAGE_SIZE - 1);
        try file.writer().writeByte(0);

        try file.seekTo(1 * PAGE_SIZE);
        const free_list_header = FreeListHeader{ .pageNumber = 1, .nextPageNumber = 0, .count = 0 };
        try free_list_header.write(FileContext{ .file = &f });

        return @This(){ .file = f };
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

        pub fn allocNewPage(this: @This()) !u32 {
            var free_list_page: u32 = 1;
            var prev_free_list_page: u32 = free_list_page;

            try this.seekTo(free_list_page * PAGE_SIZE);
            var free_list_header = try FreeListHeader.read(this);

            while (free_list_header.nextPageNumber != 0) {
                prev_free_list_page = free_list_page;
                free_list_page = free_list_header.nextPageNumber;
                try this.seekTo(free_list_page * PAGE_SIZE);
                free_list_header = try FreeListHeader.read(this);
            }

            if (free_list_header.count == 0) {
                // Allocate from end of file
                const stat = try this.file.stat();
                const new_page = ((stat.size - 1) / PAGE_SIZE) + 1;
                try this.file.seekTo(new_page * PAGE_SIZE + PAGE_SIZE - 1);
                try this.file.writer().writeByte(0);
                return @intCast(u32, new_page);
            } else {
                // Return a node from the free list
                try this.seekTo(free_list_page * PAGE_SIZE + LEAF_HEADER_SIZE + 4 * (free_list_header.count - 1));
                const reused_page = try this.readIntLittle(u32);

                // Remove 1 from the count
                free_list_header.count -= 1;

                if (free_list_page != 1 and free_list_header.count == 0) {
                    // Remove this free list page from the free list linked list
                    try this.seekTo(prev_free_list_page * PAGE_SIZE + 4);
                    try this.writeIntLittle(u32, 0);
                } else {
                    // Update free page header
                    try this.seekTo(free_list_page * PAGE_SIZE);
                    try free_list_header.write(this);
                }

                return reused_page;
            }
        }

        pub fn freePage(this: @This(), pageNumber: u32) !void {
            var free_list_page: u32 = 1;

            try this.seekTo(free_list_page * PAGE_SIZE);
            var free_list_header = try FreeListHeader.read(this);

            while (free_list_header.count == MAX_CELLS_LEN and free_list_header.nextPageNumber != 0) {
                free_list_page = free_list_header.nextPageNumber;
                try this.seekTo(free_list_page * PAGE_SIZE);
                free_list_header = try FreeListHeader.read(this);
            }

            if (free_list_header.count == MAX_CELLS_LEN) {
                unreachable; // Handle when a free list fills up
            }

            if (std.builtin.mode == .Debug) {
                // Check if the page number is already in the free list
                var i: u64 = 0;
                while (i < free_list_header.count) : (i += 1) {
                    try this.seekTo(free_list_page * PAGE_SIZE + LEAF_HEADER_SIZE + 4 * i);
                    const freed_page = try this.readIntLittle(u32);
                    std.debug.assert(pageNumber != freed_page); // There is a duplicate page in the free list!
                }
            }

            try this.seekTo(free_list_page * PAGE_SIZE + LEAF_HEADER_SIZE + 4 * free_list_header.count);
            try this.writeIntLittle(u32, pageNumber);

            free_list_header.count += 1;

            try this.seekTo(free_list_page * PAGE_SIZE);
            try free_list_header.write(this);

            if (std.builtin.mode == .Debug) {
                try this.seekTo(pageNumber * PAGE_SIZE);
                const debug_buffer = [_]u8{0xAA} ** PAGE_SIZE;
                try this.writeAll(&debug_buffer);
            }
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

        pub fn allocNewPage(this: @This()) !u32 {
            _ = this;
            unreachable;
        }

        pub fn freePage(this: @This(), pageNumber: u32) !void {
            _ = this;
            _ = pageNumber;
            unreachable;
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

        var node_page: usize = 0;

        try context.seekTo(node_page * PAGE_SIZE);
        var page_header = try NodeHeader.read(context);

        // Keep traversing internal pages until we find a leaf node
        while (page_header.pageType == .internal) {
            // Find child node in internal node
            const find_res = try findInternalCellPos(context, page_header, key);

            const cell = if (find_res.cell) |c| c else read_prev_cell: {
                std.debug.assert(!find_res.isKey);
                std.debug.assert(find_res.index > 0);
                break :read_prev_cell try readInternalCell(context, page_header, find_res.index - 1);
            };

            // append child node page number to node_path
            node_page = cell.child_node;
            try context.seekTo(node_page * PAGE_SIZE);
            page_header = try NodeHeader.read(context);
        }

        const pos = try findLeafCellPos(context, page_header, key);
        if (pos.cell == null or !pos.isKey) return null;

        const value = try allocator.alloc(u8, pos.cell.?.val_len);
        errdefer allocator.free(value);
        try readData(context, page_header, pos.cell.?.val_ptr, value);

        return value;
    }

    fn put(context: anytype, key: []const u8, val: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        var node_path_buf: [255]u32 = undefined;
        node_path_buf[0] = 0;
        var node_path_len: usize = 1;
        var node_path = node_path_buf[0..node_path_len];

        try context.seekTo(node_path[node_path.len - 1] * PAGE_SIZE);
        var page_header = try NodeHeader.read(context);

        // TODO: Make this work with multiple levels of internal nodes
        var update_rightmost_value = false;

        while (page_header.pageType == .internal) {
            // Find child node in internal node
            const find_res = try findInternalCellPos(context, page_header, key);

            const cell = if (find_res.cell) |c| c else read_prev_cell: {
                std.debug.assert(!find_res.isKey);
                std.debug.assert(find_res.index > 0);
                update_rightmost_value = true;
                break :read_prev_cell try readInternalCell(context, page_header, find_res.index - 1);
            };

            // append child node page number to node_path
            if (node_path.len >= node_path_buf.len) return error.BTreeFull;
            node_path_buf[node_path.len] = cell.child_node;
            node_path_len += 1;
            node_path = node_path_buf[0..node_path_len];

            try context.seekTo(node_path[node_path.len - 1] * PAGE_SIZE);
            page_header = NodeHeader.read(context) catch |err| switch (err) {
                error.InvalidEnumTag => {
                    std.log.warn("node_path = {any}", .{node_path});
                    return err;
                },
                else => return err,
            };
        }

        putLeaf(context, &page_header, key, val) catch |err| switch (err) {
            error.NodeFull => {
                try splitNodeAndInsertKV(context, node_path, key, val);
                return;
            },
            else => |e| return e,
        };

        if (update_rightmost_value) {
            const child_node = page_header.pageNumber;

            try context.seekTo(node_path[node_path.len - 2] * PAGE_SIZE);
            page_header = try NodeHeader.read(context);

            try deleteInternalByChild(context, &page_header, child_node);
            try putInternal(context, &page_header, key, child_node);
        }
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

    const InternalFindResult = struct {
        index: u8,
        cell: ?InternalCell,
        isKey: bool, // Existing cell is the correct key
    };

    fn findInternalCellPos(context: anytype, header: NodeHeader, key: []const u8) !InternalFindResult {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .internal);

        var cell_idx: usize = 0;
        while (cell_idx < header.numberOfCells) : (cell_idx += 1) {
            const cell = try readInternalCell(context, header, @intCast(u8, cell_idx));

            // TODO: Handle keys that go into the overflow
            var bytes: [255]u8 = undefined;
            const cell_key = bytes[0..cell.key_len];
            try readData(context, header, cell.key_ptr, cell_key);

            switch (std.mem.order(u8, key, cell_key)) {
                .gt => continue,
                .eq => return InternalFindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = true },
                .lt => return InternalFindResult{ .index = @intCast(u8, cell_idx), .cell = cell, .isKey = false },
            }
        }
        return InternalFindResult{ .index = @intCast(u8, cell_idx), .cell = null, .isKey = false };
    }

    fn readInternalCell(context: anytype, header: NodeHeader, cell_index: u8) !InternalCell {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .internal);
        std.debug.assert(cell_index < MAX_CELLS_LEN);
        if (cell_index >= header.numberOfCells) return error.OutOfBounds; // The requested cell index is beyond the end of the cell array

        try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * @as(u64, cell_index));
        const cell = try InternalCell.read(context);

        return cell;
    }

    /// Does not read data longer than 0xFE bytes
    fn readData(context: anytype, header: NodeHeader, ptr: u8, buffer: []u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        if (!(header.endOfData >= ptr)) {
            std.log.warn("", .{});
            std.log.warn("header = {}", .{header});
            std.log.warn("ptr = {}", .{ptr});
        }
        std.debug.assert(header.endOfData >= ptr);
        std.debug.assert(buffer.len < 0xFF);

        try context.seekTo(header.pageNumber * PAGE_SIZE + PAGE_SIZE - @as(u64, ptr) * 4);
        try context.readNoEof(buffer);
    }

    fn putLeaf(context: anytype, header: *NodeHeader, key: []const u8, val: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
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

        const find_res = try findLeafCellPos(context, header.*, key);

        const key_ptr = calc_key_ptr: {
            if (find_res.isKey) {
                break :calc_key_ptr find_res.cell.?.key_ptr;
            } else {
                header.endOfData += @intCast(u8, ((key.len - 1) / 4) + 1);
                break :calc_key_ptr header.endOfData;
            }
        };
        const val_ptr = header.endOfData + ((val.len - 1) / 4) + 1;

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

        try writeLeafCell(context, header.*, find_res.index, .{
            .key_ptr = @intCast(u8, key_ptr),
            .key_len = @intCast(u8, key.len),
            .val_ptr = @intCast(u8, val_ptr),
            .val_len = @intCast(u8, val.len),
        });
        try writeData(context, header.*, @intCast(u8, val_ptr), val);
        if (!find_res.isKey) {
            try writeData(context, header.*, @intCast(u8, key_ptr), key);
        }
    }

    fn putInternal(context: anytype, header: *NodeHeader, key: []const u8, childNode: u32) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .internal);
        std.debug.assert(key.len > 0); // key length must be greater than zero

        if (key.len > 0xFF) unreachable; // TODO: Remove this restriction by placing data in an overflow page

        if (header.numberOfCells >= MAX_CELLS_LEN) {
            return error.NodeFull;
        }

        const key_align_len: usize = (key.len - 1) / 4 + 1;
        const new_end_of_data: usize = header.endOfData + std.math.min(key_align_len, 64);

        if (new_end_of_data >= 255) {
            return error.NodeFull;
        }

        const find_res = try findInternalCellPos(context, header.*, key);

        const key_ptr = calc_key_ptr: {
            if (find_res.isKey) {
                break :calc_key_ptr find_res.cell.?.key_ptr;
            } else {
                header.endOfData += @intCast(u8, ((key.len - 1) / 4) + 1);
                break :calc_key_ptr header.endOfData;
            }
        };

        header.numberOfCells += if (!find_res.isKey) @as(u8, 1) else 0;
        header.endOfData = @intCast(u8, key_ptr);
        try context.seekTo(header.pageNumber * PAGE_SIZE);
        try header.write(context);

        if (find_res.cell != null and !find_res.isKey) {
            // Move all cells after cell index
            const num_cells_to_move = header.numberOfCells - 1 - find_res.index;

            // Read cells
            var cells_buf: [255]InternalCell = undefined;
            try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * @as(u64, find_res.index));
            try context.readNoEof(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));

            // Write cells to their new location, 1 cell down
            try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * (@as(u64, find_res.index) + 1));
            try context.writeAll(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));
        }

        try writeInternalCell(context, header.*, find_res.index, .{
            .key_ptr = @intCast(u8, key_ptr),
            .key_len = @intCast(u8, key.len),
            .child_node = childNode,
        });
        if (!find_res.isKey) {
            try writeData(context, header.*, @intCast(u8, key_ptr), key);
        }
    }

    fn deleteInternalByChild(context: anytype, header: *NodeHeader, childNode: u32) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .internal);

        var cell_idx: usize = 0;
        while (cell_idx < header.numberOfCells) : (cell_idx += 1) {
            const cell = try readInternalCell(context, header.*, @intCast(u8, cell_idx));
            if (cell.child_node == childNode) {
                if (cell_idx != header.numberOfCells - 1) {
                    // cell is in the middle of the list, we need to move all cells one to left
                    // Move all cells after cell index
                    const num_cells_to_move = header.numberOfCells - 1 - cell_idx;

                    // Read cells
                    var cells_buf: [255]InternalCell = undefined;
                    try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * (@as(u64, cell_idx) + 1));
                    try context.readNoEof(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));

                    // Write cells to their new location, 1 cell down
                    try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * @as(u64, cell_idx));
                    try context.writeAll(std.mem.sliceAsBytes(cells_buf[0..num_cells_to_move]));
                }

                header.numberOfCells -= 1;

                // Key ptr is at the end of the data, we decrement the endOfData by the aligned key len
                if (cell.key_ptr == header.endOfData) {
                    header.endOfData -= (cell.key_len - 1) / 4 + 1;
                }

                try context.seekTo(header.pageNumber * PAGE_SIZE);
                try header.write(context);

                return;
            }
        }

        return error.NotFound;
    }

    fn writeLeafCell(context: anytype, header: NodeHeader, cell_index: u8, cell: LeafCell) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .leaf);
        std.debug.assert(cell_index < MAX_CELLS_LEN);

        try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + LEAF_CELL_SIZE * @as(u64, cell_index));

        try context.writeAll(std.mem.asBytes(&cell));
    }

    fn writeInternalCell(context: anytype, header: NodeHeader, cell_index: u8, cell: InternalCell) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(header.pageType == .internal);
        std.debug.assert(cell_index < MAX_CELLS_LEN);

        try context.seekTo(header.pageNumber * PAGE_SIZE + LEAF_HEADER_SIZE + INTERNAL_CELL_SIZE * @as(u64, cell_index));

        try context.writeAll(std.mem.asBytes(&cell));
    }

    fn writeData(context: anytype, header: NodeHeader, ptr: u8, buffer: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(buffer.len < 0xFF);

        try context.seekTo(header.pageNumber * PAGE_SIZE + PAGE_SIZE - @as(u64, ptr) * 4);
        try context.writeAll(buffer);
    }

    fn splitNodeAndInsertKV(context: anytype, path: []const u32, new_key: []const u8, new_val: []const u8) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;
        std.debug.assert(path.len > 0);

        try context.seekTo(path[path.len - 1] * PAGE_SIZE);
        var header = try NodeHeader.read(context);

        const midpoint = header.numberOfCells / 2;

        switch (header.pageType) {
            .internal => unreachable,
            .leaf => {
                var new_leaf_left = NodeHeader{
                    .pageNumber = try context.allocNewPage(),
                    .overflowPageNumber = 0,
                    .pageType = .leaf,
                    .endOfData = 0,
                    .numberOfCells = 0,
                };
                try context.seekTo(new_leaf_left.pageNumber * PAGE_SIZE);
                try new_leaf_left.write(context);

                var new_leaf_right = NodeHeader{
                    .pageNumber = try context.allocNewPage(),
                    .overflowPageNumber = 0,
                    .pageType = .leaf,
                    .endOfData = 0,
                    .numberOfCells = 0,
                };
                try context.seekTo(new_leaf_right.pageNumber * PAGE_SIZE);
                try new_leaf_right.write(context);

                var left_key_buf: [256]u8 = undefined;
                var left_key: ?[]u8 = null;
                var right_key_buf: [256]u8 = undefined;
                var right_key: ?[]u8 = null;

                // Copy left keys
                var i: usize = 0;
                while (i < midpoint) : (i += 1) {
                    const cell = try readLeafCell(context, header, @intCast(u8, i));

                    std.debug.assert(cell.key_len < 0xFF);
                    std.debug.assert(cell.val_len < 0xFF);

                    // read key
                    left_key = left_key_buf[0..cell.key_len];
                    try readData(context, header, cell.key_ptr, left_key.?);

                    // read val
                    var val_buf: [256]u8 = undefined;
                    const val = val_buf[0..cell.val_len];
                    try readData(context, header, cell.val_ptr, val);

                    // put in left cell
                    try putLeaf(context, &new_leaf_left, left_key.?, val);
                }

                // Copy right keys
                while (i < header.numberOfCells) : (i += 1) {
                    const cell = try readLeafCell(context, header, @intCast(u8, i));

                    std.debug.assert(cell.key_len < 0xFF);
                    std.debug.assert(cell.val_len < 0xFF);

                    // read key
                    right_key = right_key_buf[0..cell.key_len];
                    try readData(context, header, cell.key_ptr, right_key.?);

                    // read val
                    var val_buf: [256]u8 = undefined;
                    const val = val_buf[0..cell.val_len];
                    try readData(context, header, cell.val_ptr, val);

                    // put in right cell
                    try putLeaf(context, &new_leaf_right, right_key.?, val);
                }

                if (std.mem.order(u8, new_key, left_key.?) != .gt) {
                    try putLeaf(context, &new_leaf_left, new_key, new_val);
                } else if (std.mem.order(u8, new_key, right_key.?) != .gt) {
                    try putLeaf(context, &new_leaf_right, new_key, new_val);
                } else {
                    try putLeaf(context, &new_leaf_right, new_key, new_val);
                    right_key = right_key_buf[0..new_key.len];
                    std.mem.copy(u8, right_key.?, new_key);
                }

                if (header.pageNumber == 0) {
                    header.overflowPageNumber = 0;
                    header.pageType = .internal;
                    header.endOfData = 0;
                    header.numberOfCells = 0;

                    try putInternal(context, &header, left_key.?, new_leaf_left.pageNumber);
                    try putInternal(context, &header, right_key.?, new_leaf_right.pageNumber);
                } else {
                    const parent_page = path[path.len - 2];
                    try context.seekTo(parent_page * PAGE_SIZE);
                    var parent_header = try NodeHeader.read(context);

                    if (parent_header.numberOfCells >= MAX_CELLS_LEN) {
                        return error.BTreeFull;
                    }

                    try deleteInternalByChild(context, &parent_header, header.pageNumber);
                    try putInternal(context, &parent_header, left_key.?, new_leaf_left.pageNumber);
                    try putInternal(context, &parent_header, right_key.?, new_leaf_right.pageNumber);

                    try context.freePage(header.pageNumber);
                }
            },
        }
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

pub const FreeListHeader = struct {
    pageNumber: u32,
    nextPageNumber: u32,
    count: u16,

    pub fn read(context: anytype) !@This() {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        const page_number = try context.readIntLittle(u32);
        const next_page_number = try context.readIntLittle(u32);
        const count = try context.readIntLittle(u16);

        return @This(){
            .pageNumber = page_number,
            .nextPageNumber = next_page_number,
            .count = count,
        };
    }

    pub fn write(this: @This(), context: anytype) !void {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        try context.writeIntLittle(u32, this.pageNumber);
        try context.writeIntLittle(u32, this.nextPageNumber);
        try context.writeIntLittle(u16, this.count);
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

const InternalCell = packed struct {
    key_ptr: u8,
    key_len: u8,
    child_node: u32,

    pub fn read(context: anytype) !@This() {
        comptime if (!isContext(@TypeOf(context))) unreachable;

        var cell: @This() = undefined;
        try context.readNoEof(std.mem.asBytes(&cell));

        return cell;
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

test "put a key, overwrite it, and retrieve it" {
    var tmp = testing.tmpDir(.{});
    //defer tmp.cleanup();

    {
        var file = try tmp.dir.createFile("fridge.btree", .{ .read = true });
        defer file.close();

        var fridge = try FileBTree.create(file);

        try fridge.put("Eggs", "6");
        try fridge.put("Eggs", "3");
    }

    {
        var file = try tmp.dir.openFile("fridge.btree", .{ .read = true, .write = true });
        defer file.close();

        var fridge = try FileBTree.init(file);

        const value = (try fridge.get(std.testing.allocator, "Eggs")).?;
        defer std.testing.allocator.free(value);
        try testing.expectEqualStrings("3", value);
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

        var library = try FileBTree.create(.{ .file = file }, .{});

        for (data_shuffled) |book| {
            try library.put(book.title, book.author);
        }
    }

    {
        var file = try tmp.dir.openFile("library.btree", .{ .read = true, .write = true });
        defer file.close();

        var library = FileBTree.init(.{ .file = file }, .{});

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

    var file = try tmp.dir.createFile("many_keys.btree", .{ .read = true });
    defer file.close();

    var library = try FileBTree.create(file);

    {
        var i: usize = 0;
        while (i < 10000) : (i += 1) {
            var key_buf: [20]u8 = undefined;
            var val_buf: [20]u8 = undefined;

            const key = try std.fmt.bufPrint(&key_buf, "{x}", .{i});
            const val = try std.fmt.bufPrint(&val_buf, "{}", .{i});

            try library.put(key, val);
        }
    }

    {
        var i: usize = 0;
        while (i < 10000) : (i += 1) {
            var key_buf: [20]u8 = undefined;
            var val_buf: [20]u8 = undefined;

            const key = try std.fmt.bufPrint(&key_buf, "{x}", .{i});
            const val = try std.fmt.bufPrint(&val_buf, "{}", .{i});

            const val_in_btree = try library.get(std.testing.allocator, key);

            try std.testing.expect(val_in_btree != null);
            defer std.testing.allocator.free(val_in_btree.?);

            try std.testing.expectEqualStrings(val, val_in_btree.?);
        }
    }
}
