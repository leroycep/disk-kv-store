const std = @import("std");

pub const PathTrie = struct {
    allocator: *std.mem.Allocator,
    labelEnds: []const usize,
    labels: []const u8,
    bitTree: []const u1,
    branches: []const u8,

    pub fn init(allocator: *std.mem.Allocator) @This() {
        return @This(){
            .allocator = allocator,
            .labelEnds = &[_]usize{},
            .labels = &[_]u8{},
            .bitTree = &[_]u1{},
            .branches = &[_]u8{},
        };
    }

    pub fn deinit(this: @This()) void {
        if (this.labels.len > 0) {
            this.allocator.free(this.labelEnds);
            this.allocator.free(this.labels);
            this.allocator.free(this.bitTree);
            this.allocator.free(this.branches);
        }
    }

    pub fn add(this: *@This(), word: []const u8) !void {
        var new_labels = std.ArrayList(u8).init(this.allocator);
        defer new_labels.deinit();

        if (this.labels.len == 0) {
            for (word) |byte| {
                try new_labels.append(byte);
                if (byte & 0b1000_0000 != 0) {
                    try new_labels.append(0b1);
                }
            }

            const new_label_ends = try this.allocator.alloc(usize, 1);
            errdefer this.allocator.free(new_label_ends);
            new_label_ends[0] = new_labels.items.len;

            const new_branches = try this.allocator.alloc(u8, 0);
            errdefer this.allocator.free(new_branches);

            this.labels = new_labels.toOwnedSlice();
            this.labelEnds = new_label_ends;
            this.branches = new_branches;
        } else {
            const Op = enum {
                none,
                replace,
                new_branch,
            };

            var label_idx: usize = 0;
            var word_idx: usize = 0;
            var operation = Op.none;

            for (word) |c, idx| {
                word_idx = idx;

                var label_byte = this.labels[label_idx];
                label_idx += 1;

                const is_another_byte = label_byte & 0b1000_0000 != 0;
                label_byte &= 0b0111_111;

                if (is_another_byte) {
                    std.debug.assert(this.labels[label_idx] == 0b1); // TODO: read other command bytes
                    label_byte |= 0b1000_0000;
                    label_idx += 1;
                }

                if (c < label_byte) {
                    // Replace current label with this
                    operation = .replace;
                    break;
                } else if (c > label_byte) {
                    // Add new label as branch off of this one
                    operation = .new_branch;
                    break;
                }
            }

            switch (operation) {
                .none => {},
                .replace => return error.Unimplemented,
                .new_branch => {},
            }
        }
    }

    fn is_literal(int: u16) bool {
        return int & 0x100 == 0;
    }

    pub fn get(this: @This(), word: []const u8) bool {
        if (this.labels.len == 0) return false;

        var fixed_buffer_stream = std.io.fixedBufferStream(this.labels);

        var label_idx: usize = 0;
        var accumulator: usize = 0;

        var limited_reader = std.io.limitedReader(fixed_buffer_stream.reader(), this.labelEnds[label_idx]);

        for (word) |c| {
            const accumulator_prev = accumulator;
            const reader = limited_reader.reader();

            var label_int: u16 = vbyte.readInt(reader, u16) catch return false;
            var prev_character_is_special = false;
            var special_int: u16 = undefined;

            while (!is_literal(label_int)) : (label_int = vbyte.readInt(reader, u16) catch unreachable) {
                accumulator += label_int & 0xFF;
                std.debug.assert(!prev_character_is_special);
                prev_character_is_special = true;
                special_int = label_int;
            }

            if (c < label_int) {
                return false;
            } else if (c > label_int) {
                if (!prev_character_is_special) return false;

                const branch_idx = std.mem.indexOfScalarPos(u8, this.branches[0..accumulator], accumulator_prev, c) orelse return false;

                // Find where the next label begins
                const opening1 = bit_tree.select1(this.bitTree, branch_idx + 2);
                const closing0 = bit_tree.findClose0(this.bitTree, opening1);
                label_idx = bit_tree.rank0(this.bitTree[0 .. closing0 + 1]);

                // Update accumulator
                const accum_opening1 = bit_tree.select1(this.bitTree, accumulator + 1);
                accumulator += bit_tree.rank1(this.bitTree[accum_opening1 + 1 .. closing0]);

                const start = this.labelEnds[label_idx - 1];
                const end = this.labelEnds[label_idx];
                fixed_buffer_stream.seekTo(start) catch unreachable;
                limited_reader = std.io.limitedReader(fixed_buffer_stream.reader(), end - start);
            }
        }

        const label_pos = fixed_buffer_stream.getPos() catch unreachable;
        return label_pos == this.labelEnds[label_idx];
    }
};

pub const vbyte = struct {
    pub fn readInt(reader: anytype, comptime T: type) !T {
        var byte = try reader.readByte();
        var int: T = (byte & 0x7F);

        while (byte & 0x80 == 0x80) {
            byte = try reader.readByte();

            int <<= 7;
            int |= (byte & 0x7F);
        }

        return int;
    }
};

pub const bit_tree = struct {
    pub fn encodeComptime(comptime S: []const u8) [S.len]u1 {
        var encoded_bits: [S.len]u1 = undefined;
        for (S) |c, i| {
            encoded_bits[i] = switch (c) {
                '(' => 1,
                ')' => 0,
                else => unreachable, // unexpected character in bit tree string
            };
        }
        return encoded_bits;
    }

    pub fn rank0(tree: []const u1) usize {
        var count: usize = 0;
        for (tree) |c| {
            switch (c) {
                0 => count += 1,
                1 => {},
            }
        }
        return count;
    }

    pub fn rank1(tree: []const u1) usize {
        var count: usize = 0;
        for (tree) |c| {
            switch (c) {
                0 => {},
                1 => count += 1,
            }
        }
        return count;
    }

    pub fn select1(tree: []const u1, ith: usize) usize {
        std.debug.assert(ith > 0);

        var count: usize = 0;
        for (tree) |c, i| {
            count += c;
            if (count == ith) return i;
        }
        unreachable;
    }

    pub fn findClose0(tree: []const u1, opening: usize) usize {
        std.debug.assert(tree[opening] == 1);
        var accum: usize = 0;
        for (tree[opening..]) |c, i| {
            switch (c) {
                0 => accum -= 1,
                1 => accum += 1,
            }
            if (accum == 0) {
                return opening + i;
            }
        }
        unreachable; // if the Bit Tree is a list of balanced parentheses, this is impossible
    }
};

test "search for string" {
    const trie = PathTrie{
        .allocator = undefined,
        .labelEnds = &[_]usize{ 7, 14, 19, 22, 22, 26, 26 },
        .labels = "t\x82\x01hreei\x82\x02a\x82\x01lg\x82\x01lelarl\x82\x01e",
        .bitTree = &bit_tree.encodeComptime("(()((()()))())"),
        .branches = "rpenuy",
    };

    const WORDS = &[_][]const u8{
        "three",
        "trial",
        "triangle",
        "triangular",
        "trie",
        "triple",
        "triply",
    };

    for (WORDS) |word| {
        try std.testing.expect(trie.get(word));
    }

    try std.testing.expect(!trie.get("tr"));
    try std.testing.expect(!trie.get("trip"));
    try std.testing.expect(!trie.get("tree"));
    try std.testing.expect(!trie.get("rectangular"));
    try std.testing.expect(!trie.get("threeial"));
}

test "put and test for string" {
    var trie = PathTrie.init(std.testing.allocator);
    defer trie.deinit();

    const WORDS = &[_][]const u8{
        "three",
        "trial",
        "triangle",
        "triangular",
        "trie",
        "triple",
        "triply",
    };

    for (WORDS) |word| {
        try trie.add(word);
        std.log.warn("trie labels = \"{}\"", .{std.zig.fmtEscapes(trie.labels)});
    }

    for (WORDS) |word| {
        std.log.warn("testing word \"{}\"", .{std.zig.fmtEscapes(word)});
        try std.testing.expect(trie.get(word));
    }

    try std.testing.expect(!trie.get("tr"));
    try std.testing.expect(!trie.get("trip"));
    try std.testing.expect(!trie.get("tree"));
    try std.testing.expect(!trie.get("rectangular"));
}
