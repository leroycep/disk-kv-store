const std = @import("std");
const tracy = @import("tracy");
const eytzinger = @import("./eytzinger.zig");

pub fn Tree(comptime K: type, V: type) type {
    const MAX_PATH_LEN = 16;

    return struct {
        allocator: *std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        rng: std.rand.DefaultPrng,
        root: ?*Node,
        freeMemory: std.AutoHashMap(usize, std.ArrayList([*]align(@alignOf(*Node)) u8)),

        const ThisTree = @This();

        pub fn init(allocator: *std.mem.Allocator) @This() {
            return @This(){
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .rng = std.rand.DefaultPrng.init(std.crypto.random.int(u64)),
                .root = null,
                .freeMemory = std.AutoHashMap(usize, std.ArrayList([*]align(@alignOf(*Node)) u8)).init(allocator),
            };
        }

        pub fn deinit(this: *@This()) void {
            var iter = this.freeMemory.iterator();
            while (iter.next()) |slots_of_size| {
                slots_of_size.value_ptr.deinit();
            }
            this.freeMemory.deinit();
            this.arena.deinit();
        }

        fn allocateMem(this: *@This(), size: usize) ![]align(@alignOf(*Node)) u8 {
            attempt_reuse: {
                const free_slots_entry = this.freeMemory.getEntry(size) orelse break :attempt_reuse;
                const free_slots = free_slots_entry.value_ptr;
                const slot = free_slots.popOrNull() orelse break :attempt_reuse;
                return slot[0..size];
            }

            return try this.arena.allocator.allocAdvanced(u8, @alignOf(*Node), size, .exact);
        }

        fn freeMem(this: *@This(), slice: []align(@alignOf(*Node)) u8) void {
            const free_slots_entry = this.freeMemory.getOrPut(slice.len) catch return;
            if (!free_slots_entry.found_existing) {
                free_slots_entry.value_ptr.* = std.ArrayList([*]align(@alignOf(*Node)) u8).init(this.allocator);
            }
            const free_slots = free_slots_entry.value_ptr;
            free_slots.append(slice.ptr) catch return;
        }

        pub fn get(this: @This(), key: K) ?V {
            const t = tracy.trace(@src());
            defer t.end();

            if (this.root == null) return null;
            const path = this.pathToLocation(key) catch unreachable;
            const leaf = path.constSlice()[path.len - 1];
            const leaf_entries = leaf.node.asLeafEntryArray();

            const idx_is_in_leaf = leaf.idx < leaf_entries.len;
            if (idx_is_in_leaf and leaf_entries[leaf.idx].key == key) {
                return leaf_entries[leaf.idx].val;
            }

            return null;
        }

        pub fn put(this: *@This(), key: K, val: V) !bool {
            const t = tracy.trace(@src());
            defer t.end();

            if (this.root != null) {
                const path = try this.pathToLocation(key);

                const leaf = path.constSlice()[path.len - 1];
                const leaf_entries = leaf.node.asLeafEntryArray();
                if (leaf.idx < leaf_entries.len and leaf_entries[leaf.idx].key == key) {
                    const new_leaf = try leaf.node.dupe(this);
                    errdefer new_leaf.free(this.allocator);
                    const new_leaf_entries = new_leaf.asLeafEntryArray();
                    new_leaf_entries[leaf.idx] = .{
                        .key = key,
                        .val = val,
                    };
                    return true;
                }

                var new_nodes = try leaf.node.dupeInsertOrSplitLeaf(this, leaf.idx, .{ .key = key, .val = val });
                errdefer {
                    for (new_nodes.constSlice()) |new_node| {
                        new_node.freeRecursive(this);
                    }
                }

                // Update internal nodes
                const t1 = tracy.trace(@src());

                var path_idx = path.len - 1;
                while (path_idx > 0) : (path_idx -= 1) {
                    errdefer t1.end();

                    const path_segment = path.constSlice()[path_idx - 1];
                    const height = @intCast(u6, path.len - (path_idx - 1));
                    new_nodes = try path_segment.node.dupeInsertOrSplitInternal(this, height, path_segment.idx, new_nodes.constSlice());
                }

                t1.end();

                switch (new_nodes.len) {
                    1 => {
                        this.root = new_nodes.constSlice()[0];
                    },
                    2 => {
                        const new_internal = try Node.initLen(this, .internal, 2);

                        const internal_keys = new_internal.asInternalKeyArray();
                        const internal_nodes = new_internal.asInternalNodeArray();

                        internal_nodes[0] = new_nodes.constSlice()[0];
                        internal_nodes[1] = new_nodes.constSlice()[1];

                        internal_keys[eytzinger.fromLinear(0, 2)] = internal_nodes[0].min();
                        internal_keys[eytzinger.fromLinear(1, 2)] = internal_nodes[1].min();

                        this.root = new_internal;
                    },
                    else => unreachable,
                }

                // Free outdated path segments
                {
                    const t2 = tracy.trace(@src());
                    defer t2.end();

                    for (path.constSlice()) |path_segment| {
                        path_segment.node.free(this);
                    }
                }
                return false;
            } else {
                this.root = try Node.initLen(this, .leaf, 1);
                this.root.?.asLeafEntryArray()[0] = .{
                    .key = key,
                    .val = val,
                };
                return false;
            }
        }

        fn pathToLocation(this: @This(), key: K) !std.BoundedArray(PathSegment, MAX_PATH_LEN) {
            const t = tracy.trace(@src());
            defer t.end();

            std.debug.assert(this.root != null);

            var path = std.BoundedArray(PathSegment, MAX_PATH_LEN).init(0) catch unreachable;
            path.append(.{ .node = this.root.?, .idx = undefined }) catch unreachable;
            while (true) {
                const current = &path.slice()[path.len - 1];

                if (current.node.nodeType == .leaf) break;

                const keys = current.node.constInternalKeyArray();
                var idx: u32 = 0;
                while (true) {
                    switch (std.math.order(keys[idx], key)) {
                        .eq => break,
                        .gt => {
                            const new_idx = eytzinger.left(idx);
                            if (new_idx >= keys.len) break;
                            idx = new_idx;
                        },
                        .lt => {
                            const new_idx = eytzinger.right(idx);
                            if (new_idx >= keys.len) break;
                            idx = new_idx;
                        },
                    }
                }

                current.idx = switch (std.math.order(keys[idx], key)) {
                    .eq, .lt => eytzinger.toLinear(idx, @intCast(u32, keys.len)),
                    .gt => eytzinger.toLinear(idx, @intCast(u32, keys.len)) -| 1,
                };
                try path.append(.{
                    .idx = undefined,
                    .node = current.node.constInternalNodeArray()[current.idx],
                });
            }

            const segment = &path.slice()[path.len - 1];
            std.debug.assert(segment.node.nodeType == .leaf);

            const entries = segment.node.asLeafEntryArray();

            segment.idx = 0;
            while (segment.idx < segment.node.len) : (segment.idx += 1) {
                if (entries[segment.idx].key >= key) {
                    return path;
                }
            }

            return path;
        }

        fn dumpTree(this: @This()) void {
            if (this.root == null) return;

            const stdout = std.io.getStdErr();
            stdout.writer().writeAll("\n\n") catch unreachable;
            this.root.?.dumpTree(stdout.writer()) catch unreachable;
        }

        pub fn countBytesUsed(this: *const @This()) usize {
            if (this.root) |root| {
                return root.countBytesUsed();
            }

            return 0;
        }

        pub fn countBytesInAllocationCache(this: *const @This()) usize {
            var bytes_used: usize = 0;

            var iter = this.freeMemory.iterator();
            while (iter.next()) |free_slots_entry| {
                const slot_size = free_slots_entry.key_ptr.*;
                const len = free_slots_entry.value_ptr.items.len;
                bytes_used += slot_size * len;
            }

            return bytes_used;
        }

        const Node = struct {
            nodeType: NodeType,
            len: usize,

            pub fn initLen(tree: *ThisTree, nodeType: NodeType, len: usize) !*@This() {
                const node_size = nodeSize(nodeType, len);

                const mem = try tree.allocateMem(node_size);

                const this = @ptrCast(*@This(), mem[0..@sizeOf(@This())]);
                this.* = .{
                    .nodeType = nodeType,
                    .len = len,
                };

                return this;
            }

            pub fn deinitRecursive(this: *@This(), allocator: *std.mem.Allocator) void {
                if (this.nodeType == .internal) {
                    for (this.constInternalEntryArray()) |child| {
                        child.node.deinitRecursive(allocator);
                    }
                }
                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]u8, this)[0..node_size];

                allocator.free(mem);
            }

            pub fn freeRecursive(this: *@This(), tree: *ThisTree) void {
                if (this.nodeType == .internal) {
                    for (this.constInternalNodeArray()) |child_node| {
                        child_node.freeRecursive(tree);
                    }
                }
                this.free(tree);
            }

            pub fn free(this: *@This(), tree: *ThisTree) void {
                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]u8, this)[0..node_size];

                tree.freeMem(mem);
            }

            pub fn dupe(this: *@This(), tree: *ThisTree) !*@This() {
                const new_this = try initLen(tree, this.nodeType, this.len);

                switch (this.nodeType) {
                    .internal => {
                        std.mem.copy(K, new_this.asInternalKeyArray(), this.asInternalKeyArray());
                        std.mem.copy(*@This(), new_this.asInternalNodeArray(), this.asInternalNodeArray());
                    },
                    .leaf => std.mem.copy(LeafEntry, new_this.asLeafEntryArray(), this.asLeafEntryArray()),
                }

                return new_this;
            }

            pub fn dupeInsertOrSplitLeaf(this: *@This(), tree: *ThisTree, idx: usize, newEntry: LeafEntry) !std.BoundedArray(*@This(), 2) {
                const t = tracy.trace(@src());
                defer t.end();

                std.debug.assert(this.nodeType == .leaf);

                var new_nodes = std.BoundedArray(*@This(), 2){ .buffer = undefined };
                errdefer {
                    for (new_nodes.slice()) |new_node| {
                        new_node.free(tree);
                    }
                }

                const entries = this.asLeafEntryArray();

                const print_debug = false;
                if (print_debug) std.debug.print("\n\n", .{});

                const height = 1;
                const max_size = @as(usize, 1) << height;
                if (this.len + 1 <= max_size) {
                    // Grow node by one
                    const new_this = try initLen(tree, this.nodeType, this.len + 1);
                    const new_entries = new_this.asLeafEntryArray();

                    std.mem.copy(LeafEntry, new_entries[0..idx], entries[0..idx]);
                    std.mem.copy(LeafEntry, new_entries[idx + 1 ..], entries[idx..]);
                    new_entries[idx] = newEntry;

                    if (print_debug) {
                        std.debug.print("\t|", .{});
                        for (new_entries[0..idx]) |_, i| {
                            std.debug.print("{}|", .{i});
                        }
                        std.debug.print("   |", .{});
                        for (new_entries[idx + 1 ..]) |_, i| {
                            std.debug.print("{}|", .{idx + 1 + i});
                        }

                        std.debug.print("\n\t|", .{});
                        for (entries[0..idx]) |_, i| {
                            std.debug.print("{}|", .{i});
                        }
                        std.debug.print("   |", .{});
                        for (entries[idx..]) |_, i| {
                            std.debug.print("{}|", .{idx + i});
                        }
                    }

                    new_nodes.append(new_this) catch unreachable;
                } else {
                    // Split node into two
                    const half_size = max_size / 2;

                    new_nodes.append(try initLen(tree, this.nodeType, half_size)) catch unreachable;
                    new_nodes.append(try initLen(tree, this.nodeType, this.len + 1 - half_size)) catch unreachable;

                    if (idx < half_size) {
                        const left_node = new_nodes.slice()[1];
                        const right_node = new_nodes.slice()[0];

                        const left_entries = left_node.asLeafEntryArray();
                        std.mem.copy(LeafEntry, left_entries[0..idx], entries[0..idx]);
                        std.mem.copy(LeafEntry, left_entries[idx + 1 ..], entries[idx .. left_entries.len - 1]);
                        left_entries[idx] = newEntry;

                        const right_entries = right_node.asLeafEntryArray();
                        std.mem.copy(LeafEntry, right_entries[0..], entries[left_entries.len - 1 ..]);

                        if (print_debug) {
                            std.debug.print("\t|", .{});
                            for (left_entries[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (left_entries[idx + 1 ..]) |_, i| {
                                std.debug.print("{}|", .{idx + 1 + i});
                            }
                            std.debug.print("   |", .{});
                            for (right_entries[0..]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }

                            std.debug.print("\n\t|", .{});
                            for (entries[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (entries[idx .. left_entries.len - 1]) |_, i| {
                                std.debug.print("{}|", .{idx + i});
                            }
                            std.debug.print("   |", .{});
                            for (entries[left_entries.len - 1 ..]) |_, i| {
                                std.debug.print("{}|", .{left_entries.len - 1 + i});
                            }
                        }

                        new_nodes.slice()[0] = left_node;
                        new_nodes.slice()[1] = right_node;
                    } else {
                        const left_node = new_nodes.slice()[0];
                        const right_node = new_nodes.slice()[1];

                        const left_entries = left_node.asLeafEntryArray();
                        std.mem.copy(LeafEntry, left_entries[0..], entries[0..left_entries.len]);

                        const right_entries = right_node.asLeafEntryArray();
                        std.mem.copy(LeafEntry, right_entries[0 .. idx - left_entries.len], entries[left_entries.len..idx]);
                        std.mem.copy(LeafEntry, right_entries[idx - left_entries.len + 1 ..], entries[idx..]);
                        right_entries[idx - left_entries.len] = newEntry;
                    }
                }
                if (print_debug) std.debug.print("\n\n", .{});

                return new_nodes;
            }

            pub fn dupeInsertOrSplitInternal(this: *@This(), tree: *ThisTree, height: u6, idx: usize, newEntries: []const *@This()) !std.BoundedArray(*@This(), 2) {
                const t = tracy.trace(@src());
                defer t.end();

                std.debug.assert(this.nodeType == .internal);

                var new_nodes = std.BoundedArray(*@This(), 2){ .buffer = undefined };
                errdefer {
                    for (new_nodes.slice()) |new_node| {
                        new_node.free(tree);
                    }
                }

                const keys = this.constInternalKeyArray();
                const nodes = this.constInternalNodeArray();

                const max_size = @as(usize, 1) << height;
                const new_len = this.len + newEntries.len - 1;
                if (new_len < max_size) {
                    // Grow node by one
                    const new_this = try initLen(tree, this.nodeType, new_len);
                    new_nodes.append(new_this) catch unreachable;

                    //const new_node_keys = new_this.asInternalKeyArray();
                    const new_node_nodes = new_this.asInternalNodeArray();

                    //std.mem.copy(K, new_node_keys[0..idx], keys[0..idx]);
                    std.mem.copy(*@This(), new_node_nodes[0..idx], nodes[0..idx]);

                    //std.mem.copy(K, new_node_keys[idx + newEntries.len - 1 ..], keys[idx..]);
                    std.mem.copy(*@This(), new_node_nodes[idx + newEntries.len - 1 ..], nodes[idx..]);

                    for (newEntries) |new_child, new_offset| {
                        //new_node_keys[idx + new_offset] = new_child.min();
                        new_node_nodes[idx + new_offset] = new_child;
                    }
                } else {
                    // Split node into two
                    const half_size = max_size / 2;

                    new_nodes.append(try initLen(tree, this.nodeType, half_size)) catch unreachable;
                    new_nodes.append(try initLen(tree, this.nodeType, this.len + 1 - half_size)) catch unreachable;

                    const print_debug = false;
                    if (print_debug) std.debug.print("\n\n", .{});
                    if (idx == half_size - 1) {
                        const left_node = new_nodes.slice()[1];
                        const right_node = new_nodes.slice()[0];

                        const left_keys = left_node.asInternalKeyArray();
                        const left_nodes = left_node.asInternalNodeArray();
                        //std.mem.copy(K, left_keys[0..idx], keys[0..idx]);
                        std.mem.copy(*@This(), left_nodes[0..idx], nodes[0..idx]);

                        const right_keys = right_node.asInternalKeyArray();
                        const right_nodes = right_node.asInternalNodeArray();
                        //std.mem.copy(K, right_keys[1..], keys[idx + 1 ..]);
                        std.mem.copy(*@This(), right_nodes[1..], nodes[idx + 1 ..]);

                        if (print_debug) {
                            std.debug.print("{}\t|", .{@src().line});
                            for (left_keys[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (right_keys[1..]) |_, i| {
                                std.debug.print("{}|", .{i + 1});
                            }

                            std.debug.print("\n{}\t|", .{@src().line});
                            for (keys[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (keys[idx + 1 ..]) |_, i| {
                                std.debug.print("{}|", .{idx + 1 + i});
                            }
                        }

                        new_nodes.slice()[0] = left_node;
                        new_nodes.slice()[1] = right_node;
                    } else if (idx < half_size) {
                        const left_node = new_nodes.slice()[1];
                        const right_node = new_nodes.slice()[0];

                        const left_keys = left_node.asInternalKeyArray();
                        const left_nodes = left_node.asInternalNodeArray();
                        //std.mem.copy(K, left_keys[0..idx], keys[0..idx]);
                        std.mem.copy(*@This(), left_nodes[0..idx], nodes[0..idx]);
                        //std.mem.copy(K, left_keys[idx + 1 ..], keys[idx .. left_keys.len - 1]);
                        std.mem.copy(*@This(), left_nodes[idx + 1 ..], nodes[idx .. left_nodes.len - 1]);

                        const right_keys = right_node.asInternalKeyArray();
                        const right_nodes = right_node.asInternalNodeArray();
                        //std.mem.copy(K, right_keys[0..], keys[left_keys.len - 1 ..]);
                        std.mem.copy(*@This(), right_nodes[0..], nodes[left_keys.len - 1 ..]);

                        if (print_debug) {
                            std.debug.print("{}\t|", .{@src().line});
                            for (left_keys[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (left_keys[idx + 1 ..]) |_, i| {
                                std.debug.print("{}|", .{idx + 1 + i});
                            }
                            std.debug.print("   |", .{});
                            for (right_keys) |_, i| {
                                std.debug.print("{}|", .{i});
                            }

                            std.debug.print("\n{}\t|", .{@src().line});
                            for (keys[0..idx]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (keys[idx .. left_keys.len - 1]) |_, i| {
                                std.debug.print("{}|", .{idx + i});
                            }
                            std.debug.print("   |", .{});
                            for (keys[left_keys.len..]) |_, i| {
                                std.debug.print("{}|", .{left_keys.len + i});
                            }
                        }

                        new_nodes.slice()[0] = left_node;
                        new_nodes.slice()[1] = right_node;
                    } else {
                        const left_node = new_nodes.slice()[0];
                        const right_node = new_nodes.slice()[1];

                        const left_keys = left_node.asInternalKeyArray();
                        const left_nodes = left_node.asInternalNodeArray();
                        //std.mem.copy(K, left_keys[0..], keys[0..left_keys.len]);
                        std.mem.copy(*@This(), left_nodes[0..], nodes[0..left_nodes.len]);

                        const right_keys = right_node.asInternalKeyArray();
                        const right_nodes = right_node.asInternalNodeArray();
                        //std.mem.copy(K, right_keys[0 .. idx - left_keys.len], keys[left_keys.len..idx]);
                        std.mem.copy(*@This(), right_nodes[0 .. idx - left_nodes.len], nodes[left_nodes.len..idx]);
                        //std.mem.copy(K, right_keys[idx - left_keys.len + 2 ..], keys[idx + 1 ..]);
                        std.mem.copy(*@This(), right_nodes[idx - left_nodes.len + 2 ..], nodes[idx + 1 ..]);

                        if (print_debug) {
                            std.debug.print("{}\t|", .{@src().line});
                            for (left_keys) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (right_keys[0 .. idx - left_keys.len]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (right_keys[idx - left_keys.len + 2 ..]) |_, i| {
                                std.debug.print("{}|", .{idx - left_keys.len + 2 + i});
                            }

                            std.debug.print("\n{}\t|", .{@src().line});
                            for (keys[0..left_keys.len]) |_, i| {
                                std.debug.print("{}|", .{i});
                            }
                            std.debug.print("   |", .{});
                            for (keys[left_keys.len..idx]) |_, i| {
                                std.debug.print("{}|", .{left_keys.len + i});
                            }
                            std.debug.print("   |", .{});
                            for (keys[idx + 1 ..]) |_, i| {
                                std.debug.print("{}|", .{idx + 1 + i});
                            }
                        }
                    }
                    if (print_debug) std.debug.print("\n\n", .{});

                    const left_len = new_nodes.constSlice()[0].len;
                    for (newEntries) |new_child, new_offset| {
                        const node_idx_to_insert = (idx + new_offset) / left_len;
                        const update_idx = (idx + new_offset) - left_len * node_idx_to_insert;

                        const node = new_nodes.constSlice()[node_idx_to_insert];
                        node.asInternalKeyArray()[update_idx] = new_child.min();
                        node.asInternalNodeArray()[update_idx] = new_child;
                    }
                }

                for (new_nodes.slice()) |new_node| {
                    buildKeyTree(
                        new_node.asInternalKeyArray(),
                        new_node.asInternalNodeArray(),
                    );
                }

                return new_nodes;
            }

            fn buildKeyTree(keys: []K, children: []const *@This()) void {
                std.debug.assert(keys.len == children.len);

                for (children) |node, linear_idx| {
                    const eytzinger_idx = eytzinger.fromLinear(@intCast(u32, linear_idx), @intCast(u32, children.len));
                    keys[eytzinger_idx] = node.min();
                }
            }

            fn nodeSize(nodeType: NodeType, len: usize) usize {
                const entry_size: usize = switch (nodeType) {
                    .internal => @sizeOf(K) + @sizeOf(*@This()),
                    .leaf => @sizeOf(LeafEntry),
                };
                return len * entry_size + @sizeOf(@This());
            }

            pub fn asInternalKeyArray(this: *@This()) []K {
                std.debug.assert(this.nodeType == .internal);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]u8, this)[0..node_size];

                return @ptrCast([*]K, mem[@sizeOf(@This())..])[0..this.len];
            }

            pub fn asInternalNodeArray(this: *@This()) []*@This() {
                std.debug.assert(this.nodeType == .internal);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]u8, this)[0..node_size];

                const key_array_size = @sizeOf(K) * this.len;

                return @ptrCast([*]*@This(), mem[@sizeOf(@This()) + key_array_size ..])[0..this.len];
            }

            pub fn constInternalKeyArray(this: *const @This()) []const K {
                std.debug.assert(this.nodeType == .internal);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]const u8, this)[0..node_size];

                return @ptrCast([*]const K, mem[@sizeOf(@This())..])[0..this.len];
            }

            pub fn constInternalNodeArray(this: *const @This()) []const *@This() {
                std.debug.assert(this.nodeType == .internal);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]const u8, this)[0..node_size];

                const key_array_size = @sizeOf(K) * this.len;

                return @ptrCast([*]const *@This(), mem[@sizeOf(@This()) + key_array_size ..])[0..this.len];
            }

            pub fn asLeafEntryArray(this: *@This()) []LeafEntry {
                std.debug.assert(this.nodeType == .leaf);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]u8, this)[0..node_size];

                return @ptrCast([*]LeafEntry, mem[@sizeOf(@This())..])[0..this.len];
            }

            pub fn constLeafEntryArray(this: *const @This()) []const LeafEntry {
                std.debug.assert(this.nodeType == .leaf);

                const node_size = nodeSize(this.nodeType, this.len);
                const mem = @ptrCast([*]const u8, this)[0..node_size];

                return @ptrCast([*]const LeafEntry, mem[@sizeOf(@This())..])[0..this.len];
            }

            pub fn min(this: *const @This()) K {
                switch (this.nodeType) {
                    .leaf => return this.constLeafEntryArray()[0].key,
                    .internal => {
                        const keys = this.constInternalKeyArray();
                        return keys[eytzinger.fromLinear(0, @intCast(u32, keys.len))];
                    },
                }
            }

            fn dumpTree(this: *const @This(), writer: anytype) @TypeOf(writer).Error!void {
                try writer.print("{*} ", .{this});
                switch (this.nodeType) {
                    .leaf => {
                        const entries = this.constLeafEntryArray();

                        try writer.writeAll("leaf {");
                        for (entries) |entry| {
                            try writer.print("{} = {},", .{ entry.key, entry.val });
                        }
                        try writer.writeAll("}\n");
                    },
                    .internal => {
                        const keys = this.constInternalKeyArray();
                        const nodes = this.constInternalNodeArray();

                        try writer.writeAll("internal {");
                        for (nodes) |node, idx| {
                            try writer.print("{} = {*},", .{ keys[eytzinger.fromLinear(@intCast(u32, idx), @intCast(u32, keys.len))], node });
                        }
                        try writer.writeAll("}\n");

                        for (nodes) |node| {
                            try node.dumpTree(writer);
                        }
                    },
                }
            }

            fn countBytesUsed(this: *const @This()) usize {
                var bytes_used: usize = nodeSize(this.nodeType, this.len);
                if (this.nodeType == .internal) {
                    const entries = this.constInternalEntryArray();

                    for (entries) |entry| {
                        bytes_used += entry.node.countBytesUsed();
                    }
                }
                return bytes_used;
            }
        };

        const NodeType = enum {
            leaf,
            internal,
        };

        const LeafEntry = struct {
            key: K,
            val: V,
        };

        const PathSegment = struct {
            node: *Node,
            idx: usize,
        };
    };
}

test "put in keys and values and retrieve by key" {
    const Entry = struct {
        key: i64,
        val: i64,
    };
    var cases = std.ArrayList([]const Entry).init(std.testing.allocator);
    defer cases.deinit();

    // Case that found off by 1 error in dupe insert or split internal
    try cases.append(&.{
        .{ .key = 1252075908893741079, .val = 3354519622996530995 },
        .{ .key = -9122029241647599558, .val = -8875707323772236480 },
        .{ .key = 3066288812951245061, .val = 3382948815761252436 },
        .{ .key = 8638083922624639840, .val = -5998269892568312676 },
        .{ .key = -231486179338831356, .val = 1835017602961901510 },
    });

    // Case that found an off by 1 error in dupe insert or split leaf
    try cases.append(&.{
        .{ .key = 8741602964818778106, .val = 2584025519909794368 },
        .{ .key = 698897563146389788, .val = -2638563031019662480 },
        .{ .key = 3579074129189551850, .val = -8410400706168025969 },
        .{ .key = -2188343147285029592, .val = 1700492773575179783 },
        .{ .key = -5102797669907719704, .val = -2995794415761794483 },
    });

    // Case that found memory leak. Memory leak turned out to be calling `free`
    // on child nodes instead of freeRecursive.
    try cases.append(&.{
        .{ .key = -290458009884207260, .val = -9186000433903606205 },
        .{ .key = -8544764128017980972, .val = -8642126939529302081 },
        .{ .key = -6671446766978227772, .val = 1297130194270218659 },
        .{ .key = -6274274220881746536, .val = 3322318109224525298 },
        .{ .key = -6274166942672340636, .val = -689512127275117532 },
    });

    var case_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer case_arena.deinit();

    {
        const cases_to_generate = 1;
        var i: usize = 0;
        while (i < cases_to_generate) : (i += 1) {
            const allocator = &case_arena.allocator;

            var prng = std.rand.DefaultPrng.init(std.crypto.random.int(u64));
            const random = &prng.random;

            const len = random.uintAtMost(usize, 10_000);

            const entries = try allocator.alloc(Entry, len);
            for (entries) |*entry| {
                entry.* = .{
                    .key = random.int(i64),
                    .val = random.int(i64),
                };
            }

            try cases.append(entries);
        }
    }

    var skipped_cases: usize = 0;
    for (cases.items) |entries| {
        var tree = Tree(i64, i64).init(std.testing.allocator);
        defer tree.deinit();

        for (entries) |entry| {
            if (try tree.put(entry.key, entry.val)) {
                skipped_cases += 1;
                continue; // skip this case if there is a duplicate key
            }
        }

        for (entries) |entry| {
            errdefer {
                std.log.err("entry = {}", .{entry});
                tree.dumpTree();
            }
            try std.testing.expectEqual(@as(?i64, entry.val), tree.get(entry.key));
        }
    }

    if (skipped_cases == cases.items.len) return error.SkipZigTest;
}

comptime {
    _ = eytzinger;
}
