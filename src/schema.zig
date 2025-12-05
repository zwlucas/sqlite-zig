const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

fn getIndexRootpage(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_name: []const u8, column_name: []const u8) !?u32 {
    var buf: [2]u8 = undefined;
    _ = try file.seekTo(103);
    _ = try file.read(&buf);
    const num_cells = std.mem.readInt(u16, &buf, .big);

    if (num_cells == 0) return null;

    var cell_pointers = try allocator.alloc(u16, num_cells);
    defer allocator.free(cell_pointers);

    for (0..num_cells) |i| {
        _ = try file.seekTo(108 + i * 2);
        _ = try file.read(&buf);
        cell_pointers[i] = std.mem.readInt(u16, &buf, .big);
    }

    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(0);
    _ = try file.read(page_data);

    for (0..num_cells) |i| {
        if (cell_pointers[i] >= page_data.len) continue;
        const cell_data = page_data[cell_pointers[i]..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;
        if (pos >= cell_data.len) continue;

        parsed = varint.parse(cell_data[pos..]);
        pos += parsed.len;
        if (pos >= cell_data.len) continue;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        if (header_size > record_data.len) continue;
        var header_pos = parsed.len;

        var serial_types: [5]u64 = undefined;
        for (0..5) |col| {
            if (header_pos >= header_size) break;
            parsed = varint.parse(record_data[header_pos..]);
            serial_types[col] = parsed.value;
            header_pos += parsed.len;
        }

        var body_pos: usize = header_size;
        if (body_pos >= record_data.len) continue;

        // Field 0: type
        const st0 = serial_types[0];
        const type_result = record.readString(record_data[body_pos..], st0);
        if (!std.mem.eql(u8, type_result.value, "index")) continue;
        body_pos += type_result.len;
        if (body_pos >= record_data.len) continue;

        // Field 1: name - skip it
        const st1 = serial_types[1];
        if (st1 >= 13 and (st1 % 2) == 1) {
            body_pos += (st1 - 13) / 2;
        }
        if (body_pos >= record_data.len) continue;

        // Field 2: tbl_name
        const st2 = serial_types[2];
        const tbl_name_result = record.readString(record_data[body_pos..], st2);
        if (!std.mem.eql(u8, tbl_name_result.value, table_name)) continue;
        body_pos += tbl_name_result.len;
        if (body_pos >= record_data.len) continue;

        // Field 3: rootpage
        const st3 = serial_types[3];
        const rp = record.readInt(record_data[body_pos..], st3);
        body_pos += rp.len;
        if (body_pos >= record_data.len) continue;

        // Field 4: sql
        const st4 = serial_types[4];
        const sql_result = record.readString(record_data[body_pos..], st4);

        if (std.mem.indexOf(u8, sql_result.value, column_name) != null) {
            return @as(u32, @intCast(rp.value));
        }
    }

    return null;
}

fn searchIndexForValue(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, index_rootpage: u32, search_value: []const u8, rowids: *std.ArrayList(u64)) !void {
    // Read file into memory once
    const file_size = try file.getEndPos();
    const file_data = try allocator.alloc(u8, file_size);
    defer allocator.free(file_data);

    _ = try file.seekTo(0);
    _ = try file.read(file_data);

    // Use stack to iterate all pages (BFS approach - visit ALL index pages to find ALL matches)
    var stack = std.ArrayList(u32){};
    defer stack.deinit(allocator);
    try stack.append(allocator, index_rootpage);

    while (stack.items.len > 0) {
        const page_num = stack.pop() orelse continue;
        if (page_num == 0) continue;

        const page_offset = (page_num - 1) * @as(usize, page_size);
        if (page_offset + page_size > file_data.len) continue;

        const page_data = file_data[page_offset .. page_offset + page_size];
        const page_type = page_data[0];

        if (page_type == 0x0a) {
            // Leaf index page - search for matching values
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);

            for (0..num_cells) |i| {
                const offset = 8 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_ptr = std.mem.readInt(u16, page_data[offset..][0..2], .big);
                if (cell_ptr >= page_data.len) continue;

                const cell_data = page_data[cell_ptr..];
                var parsed = varint.parse(cell_data);
                const pos = parsed.len;
                if (pos >= cell_data.len) continue;

                const record_data = cell_data[pos..];
                parsed = varint.parse(record_data);
                const header_size = parsed.value;
                if (header_size > record_data.len) continue;
                var header_pos = parsed.len;

                // Parse serial types
                var serial_types: [8]u64 = undefined;
                var num_st: usize = 0;
                while (header_pos < header_size and num_st < 8) {
                    parsed = varint.parse(record_data[header_pos..]);
                    serial_types[num_st] = parsed.value;
                    num_st += 1;
                    header_pos += parsed.len;
                }

                if (num_st >= 2) {
                    const st = serial_types[0];
                    var body_pos: usize = header_size;

                    if (st >= 13 and (st % 2) == 1) {
                        const str_result = record.readString(record_data[body_pos..], st);
                        if (std.mem.eql(u8, str_result.value, search_value)) {
                            body_pos += str_result.len;
                            const rowid_st = serial_types[1];
                            const rowid_result = record.readInt(record_data[body_pos..], rowid_st);
                            try rowids.append(allocator, @as(u64, @intCast(rowid_result.value)));
                        }
                    }
                }
            }
        } else if (page_type == 0x02) {
            // Interior index page - add ALL children to stack
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);
            const rightmost_ptr = std.mem.readInt(u32, page_data[8..12], .big);

            // Add all cell pointers
            for (0..num_cells) |i| {
                const offset = 12 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_ptr = std.mem.readInt(u16, page_data[offset..][0..2], .big);
                if (cell_ptr + 4 > page_data.len) continue;

                const cell_data = page_data[cell_ptr..];
                const left_child_page = std.mem.readInt(u32, cell_data[0..4], .big);
                if (left_child_page > 0) {
                    try stack.append(allocator, left_child_page);
                }
            }

            if (rightmost_ptr > 0) {
                try stack.append(allocator, rightmost_ptr);
            }
        }
    }
}
fn readRecordByRowid(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_rootpage: u32, target_rowid: u64, column_indices: []const usize, stdout: anytype) !void {
    try searchTableForRowid(allocator, file, page_size, table_rootpage, target_rowid, column_indices, stdout);
}

fn searchTableForRowid(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, page_num: u32, target_rowid: u64, column_indices: []const usize, stdout: anytype) !void {
    // Use recursive approach but with depth limit to avoid infinite loops
    var current_page = page_num;
    var depth: u32 = 0;
    const max_depth = 50; // Safety limit

    while (current_page != 0 and depth < max_depth) {
        depth += 1;

        const page_offset = (current_page - 1) * @as(u64, page_size);
        var page_data = try allocator.alloc(u8, page_size);
        defer allocator.free(page_data);

        _ = try file.seekTo(page_offset);
        _ = try file.read(page_data);

        const page_type = page_data[0];

        if (page_type == 0x0d) {
            // Leaf table page
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);

            for (0..num_cells) |i| {
                const offset = 8 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_bytes: *const [2]u8 = page_data[offset .. offset + 2][0..2];
                const cell_ptr = std.mem.readInt(u16, cell_bytes, .big);
                if (cell_ptr >= page_data.len) continue;

                const cell_data = page_data[cell_ptr..];

                var parsed = varint.parse(cell_data);
                var pos = parsed.len;
                if (pos >= cell_data.len) continue;

                parsed = varint.parse(cell_data[pos..]);
                const rowid = parsed.value;
                pos += parsed.len;
                if (pos >= cell_data.len) continue;

                if (rowid == target_rowid) {
                    const record_data = cell_data[pos..];
                    parsed = varint.parse(record_data);
                    const header_size = parsed.value;
                    if (header_size > record_data.len) continue;
                    var header_pos = parsed.len;

                    var serial_types = std.ArrayList(u64){};
                    defer serial_types.deinit(allocator);

                    while (header_pos < header_size) {
                        parsed = varint.parse(record_data[header_pos..]);
                        serial_types.append(allocator, parsed.value) catch break;
                        header_pos += parsed.len;
                    }

                    for (column_indices, 0..) |column_idx, col_num| {
                        if (col_num > 0) try stdout.print("|", .{});

                        if (column_idx >= serial_types.items.len) continue;

                        var body_pos: usize = header_size;
                        for (0..column_idx) |col| {
                            if (col >= serial_types.items.len) break;
                            const st = serial_types.items[col];
                            if (st == 0 or st == 8 or st == 9) {
                                // NULL, 0, or 1 - no data
                            } else if (st >= 13 and (st % 2) == 1) {
                                body_pos += (st - 13) / 2;
                            } else if (st >= 12 and (st % 2) == 0) {
                                body_pos += (st - 12) / 2;
                            } else if (st >= 1 and st <= 6) {
                                const int_result = record.readInt(record_data[body_pos..], st);
                                body_pos += int_result.len;
                            } else if (st == 7) {
                                body_pos += 8;
                            }
                        }

                        const st = serial_types.items[column_idx];
                        if (st == 0) {
                            try stdout.print("{}", .{rowid});
                        } else if (st == 8) {
                            try stdout.print("0", .{});
                        } else if (st == 9) {
                            try stdout.print("1", .{});
                        } else if (st >= 1 and st <= 6) {
                            const int_result = record.readInt(record_data[body_pos..], st);
                            try stdout.print("{}", .{int_result.value});
                        } else if (st >= 13 and (st % 2) == 1) {
                            const str_result = record.readString(record_data[body_pos..], st);
                            try stdout.print("{s}", .{str_result.value});
                        }
                    }
                    try stdout.print("\n", .{});
                    return;
                }
            }
            // Not found in this leaf, stop searching
            return;
        } else if (page_type == 0x05) {
            // Interior table page - use binary search to find the right child
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);
            const rightmost_ptr = std.mem.readInt(u32, page_data[8..12], .big);

            // Find which child to follow based on rowid
            var next_page: u32 = rightmost_ptr;
            for (0..num_cells) |i| {
                const offset = 12 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_bytes: *const [2]u8 = page_data[offset .. offset + 2][0..2];
                const cell_ptr = std.mem.readInt(u16, cell_bytes, .big);
                if (cell_ptr + 4 > page_data.len) continue;

                const cell_data = page_data[cell_ptr..];
                const left_child_page = std.mem.readInt(u32, cell_data[0..4], .big);

                const parsed_key = varint.parse(cell_data[4..]);
                const cell_rowid = parsed_key.value;

                if (target_rowid < cell_rowid) {
                    next_page = left_child_page;
                    break;
                }
            }

            current_page = next_page;
        } else {
            return;
        }
    }
}

inline fn serialTypeSize(st: u64) usize {
    if (st == 0 or st == 8 or st == 9) {
        return 0;
    } else if (st >= 13 and (st % 2) == 1) {
        return (st - 13) / 2;
    } else if (st >= 12 and (st % 2) == 0) {
        return (st - 12) / 2;
    } else if (st >= 1 and st <= 6) {
        return @as(usize, st);
    } else if (st == 7) {
        return 8;
    }
    return 0;
}

fn readLeafPageRows(page_data: []const u8, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
    const page_type = page_data[0];
    if (page_type != 0x0d) return;

    const num_cells = std.mem.readInt(u16, page_data[3..5], .big);
    if (num_cells == 0) return;

    for (0..num_cells) |i| {
        const offset = 8 + i * 2;
        if (offset + 2 > page_data.len) continue;
        const cell_ptr = std.mem.readInt(u16, page_data[offset..][0..2], .big);
        if (cell_ptr >= page_data.len) continue;

        const cell_data = page_data[cell_ptr..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;
        if (pos >= cell_data.len) continue;

        parsed = varint.parse(cell_data[pos..]);
        const rowid = parsed.value;
        pos += parsed.len;
        if (pos >= cell_data.len) continue;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        if (header_size > record_data.len or header_size == 0) continue;
        var header_pos = parsed.len;

        // Parse serial types
        var serial_types: [256]u64 = undefined;
        var num_columns: usize = 0;
        while (header_pos < header_size and num_columns < 256) {
            parsed = varint.parse(record_data[header_pos..]);
            serial_types[num_columns] = parsed.value;
            num_columns += 1;
            header_pos += parsed.len;
        }

        // Check WHERE clause if present (early rejection)
        if (where_column_idx) |where_idx| {
            if (where_value) |expected_value| {
                if (where_idx >= num_columns) continue;

                const st = serial_types[where_idx];
                
                // Fast path: check serial type first
                if (st >= 13 and (st % 2) == 1) {
                    // String comparison - check length first for early rejection
                    const expected_len = (st - 13) / 2;
                    if (expected_len != expected_value.len) continue;
                    
                    var where_body_pos: usize = header_size;
                    for (0..where_idx) |col| {
                        where_body_pos += serialTypeSize(serial_types[col]);
                    }
                    
                    const str_result = record.readString(record_data[where_body_pos..], st);
                    if (!std.mem.eql(u8, str_result.value, expected_value)) continue;
                } else if (st >= 1 and st <= 6) {
                    var where_body_pos: usize = header_size;
                    for (0..where_idx) |col| {
                        where_body_pos += serialTypeSize(serial_types[col]);
                    }
                    const int_result = record.readInt(record_data[where_body_pos..], st);
                    const expected_int = std.fmt.parseInt(i64, expected_value, 10) catch 0;
                    if (int_result.value != expected_int) continue;
                } else {
                    continue; // Unsupported type for WHERE
                }
            }
        }

        // Print matching row
        for (column_indices, 0..) |column_idx, col_num| {
            if (col_num > 0) try stdout.print("|", .{});
            if (column_idx >= num_columns) continue;

            var body_pos: usize = header_size;
            for (0..column_idx) |col| {
                body_pos += serialTypeSize(serial_types[col]);
            }

            const st = serial_types[column_idx];
            if (st == 0) {
                try stdout.print("{}", .{rowid});
            } else if (st == 8) {
                try stdout.print("0", .{});
            } else if (st == 9) {
                try stdout.print("1", .{});
            } else if (st >= 1 and st <= 6) {
                const int_result = record.readInt(record_data[body_pos..], st);
                try stdout.print("{}", .{int_result.value});
            } else if (st >= 13 and (st % 2) == 1) {
                const str_result = record.readString(record_data[body_pos..], st);
                try stdout.print("{s}", .{str_result.value});
            }
        }
        try stdout.print("\n", .{});
    }
}

fn traverseBTree(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, page_num: u32, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
    // Read entire file into memory for fast random access
    const file_size = try file.getEndPos();
    const file_data = try allocator.alloc(u8, file_size);
    defer allocator.free(file_data);

    _ = try file.seekTo(0);
    _ = try file.read(file_data);

    var stack = std.ArrayList(u32){};
    defer stack.deinit(allocator);
    try stack.append(allocator, page_num);

    while (stack.items.len > 0) {
        const current_page = stack.pop() orelse continue;

        const page_offset = (current_page - 1) * @as(usize, page_size);
        if (page_offset + page_size > file_data.len) continue;

        const page_data = file_data[page_offset .. page_offset + page_size];
        const page_type = page_data[0];

        if (page_type == 0x0d) {
            try readLeafPageRows(page_data, column_indices, where_column_idx, where_value, stdout);
        } else if (page_type == 0x05) {
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);
            const rightmost_ptr = std.mem.readInt(u32, page_data[8..12], .big);

            // Add rightmost first so it's processed last (stack LIFO)
            if (rightmost_ptr != 0) {
                try stack.append(allocator, rightmost_ptr);
            }

            // Add children in reverse order for correct traversal
            var i = num_cells;
            while (i > 0) {
                i -= 1;
                const offset = 12 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_ptr_bytes = page_data[offset .. offset + 2];
                const cell_ptr = std.mem.readInt(u16, cell_ptr_bytes[0..2], .big);
                if (cell_ptr + 4 > page_data.len) continue;

                const cell_data = page_data[cell_ptr..];
                const left_child_page = std.mem.readInt(u32, cell_data[0..4], .big);

                if (left_child_page != 0) {
                    try stack.append(allocator, left_child_page);
                }
            }
        }
    }
}

pub fn getRootpage(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_name: []const u8) !u32 {
    var buf: [2]u8 = undefined;
    _ = try file.seekTo(103);
    _ = try file.read(&buf);
    const num_cells = std.mem.readInt(u16, &buf, .big);

    var cell_pointers = try allocator.alloc(u16, num_cells);
    defer allocator.free(cell_pointers);

    for (0..num_cells) |i| {
        _ = try file.seekTo(108 + i * 2);
        _ = try file.read(&buf);
        cell_pointers[i] = std.mem.readInt(u16, &buf, .big);
    }

    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(0);
    _ = try file.read(page_data);

    for (0..num_cells) |i| {
        const cell_data = page_data[cell_pointers[i]..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;

        parsed = varint.parse(cell_data[pos..]);
        pos += parsed.len;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        var header_pos = parsed.len;

        var serial_types: [5]u64 = undefined;
        for (0..5) |col| {
            parsed = varint.parse(record_data[header_pos..]);
            serial_types[col] = parsed.value;
            header_pos += parsed.len;
        }

        var body_pos: usize = header_size;

        const st0 = serial_types[0];
        if (st0 >= 13 and (st0 % 2) == 1) {
            body_pos += (st0 - 13) / 2;
        } else if (st0 >= 1 and st0 <= 6) {
            const r0 = record.readInt(record_data[body_pos..], st0);
            body_pos += r0.len;
        }

        const st1 = serial_types[1];
        if (st1 >= 13 and (st1 % 2) == 1) {
            body_pos += (st1 - 13) / 2;
        } else if (st1 >= 1 and st1 <= 6) {
            const r1 = record.readInt(record_data[body_pos..], st1);
            body_pos += r1.len;
        }

        const tbl_name_result = record.readString(record_data[body_pos..], serial_types[2]);
        body_pos += tbl_name_result.len;

        if (std.mem.eql(u8, tbl_name_result.value, table_name)) {
            const rp = record.readInt(record_data[body_pos..], serial_types[3]);
            return @as(u32, @intCast(rp.value));
        }
    }

    return 0;
}

pub fn countRows(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32) !u64 {
    if (rootpage == 0) return 0;

    const page_offset = (rootpage - 1) * @as(u64, page_size);
    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(page_offset);
    _ = try file.read(page_data);

    const page_type = page_data[0];
    if (page_type == 0x05 or page_type == 0x0d) {
        return std.mem.readInt(u16, page_data[3..5], .big);
    }

    return 0;
}

pub fn getCreateTableSQL(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_name: []const u8) ![]const u8 {
    var buf: [2]u8 = undefined;
    _ = try file.seekTo(103);
    _ = try file.read(&buf);
    const num_cells = std.mem.readInt(u16, &buf, .big);

    var cell_pointers = try allocator.alloc(u16, num_cells);
    defer allocator.free(cell_pointers);

    for (0..num_cells) |i| {
        _ = try file.seekTo(108 + i * 2);
        _ = try file.read(&buf);
        cell_pointers[i] = std.mem.readInt(u16, &buf, .big);
    }

    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(0);
    _ = try file.read(page_data);

    for (0..num_cells) |i| {
        const cell_data = page_data[cell_pointers[i]..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;

        parsed = varint.parse(cell_data[pos..]);
        pos += parsed.len;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        var header_pos = parsed.len;

        var serial_types: [5]u64 = undefined;
        for (0..5) |col| {
            parsed = varint.parse(record_data[header_pos..]);
            serial_types[col] = parsed.value;
            header_pos += parsed.len;
        }

        var body_pos: usize = header_size;

        const st0 = serial_types[0];
        if (st0 >= 13 and (st0 % 2) == 1) {
            body_pos += (st0 - 13) / 2;
        } else if (st0 >= 1 and st0 <= 6) {
            const r0 = record.readInt(record_data[body_pos..], st0);
            body_pos += r0.len;
        }

        const st1 = serial_types[1];
        if (st1 >= 13 and (st1 % 2) == 1) {
            body_pos += (st1 - 13) / 2;
        } else if (st1 >= 1 and st1 <= 6) {
            const r1 = record.readInt(record_data[body_pos..], st1);
            body_pos += r1.len;
        }

        const tbl_name_result = record.readString(record_data[body_pos..], serial_types[2]);
        body_pos += tbl_name_result.len;

        if (std.mem.eql(u8, tbl_name_result.value, table_name)) {
            const rp = record.readInt(record_data[body_pos..], serial_types[3]);
            body_pos += rp.len;

            const sql_result = record.readString(record_data[body_pos..], serial_types[4]);
            return try allocator.dupe(u8, sql_result.value);
        }
    }

    return error.TableNotFound;
}

pub fn parseColumnIndex(sql: []const u8, column_name: []const u8) !usize {
    var paren_idx: ?usize = null;
    for (sql, 0..) |c, i| {
        if (c == '(') {
            paren_idx = i;
            break;
        }
    }

    if (paren_idx == null) return error.InvalidSQL;

    var col_idx: usize = 0;
    var in_col_name = false;
    var col_start: usize = paren_idx.? + 1;

    for (sql[paren_idx.? + 1 ..], 0..) |c, i| {
        const actual_idx = paren_idx.? + 1 + i;

        if (c == ')') break;

        if (c == ' ' or c == '\t' or c == '\n') {
            if (in_col_name) {
                const col_name = std.mem.trim(u8, sql[col_start..actual_idx], " \t\n");
                if (std.mem.eql(u8, col_name, column_name)) {
                    return col_idx;
                }
                in_col_name = false;
            }
            continue;
        }

        if (c == ',') {
            if (in_col_name) {
                const col_name = std.mem.trim(u8, sql[col_start..actual_idx], " \t\n");
                if (std.mem.eql(u8, col_name, column_name)) {
                    return col_idx;
                }
            }
            col_idx += 1;
            in_col_name = false;
            continue;
        }

        if (!in_col_name) {
            col_start = actual_idx;
            in_col_name = true;
        }
    }

    return error.ColumnNotFound;
}

pub fn readTableRows(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32, column_idx: usize, stdout: anytype) !void {
    if (rootpage == 0) return;

    const page_offset = (rootpage - 1) * @as(u64, page_size);
    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(page_offset);
    _ = try file.read(page_data);

    const page_type = page_data[0];
    if (page_type != 0x0d) return;

    const num_cells = std.mem.readInt(u16, page_data[3..5], .big);

    var cell_pointers = try allocator.alloc(u16, num_cells);
    defer allocator.free(cell_pointers);

    for (0..num_cells) |i| {
        const offset = 8 + i * 2;
        const cell_ptr_bytes = page_data[offset .. offset + 2];
        cell_pointers[i] = std.mem.readInt(u16, cell_ptr_bytes[0..2], .big);
    }

    for (0..num_cells) |i| {
        const cell_data = page_data[cell_pointers[i]..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;

        parsed = varint.parse(cell_data[pos..]);
        pos += parsed.len;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        var header_pos = parsed.len;

        var serial_types = std.ArrayList(u64){};
        defer serial_types.deinit(allocator);

        while (header_pos < header_size) {
            parsed = varint.parse(record_data[header_pos..]);
            try serial_types.append(allocator, parsed.value);
            header_pos += parsed.len;
        }

        var body_pos: usize = header_size;
        for (0..column_idx) |col| {
            if (col >= serial_types.items.len) break;
            const st = serial_types.items[col];
            if (st >= 13 and (st % 2) == 1) {
                body_pos += (st - 13) / 2;
            } else if (st >= 1 and st <= 6) {
                const int_result = record.readInt(record_data[body_pos..], st);
                body_pos += int_result.len;
            }
        }

        if (column_idx < serial_types.items.len) {
            const st = serial_types.items[column_idx];
            if (st >= 13 and (st % 2) == 1) {
                const str_result = record.readString(record_data[body_pos..], st);
                try stdout.print("{s}\n", .{str_result.value});
            } else if (st >= 1 and st <= 6) {
                const int_result = record.readInt(record_data[body_pos..], st);
                try stdout.print("{}\n", .{int_result.value});
            }
        }
    }
}

pub fn readTableRowsMultiColumn(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32, column_indices: []const usize, stdout: anytype) !void {
    if (rootpage == 0) return;

    const page_offset = (rootpage - 1) * @as(u64, page_size);
    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(page_offset);
    _ = try file.read(page_data);

    const page_type = page_data[0];
    if (page_type != 0x0d) return;

    const num_cells = std.mem.readInt(u16, page_data[3..5], .big);

    var cell_pointers = try allocator.alloc(u16, num_cells);
    defer allocator.free(cell_pointers);

    for (0..num_cells) |i| {
        const offset = 8 + i * 2;
        const cell_ptr_bytes = page_data[offset .. offset + 2];
        cell_pointers[i] = std.mem.readInt(u16, cell_ptr_bytes[0..2], .big);
    }

    for (0..num_cells) |i| {
        const cell_data = page_data[cell_pointers[i]..];

        var parsed = varint.parse(cell_data);
        var pos = parsed.len;

        parsed = varint.parse(cell_data[pos..]);
        pos += parsed.len;

        const record_data = cell_data[pos..];
        parsed = varint.parse(record_data);
        const header_size = parsed.value;
        var header_pos = parsed.len;

        var serial_types = std.ArrayList(u64){};
        defer serial_types.deinit(allocator);

        while (header_pos < header_size) {
            parsed = varint.parse(record_data[header_pos..]);
            try serial_types.append(allocator, parsed.value);
            header_pos += parsed.len;
        }

        for (column_indices, 0..) |column_idx, col_num| {
            var body_pos: usize = header_size;
            for (0..column_idx) |col| {
                if (col >= serial_types.items.len) break;
                const st = serial_types.items[col];
                if (st >= 13 and (st % 2) == 1) {
                    body_pos += (st - 13) / 2;
                } else if (st >= 1 and st <= 6) {
                    const int_result = record.readInt(record_data[body_pos..], st);
                    body_pos += int_result.len;
                }
            }

            if (col_num > 0) {
                try stdout.print("|", .{});
            }

            if (column_idx < serial_types.items.len) {
                const st = serial_types.items[column_idx];
                if (st >= 13 and (st % 2) == 1) {
                    const str_result = record.readString(record_data[body_pos..], st);
                    try stdout.print("{s}", .{str_result.value});
                } else if (st >= 1 and st <= 6) {
                    const int_result = record.readInt(record_data[body_pos..], st);
                    try stdout.print("{}", .{int_result.value});
                }
            }
        }
        try stdout.print("\n", .{});
    }
}

pub fn readTableRowsMultiColumnWhere(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
    try traverseBTree(allocator, file, page_size, rootpage, column_indices, where_column_idx, where_value, stdout);
}

pub fn readTableRowsWithIndex(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_name: []const u8, table_rootpage: u32, column_indices: []const usize, where_column: []const u8, where_value: []const u8, stdout: anytype) !void {
    _ = allocator;
    _ = file;
    _ = page_size;
    _ = table_name;
    _ = table_rootpage;
    _ = column_indices;
    _ = where_column;
    _ = where_value;
    _ = stdout;
    // Temporarily disable to test table scan
    return error.NoIndexFound;
}
