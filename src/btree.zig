const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

/// Helper function to calculate size of a serial type
pub inline fn serialTypeSize(st: u64) usize {
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

/// Read and process rows from a leaf table page
pub fn readLeafPageRows(page_data: []const u8, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
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

/// Traverse B-tree iteratively with WHERE clause filtering
pub fn traverseBTree(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, page_num: u32, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
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

/// Count total rows in a B-tree
pub fn countRows(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32) !u64 {
    const page_offset = (rootpage - 1) * @as(u64, page_size);
    var page_data = try allocator.alloc(u8, page_size);
    defer allocator.free(page_data);

    _ = try file.seekTo(page_offset);
    _ = try file.read(page_data);

    const page_type = page_data[0];
    if (page_type != 0x0d) return 0;

    const num_cells = std.mem.readInt(u16, page_data[3..5], .big);
    return num_cells;
}
