const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");
const btree = @import("btree.zig");
const index = @import("index.zig");
const schema = @import("schema.zig");

/// Read a specific record by rowid from table
fn readRecordByRowid(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_rootpage: u32, target_rowid: u64, column_indices: []const usize, stdout: anytype) !void {
    try searchTableForRowid(allocator, file, page_size, table_rootpage, target_rowid, column_indices, stdout);
}

/// Search for a specific rowid in table B-tree
fn searchTableForRowid(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, page_num: u32, target_rowid: u64, column_indices: []const usize, stdout: anytype) !void {
    var current_page = page_num;
    var depth: u32 = 0;
    const max_depth = 50;

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
                        } else if (st == 7) {
                            const float_result = record.readFloat(record_data[body_pos..]);
                            try stdout.print("{d}", .{float_result.value});
                        } else if (st >= 13 and (st % 2) == 1) {
                            const str_result = record.readString(record_data[body_pos..], st);
                            try stdout.print("{s}", .{str_result.value});
                        }
                    }
                    try stdout.print("\n", .{});
                    return;
                }
            }
            return;
        } else if (page_type == 0x05) {
            // Interior table page
            const num_cells = std.mem.readInt(u16, page_data[3..5], .big);

            var next_page: u32 = 0;
            for (0..num_cells) |i| {
                const offset = 12 + i * 2;
                if (offset + 2 > page_data.len) continue;
                const cell_ptr = std.mem.readInt(u16, page_data[offset..][0..2], .big);
                if (cell_ptr + 8 > page_data.len) continue;

                const cell_data = page_data[cell_ptr..];
                const left_child_page = std.mem.readInt(u32, cell_data[0..4], .big);

                const parsed = varint.parse(cell_data[4..]);
                const key = parsed.value;

                if (target_rowid <= key) {
                    next_page = left_child_page;
                    break;
                }
            }

            if (next_page == 0) {
                next_page = std.mem.readInt(u32, page_data[8..12], .big);
            }

            current_page = next_page;
        } else {
            return;
        }
    }
}

/// Read table rows - single column
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

        if (column_idx >= serial_types.items.len) continue;

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

/// Read table rows - multiple columns
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

/// Read table rows with WHERE clause - uses optimized B-tree traversal
pub fn readTableRowsMultiColumnWhere(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, rootpage: u32, column_indices: []const usize, where_column_idx: ?usize, where_value: ?[]const u8, stdout: anytype) !void {
    try btree.traverseBTree(allocator, file, page_size, rootpage, column_indices, where_column_idx, where_value, stdout);
}

/// Read table rows using index scan
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
    // Index scan disabled - use optimized table scan
    return error.NoIndexFound;
}
