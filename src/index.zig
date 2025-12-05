const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

/// Search for an index by table and column name
pub fn getIndexRootpage(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, table_name: []const u8, column_name: []const u8) !?u32 {
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

        const type_st = serial_types[0];
        const name_st = serial_types[1];
        const tbl_name_st = serial_types[2];

        var body_pos: usize = header_size;
        const type_result = record.readString(record_data[body_pos..], type_st);
        if (!std.mem.eql(u8, type_result.value, "index")) continue;

        body_pos += type_result.len;
        const name_result = record.readString(record_data[body_pos..], name_st);

        body_pos += name_result.len;
        const tbl_name_result = record.readString(record_data[body_pos..], tbl_name_st);

        const expected_index_name = try std.fmt.allocPrint(allocator, "idx_{s}_{s}", .{ table_name, column_name });
        defer allocator.free(expected_index_name);

        if (std.mem.eql(u8, name_result.value, expected_index_name) and
            std.mem.eql(u8, tbl_name_result.value, table_name))
        {
            body_pos += tbl_name_result.len;
            const rootpage_st = serial_types[3];
            const rootpage_result = record.readInt(record_data[body_pos..], rootpage_st);
            return @as(u32, @intCast(rootpage_result.value));
        }
    }

    return null;
}

/// Search index B-tree for matching values and collect rowids
pub fn searchIndexForValue(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, index_rootpage: u32, search_value: []const u8, rowids: *std.ArrayList(u64)) !void {
    // Read file into memory once
    const file_size = try file.getEndPos();
    const file_data = try allocator.alloc(u8, file_size);
    defer allocator.free(file_data);

    _ = try file.seekTo(0);
    _ = try file.read(file_data);

    // Use stack to iterate all pages
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
