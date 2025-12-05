const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

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
