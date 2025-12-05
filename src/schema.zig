const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

/// Get the rootpage number for a table by name
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
        const rootpage_st = serial_types[3];

        var body_pos: usize = header_size;
        const type_result = record.readString(record_data[body_pos..], type_st);
        if (!std.mem.eql(u8, type_result.value, "table")) continue;

        body_pos += type_result.len;
        const name_result = record.readString(record_data[body_pos..], name_st);
        if (!std.mem.eql(u8, name_result.value, table_name)) continue;

        body_pos += name_result.len;
        body_pos += (serial_types[2] - 13) / 2;

        const rootpage_result = record.readInt(record_data[body_pos..], rootpage_st);
        return @as(u32, @intCast(rootpage_result.value));
    }

    return error.TableNotFound;
}

/// Get CREATE TABLE SQL for a table
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
        const sql_st = serial_types[4];

        var body_pos: usize = header_size;
        const type_result = record.readString(record_data[body_pos..], type_st);
        if (!std.mem.eql(u8, type_result.value, "table")) continue;

        body_pos += type_result.len;
        const name_result = record.readString(record_data[body_pos..], name_st);
        if (!std.mem.eql(u8, name_result.value, table_name)) continue;

        body_pos += name_result.len;
        body_pos += (serial_types[2] - 13) / 2;

        const rootpage_result = record.readInt(record_data[body_pos..], serial_types[3]);
        body_pos += rootpage_result.len;

        const sql_result = record.readString(record_data[body_pos..], sql_st);
        return try allocator.dupe(u8, sql_result.value);
    }

    return error.TableNotFound;
}

/// Parse column index from CREATE TABLE SQL
pub fn parseColumnIndex(sql: []const u8, column_name: []const u8) !usize {
    var in_parens = false;
    var paren_start: usize = 0;

    for (sql, 0..) |c, i| {
        if (c == '(') {
            if (!in_parens) {
                paren_start = i + 1;
                in_parens = true;
            }
        } else if (c == ')') {
            if (in_parens) {
                const columns_part = sql[paren_start..i];

                var column_idx: usize = 0;
                var start: usize = 0;
                var in_col = false;

                for (columns_part, 0..) |ch, j| {
                    if (!in_col) {
                        if (ch != ' ' and ch != '\t' and ch != '\n' and ch != '\r') {
                            start = j;
                            in_col = true;
                        }
                    } else {
                        if (ch == ',' or j == columns_part.len - 1) {
                            var end = j;
                            if (j == columns_part.len - 1 and ch != ',') {
                                end = j + 1;
                            }

                            const col_def = columns_part[start..end];

                            var space_idx: usize = 0;
                            for (col_def, 0..) |col_ch, col_i| {
                                if (col_ch == ' ' or col_ch == '\t') {
                                    space_idx = col_i;
                                    break;
                                }
                            }

                            const col_name = if (space_idx > 0) col_def[0..space_idx] else col_def;

                            if (std.mem.eql(u8, col_name, column_name)) {
                                return column_idx;
                            }

                            column_idx += 1;
                            in_col = false;
                        }
                    }
                }
            }
        }
    }

    return error.ColumnNotFound;
}
