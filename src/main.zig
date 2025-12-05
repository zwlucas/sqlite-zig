const std = @import("std");
var stdout_buffer: [1024]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

fn parseVarint(data: []const u8) struct { value: u64, len: usize } {
    var result: u64 = 0;
    var i: usize = 0;

    while (i < data.len and i < 9) : (i += 1) {
        const byte = data[i];
        result |= @as(u64, byte & 0x7f) << @as(u6, @intCast(i * 7));

        if ((byte & 0x80) == 0) {
            return .{ .value = result, .len = i + 1 };
        }
    }

    return .{ .value = result, .len = i };
}

fn readStringFromRecord(data: []const u8, serial_type: u64) struct { value: []const u8, len: usize } {
    if (serial_type >= 13 and (serial_type % 2) == 1) {
        const size = (serial_type - 13) / 2;
        return .{ .value = data[0..size], .len = size };
    }
    return .{ .value = "", .len = 0 };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        std.process.exit(1);
    }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    if (std.mem.eql(u8, command, ".dbinfo")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        var buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&buf);
        const page_size = std.mem.readInt(u16, &buf, .big);
        try stdout.print("database page size: {}\n", .{page_size});

        _ = try file.seekTo(103);
        _ = try file.read(&buf);
        const num_tables = std.mem.readInt(u16, &buf, .big);
        try stdout.print("number of tables: {}\n", .{num_tables});
        try stdout.flush();
    } else if (std.mem.eql(u8, command, ".tables")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        var buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&buf);
        const page_size = std.mem.readInt(u16, &buf, .big);

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

        var first_table = true;

        for (0..num_cells) |i| {
            const cell_offset = cell_pointers[i];
            const cell_data = page_data[cell_offset..];

            var parsed = parseVarint(cell_data);
            var pos = parsed.len;

            parsed = parseVarint(cell_data[pos..]);
            pos += parsed.len;

            const record_start = pos;
            const record_data = cell_data[record_start..];

            parsed = parseVarint(record_data);
            const header_size = parsed.value;
            var header_pos = parsed.len;

            var serial_types: [5]u64 = undefined;
            for (0..5) |col| {
                parsed = parseVarint(record_data[header_pos..]);
                serial_types[col] = parsed.value;
                header_pos += parsed.len;
            }

            var body_pos: usize = header_size;

            for (0..2) |col| {
                const serial_type = serial_types[col];
                if (serial_type >= 13 and (serial_type % 2) == 1) {
                    const size = (serial_type - 13) / 2;
                    body_pos += size;
                }
            }

            const tbl_name_type = serial_types[2];
            const tbl_name = readStringFromRecord(record_data[body_pos..], tbl_name_type);

            const body_pos_type: usize = header_size;
            const type_serial = serial_types[0];
            const type_str = readStringFromRecord(record_data[body_pos_type..], type_serial);

            if (std.mem.eql(u8, type_str.value, "table")) {
                if (!std.mem.eql(u8, tbl_name.value, "sqlite_sequence")) {
                    if (!first_table) {
                        try stdout.print(" ", .{});
                    }
                    try stdout.print("{s}", .{tbl_name.value});
                    first_table = false;
                }
            }
        }

        if (!first_table) {
            try stdout.print("\n", .{});
        }
        try stdout.flush();
    }
}
