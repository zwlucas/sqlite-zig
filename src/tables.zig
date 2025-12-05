const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");

pub fn showTables(allocator: std.mem.Allocator, file: *std.fs.File, page_size: u16, stdout: anytype) !void {
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

    var first = true;

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

        for (0..2) |col| {
            const st = serial_types[col];
            if (st >= 13 and (st % 2) == 1) {
                body_pos += (st - 13) / 2;
            }
        }

        const type_result = record.readString(record_data[header_size..], serial_types[0]);
        const tbl_name = record.readString(record_data[body_pos..], serial_types[2]);

        if (std.mem.eql(u8, type_result.value, "table") and !std.mem.eql(u8, tbl_name.value, "sqlite_sequence")) {
            if (!first) try stdout.print(" ", .{});
            try stdout.print("{s}", .{tbl_name.value});
            first = false;
        }
    }

    if (!first) try stdout.print("\n", .{});
}
