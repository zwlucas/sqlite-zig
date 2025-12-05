const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");
const schema = @import("schema.zig");
const tables = @import("tables.zig");

var stdout_buffer: [1024]u8 = undefined;
var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
const stdout = &stdout_writer.interface;

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

    var file = try std.fs.cwd().openFile(args[1], .{});
    defer file.close();

    var buf: [2]u8 = undefined;
    _ = try file.seekTo(16);
    _ = try file.read(&buf);
    const page_size = std.mem.readInt(u16, &buf, .big);

    if (std.mem.eql(u8, args[2], ".dbinfo")) {
        _ = try file.seekTo(103);
        _ = try file.read(&buf);
        const num_tables = std.mem.readInt(u16, &buf, .big);
        try stdout.print("database page size: {}\n", .{page_size});
        try stdout.print("number of tables: {}\n", .{num_tables});
    } else if (std.mem.eql(u8, args[2], ".tables")) {
        try tables.showTables(allocator, &file, page_size, stdout);
    } else if (std.mem.startsWith(u8, args[2], "SELECT") or std.mem.startsWith(u8, args[2], "select")) {
        // Parse SELECT query
        // Find "FROM" or "from" keyword
        const from_idx_upper = std.mem.indexOf(u8, args[2], "FROM");
        const from_idx_lower = std.mem.indexOf(u8, args[2], "from");
        const from_idx = from_idx_upper orelse from_idx_lower orelse return error.InvalidQuery;

        // Extract column part (between SELECT and FROM)
        const select_len: usize = 6; // length of "SELECT" or "select"
        const column_part = std.mem.trim(u8, args[2][select_len..from_idx], " \t\n");

        // Extract table name (after FROM)
        const from_end = from_idx + 4; // length of "FROM"
        const table_name = std.mem.trim(u8, args[2][from_end..], " \t\n");

        // Check if this is COUNT(*)
        if (std.mem.indexOf(u8, column_part, "count(") != null or std.mem.indexOf(u8, column_part, "COUNT(") != null) {
            const rootpage = try schema.getRootpage(allocator, &file, page_size, table_name);
            const row_count = try schema.countRows(allocator, &file, page_size, rootpage);
            try stdout.print("{}\n", .{row_count});
        } else {
            // Get the CREATE TABLE statement to find column order
            const create_sql = try schema.getCreateTableSQL(allocator, &file, page_size, table_name);
            defer allocator.free(create_sql);

            // Parse column names (split by comma)
            var column_list = std.ArrayList([]const u8){};
            defer column_list.deinit(allocator);

            var col_tokens = std.mem.tokenizeScalar(u8, column_part, ',');
            while (col_tokens.next()) |col| {
                const trimmed = std.mem.trim(u8, col, " \t\n");
                try column_list.append(allocator, trimmed);
            }

            // Get the table's root page
            const rootpage = try schema.getRootpage(allocator, &file, page_size, table_name);

            if (column_list.items.len == 1) {
                // Single column query
                const column_idx = try schema.parseColumnIndex(create_sql, column_list.items[0]);
                try schema.readTableRows(allocator, &file, page_size, rootpage, column_idx, stdout);
            } else {
                // Multiple column query
                var column_indices = std.ArrayList(usize){};
                defer column_indices.deinit(allocator);

                for (column_list.items) |col_name| {
                    const idx = try schema.parseColumnIndex(create_sql, col_name);
                    try column_indices.append(allocator, idx);
                }

                try schema.readTableRowsMultiColumn(allocator, &file, page_size, rootpage, column_indices.items, stdout);
            }
        }
    } else {
        var tokens = std.mem.tokenizeScalar(u8, args[2], ' ');
        var last_token: []const u8 = "";
        while (tokens.next()) |token| {
            last_token = token;
        }

        const rootpage = try schema.getRootpage(allocator, &file, page_size, last_token);
        const row_count = try schema.countRows(allocator, &file, page_size, rootpage);

        try stdout.print("{}\n", .{row_count});
    }

    try stdout.flush();
}
