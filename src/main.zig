const std = @import("std");
const varint = @import("varint.zig");
const record = @import("record.zig");
const schema = @import("schema.zig");
const tables = @import("tables.zig");
const query = @import("query.zig");
const btree = @import("btree.zig");

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

        // Find WHERE clause if it exists
        const where_idx_upper = std.mem.indexOf(u8, args[2][from_idx..], "WHERE");
        const where_idx_lower = std.mem.indexOf(u8, args[2][from_idx..], "where");
        const where_idx_rel = where_idx_upper orelse where_idx_lower;

        // Extract table name (after FROM, before WHERE if exists)
        const from_end = from_idx + 4; // length of "FROM"
        const table_end = if (where_idx_rel) |idx| from_idx + idx else args[2].len;
        const table_name = std.mem.trim(u8, args[2][from_end..table_end], " \t\n");

        // Parse WHERE clause if it exists
        var where_column: ?[]const u8 = null;
        var where_value: ?[]const u8 = null;
        if (where_idx_rel) |idx| {
            const where_start = from_idx + idx + 5; // +5 for "WHERE"
            const where_clause = std.mem.trim(u8, args[2][where_start..], " \t\n");

            // Parse WHERE clause: "column = 'value'"
            if (std.mem.indexOf(u8, where_clause, "=")) |eq_idx| {
                where_column = std.mem.trim(u8, where_clause[0..eq_idx], " \t\n");
                var value_part = std.mem.trim(u8, where_clause[eq_idx + 1 ..], " \t\n");

                // Remove quotes from value
                if (value_part.len >= 2 and value_part[0] == '\'' and value_part[value_part.len - 1] == '\'') {
                    where_value = value_part[1 .. value_part.len - 1];
                } else {
                    where_value = value_part;
                }
            }
        }

        // Check if this is COUNT(*)
        if (std.mem.indexOf(u8, column_part, "count(") != null or std.mem.indexOf(u8, column_part, "COUNT(") != null) {
            const rootpage = try schema.getRootpage(allocator, &file, page_size, table_name);
            const row_count = try btree.countRows(allocator, &file, page_size, rootpage);
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

            // Get WHERE column index if present
            var where_column_idx: ?usize = null;
            if (where_column) |col| {
                where_column_idx = try schema.parseColumnIndex(create_sql, col);
            }

            if (column_list.items.len == 1) {
                // Single column query
                const column_idx = try schema.parseColumnIndex(create_sql, column_list.items[0]);
                try query.readTableRows(allocator, &file, page_size, rootpage, column_idx, stdout);
            } else {
                // Multiple column query
                var column_indices = std.ArrayList(usize){};
                defer column_indices.deinit(allocator);

                for (column_list.items) |col_name| {
                    const idx = try schema.parseColumnIndex(create_sql, col_name);
                    try column_indices.append(allocator, idx);
                }

                // Try to use index scan if WHERE clause exists
                if (where_column != null and where_value != null) {
                    // Try index scan first
                    query.readTableRowsWithIndex(allocator, &file, page_size, table_name, rootpage, column_indices.items, where_column.?, where_value.?, stdout) catch |err| {
                        if (err == error.NoIndexFound) {
                            // Fall back to table scan
                            try query.readTableRowsMultiColumnWhere(allocator, &file, page_size, rootpage, column_indices.items, where_column_idx, where_value, stdout);
                        } else {
                            return err;
                        }
                    };
                } else {
                    try query.readTableRowsMultiColumnWhere(allocator, &file, page_size, rootpage, column_indices.items, where_column_idx, where_value, stdout);
                }
            }
        }
    } else {
        var tokens = std.mem.tokenizeScalar(u8, args[2], ' ');
        var last_token: []const u8 = "";
        while (tokens.next()) |token| {
            last_token = token;
        }

        const rootpage = try schema.getRootpage(allocator, &file, page_size, last_token);
        const row_count = try btree.countRows(allocator, &file, page_size, rootpage);

        try stdout.print("{}\n", .{row_count});
    }

    try stdout.flush();
}
