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
        // Parse SELECT query: "SELECT column FROM table"
        var tokens = std.mem.tokenizeScalar(u8, args[2], ' ');

        // Skip "SELECT"
        _ = tokens.next();

        // Get column name or aggregate function
        const column_name = tokens.next() orelse return error.InvalidQuery;

        // Skip "FROM" or "from"
        _ = tokens.next();

        // Get table name
        const table_name = tokens.next() orelse return error.InvalidQuery;

        // Check if this is COUNT(*)
        if (std.mem.indexOf(u8, column_name, "count(") != null or std.mem.indexOf(u8, column_name, "COUNT(") != null) {
            const rootpage = try schema.getRootpage(allocator, &file, page_size, table_name);
            const row_count = try schema.countRows(allocator, &file, page_size, rootpage);
            try stdout.print("{}\n", .{row_count});
        } else {
            // Get the CREATE TABLE statement to find column order
            const create_sql = try schema.getCreateTableSQL(allocator, &file, page_size, table_name);
            defer allocator.free(create_sql);

            // Find the column index
            const column_idx = try schema.parseColumnIndex(create_sql, column_name);

            // Get the table's root page
            const rootpage = try schema.getRootpage(allocator, &file, page_size, table_name);

            // Read and print all rows
            try schema.readTableRows(allocator, &file, page_size, rootpage, column_idx, stdout);
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
