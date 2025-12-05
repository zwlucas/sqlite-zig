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
