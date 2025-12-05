const std = @import("std");

pub fn readString(data: []const u8, serial_type: u64) struct { value: []const u8, len: usize } {
    if (serial_type >= 13 and (serial_type % 2) == 1) {
        const size = (serial_type - 13) / 2;
        return .{ .value = data[0..size], .len = size };
    }
    return .{ .value = "", .len = 0 };
}

pub fn readInt(data: []const u8, serial_type: u64) struct { value: i64, len: usize } {
    return switch (serial_type) {
        1 => .{ .value = @as(i64, @as(i8, @intCast(data[0]))), .len = 1 },
        2 => .{ .value = @as(i64, std.mem.readInt(i16, data[0..2], .big)), .len = 2 },
        3 => blk: {
            const b = [_]i32{ @intCast(data[0]), @intCast(data[1]), @intCast(data[2]) };
            break :blk .{ .value = @as(i64, (b[0] << 16) | (b[1] << 8) | b[2]), .len = 3 };
        },
        4 => .{ .value = @as(i64, std.mem.readInt(i32, data[0..4], .big)), .len = 4 },
        5 => blk: {
            const b = [_]i64{ @intCast(data[0]), @intCast(data[1]), @intCast(data[2]), @intCast(data[3]), @intCast(data[4]), @intCast(data[5]) };
            break :blk .{ .value = (b[0] << 40) | (b[1] << 32) | (b[2] << 24) | (b[3] << 16) | (b[4] << 8) | b[5], .len = 6 };
        },
        6 => .{ .value = std.mem.readInt(i64, data[0..8], .big), .len = 8 },
        else => .{ .value = 0, .len = 0 },
    };
}
