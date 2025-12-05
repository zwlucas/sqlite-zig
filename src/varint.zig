const std = @import("std");

pub fn parse(data: []const u8) struct { value: u64, len: usize } {
    var result: u64 = 0;
    var i: usize = 0;

    while (i < data.len and i < 9) : (i += 1) {
        const byte = data[i];
        result |= @as(u64, byte & 0x7f) << @as(u6, @intCast(i * 7));
        if ((byte & 0x80) == 0) return .{ .value = result, .len = i + 1 };
    }

    return .{ .value = result, .len = i };
}
