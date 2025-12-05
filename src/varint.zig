const std = @import("std");

pub fn parse(data: []const u8) struct { value: u64, len: usize } {
    var result: u64 = 0;
    var i: usize = 0;

    while (i < data.len and i < 9) : (i += 1) {
        const byte = data[i];
        if ((byte & 0x80) != 0) {
            result = (result << 7) | @as(u64, byte & 0x7f);
        } else {
            result = (result << 7) | @as(u64, byte);
            return .{ .value = result, .len = i + 1 };
        }
    }

    return .{ .value = result, .len = i };
}
