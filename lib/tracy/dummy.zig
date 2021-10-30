// This is a dummy version of the tracy library to substitute during tests.

const std = @import("std");

pub fn trace(src: std.builtin.SourceLocation) Trace {
    _ = src;
    return Trace{};
}

const Trace = struct {
    pub fn end(this: @This()) void {
        _ = this;
    }
};
