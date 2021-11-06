const std = @import("std");

const DUMMY_TRACY_PKG = std.build.Pkg{
    .name = "tracy",
    .path = .{ .path = "./lib/tracy/dummy.zig" },
};

const TRACY_PKG = std.build.Pkg{
    .name = "tracy",
    .path = .{ .path = "./lib/tracy/zig-tracy/src/lib.zig" },
};

const DISK_KV_STORE_WITH_TRACY = std.build.Pkg{
    .name = "disk-kv-store",
    .path = .{ .path = "./src/main.zig" },
    .dependencies = &.{TRACY_PKG},
};

const DISK_KV_STORE_WITHOUT_TRACY = std.build.Pkg{
    .name = "disk-kv-store",
    .path = .{ .path = "./src/main.zig" },
    .dependencies = &.{DUMMY_TRACY_PKG},
};

const ZIG_CLAP_PKG = std.build.Pkg{
    .name = "clap",
    .path = .{ .path = "./lib/zig-clap/clap.zig" },
};

const Benchmark = struct {
    name: []const u8,
    description: []const u8,
    path: []const u8,
    sqlite: bool = false,
};

const benchmarks = [_]Benchmark{
    .{
        .name = "exponential_mem",
        .description = "Test the speed of the in-memory exponential tree",
        .path = "./benchmark/exponential_mem.zig",
    },
    .{
        .name = "space_used",
        .description = "Test how much space is used by the in-memory exponential tree",
        .path = "./benchmark/space_used.zig",
    },
    .{
        .name = "sqlite_for_comparison",
        .description = "Test against sqlite to get an idea of the relative speed",
        .path = "./benchmark/sqlite_for_comparison.zig",
        .sqlite = true,
    },
    .{
        .name = "array_for_comparison",
        .description = "Test against an array to get an idea of the relative speed",
        .path = "./benchmark/sorted_array_for_comparison.zig",
    },
};

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();
    const target = b.standardTargetOptions(.{});

    const lib = b.addStaticLibrary("disk-btree", "src/main.zig");
    lib.addPackage(DUMMY_TRACY_PKG);
    lib.setBuildMode(mode);
    lib.install();

    var main_tests = b.addTest("src/main.zig");
    main_tests.setBuildMode(mode);
    main_tests.setTarget(target);
    main_tests.addPackagePath("tracy", "./lib/tracy/dummy.zig");

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Benchmarks
    const tracy_enabled = b.option(bool, "tracy", "enable support for profiling with Tracy") orelse false;

    const benchmark_target = if (!target.isGnuLibC() or !tracy_enabled) target else glib_2_18_target: {
        var b_target = target;
        b_target.glibc_version = std.builtin.Version{ .major = 2, .minor = 18 };
        break :glib_2_18_target b_target;
    };

    const build_benchmarks_step = b.step("benchmarks", "Build all benchmarks");
    const run_benchmarks_step = b.step("benchmark-run", "Run all benchmarks");

    inline for (benchmarks) |bench| {
        const benchmark = b.addExecutable("benchmark-" ++ bench.name, bench.path);
        benchmark.setBuildMode(mode);
        benchmark.setTarget(benchmark_target);

        benchmark.addPackage(ZIG_CLAP_PKG);

        if (tracy_enabled) {
            benchmark.addPackage(DISK_KV_STORE_WITH_TRACY);
            benchmark.addPackage(TRACY_PKG);
            benchmark.addIncludeDir("./lib/tracy/tracy");
            benchmark.addCSourceFile(
                "./lib/tracy/tracy/TracyClient.cpp",
                &.{ "-DTRACY_ENABLE", "-fno-sanitize=undefined" },
            );
            benchmark.linkSystemLibrary("c++");
        } else {
            benchmark.addPackage(DISK_KV_STORE_WITHOUT_TRACY);
            benchmark.addPackage(DUMMY_TRACY_PKG);
        }

        if (bench.sqlite) {
            benchmark.linkSystemLibrary("sqlite3");
            benchmark.linkLibC();
        }

        const benchmark_install = b.addInstallArtifact(benchmark);
        build_benchmarks_step.dependOn(&benchmark_install.step);
        const build_benchmark_step = b.step("benchmark-" ++ bench.name, "(Build) " ++ bench.description);
        build_benchmark_step.dependOn(&benchmark_install.step);

        const benchmark_run = benchmark.run();
        if (b.args) |args| {
            benchmark_run.addArgs(args);
        }

        const run_step = b.step("benchmark-run-" ++ bench.name, "(Run) " ++ bench.description);
        run_step.dependOn(&benchmark_run.step);

        run_benchmarks_step.dependOn(&benchmark_run.step);
    }
}
