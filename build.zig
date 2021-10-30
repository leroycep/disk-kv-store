const std = @import("std");

const TRACY_PKG = std.build.Pkg{
    .name = "tracy",
    .path = .{ .path = "./lib/tracy/zig-tracy/src/lib.zig" },
};

const BENCHMARK_LIB_PKG = std.build.Pkg{
    .name = "disk-kv-store",
    .path = .{ .path = "./src/main.zig" },
    .dependencies = &.{TRACY_PKG},
};

pub fn build(b: *std.build.Builder) void {
    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const lib = b.addStaticLibrary("disk-btree", "src/main.zig");
    lib.addPackagePath("tracy", "./lib/tracy/dummy.zig");
    lib.setBuildMode(mode);
    lib.install();

    var main_tests = b.addTest("src/main.zig");
    main_tests.setBuildMode(mode);
    main_tests.addPackagePath("tracy", "./lib/tracy/dummy.zig");

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Benchmarks
    const target = b.standardTargetOptions(.{});
    const benchmark_target = if (!target.isGnuLibC()) target else glib_2_18_target: {
        var b_target = target;
        b_target.glibc_version = std.builtin.Version{ .major = 2, .minor = 18 };
        break :glib_2_18_target b_target;
    };

    const build_benchmarks_step = b.step("benchmarks", "Build all benchmarks");
    const run_benchmarks_step = b.step("benchmark-run", "Run all benchmarks");

    {
        const benchmark = b.addExecutable("benchmark-exponential_mem", "benchmark/exponential_mem.zig");
        benchmark.setBuildMode(mode);
        benchmark.setTarget(benchmark_target);
        benchmark.addPackage(BENCHMARK_LIB_PKG);
        benchmark.addPackage(TRACY_PKG);
        benchmark.addIncludeDir("./lib/tracy/tracy");
        benchmark.addCSourceFile(
            "./lib/tracy/tracy/TracyClient.cpp",
            &.{ "-DTRACY_ENABLE", "-fno-sanitize=undefined" },
        );
        benchmark.linkSystemLibrary("c++");

        const benchmark_install = b.addInstallArtifact(benchmark);
        build_benchmarks_step.dependOn(&benchmark_install.step);

        const benchmark_run = benchmark.run();
        const run_step = b.step("benchmark-exponential_mem", "Run the exponential mem benchmark");
        run_step.dependOn(&benchmark_run.step);

        run_benchmarks_step.dependOn(&benchmark_run.step);
    }
}
