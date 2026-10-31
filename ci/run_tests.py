#!/usr/bin/env python3
"""Run the same C++ checks locally and in GitHub Actions, preserving failures."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
SMOKE_FILTER = "^(WebApiSmoke|PlatformConfigurationStatic|EnvironmentPreflightHelp)$"
RELEASE_FILTER = "^(WebApiSmoke|PlatformConfigurationStatic|EnvironmentPreflightHelp|EnvironmentPreflightCli|GenericFsHarnessHelp|IoTDBClientHarnessHelp)$"


def run_step(name, command, directory, env):
    print(f"[{name}] {' '.join(map(str, command))}", flush=True)
    log_path = directory / f"{name}.log"
    with log_path.open("w") as log:
        result = subprocess.run(list(map(str, command)), cwd=ROOT, env=env,
                                stdout=log, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        print(log_path.read_text(errors="replace")[-20000:], file=sys.stderr)
        print(f"{name} failed ({result.returncode}); full log: {log_path}", file=sys.stderr)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("coverage", "sanitizer", "release"))
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--sdk-root", type=Path,
                        default=os.environ.get("IOTDB_SDK_ROOT"))
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    build = args.build_dir.resolve()
    if build == ROOT or args.jobs < 1:
        parser.error("use a separate build directory and at least one build job")
    if not args.sdk_root:
        parser.error("set IOTDB_SDK_ROOT or pass --sdk-root")
    sdk = args.sdk_root.resolve()
    for relative in ("include/Common.h", "include/TableSession.h", "lib/libiotdb_session.so"):
        if not (sdk / relative).is_file():
            parser.error(f"SDK is missing {relative}: {sdk}")
    required = ["cmake", "ctest", os.environ.get("CXX", "g++-11")]
    if args.mode == "coverage":
        required.append("gcovr")
    for tool in required:
        if not shutil.which(tool):
            parser.error(f"required executable is unavailable: {tool}")
    build.mkdir(parents=True, exist_ok=True)
    # A failed/aborted run must never be validated against an earlier report.
    for name in ("ut-results.xml", "smoke-results.xml", "coverage-summary.json",
                 "coverage.xml", "coverage.html", "coverage.txt",
                 "sanitizer-results.xml", "sanitizer.log", "release-results.xml", "summary.md"):
        (build / name).unlink(missing_ok=True)
    env = dict(os.environ)
    dependency_prefix = sdk.parent
    # Force the verified dependency prefix ahead of any runner-installed packages.
    env["CMAKE_PREFIX_PATH"] = str(dependency_prefix)
    env["PKG_CONFIG_PATH"] = str(dependency_prefix / "lib/pkgconfig")
    env["LD_LIBRARY_PATH"] = f"{dependency_prefix / 'lib'}:{sdk / 'lib'}"
    env["ECCODES_DEFINITION_PATH"] = str(dependency_prefix / "share/eccodes/definitions")
    env["ECCODES_SAMPLES_PATH"] = str(dependency_prefix / "share/eccodes/samples")
    for key in ("GTEST_FILTER", "GTEST_REPEAT", "GTEST_TOTAL_SHARDS", "GTEST_SHARD_INDEX",
                "GTEST_SHARD_STATUS_FILE", "GTEST_ALSO_RUN_DISABLED_TESTS"):
        env.pop(key, None)
    compiler = env.get("CXX", "g++-11")
    if args.mode == "coverage":
        flags = "--coverage -fprofile-update=atomic"
        link_flags = "--coverage"
    elif args.mode == "sanitizer":
        flags = "-fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie"
        link_flags = "-fsanitize=address,undefined -no-pie"
        env["ASAN_OPTIONS"] = "detect_leaks=1"
        env["UBSAN_OPTIONS"] = "print_stacktrace=1:halt_on_error=1"
    else:
        flags = link_flags = ""
    versions = {}
    for tool in required:
        result = subprocess.run([tool, "--version"], env=env, capture_output=True,
                                text=True, check=False)
        versions[tool] = result.stdout.splitlines()[:1]
    versions["sdk_root"] = str(sdk)
    versions["mode"] = args.mode
    (build / "toolchain.json").write_text(json.dumps(versions, indent=2) + "\n")
    # A compiler change in an existing cache makes CMake configure again without
    # our instrumentation flags. Start with a fresh cache on every invocation.
    configured = run_step("configure", [
        "cmake", "--fresh", "-S", ROOT, "-B", build,
        f"-DCMAKE_BUILD_TYPE={'Release' if args.mode == 'release' else 'Debug'}", "-DBUILD_TESTS=ON",
        "-DENABLE_FUSE_INTEGRATION_TESTS=OFF", f"-DCMAKE_CXX_COMPILER={compiler}",
        f"-DCMAKE_PREFIX_PATH={dependency_prefix}",
        f"-DGTest_DIR={dependency_prefix / 'lib/cmake/GTest'}",
        f"-DIOTDB_ROOT={sdk}", f"-DIOTDB_SESSION_LIB={sdk / 'lib/libiotdb_session.so'}",
        f"-DCMAKE_CXX_FLAGS={flags}", f"-DCMAKE_EXE_LINKER_FLAGS={link_flags}",
    ], build, env)
    if not configured:
        return 1
    command = ["cmake", "--build", build, "--parallel", args.jobs]
    if args.mode == "sanitizer":
        command += ["--target", "unit_tests"]
    elif args.mode == "release":
        command += ["--target", "iotdb_fs", "iotdb_fs_web", "grib_client",
                    "iotdb_fs_preflight", "iotdb_client_integration", "generic_fs_integration"]
    if not run_step("build", command, build, env):
        return 1
    if args.mode == "coverage":
        for path in (build / "tests/unit").rglob("*.gcda"):
            path.unlink()
        unit_ok = run_step("ut", ["ctest", "--test-dir", build, "-L", "unit",
            "--no-tests=error", "--output-on-failure", "-j", args.jobs,
            "--output-junit", "ut-results.xml"], build, env)
        smoke_ok = run_step("smoke", ["ctest", "--test-dir", build, "-R", SMOKE_FILTER,
            "--no-tests=error", "--output-on-failure", "--output-junit", "smoke-results.xml"],
            build, env)
        coverage_ok = run_step("coverage", ["gcovr", "--root", ROOT,
            "--filter", "src/", "--filter", "include/iotdbfs/",
            "--json-summary", build / "coverage-summary.json",
            "--html-details", build / "coverage.html", "--xml", build / "coverage.xml",
            "--txt", build / "coverage.txt", "--fail-under-line", "95", build / "tests/unit"],
            build, env)
        checks_ok = unit_ok and smoke_ok and coverage_ok
    elif args.mode == "sanitizer":
        checks_ok = run_step("sanitizer", [build / "bin/unit_tests", "--gtest_filter=*",
            "--gtest_shuffle", "--gtest_random_seed=916", "--gtest_repeat=10",
            f"--gtest_output=xml:{build / 'sanitizer-results.xml'}"], build, env)
    else:
        integration_ok = run_step("integration", ["ctest", "--test-dir", build,
            "-R", RELEASE_FILTER, "--no-tests=error", "--output-on-failure",
            "--output-junit", "release-results.xml"], build, env)
        install_ok = run_step("install-check", [sys.executable, ROOT / "ci/check_install.py",
            "--build-dir", build], build, env)
        checks_ok = integration_ok and install_ok
    reports_ok = run_step("report-check", [sys.executable, ROOT / "ci/check_reports.py",
        args.mode, "--build-dir", build, "--summary-file", build / "summary.md"], build, env)
    summary = build / "summary.md"
    if not summary.exists():
        summary.write_text(f"{args.mode}: FAILED; inspect the attached logs.\n")
    elif not (checks_ok and reports_ok):
        summary.write_text(f"{args.mode}: FAILED; at least one command or report check failed.\n\n"
                           + summary.read_text())
    if env.get("GITHUB_STEP_SUMMARY"):
        with Path(env["GITHUB_STEP_SUMMARY"]).open("a") as output:
            output.write(summary.read_text() + "\n")
    print(summary.read_text(), end="")
    return 0 if checks_ok and reports_ok else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"CI execution error: {error}", file=sys.stderr)
        sys.exit(1)
