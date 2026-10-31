"""Regressions for failure propagation and stale-result removal in the runner."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("ci_run_tests", Path(__file__).parents[1] / "run_tests.py")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def exercise(self, mode, failed_step=None):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sdk, build = root / "sdk", root / "build"
            for relative in ("include/Common.h", "include/TableSession.h", "lib/libiotdb_session.so"):
                path = sdk / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("test fixture")
            build.mkdir()
            reports = ["ut-results.xml", "smoke-results.xml", "coverage-summary.json",
                       "sanitizer-results.xml", "sanitizer.log", "release-results.xml", "summary.md"]
            for name in reports:
                (build / name).write_text("old successful report")
            counter = build / "tests/unit/old.gcda"
            counter.parent.mkdir(parents=True)
            counter.write_text("stale counters")
            called = []

            def execute(name, command, directory, env):
                called.append(name)
                self.assertNotIn("GTEST_FILTER", env)
                if name == "configure":
                    for report in reports:
                        self.assertFalse((build / report).exists(), report)
                if name == "ut":
                    self.assertFalse(counter.exists())
                if name == "report-check":
                    # Even a valid report cannot cancel a command's failed status.
                    (build / "summary.md").write_text("Report contents are valid.\n")
                return name != failed_step

            args = ["run_tests.py", mode, "--build-dir", str(build), "--sdk-root", str(sdk)]
            with patch.object(sys, "argv", args), \
                 patch.dict(os.environ, {"GTEST_FILTER": "OneTest.*"}, clear=True), \
                 patch.object(runner.shutil, "which", return_value="/usr/bin/tool"), \
                 patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "version\n", "")), \
                 patch.object(runner, "run_step", side_effect=execute):
                result = runner.main()
            summary = (build / "summary.md").read_text() if (build / "summary.md").exists() else ""
            return result, called, summary

    def test_failed_unit_command_is_not_hidden_by_successful_coverage_and_reports(self):
        code, called, summary = self.exercise("coverage", "ut")
        self.assertEqual(code, 1)
        self.assertIn("coverage", called)
        self.assertIn("report-check", called)
        self.assertIn("FAILED", summary)

    def test_each_smoke_coverage_and_report_failure_blocks_success(self):
        for step in ("smoke", "coverage", "report-check"):
            with self.subTest(step=step):
                code, _, summary = self.exercise("coverage", step)
                self.assertEqual(code, 1)
                self.assertIn("FAILED", summary)

    def test_failed_sanitizer_process_blocks_success_despite_valid_final_xml(self):
        code, called, summary = self.exercise("sanitizer", "sanitizer")
        self.assertEqual(code, 1)
        self.assertIn("report-check", called)
        self.assertIn("FAILED", summary)

    def test_failed_build_does_not_run_stale_binary_or_validate_old_reports(self):
        code, called, _ = self.exercise("coverage", "build")
        self.assertEqual(code, 1)
        self.assertNotIn("ut", called)
        self.assertNotIn("report-check", called)

    def test_complete_success_clears_stale_results_and_filters(self):
        for mode in ("coverage", "sanitizer", "release"):
            with self.subTest(mode=mode):
                code, _, summary = self.exercise(mode)
                self.assertEqual(code, 0)
                self.assertNotIn("FAILED", summary)

    def test_release_failure_is_not_hidden_by_successful_test_reports(self):
        for step in ("integration", "install-check", "report-check"):
            with self.subTest(step=step):
                code, called, summary = self.exercise("release", step)
                self.assertEqual(code, 1)
                self.assertIn("install-check", called)
                self.assertIn("report-check", called)
                self.assertIn("FAILED", summary)

    @unittest.skipUnless(
        shutil.which("cmake") and Path("/usr/bin/g++").is_file()
        and Path("/usr/bin/g++-11").is_file(),
        "requires CMake and the Linux CI GCC 11 toolchain",
    )
    def test_reused_build_retains_instrumentation_after_compiler_change(self):
        """A successful CMake reconfigure can silently discard command-line flags."""
        real_run = subprocess.run
        real_step = runner.run_step
        real_which = shutil.which
        for mode, compile_flags, link_flags in (
            ("coverage", "--coverage -fprofile-update=atomic", "--coverage"),
            ("sanitizer", "-fsanitize=address,undefined -fno-omit-frame-pointer -fno-pie",
             "-fsanitize=address,undefined -no-pie"),
        ):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                project, sdk, build = root / "project", root / "sdk", root / "build"
                project.mkdir()
                (project / "CMakeLists.txt").write_text(
                    "cmake_minimum_required(VERSION 3.16)\n"
                    "project(CompilerChangeRegression LANGUAGES CXX)\n"
                    "add_executable(unit_tests main.cpp)\n"
                )
                (project / "main.cpp").write_text("int main() { return 0; }\n")
                for relative in ("include/Common.h", "include/TableSession.h",
                                 "lib/libiotdb_session.so"):
                    path = sdk / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("unused dependency fixture")
                original = real_run([
                    "cmake", "-S", str(project), "-B", str(build),
                    "-DCMAKE_CXX_COMPILER=/usr/bin/g++",
                ], capture_output=True, text=True, check=False)
                self.assertEqual(original.returncode, 0, original.stdout + original.stderr)
                configured = []

                def execute(name, command, directory, env):
                    if name == "configure":
                        success = real_step(name, command, directory, env)
                        self.assertTrue(success, (build / "configure.log").read_text())
                        cache = dict(line.split("=", 1) for line in
                                     (build / "CMakeCache.txt").read_text().splitlines()
                                     if ":" in line and "=" in line and not line.startswith("//"))
                        self.assertEqual(cache.get("CMAKE_CXX_FLAGS:STRING"), compile_flags)
                        self.assertEqual(cache.get("CMAKE_EXE_LINKER_FLAGS:STRING"), link_flags)
                        self.assertEqual(cache.get("CMAKE_BUILD_TYPE:STRING"), "Debug")
                        configured.append(name)
                        return success
                    if name == "report-check":
                        (build / "summary.md").write_text("Report contents are valid.\n")
                    return True

                def run(command, *args, **kwargs):
                    if command == ["gcovr", "--version"]:
                        return subprocess.CompletedProcess(command, 0, "gcovr fixture\n", "")
                    return real_run(command, *args, **kwargs)

                def which(command):
                    return "gcovr fixture" if command == "gcovr" else real_which(command)

                args = ["run_tests.py", mode, "--build-dir", str(build),
                        "--sdk-root", str(sdk)]
                with patch.object(sys, "argv", args), \
                     patch.dict(os.environ, {"CXX": "/usr/bin/g++-11", "GITHUB_STEP_SUMMARY": ""}), \
                     patch.object(runner, "ROOT", project), \
                     patch.object(runner.shutil, "which", side_effect=which), \
                     patch.object(runner.subprocess, "run", side_effect=run), \
                     patch.object(runner, "run_step", side_effect=execute):
                    self.assertEqual(runner.main(), 0)
                self.assertEqual(configured, ["configure"])


if __name__ == "__main__":
    unittest.main()
