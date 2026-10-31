import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

from ci import check_reports as reports


class ReportChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.build = Path(self.directory.name)
        self.scope = self.build / "scope.txt"
        self.scope.write_text("# Expected business sources\nsrc/one.cpp\n\nsrc/two.cpp\n", encoding="utf-8")
        self.coverage = {
            "line_total": 200, "line_covered": 190, "line_percent": 95.0,
            "files": [
                {"filename": "src/one.cpp", "line_total": 100, "line_covered": 100},
                {"filename": "src/two.cpp", "line_total": 100, "line_covered": 90},
            ],
        }
        self.write_coverage()
        self.write_ctest("ut-results.xml", ["Example.First", "Example.Second"])
        self.write_ctest("smoke-results.xml", sorted(reports.SMOKE_TESTS))
        self.write_gtest()
        self.log = self.make_log()
        self.write_log(self.log)

    def write_coverage(self):
        (self.build / "coverage-summary.json").write_text(json.dumps(self.coverage), encoding="utf-8")

    def write_ctest(self, filename, names):
        root = ET.Element("testsuite", tests=str(len(names)), failures="0", disabled="0", skipped="0")
        for name in names:
            ET.SubElement(root, "testcase", name=name, classname=name, status="run")
        ET.ElementTree(root).write(self.build / filename, encoding="utf-8", xml_declaration=True)

    def write_gtest(self):
        root = ET.Element("testsuites", tests="2", failures="0", disabled="0", errors="0", random_seed="925")
        suite = ET.SubElement(root, "testsuite", name="Example", tests="2", failures="0",
                              errors="0", skipped="0", disabled="0")
        for name in ("First", "Second"):
            ET.SubElement(suite, "testcase", name=name, classname="Example", status="run", result="completed")
        ET.ElementTree(root).write(self.build / "sanitizer-results.xml", encoding="utf-8", xml_declaration=True)

    def make_log(self):
        blocks = []
        for iteration in range(1, 11):
            names = ["Example.First", "Example.Second"]
            if iteration % 2 == 0:
                names.reverse()
            block = [f"Repeating all tests (iteration {iteration}) . . .", "",
                     f"Note: Randomizing tests' orders with a seed of {915 + iteration} .",
                     "[==========] Running 2 tests from 1 test suite."]
            for name in names:
                block.extend([f"[ RUN      ] {name}", f"[       OK ] {name} (0 ms)"])
            block.extend(["[==========] 2 tests from 1 test suite ran. (1 ms total)",
                          "[  PASSED  ] 2 tests.", ""])
            blocks.append("\n".join(block))
        return "Running main() from gtest_main.cc\n\n" + "\n".join(blocks)

    def write_log(self, text):
        (self.build / "sanitizer.log").write_text(text, encoding="utf-8")

    def edit_xml(self, filename, edit):
        path = self.build / filename
        tree = ET.parse(path)
        edit(tree.getroot())
        tree.write(path, encoding="utf-8", xml_declaration=True)

    def check_coverage(self):
        return reports.check_coverage(self.build, self.scope)

    def test_exact_95_percent_and_all_required_reports_pass(self):
        message = self.check_coverage()
        self.assertIn("2 UTs, 3 smoke tests", message)
        self.assertIn("190/200 lines (95.00%)", message)

    def test_release_requires_exact_integration_test_set(self):
        self.write_ctest("release-results.xml", sorted(reports.RELEASE_TESTS))
        self.assertIn("6 tests", reports.check_release(self.build))
        self.write_ctest("release-results.xml", sorted(reports.SMOKE_TESTS))
        with self.assertRaisesRegex(reports.ReportError, "expected exactly"):
            reports.check_release(self.build)

    def test_release_skipped_or_failed_cases_are_rejected(self):
        for tag in ("failure", "skipped"):
            with self.subTest(tag=tag):
                self.write_ctest("release-results.xml", sorted(reports.RELEASE_TESTS))
                self.edit_xml("release-results.xml", lambda root: ET.SubElement(root.find("testcase"), tag))
                with self.assertRaises(reports.ReportError):
                    reports.check_release(self.build)

    def test_coverage_uses_counts_not_rounded_percentage(self):
        self.coverage.update(line_total=2500, line_covered=2374, line_percent=95.0)
        self.coverage["files"][0].update(line_total=1000, line_covered=950)
        self.coverage["files"][1].update(line_total=1500, line_covered=1424)
        self.write_coverage()
        with self.assertRaisesRegex(reports.ReportError, "94.9600%.*below 95%"):
            self.check_coverage()

    def test_each_required_coverage_report_must_exist(self):
        for filename in ("ut-results.xml", "smoke-results.xml", "coverage-summary.json"):
            with self.subTest(filename=filename):
                path = self.build / filename
                contents = path.read_bytes()
                path.unlink()
                with self.assertRaisesRegex(reports.ReportError, "cannot read"):
                    self.check_coverage()
                path.write_bytes(contents)

    def test_malformed_xml_and_wrong_document_roots_fail(self):
        path = self.build / "ut-results.xml"
        for contents in ("", "<testsuite", "<other/>", "<testsuites tests='0'/>"):
            with self.subTest(contents=contents):
                path.write_text(contents, encoding="utf-8")
                with self.assertRaises(reports.ReportError):
                    self.check_coverage()

    def test_xml_entities_are_not_accepted(self):
        (self.build / "ut-results.xml").write_text(
            '<!DOCTYPE testsuite [<!ENTITY x "value">]><testsuite tests="0"/>', encoding="utf-8")
        with self.assertRaisesRegex(reports.ReportError, "entities or DTDs"):
            self.check_coverage()

    def test_empty_test_suite_is_not_a_success(self):
        self.write_ctest("ut-results.xml", [])
        with self.assertRaisesRegex(reports.ReportError, "no tests executed"):
            self.check_coverage()

    def test_declared_test_totals_must_match_testcase_elements(self):
        self.edit_xml("ut-results.xml", lambda root: root.set("tests", "3"))
        with self.assertRaisesRegex(reports.ReportError, "declared 3 tests.*2 testcases"):
            self.check_coverage()

    def test_test_counters_reject_missing_negative_and_nonnumeric_values(self):
        for value in (None, "-1", "2.0", "true", ""):
            with self.subTest(value=value):
                self.write_ctest("ut-results.xml", ["Example.First", "Example.Second"])
                self.edit_xml("ut-results.xml", lambda root: root.attrib.pop("tests") if value is None
                              else root.set("tests", value))
                with self.assertRaisesRegex(reports.ReportError, "invalid or missing tests"):
                    self.check_coverage()

    def test_nonzero_failure_error_skip_or_disabled_counters_fail(self):
        for counter in ("failures", "errors", "skipped", "disabled"):
            with self.subTest(counter=counter):
                self.write_ctest("ut-results.xml", ["Example.First", "Example.Second"])
                self.edit_xml("ut-results.xml", lambda root: root.set(counter, "1"))
                with self.assertRaisesRegex(reports.ReportError, counter):
                    self.check_coverage()

    def test_failed_nodes_are_detected_even_when_summary_claims_zero(self):
        for tag in ("failure", "error", "skipped", "disabled"):
            with self.subTest(tag=tag):
                self.write_ctest("ut-results.xml", ["Example.First", "Example.Second"])
                self.edit_xml("ut-results.xml", lambda root: ET.SubElement(root.find("testcase"), tag))
                with self.assertRaisesRegex(reports.ReportError, "testcase contains"):
                    self.check_coverage()

    def test_not_run_and_duplicate_testcases_fail(self):
        self.edit_xml("ut-results.xml", lambda root: root.find("testcase").set("status", "notrun"))
        with self.assertRaisesRegex(reports.ReportError, "did not complete"):
            self.check_coverage()
        self.write_ctest("ut-results.xml", ["Example.First", "Example.First"])
        with self.assertRaisesRegex(reports.ReportError, "duplicate testcase"):
            self.check_coverage()

    def test_smoke_set_must_be_exact_not_just_the_same_count(self):
        for names in (["WebApiSmoke", "PlatformConfigurationStatic", "Unrelated"],
                      ["WebApiSmoke"], sorted(reports.SMOKE_TESTS) + ["Extra"]):
            with self.subTest(names=names):
                self.write_ctest("smoke-results.xml", names)
                with self.assertRaisesRegex(reports.ReportError, "expected exactly"):
                    self.check_coverage()

    def test_malformed_and_nonfinite_json_fail(self):
        for contents in ("", "{", "[]", "null", '{"line_total": NaN}'):
            with self.subTest(contents=contents):
                (self.build / "coverage-summary.json").write_text(contents, encoding="utf-8")
                with self.assertRaises(reports.ReportError):
                    self.check_coverage()

    def test_duplicate_json_keys_do_not_silently_override_counts(self):
        (self.build / "coverage-summary.json").write_text(
            '{"line_total": 200, "line_total": 100, "line_covered": 100}', encoding="utf-8")
        with self.assertRaisesRegex(reports.ReportError, "duplicate key line_total"):
            self.check_coverage()

    def test_coverage_counts_must_be_positive_integral_and_bounded(self):
        for total, covered in ((0, 0), (-1, 0), (100, -1), (100, 101),
                               (True, 1), (100, True), (100.0, 95), (100, 95.0),
                               ("100", 95), (100, None)):
            with self.subTest(total=total, covered=covered):
                self.coverage.update(line_total=total, line_covered=covered)
                self.write_coverage()
                with self.assertRaisesRegex(reports.ReportError, "invalid line_total or line_covered"):
                    self.check_coverage()

    def test_missing_and_invalid_per_file_counts_fail(self):
        for files in (None, [], {}, [None], [{"filename": "src/one.cpp"}]):
            with self.subTest(files=files):
                self.coverage["files"] = files
                self.write_coverage()
                with self.assertRaises(reports.ReportError):
                    self.check_coverage()

    def test_per_file_sum_must_equal_root_totals(self):
        self.coverage["files"][0]["line_covered"] = 99
        self.write_coverage()
        with self.assertRaisesRegex(reports.ReportError, "per-file sums"):
            self.check_coverage()

    def test_shrinking_scope_cannot_turn_partial_coverage_into_success(self):
        self.coverage.update(line_total=100, line_covered=100)
        self.coverage["files"].pop()
        self.write_coverage()
        with self.assertRaisesRegex(reports.ReportError, "missing: src/two.cpp"):
            self.check_coverage()

    def test_unexpected_and_duplicate_coverage_files_fail(self):
        self.coverage["files"][1]["filename"] = "src/unexpected.cpp"
        self.write_coverage()
        with self.assertRaisesRegex(reports.ReportError, "extra: src/unexpected.cpp"):
            self.check_coverage()
        self.coverage["files"][1]["filename"] = "src/one.cpp"
        self.write_coverage()
        with self.assertRaisesRegex(reports.ReportError, "duplicate coverage file"):
            self.check_coverage()

    def test_absolute_non_normalized_or_traversing_coverage_paths_fail(self):
        for name in ("/src/one.cpp", "../src/one.cpp", "src/./one.cpp", "src//one.cpp", "src\\one.cpp", ""):
            with self.subTest(name=name):
                self.coverage["files"][0]["filename"] = name
                self.write_coverage()
                with self.assertRaisesRegex(reports.ReportError, "repository-relative filename"):
                    self.check_coverage()

    def test_missing_empty_and_duplicate_scope_files_fail(self):
        self.scope.unlink()
        with self.assertRaisesRegex(reports.ReportError, "cannot read"):
            self.check_coverage()
        for contents in ("# only comments\n", "src/one.cpp\nsrc/one.cpp\n"):
            self.scope.write_text(contents, encoding="utf-8")
            with self.assertRaisesRegex(reports.ReportError, "scope must be nonempty"):
                self.check_coverage()

    def test_complete_sanitizer_rounds_with_matching_xml_pass(self):
        self.assertIn("2 UTs × 10 shuffled rounds", reports.check_sanitizer(self.build))

    def test_each_sanitizer_report_must_exist(self):
        for filename in ("sanitizer-results.xml", "sanitizer.log"):
            with self.subTest(filename=filename):
                path = self.build / filename
                contents = path.read_bytes()
                path.unlink()
                with self.assertRaisesRegex(reports.ReportError, "cannot read"):
                    reports.check_sanitizer(self.build)
                path.write_bytes(contents)

    def test_sanitizer_nested_suite_totals_must_match(self):
        self.edit_xml("sanitizer-results.xml", lambda root: root.find("testsuite").set("tests", "3"))
        with self.assertRaisesRegex(reports.ReportError, "declared 3 tests"):
            reports.check_sanitizer(self.build)

    def test_sanitizer_requires_completed_cases_and_final_shuffle_seed(self):
        self.edit_xml("sanitizer-results.xml", lambda root: root.find("testsuite/testcase").set("result", "suppressed"))
        with self.assertRaisesRegex(reports.ReportError, "did not complete"):
            reports.check_sanitizer(self.build)
        self.write_gtest()
        self.edit_xml("sanitizer-results.xml", lambda root: root.set("random_seed", "916"))
        with self.assertRaisesRegex(reports.ReportError, "random_seed=925"):
            reports.check_sanitizer(self.build)

    def test_stale_xml_with_same_count_but_other_cases_fails(self):
        self.edit_xml("sanitizer-results.xml", lambda root: root.find("testsuite/testcase").set("name", "PreviousBuild"))
        with self.assertRaisesRegex(reports.ReportError, "testcases disagree"):
            reports.check_sanitizer(self.build)

    def test_truncated_and_duplicate_round_logs_fail(self):
        for log in ("", self.log[:self.log.index("Repeating all tests (iteration 10)")],
                    self.log + self.log,
                    self.log.replace("iteration 10", "iteration 9")):
            with self.subTest(length=len(log)):
                self.write_log(log)
                with self.assertRaisesRegex(reports.ReportError, "iterations 1 through 10"):
                    reports.check_sanitizer(self.build)

    def test_tenth_round_must_finish_not_just_start(self):
        self.write_log(self.log[:self.log.rfind("[  PASSED  ]")])
        with self.assertRaisesRegex(reports.ReportError, "iteration 10.*incomplete"):
            reports.check_sanitizer(self.build)

    def test_every_round_must_use_expected_seed_and_full_test_count(self):
        for log in (self.log.replace("seed of 916", "seed of 17"),
                    self.log.replace("Running 2 tests", "Running 1 test", 1),
                    self.log.replace("2 tests from 1 test suite ran.", "1 test from 1 test suite ran.", 1),
                    self.log.replace("[  PASSED  ] 2 tests.", "[  PASSED  ] 1 test.", 1)):
            with self.subTest(log=log[:160]):
                self.write_log(log)
                with self.assertRaises(reports.ReportError):
                    reports.check_sanitizer(self.build)

    def test_early_round_failure_is_rejected_even_if_final_xml_passes(self):
        for diagnostic in ("[  FAILED  ] Example.First", "[  SKIPPED ] Example.First",
                           "YOU HAVE 1 DISABLED TEST", "Note: Google Test filter = Example.*",
                           "Note: This is test shard 1 of 2."):
            with self.subTest(diagnostic=diagnostic):
                self.write_log(self.log.replace(
                    "Repeating all tests (iteration 2)", diagnostic + "\nRepeating all tests (iteration 2)", 1))
                with self.assertRaisesRegex(reports.ReportError, "diagnostic in log"):
                    reports.check_sanitizer(self.build)

    def test_sanitizer_diagnostics_anywhere_in_log_are_rejected(self):
        for diagnostic in ("ERROR: AddressSanitizer: heap-use-after-free",
                           "ERROR: LeakSanitizer: detected memory leaks", "AddressSanitizer:DEADLYSIGNAL",
                           "source.cpp:1: runtime error: signed integer overflow",
                           "SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior"):
            with self.subTest(diagnostic=diagnostic):
                self.write_log(self.log + "\n" + diagnostic)
                with self.assertRaisesRegex(reports.ReportError, "sanitizer diagnostic"):
                    reports.check_sanitizer(self.build)

    def test_summary_counts_without_actual_testcase_records_do_not_pass(self):
        self.write_log(self.log.replace("[ RUN      ] Example.First\n", "", 1))
        with self.assertRaisesRegex(reports.ReportError, "missing testcase records"):
            reports.check_sanitizer(self.build)

    def test_completion_summaries_must_follow_testcase_execution(self):
        summary = "[  PASSED  ] 2 tests.\n"
        self.write_log(self.log.replace(summary, "", 1).replace(
            "[ RUN      ] Example.First", summary + "[ RUN      ] Example.First", 1))
        with self.assertRaisesRegex(reports.ReportError, "out-of-order completion"):
            reports.check_sanitizer(self.build)

    def test_oversized_counters_and_deep_json_are_report_errors(self):
        self.edit_xml("ut-results.xml", lambda root: root.set("tests", "9" * 5000))
        with self.assertRaises(reports.ReportError):
            self.check_coverage()
        self.write_ctest("ut-results.xml", ["Example.First", "Example.Second"])
        path = self.build / "coverage-summary.json"
        for contents in ('{"line_total":' + '9' * 5000 + '}', '[' * 2000 + ']' * 2000):
            with self.subTest(length=len(contents)):
                path.write_text(contents, encoding="utf-8")
                with self.assertRaises(reports.ReportError):
                    self.check_coverage()

    def test_duplicate_or_unfinished_cases_cannot_match_the_report(self):
        self.write_log(self.log.replace("[       OK ] Example.First", "[       OK ] Example.Second", 1))
        with self.assertRaisesRegex(reports.ReportError, "unfinished testcase"):
            reports.check_sanitizer(self.build)
        self.write_log(self.log.replace("Example.Second", "Example.First"))
        with self.assertRaisesRegex(reports.ReportError, "testcases disagree"):
            reports.check_sanitizer(self.build)

    def test_cli_appends_success_summary_and_prints_compact_result(self):
        summary = self.build / "summary.md"
        summary.write_text("Earlier result\n", encoding="utf-8")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = reports.main(["sanitizer", "--build-dir", str(self.build), "--summary-file", str(summary)])
        self.assertEqual(result, 0)
        self.assertEqual(summary.read_text(encoding="utf-8"), "Earlier result\n" + output.getvalue())
        self.assertEqual(len(output.getvalue().splitlines()), 1)

    def test_cli_rejects_bad_input_without_traceback_or_success_summary(self):
        self.write_log("incomplete")
        summary = self.build / "summary.md"
        result = subprocess.run([sys.executable, str(Path(reports.__file__)), "sanitizer",
                                 "--build-dir", str(self.build), "--summary-file", str(summary)],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("CI report check failed:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(result.stdout)
        self.assertFalse(summary.exists())

    def test_invalid_utf8_and_unwritable_summary_are_clear_errors(self):
        path = self.build / "sanitizer.log"
        path.write_bytes(b"\xff")
        with contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(reports.main(["sanitizer", "--build-dir", str(self.build)]), 1)
        self.assertIn("cannot read", error.getvalue())
        self.write_log(self.log)
        with contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(reports.main(["sanitizer", "--build-dir", str(self.build),
                                           "--summary-file", str(self.build / "missing" / "summary.md")]), 1)
        self.assertNotIn("Traceback", error.getvalue())


if __name__ == "__main__":
    unittest.main()
