#!/usr/bin/env python3
"""Fail closed when the unit-test, smoke, coverage, or sanitizer reports disagree."""

import argparse
from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
import re
import sys
import xml.etree.ElementTree as ET


DEFAULT_SCOPE = Path(__file__).resolve().parent / "coverage-scope.txt"
SMOKE_TESTS = {"WebApiSmoke", "PlatformConfigurationStatic", "EnvironmentPreflightHelp"}
RELEASE_TESTS = SMOKE_TESTS | {"EnvironmentPreflightCli", "GenericFsHarnessHelp", "IoTDBClientHarnessHelp"}


class ReportError(ValueError):
    """A missing, incomplete, inconsistent, or failing CI report."""


@dataclass(frozen=True)
class TestReport:
    count: int
    names: frozenset
    suites: int


def _read(path):
    try:
        return Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error


def _xml_integer(element, name, path, required=False):
    value = element.get(name)
    if value is None and not required:
        return 0
    if value is None or not re.fullmatch(r"[0-9]+", value):
        raise ReportError(f"{path}: {element.tag} has invalid or missing {name}")
    try:
        return int(value)
    except ValueError as error:
        raise ReportError(f"{path}: {element.tag} has oversized {name}") from error


def check_test_xml(path, *, gtest=False):
    """Check all counters and actual testcase nodes, not just the top-level status."""
    text = _read(path)
    # These reports never need custom entities or a DTD.
    if "<!DOCTYPE" in text or "<!ENTITY" in text:
        raise ReportError(f"{path}: XML declarations for entities or DTDs are not allowed")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as error:
        raise ReportError(f"{path}: malformed XML: {error}") from error
    expected_root = "testsuites" if gtest else "testsuite"
    if root.tag != expected_root:
        raise ReportError(f"{path}: expected {expected_root} XML root")
    containers = [node for node in root.iter() if node.tag in {"testsuite", "testsuites"}]
    for container in containers:
        declared = _xml_integer(container, "tests", path, required=True)
        actual = len(list(container.iter("testcase")))
        if declared != actual:
            raise ReportError(f"{path}: declared {declared} tests but contains {actual} testcases")
        for counter in ("failures", "errors", "disabled", "skipped"):
            if _xml_integer(container, counter, path):
                raise ReportError(f"{path}: {container.tag} reports {counter}")
    cases = list(root.iter("testcase"))
    if not cases:
        raise ReportError(f"{path}: no tests executed")
    if any(node.tag in {"failure", "error", "skipped", "disabled"} for node in root.iter()):
        raise ReportError(f"{path}: testcase contains a failure, error, skip, or disabled result")
    suites = list(root.iter("testsuite"))
    if sum(len(suite.findall("testcase")) for suite in suites) != len(cases):
        raise ReportError(f"{path}: testcase is outside a testsuite")
    names = set()
    for case in cases:
        name = case.get("name")
        if not name or not case.get("classname"):
            raise ReportError(f"{path}: testcase is missing its name or classname")
        if case.get("status") != "run" or (gtest and case.get("result") != "completed"):
            raise ReportError(f"{path}: testcase {name} did not complete")
        identity = f"{case.get('classname')}.{name}" if gtest else name
        if identity in names:
            raise ReportError(f"{path}: duplicate testcase {identity}")
        names.add(identity)
    if gtest and _xml_integer(root, "random_seed", path, required=True) != 925:
        raise ReportError(f"{path}: final sanitizer XML must have random_seed=925")
    return TestReport(len(cases), frozenset(names), len(suites))


def _filename(value, description):
    if (not isinstance(value, str) or not value or "\\" in value
            or PurePosixPath(value).is_absolute() or ".." in PurePosixPath(value).parts
            or str(PurePosixPath(value)) != value):
        raise ReportError(f"{description}: expected a normalized repository-relative filename")
    return value


def _scope(path):
    names = []
    for raw in _read(path).splitlines():
        value = raw.strip()
        if value and not value.startswith("#"):
            names.append(_filename(value, str(path)))
    if not names or len(names) != len(set(names)):
        raise ReportError(f"{path}: coverage scope must be nonempty with no duplicate files")
    return set(names)


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReportError(f"coverage JSON contains duplicate key {key}")
        result[key] = value
    return result


def _invalid_json_constant(value):
    raise ReportError(f"coverage JSON contains non-finite number {value}")


def _counts(value, description):
    if not isinstance(value, dict):
        raise ReportError(f"{description}: expected an object with line counts")
    total = value.get("line_total")
    covered = value.get("line_covered")
    if (type(total) is not int or type(covered) is not int
            or not 0 < total <= sys.maxsize or not 0 <= covered <= total):
        raise ReportError(f"{description}: invalid line_total or line_covered")
    return covered, total


def check_coverage(build_dir, scope_file=DEFAULT_SCOPE):
    build_dir = Path(build_dir)
    unit = check_test_xml(build_dir / "ut-results.xml")
    smoke = check_test_xml(build_dir / "smoke-results.xml")
    if smoke.names != SMOKE_TESTS:
        raise ReportError("smoke-results.xml: expected exactly " + ", ".join(sorted(SMOKE_TESTS)))
    path = build_dir / "coverage-summary.json"
    try:
        summary = json.loads(_read(path), object_pairs_hook=_json_object,
                             parse_constant=_invalid_json_constant)
    except (ValueError, RecursionError) as error:
        raise ReportError(f"{path}: malformed JSON: {error}") from error
    covered, total = _counts(summary, str(path))
    files = summary.get("files")
    if not isinstance(files, list) or not files:
        raise ReportError(f"{path}: missing nonempty per-file coverage")
    names = set()
    summed_covered = summed_total = 0
    for record in files:
        file_covered, file_total = _counts(record, str(path))
        filename = _filename(record.get("filename"), str(path))
        if filename in names:
            raise ReportError(f"{path}: duplicate coverage file {filename}")
        names.add(filename)
        summed_covered += file_covered
        summed_total += file_total
    if (covered, total) != (summed_covered, summed_total):
        raise ReportError(f"{path}: total line counts disagree with per-file sums")
    expected = _scope(scope_file)
    if names != expected:
        missing = ", ".join(sorted(expected - names)) or "none"
        extra = ", ".join(sorted(names - expected)) or "none"
        raise ReportError(f"{path}: coverage scope mismatch; missing: {missing}; extra: {extra}")
    if covered * 100 < total * 95:
        raise ReportError(f"line coverage {covered}/{total} ({100 * covered / total:.4f}%) is below 95%")
    return (f"Coverage gate passed: {unit.count} UTs, {smoke.count} smoke tests; "
            f"{covered}/{total} lines ({100 * covered / total:.2f}%), {len(names)} files.")


SANITIZER_DIAGNOSTIC = re.compile(
    r"AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer|ThreadSanitizer|MemorySanitizer"
    r"|\bruntime error:|\bDEADLYSIGNAL\b|\[\s*(?:FAILED|SKIPPED)\s*\]"
    r"|\bYOU HAVE\s+\d+\s+DISABLED TEST|Note: Google Test filter|Note: This is test shard",
    re.IGNORECASE)


def check_sanitizer(build_dir):
    build_dir = Path(build_dir)
    report = check_test_xml(build_dir / "sanitizer-results.xml", gtest=True)
    path = build_dir / "sanitizer.log"
    log = _read(path)
    if SANITIZER_DIAGNOSTIC.search(log):
        raise ReportError(f"{path}: failure, skip, filtering, or sanitizer diagnostic in log")
    iterations = list(re.finditer(r"^Repeating all tests \(iteration (\d+)\) \. \. \.$", log, re.MULTILINE))
    if [match[1] for match in iterations] != [str(number) for number in range(1, 11)]:
        raise ReportError(f"{path}: expected exactly iterations 1 through 10 in order")
    prefix = log[:iterations[0].start()]
    if re.search(r"^\[\s*(?:RUN|OK|PASSED|==========)\s*\]", prefix, re.MULTILINE):
        raise ReportError(f"{path}: test output before the first iteration")
    for index, marker in enumerate(iterations):
        stop = iterations[index + 1].start() if index + 1 < len(iterations) else len(log)
        block = log[marker.end():stop]
        seeds = list(re.finditer(r"^Note: Randomizing tests' orders with a seed of (\d+) \.$", block, re.MULTILINE))
        if [seed[1] for seed in seeds] != [str(916 + index)]:
            raise ReportError(f"{path}: iteration {index + 1} has wrong or missing shuffle seed")
        started = list(re.finditer(r"^\[==========\] Running (\d+) tests? from (\d+) test suites?\.$", block, re.MULTILINE))
        ended = list(re.finditer(r"^\[==========\] (\d+) tests? from (\d+) test suites? ran\. \([^\n]*\)$", block, re.MULTILINE))
        passed = list(re.finditer(r"^\[\s*PASSED\s*\] (\d+) tests?\.$", block, re.MULTILINE))
        expected = [(str(report.count), str(report.suites))]
        if ([match.groups() for match in started] != expected
                or [match.groups() for match in ended] != expected
                or [match[1] for match in passed] != [str(report.count)]):
            raise ReportError(f"{path}: iteration {index + 1} has incomplete or inconsistent test totals")
        events = list(re.finditer(r"^\[\s*(RUN|OK)\s*\]\s+(\S+)[^\n]*$", block, re.MULTILINE))
        completed = []
        if len(events) != 2 * report.count:
            raise ReportError(f"{path}: iteration {index + 1} has missing testcase records")
        if not (seeds[0].end() < started[0].start() < events[0].start()
                and events[-1].end() < ended[0].start() < passed[0].start()):
            raise ReportError(f"{path}: iteration {index + 1} has out-of-order completion records")
        for offset in range(0, len(events), 2):
            run, ok = (event.groups() for event in events[offset:offset + 2])
            if run[0] != "RUN" or ok != ("OK", run[1]):
                raise ReportError(f"{path}: iteration {index + 1} has an unfinished testcase")
            completed.append(run[1])
        if len(set(completed)) != report.count or set(completed) != report.names:
            raise ReportError(f"{path}: iteration {index + 1} testcases disagree with final XML")
    return f"Sanitizer gate passed: {report.count} UTs × 10 shuffled rounds; seeds 916–925; no diagnostics."


def check_release(build_dir):
    report = check_test_xml(Path(build_dir) / "release-results.xml")
    if report.names != RELEASE_TESTS:
        raise ReportError("release-results.xml: expected exactly " + ", ".join(sorted(RELEASE_TESTS)))
    return f"Release integration reports passed: {report.count} tests; no failures or skips."


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("coverage", "sanitizer", "release"))
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--summary-file", type=Path)
    args = parser.parse_args(argv)
    try:
        check = {"coverage": check_coverage, "sanitizer": check_sanitizer, "release": check_release}
        message = check[args.mode](args.build_dir)
        if args.summary_file is not None:
            with args.summary_file.open("a", encoding="utf-8") as summary:
                summary.write(message + "\n")
        print(message)
        return 0
    except (ReportError, OSError) as error:
        print(f"CI report check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
