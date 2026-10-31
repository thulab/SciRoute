#!/usr/bin/env python3
"""Exercise the built preflight CLI without an IoTDB server or FUSE mount."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest


BINARY = None
FS_BINARY = None
WEB_BINARY = None
PASSWORD = "preflight-cli-private-value-916"


class PreflightCliTests(unittest.TestCase):
    def setUp(self):
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("IOTDB_")
        }
        self.temp_directory = tempfile.TemporaryDirectory(prefix="preflight-cli-")
        self.addCleanup(self.temp_directory.cleanup)
        self.directory = Path(self.temp_directory.name)
        self.missing_fs = self.directory / "missing-fs"
        self.missing_web = self.directory / "missing-web"

    def run_cli(self, *arguments, environment=None):
        result = subprocess.run(
            [str(BINARY), *map(str, arguments)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=self.environment if environment is None else environment,
            timeout=15,
            check=False,
        )
        self.assertNotIn(PASSWORD, result.stdout)
        self.assertNotIn(PASSWORD, result.stderr)
        return result

    def records(self, result):
        self.assertTrue(result.stdout, result.stderr)
        records = []
        for line in result.stdout.splitlines():
            with self.subTest(output=line):
                record = json.loads(line)
                self.assertIsInstance(record, dict)
                self.assertEqual(set(record), {"check", "status", "code", "message"})
                for value in record.values():
                    self.assertIsInstance(value, str)
                self.assertIn(record["status"], {"pass", "fail", "skip"})
                self.assertTrue(record["message"])
                records.append(record)
        return records

    def valid_arguments(self):
        return [
            "--db", "preflight_ci",
            "--fs-bin", str(self.missing_fs),
            "--web-bin", str(self.missing_web),
            "--host", "127.0.0.1",
            "--pass", PASSWORD,
            "--timeout-ms", "100",
        ]

    def assert_invalid_arguments(self, arguments, environment=None):
        result = self.run_cli(*arguments, environment=environment)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        records = self.records(result)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["check"], "config")
        self.assertEqual(records[0]["status"], "fail")
        self.assertEqual(records[0]["code"], "INVALID_ARGUMENT")

    def run_without_server(self, extra_arguments=(), environment=None):
        # Keep the port reserved without listening, avoiding races with services
        # that might acquire a port between discovery and subprocess startup.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved_port:
            reserved_port.bind(("127.0.0.1", 0))
            port = reserved_port.getsockname()[1]
            result = self.run_cli(
                *self.valid_arguments(),
                "--port", str(port),
                *extra_arguments,
                environment=environment,
            )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        records = self.records(result)
        checks = {record["check"]: record for record in records}
        self.assertEqual(len(checks), len(records), "duplicate check records")
        self.assertEqual(
            set(checks),
            {
                "iotdb_port", "fuse_device", "fusermount", "fs_binary",
                "web_binary", "fs_dependencies", "web_dependencies", "iotdb_schema",
            },
        )
        self.assertEqual(checks["iotdb_port"]["status"], "fail")
        self.assertEqual(checks["iotdb_port"]["code"], "IOTDB_PORT_UNREACHABLE")
        self.assertIn(f"127.0.0.1:{port}", checks["iotdb_port"]["message"])
        self.assertEqual(checks["iotdb_schema"]["status"], "fail")
        self.assertNotEqual(checks["iotdb_schema"]["code"], "IOTDB_SCHEMA_REGION_READY")
        return checks

    def assert_binary_failure(self, checks, name, code):
        self.assertEqual(checks[f"{name}_binary"]["status"], "fail")
        self.assertEqual(checks[f"{name}_binary"]["code"], code)
        self.assertEqual(checks[f"{name}_dependencies"]["status"], "skip")
        self.assertEqual(
            checks[f"{name}_dependencies"]["code"],
            "DEPENDENCY_NOT_CHECKED_BINARY_INVALID",
        )

    def test_help(self):
        for option in ("--help", "-h"):
            with self.subTest(option=option):
                result = self.run_cli(option)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Usage:", result.stdout)
                self.assertIn("--db DATABASE", result.stdout)
                self.assertIn("--timeout-ms", result.stdout)
                self.assertEqual(result.stderr, "")

    def test_no_arguments(self):
        self.assert_invalid_arguments([])

    def test_invalid_ports(self):
        for port in ("0", "-1", "65536", "abc", "12x", "", "99999999999999999999"):
            with self.subTest(port=port):
                self.assert_invalid_arguments([*self.valid_arguments(), "--port", port])

    def test_invalid_timeouts(self):
        for timeout in ("0", "-1", "60001", "abc", "100ms", ""):
            with self.subTest(timeout=timeout):
                self.assert_invalid_arguments(
                    [*self.valid_arguments(), "--timeout-ms", timeout]
                )

    def test_invalid_database_names(self):
        for database in ("", "1database", "_database", "has space", "has-dash", "a;DROP TABLE t"):
            with self.subTest(database=database):
                self.assert_invalid_arguments([*self.valid_arguments(), "--db", database])

    def test_missing_option_values(self):
        for option in (
            "--db", "--host", "--port", "--user", "--pass",
            "--fs-bin", "--web-bin", "--timeout-ms",
        ):
            with self.subTest(option=option):
                self.assert_invalid_arguments([*self.valid_arguments(), option])

    def test_unknown_option(self):
        self.assert_invalid_arguments([*self.valid_arguments(), "--unknown", "value"])

    def test_empty_required_values(self):
        for option in ("--host", "--user", "--pass", "--fs-bin", "--web-bin"):
            with self.subTest(option=option):
                self.assert_invalid_arguments([*self.valid_arguments(), option, ""])

    def test_invalid_environment_port(self):
        environment = {**self.environment, "IOTDB_PORT": "65536", "IOTDB_PASS": PASSWORD}
        self.assert_invalid_arguments(self.valid_arguments(), environment=environment)

    def test_unreachable_port_and_missing_binaries(self):
        checks = self.run_without_server()
        self.assert_binary_failure(checks, "fs", "BINARY_MISSING")
        self.assert_binary_failure(checks, "web", "BINARY_MISSING")
        self.assertEqual(checks["fs_binary"]["message"], str(self.missing_fs))
        self.assertEqual(checks["web_binary"]["message"], str(self.missing_web))

    def test_relative_binary_paths(self):
        checks = self.run_without_server(
            ["--fs-bin", "relative-fs", "--web-bin", "relative-web"]
        )
        self.assert_binary_failure(checks, "fs", "BINARY_PATH_NOT_ABSOLUTE")
        self.assert_binary_failure(checks, "web", "BINARY_PATH_NOT_ABSOLUTE")

    def test_non_executable_binaries(self):
        non_executable = self.directory / "not-executable"
        non_executable.write_text("not an executable\n", encoding="ascii")
        non_executable.chmod(0o600)
        checks = self.run_without_server(
            ["--fs-bin", str(non_executable), "--web-bin", str(non_executable)]
        )
        self.assert_binary_failure(checks, "fs", "BINARY_NOT_EXECUTABLE")
        self.assert_binary_failure(checks, "web", "BINARY_NOT_EXECUTABLE")

    def test_real_binaries_and_runtime_dependencies(self):
        checks = self.run_without_server(
            ["--fs-bin", str(FS_BINARY), "--web-bin", str(WEB_BINARY)]
        )
        for name, path in (("fs", FS_BINARY), ("web", WEB_BINARY)):
            with self.subTest(binary=name):
                self.assertEqual(checks[f"{name}_binary"]["status"], "pass")
                self.assertEqual(checks[f"{name}_binary"]["code"], "BINARY_READY")
                self.assertEqual(checks[f"{name}_binary"]["message"], str(path))
                self.assertEqual(checks[f"{name}_dependencies"]["status"], "pass")
                self.assertEqual(checks[f"{name}_dependencies"]["code"], "DEPENDENCIES_READY")


def executable(value):
    path = Path(value).resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise argparse.ArgumentTypeError(f"not an executable file: {path}")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=executable, help="built iotdb_fs_preflight")
    parser.add_argument("--fs-bin", type=executable, help="built iotdb_fs (defaults to --bin)")
    parser.add_argument("--web-bin", type=executable, help="built iotdb_fs_web (defaults to --bin)")
    arguments = parser.parse_args()
    BINARY = arguments.bin
    FS_BINARY = arguments.fs_bin or BINARY
    WEB_BINARY = arguments.web_bin or BINARY
    unittest.main(argv=[sys.argv[0]], verbosity=2)
