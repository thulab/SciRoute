"""Acceptance-harness regressions; no network or compiled probe required."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import h5py
import numpy as np

SPEC = importlib.util.spec_from_file_location("h00_runner", Path(__file__).with_name("run_h00.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class H00RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def report(self, **fields):
        return {"schema_version": "iotdbfs-hdf5-h00-probe/1", "status": "unsupported",
                "library_version": "1.14.4", "library_threadsafe": True,
                "dynamic_plugins_enabled": False, **fields}

    def invoke_with_report(self, result, returncode, selection=None):
        def process(command, **kwargs):
            (self.root / "case.json").write_text(json.dumps(result))
            return subprocess.CompletedProcess(command, returncode, "", "")
        with mock.patch.object(runner.subprocess, "run", side_effect=process):
            return runner.invoke(Path("/unused/probe"), self.root / "source.h5",
                                 selection or {"dataset": "/data", "start": [0], "count": [1],
                                               "support": False}, self.root, "case")

    def test_stale_report_cannot_hide_probe_failure(self):
        (self.root / "case.json").write_text(json.dumps(self.report()))
        (self.root / "case.bin").write_bytes(b"old values")
        with mock.patch.object(runner.subprocess, "run", return_value=
                               subprocess.CompletedProcess([], 9, "", "failed before report")):
            with self.assertRaisesRegex(RuntimeError, "did not write report"):
                runner.invoke(Path("/unused/probe"), self.root / "source.h5",
                              {"dataset": "/data", "start": [0], "count": [1], "support": False},
                              self.root, "case")
        self.assertFalse((self.root / "case.bin").exists())

    def test_unsupported_requires_clean_exit_and_disabled_plugins(self):
        self.assertEqual(self.invoke_with_report(self.report(), 0)["status"], "unsupported")
        with self.assertRaisesRegex(RuntimeError, "expected unsupported"):
            self.invoke_with_report(self.report(), -11)
        with self.assertRaisesRegex(RuntimeError, "plugins must be disabled"):
            self.invoke_with_report(self.report(dynamic_plugins_enabled=True), 0)

    def test_corrupt_requires_checksum_error_and_expected_failure_exit(self):
        selection = {"dataset": "/data", "start": [0], "count": [1], "expected_status": "corrupt"}
        report = self.report(status="error", error="Fletcher32 checksum mismatch")
        self.assertEqual(self.invoke_with_report(report, 1, selection)["status"], "error")
        with self.assertRaisesRegex(RuntimeError, "expected checksum"):
            self.invoke_with_report(report, -11, selection)

    def make_trace(self, path, ranges):
        return {"source_bytes": path.stat().st_size,
                "ranges": [{"offset": a, "length": b} for a, b in ranges],
                "raw_payload_calls": len(ranges), "raw_payload_bytes": sum(b for a, b in ranges)}

    def test_projection_excludes_both_chunked_and_contiguous_dataset_payloads(self):
        for chunks in (None, (2, 3)):
            with self.subTest(chunks=chunks):
                path = self.root / ("chunked.h5" if chunks else "contiguous.h5")
                with h5py.File(path, "w") as file:
                    file.create_dataset("a", data=np.ones((4, 5), dtype="<f4"))
                    file.create_dataset("b", data=np.zeros((4, 5), dtype="<f4"), chunks=chunks)
                with h5py.File(path, "r") as file:
                    a = file["a"]
                    trace = self.make_trace(path, [(a.id.get_offset(), a.id.get_storage_size())])
                    runner.validate_ranges(trace, path, file, ["/b"])
                    b = file["b"]
                    if chunks:
                        chunk = b.id.get_chunk_info(0)
                        b_offset, b_size = chunk.byte_offset, chunk.size
                    else:
                        b_offset, b_size = b.id.get_offset(), b.id.get_storage_size()
                    trace = self.make_trace(path, [(b_offset, b_size)])
                    with self.assertRaisesRegex(RuntimeError, "excluded dataset /b"):
                        runner.validate_ranges(trace, path, file, ["/b"])

    def test_empty_excluded_dataset_cannot_make_projection_test_vacuous(self):
        path = self.root / "empty.h5"
        with h5py.File(path, "w") as file:
            file.create_dataset("b", shape=(10,), dtype="<f4", chunks=(2,))
        with h5py.File(path, "r") as file:
            with self.assertRaisesRegex(RuntimeError, "vacuous"):
                runner.validate_ranges(self.make_trace(path, []), path, file, ["/b"])

    def test_range_bounds_and_accounting_are_checked(self):
        path = self.root / "source.bin"
        path.write_bytes(b"x" * 20)
        for offset, length in ((-1, 1), (0, -1), (0, 0), (19, 2), (21, 1)):
            with self.subTest(offset=offset, length=length), self.assertRaisesRegex(RuntimeError, "bounds"):
                runner.validate_ranges(self.make_trace(path, [(offset, length)]), path, None, [])
        trace = self.make_trace(path, [(0, 2)])
        trace["raw_payload_calls"] = 9
        with self.assertRaisesRegex(RuntimeError, "call count"):
            runner.validate_ranges(trace, path, None, [])
        trace["raw_payload_calls"] = 1
        trace["raw_payload_bytes"] = 4
        with self.assertRaisesRegex(RuntimeError, "byte count"):
            runner.validate_ranges(trace, path, None, [])


if __name__ == "__main__":
    unittest.main()
