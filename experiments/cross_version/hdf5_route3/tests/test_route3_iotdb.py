#!/usr/bin/env python3
"""Offline route3 glue checks; no database, FUSE mount or service operations."""
import unittest
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from experiments.cross_version.hdf5_route3 import route3_iotdb as route


class RequestTests(unittest.TestCase):
    def test_latest_and_timestamp_between(self):
        latest = route.selector("/trial")
        self.assertEqual(latest["versions"], {"kind": "latest"})
        request = route.discovery_request("/trial", {"op": "and", "args": []},
                                          "between", [1700, 1901])
        self.assertEqual(request["request"]["sources"]["versions"],
                         {"kind": "between", "lower_inclusive": 1700, "upper_exclusive": 1901})
        self.assertEqual(request["request"]["max_sources"], 1024)
        self.assertEqual(request["request"]["max_catalog_bytes"], 64 * 1024 * 1024)

    def test_no_all_or_ordinal_or_bad_scope(self):
        for selection, fence in (("all", None), ("between", [8, 8]),
                                 ("between", [8, 7]), ("between", [True, 2])):
            with self.assertRaises(RuntimeError):
                route.selector("/trial", selection, fence)
        for path in ("trial", "/trial/../escape", "/trial//file", "/trial/", "/trial\0"):
            with self.assertRaises(RuntimeError):
                route.selector(path)
        with self.assertRaises(RuntimeError):
            route.selector("/trial", exact_path="/trial2/file")

    def test_typed_roi_uses_exact_binding_and_physical_axes(self):
        row = {"path": "/trial/raw/f.h5", "version": 1792300123,
               "dataset": "/data/u0000"}
        request = route.typed_request(row, [0, 0, 16, 16], [1, 1, 64, 64])
        self.assertEqual(request["request"]["sources"]["versions"],
                         {"kind": "exact", "exact": row["version"]})
        subset = request["request"]["subset"]
        self.assertEqual(subset["projection"], ["hdf5.value:%2Fdata%2Fu0000"])
        self.assertEqual(subset["slices"][2], {"dimension": "hdf5.axis.2",
            "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": 16}},
            "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": 80}}})

    def test_scalar_equality_encoding(self):
        result = route.equality_predicate({"shortName": "t", "level": 850})
        self.assertEqual(result, {"kind": "and", "operands": [
            {"kind": "compare", "field": "hdf5.attr:level", "operator": "eq",
             "value": {"type": "int64", "value": 850}},
            {"kind": "compare", "field": "hdf5.attr:shortName", "operator": "eq",
             "value": {"type": "utf8", "value": "t"}}]})

    def test_rounds_must_cover_same_paths_and_not_overlap(self):
        items = [{"path": "/trial/a.h5", "source": "a"}]
        with self.assertRaises(RuntimeError):
            route.publish_round({}, items, 2, {"/trial/b.h5": 4})
        with patch.object(route, "publish_one", return_value={"path": "/trial/a.h5", "version": 4}):
            with self.assertRaises(RuntimeError):
                route.publish_round({}, items, 2, {"/trial/a.h5": 5})

    def test_bad_roi(self):
        for origin, shape in (([0], [0]), ([0], [1, 1]), ([-1], [1]),
                              ([False], [1]), ([2**64-1], [1])):
            with self.assertRaises(RuntimeError):
                route.roi_slices(origin, shape)

    def test_discovery_validates_fence_scope_and_duplicates(self):
        row = {"path": "/trial/f.h5", "version": 12, "dataset_path": "/data/u0"}
        with patch.object(route, "_call", return_value=({"datasets": [row]}, [], {"total_ns": 5}, 7)):
            rows, stats, offset = route.discover({}, "/trial", None, "between", [10, 20])
        self.assertEqual(rows, [{"path": row["path"], "version": 12, "dataset": "/data/u0"}])
        self.assertEqual((stats["total_ns"], offset), (5, 7))
        for response in ([row, row], [dict(row, version=20)], [dict(row, path="/trial2/f.h5")]):
            with patch.object(route, "_call", return_value=({"datasets": response}, [], {}, 0)):
                with self.assertRaises(RuntimeError):
                    route.discover({}, "/trial", None, "between", [10, 20])

    def test_round_fence_uses_actual_versions_and_checkpoints(self):
        previous, checkpoints = {}, []
        items = [{"path": "/trial/a.h5", "source": "a"},
                 {"path": "/trial/b.h5", "source": "b"}]
        def publish(context, source, path, ordinal, **kwargs):
            self.assertIsNone(kwargs["previous"])
            return {"path": path, "version": {"a": 1001, "b": 1023}[source], "ordinal": ordinal}
        with patch.object(route, "publish_one", side_effect=publish):
            result = route.publish_round({}, items, 1, previous, checkpoint=checkpoints.append)
        self.assertEqual(result["fence"], [1001, 1024])
        self.assertEqual(previous, {"/trial/a.h5": 1001, "/trial/b.h5": 1023})
        self.assertEqual(checkpoints, result["bindings"])

    def test_typed_values_own_arrays_and_exact_provenance(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("NumPy unavailable in local offline test runtime")
        row = {"path": "/trial/a.h5", "version": 17, "dataset": "/data/u0"}
        array, mask = np.arange(4, dtype="<f4"), np.ones(4, dtype="u1")
        part = {"array": array, "mask": mask, "meta": {b"iotdbfs.sources": b"1",
                b"iotdbfs.source.0.path": b"/trial/a.h5", b"iotdbfs.source.0.version": b"17"}}
        with patch.object(route, "_call", return_value=({}, [part], {"total_ns": 5}, 11)):
            owned, calls, offset = route.typed_values({}, [row], [0, 0], [2, 2])
        array[:] = 100
        self.assertEqual(owned[0]["array"].tolist(), [0, 1, 2, 3])
        self.assertEqual((len(calls), offset), (1, 11))
        part["meta"][b"iotdbfs.source.0.version"] = b"18"
        with patch.object(route, "_call", return_value=({}, [part], {}, 0)):
            with self.assertRaises(RuntimeError):
                route.typed_values({}, [row], [0, 0], [2, 2])

    def test_launcher_receipts_add_existing_frontend_capacity_setting(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            daemon, launcher = root / "iotdb_fs", root / "launcher.py"
            daemon.write_bytes(b"frozen test placeholder")
            def prior(path, daemon, prefix, journal, initialize):
                settings = {"storage.version_directory.table_prefix": prefix,
                            "storage.version_directory.initialize": str(initialize).lower()}
                path.write_text("#!/usr/bin/python3\nSETTINGS=" + repr(settings) + "\n")
                path.chmod(0o700)
                return {"path": str(path), "binary": str(daemon), "binary_sha256": route.V04_SHA,
                        "settings": settings, "sha256": "previous"}
            actual_sha = route.sha
            def sha(path):
                return route.V04_SHA if Path(path) == daemon else actual_sha(path)
            with patch.object(route, "sha", side_effect=sha), \
                 patch.dict("sys.modules", {"experiments.cross_version.version_directory.v03_n128": SimpleNamespace(launcher=prior)}):
                receipt = route.create_launcher(launcher, daemon, "vd_route3_test", root / "journal")
            self.assertEqual(receipt["settings"]["query.frontend.max_registrations"], "8192")
            self.assertEqual(receipt["sha256"], hashlib.sha256(launcher.read_bytes()).hexdigest())
            self.assertIn("'query.frontend.max_registrations': '8192'", launcher.read_text())
            self.assertEqual(launcher.stat().st_mode & 0o777, 0o700)


if __name__ == "__main__":
    unittest.main()
