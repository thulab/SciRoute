"""Focused native route-3 ABI tests; set ROUTE3_NATIVE_LIBRARY to the .so."""
import json
import os
from pathlib import Path
import tempfile
import unittest

import h5py
import numpy as np

from experiments.cross_version.hdf5_route3.route3_native import NativeContext, NativeError, counter_delta

LIBRARY = os.environ.get("ROUTE3_NATIVE_LIBRARY")
PREDICATE = {"shortName": "t", "typeOfLevel": "isobaricInhPa",
             "level": 850, "step": 0, "validityDate": 20260101,
             "validityTime": 0}


@unittest.skipUnless(LIBRARY, "ROUTE3_NATIVE_LIBRARY is required")
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sources = []
        for ordinal in range(1, 4):
            for file_index in range(3):
                path = self.root / ("v%d-f%d.h5" % (ordinal, file_index))
                with h5py.File(path, "w") as file:
                    for slot in range(2):
                        values = (np.arange(128 * 128, dtype=np.float32) +
                                  ordinal * 100000 + file_index * 1000 + slot)
                        ds = file.create_dataset("/fields/d%02d" % slot,
                            data=values.reshape(1, 1, 128, 128),
                            chunks=(1, 1, 32, 32), compression="gzip",
                            compression_opts=1, shuffle=True)
                        attrs = {**PREDICATE}
                        # Match target slot shifts across files/versions.
                        if file_index == 2 or slot != (file_index + ordinal) % 2:
                            attrs["level"] = 500
                        for key, value in attrs.items():
                            ds.attrs[key] = (np.bytes_(value) if isinstance(value, str)
                                             else np.int64(value))
                stat = path.stat()
                self.sources.append({"logical_path": "/logical/f%d.h5" % file_index,
                    "ordinal": ordinal, "version": 1000 + ordinal * 10 + file_index,
                    "path": str(path), "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns})
        self.catalog = self.root / "catalog.json"
        self.catalog.write_text(json.dumps({"protocol": "iotdbfs-route3-publication/1",
                                            "sources": self.sources}))
        self.config = {"catalog_path": str(self.catalog),
                       "index_path": str(self.root / "catalog.sqlite")}
        self.context = NativeContext(LIBRARY, {**self.config, "build_index": True})

    def tearDown(self):
        self.context.close()
        self.temp.cleanup()

    def test_discovery_scan_index_and_history_pruning(self):
        for selection, count, units in [
                ({"kind": "latest"}, 3, 2),
                ({"kind": "ordinal", "lower": 1, "upper": 2}, 3, 2),
                ({"kind": "between", "lower": 1010, "upper": 1030}, 6, 4)]:
            with self.subTest(selection=selection):
                before = self.context.stats()
                scan = self.context.select("scan", selection, PREDICATE)
                diff = counter_delta(before, self.context.stats())
                index = self.context.select("index", selection, PREDICATE)
                self.assertEqual(scan["units"], index["units"])
                self.assertEqual(scan["selected_sources"], count)
                self.assertEqual(len(scan["units"]), units)
                self.assertEqual(diff["attribute_reads"], count * 2 * 6)
                self.assertEqual(diff["scan_source_visits"], count)
                self.assertEqual(scan["candidate_datasets"], count * 2)
                self.assertEqual(index["candidate_datasets"], units)

    def test_scan_catalog_prunes_unselected_history_before_visiting_sources(self):
        # The complete catalog has nine sources. A selected older round and
        # Latest each visit only three; an empty timestamp window visits none.
        for selection, count in [
                ({"kind": "ordinal", "lower": 2, "upper": 3}, 3),
                ({"kind": "latest"}, 3),
                ({"kind": "between", "lower": 2000, "upper": 2001}, 0)]:
            before = self.context.stats()
            row = self.context.select("scan", selection, PREDICATE)
            delta = counter_delta(before, self.context.stats())
            self.assertEqual(delta["scan_source_visits"], count)
            self.assertEqual(delta["attribute_reads"], count * 2 * 6)
            self.assertEqual(row["selected_sources"], count)

    def test_roi_aligned_misaligned_actual_owned_arrays(self):
        unit = self.context.select("index", {"kind": "latest"}, PREDICATE)["units"][0]
        for origin in ([0, 0, 0, 0], [0, 0, 16, 16]):
            values, mask = self.context.read(unit["path"], unit["dataset"], origin,
                                              [1, 1, 64, 64])
            self.assertTrue(values.flags.owndata)
            self.assertTrue(mask.flags.owndata)
            self.assertEqual(values.dtype, np.dtype("float32"))
            self.assertEqual(values.shape, (1, 1, 64, 64))
            with h5py.File(unit["path"], "r") as file:
                expected = file[unit["dataset"]][tuple(slice(o, o + n)
                    for o, n in zip(origin, [1, 1, 64, 64]))]
            np.testing.assert_array_equal(values, expected)
            np.testing.assert_array_equal(mask, np.ones(values.shape, np.uint8))

    def test_bounded_handles_and_warm_reader(self):
        for source in self.sources:
            self.context.read(source["path"], "/fields/d00", [0] * 4, [1, 1, 64, 64])
        stats = self.context.stats()
        self.assertLessEqual(stats["resident_files"], 8)
        self.assertLessEqual(stats["resident_datasets"], 8)
        before = stats
        source = self.sources[-1]
        self.context.read(source["path"], "/fields/d00", [0] * 4, [1, 1, 64, 64])
        diff = counter_delta(before, self.context.stats())
        self.assertEqual(diff["file_opens"], 0)
        self.assertEqual(diff["dataset_opens"], 0)
        self.assertEqual(diff["read_calls"], 1)
        self.assertEqual(diff["value_bytes"], 64 * 64 * 5)

    def test_stale_source_fails_index_and_read(self):
        source = self.sources[-3]
        self.context.read(source["path"], "/fields/d00", [0] * 4, [1, 1, 1, 1])
        os.utime(source["path"], ns=(source["mtime_ns"] + 1000000000,
                                    source["mtime_ns"] + 1000000000))
        with self.assertRaisesRegex(NativeError, "stale"):
            self.context.select("index", {"kind": "latest"}, PREDICATE)
        with self.assertRaisesRegex(NativeError, "stale"):
            self.context.read(source["path"], "/fields/d00", [0] * 4, [1, 1, 1, 1])

    def test_invalid_roi_and_missing_dataset_fail_without_retry(self):
        source = self.sources[0]
        with self.assertRaisesRegex(NativeError, "ROI outside"):
            self.context.read(source["path"], "/fields/d00", [0, 0, 100, 100],
                              [1, 1, 64, 64])
        with self.assertRaises(NativeError):
            self.context.read(source["path"], "/absent", [0] * 4, [1] * 4)
        with self.assertRaises(ValueError):
            self.context.read(source["path"], "/fields/d00", [0] * 4, [1, 1, 0, 1])
        self.assertEqual(self.context.stats()["read_calls"], 0)

    def test_existing_index_identity_and_constructor_failures(self):
        with NativeContext(LIBRARY, self.config) as other:
            self.assertEqual(other.select("index", {"kind": "latest"}, PREDICATE)["units"],
                             self.context.select("index", {"kind": "latest"}, PREDICATE)["units"])
        changed = self.root / "changed.json"
        changed.write_text(json.dumps({"protocol": "iotdbfs-route3-publication/1",
                                      "sources": self.sources[:-1]}))
        with NativeContext(LIBRARY, {**self.config, "catalog_path": str(changed)}) as other:
            with self.assertRaisesRegex(NativeError, "identity mismatch"):
                other.select("index", {"kind": "latest"}, PREDICATE)
        with self.assertRaisesRegex(NativeError, "cache budget"):
            NativeContext(LIBRARY, {**self.config, "cache_files": 9})
        with self.assertRaisesRegex(NativeError, "index exists"):
            NativeContext(LIBRARY, {**self.config, "build_index": True})

    def test_context_cache_isolation_and_bad_source_paths(self):
        source = self.sources[0]
        with NativeContext(LIBRARY, self.config) as other:
            self.context.read(source["path"], "/fields/d00", [0] * 4, [1] * 4)
            self.assertEqual(other.stats()["resident_files"], 0)
            other.read(source["path"], "/fields/d00", [0] * 4, [1] * 4)
            self.assertEqual(other.stats()["file_opens"], 1)
            self.assertEqual(other.stats()["file_cache_hits"], 0)
            other.close()
            values, mask = self.context.read(source["path"], "/fields/d00",
                                             [0] * 4, [1] * 4)
            self.assertEqual(mask.item(), 1)
        link = self.root / "alias.h5"
        link.symlink_to(source["path"])
        for path in (str(link), str(self.root / "absent.h5")):
            with self.assertRaisesRegex(NativeError, "regular file"):
                self.context.read(path, "/fields/d00", [0] * 4, [1] * 4)
        with self.assertRaisesRegex(NativeError, "absolute"):
            self.context.read("relative.h5", "/fields/d00", [0] * 4, [1] * 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
