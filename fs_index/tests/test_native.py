"""Native ABI functional fixtures, never performance/scientific case results."""
import ctypes
import ctypes.util
import json
import os
from pathlib import Path
import tempfile
import unittest

import h5py
import numpy as np

from fs_index.native import NativeContext, NativeError

LIBRARY = os.environ.get("FS_INDEX_NATIVE_LIBRARY")
TARGET = {"shortName": "t", "typeOfLevel": "isobaricInhPa", "level": 500,
          "step": 0, "validityDate": 20261021, "validityTime": 0}


def make_grib(path, ordinal, file_index):
    """Use actual ecCodes setters; no dependency on optional Python bindings."""
    lib = ctypes.CDLL(ctypes.util.find_library("eccodes"))
    lib.codes_grib_handle_new_from_samples.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.codes_grib_handle_new_from_samples.restype = ctypes.c_void_p
    lib.codes_set_long.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_long]
    lib.codes_set_double.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_double]
    lib.codes_set_string.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
                                    ctypes.POINTER(ctypes.c_size_t)]
    lib.codes_set_double_array.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                          ctypes.POINTER(ctypes.c_double), ctypes.c_size_t]
    lib.codes_get_message.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
                                     ctypes.POINTER(ctypes.c_size_t)]
    lib.codes_handle_delete.argtypes = [ctypes.c_void_p]
    with path.open("wb") as stream:
        for slot in range(2):
            handle = lib.codes_grib_handle_new_from_samples(None, b"regular_ll_pl_grib2")
            if not handle:
                raise RuntimeError("ecCodes regular_ll_pl_grib2 sample unavailable")
            try:
                for key, value in {"Ni": 64, "Nj": 64, "dataDate": 20261021,
                                   "dataTime": 0, "forecastTime": 0,
                                   "level": 500 if slot == file_index else 700,
                                   "bitsPerValue": 24}.items():
                    if lib.codes_set_long(handle, key.encode(), value):
                        raise RuntimeError("cannot set GRIB key " + key)
                for key, value in {"latitudeOfFirstGridPointInDegrees": 90,
                                   "longitudeOfFirstGridPointInDegrees": 0,
                                   "latitudeOfLastGridPointInDegrees": 27,
                                   "longitudeOfLastGridPointInDegrees": 63,
                                   "iDirectionIncrementInDegrees": 1,
                                   "jDirectionIncrementInDegrees": 1}.items():
                    if lib.codes_set_double(handle, key.encode(), value):
                        raise RuntimeError("cannot set GRIB coordinate " + key)
                value = b"t"
                length = ctypes.c_size_t(len(value))
                if lib.codes_set_string(handle, b"shortName", value, ctypes.byref(length)):
                    raise RuntimeError("cannot set GRIB shortName")
                values = np.arange(4096, dtype=np.float64) + ordinal * 10000 + slot
                if lib.codes_set_double_array(handle, b"values",
                        values.ctypes.data_as(ctypes.POINTER(ctypes.c_double)), values.size):
                    raise RuntimeError("cannot set GRIB values")
                pointer, length = ctypes.c_void_p(), ctypes.c_size_t()
                if lib.codes_get_message(handle, ctypes.byref(pointer), ctypes.byref(length)):
                    raise RuntimeError("cannot encode GRIB")
                stream.write(ctypes.string_at(pointer, length.value))
            finally:
                lib.codes_handle_delete(handle)


@unittest.skipUnless(LIBRARY, "FS_INDEX_NATIVE_LIBRARY required")
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sources = []
        for format in ("grib", "hdf5"):
            for ordinal in (1, 2, 3):
                for file_index in (0, 1, 2):
                    path = self.root / ("%s-v%d-f%d" % (format, ordinal, file_index))
                    if format == "hdf5":
                        with h5py.File(path, "w") as file:
                            for slot in (0, 1):
                                data = np.arange(4096, dtype=np.float32) + ordinal * 10000 + slot
                                ds = file.create_dataset("/d%d" % slot,
                                    data=data.reshape(1, 1, 64, 64), chunks=(1, 1, 32, 32),
                                    compression="gzip", compression_opts=1, shuffle=True)
                                attrs = {**TARGET, "level": 500 if slot == file_index else 700}
                                for key, value in attrs.items():
                                    ds.attrs[key] = np.bytes_(value) if isinstance(value, str) else np.int64(value)
                    else:
                        make_grib(path, ordinal, file_index)
                    st = path.stat()
                    self.sources.append({"path": str(path), "logical_path": "/%s/f%d" % (format, file_index),
                        "format": format, "ordinal": ordinal, "version": ordinal * 100 + file_index,
                        "size": st.st_size, "mtime_ns": st.st_mtime_ns})
        self.catalog = self.root / "catalog.json"
        self.catalog.write_text(json.dumps({"protocol": "iotdbfs-fixed-native/1", "sources": self.sources}))
        self.config = {"catalog_path": str(self.catalog), "index_path": str(self.root / "index.sqlite")}
        self.context = NativeContext(LIBRARY, {**self.config, "build_index": True})
        self.scope = sorted({s["logical_path"] for s in self.sources})

    def tearDown(self):
        self.context.close()
        self.temp.cleanup()

    def test_all_selection_equivalence_and_pruning(self):
        for selection, source_count, unit_count in [
                ({"kind": "latest"}, 6, 4),
                ({"kind": "ordinal", "ordinals": [1]}, 6, 4),
                ({"kind": "between", "ordinals": [2, 3]}, 12, 8),
                ({"kind": "all"}, 18, 12)]:
            with self.subTest(selection=selection):
                before = self.context.stats()
                scan = self.context.select("scan", selection, TARGET, self.scope)
                index = self.context.select("index", selection, TARGET, self.scope)
                self.assertEqual(scan, index)
                self.assertEqual(source_count, scan["selected_sources"])
                self.assertEqual(unit_count, len(scan["units"]))
                self.assertEqual(source_count,
                    self.context.stats()["scan_source_visits"] - before["scan_source_visits"])

    def test_owned_results_for_both_formats(self):
        units = self.context.select("index", {"kind": "latest"}, TARGET, self.scope)["units"]
        self.assertEqual({u["format"] for u in units}, {"hdf5", "grib"})
        for unit in units:
            values, mask = self.context.read(unit)
            slot = int(unit["dataset"][-1:]) if unit["format"] == "hdf5" else int(unit["logical_path"][-1:])
            expected = (np.arange(4096, dtype=np.float32) + unit["ordinal"] * 10000 + slot).reshape(values.shape)
            np.testing.assert_array_equal(values, expected)
            np.testing.assert_array_equal(mask, np.ones(values.shape, dtype=np.uint8))
            self.assertTrue(values.flags.owndata and mask.flags.owndata)
            self.assertEqual(values.dtype, np.dtype("float32"))
            self.assertEqual(mask.dtype, np.dtype("uint8"))

    def test_scope_queries_are_not_answer_cache(self):
        first = self.scope[:1]
        second = self.scope[1:2]
        for scope in (first, second, first, self.scope):
            a = self.context.select("scan", {"kind": "latest"}, TARGET, scope)
            b = self.context.select("index", {"kind": "latest"}, TARGET, scope)
            self.assertEqual(a, b)
            self.assertTrue(all(u["logical_path"] in scope for u in b["units"]))

    def test_stale_source_rejected_in_both_discovery_and_read(self):
        row = self.context.select("index", {"kind": "latest"}, TARGET, self.scope)["units"][0]
        path = Path(row["native_path"])
        with path.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaises(NativeError):
            self.context.select("index", {"kind": "latest"}, TARGET, self.scope)
        with self.assertRaises(NativeError):
            self.context.read(row)

    def test_forged_binding_and_invalid_scope_rejected(self):
        row = self.context.select("index", {"kind": "latest"}, TARGET, self.scope)["units"][0]
        with self.assertRaises(NativeError):
            self.context.read({**row, "version": row["version"] + 1})
        with self.assertRaises(NativeError):
            self.context.select("index", {"kind": "latest"}, TARGET, self.scope[::-1])
        with self.assertRaises(NativeError):
            self.context.select("index", {"kind": "ordinal", "ordinals": []}, TARGET, self.scope)

    def test_actual_explain_uses_index_query_sql_and_bindings(self):
        before = self.context.stats()
        for selection in ({"kind": "latest"}, {"kind": "all"},
                          {"kind": "ordinal", "ordinals": [1]},
                          {"kind": "between", "ordinals": [2, 3]}):
            plan = self.context.explain("index", selection, TARGET, self.scope)
            self.assertTrue(plan["sql"].startswith("SELECT u.source,"))
            self.assertEqual(plan["bindings"], ["t", "isobaricInhPa", 500, 0, 20261021, 0])
            self.assertTrue(plan["plan"])
            self.assertTrue(all("detail" in item for item in plan["plan"]))
            a = self.context.select("scan", selection, TARGET, self.scope)
            b = self.context.select("index", selection, TARGET, self.scope)
            self.assertEqual(a, b)
        self.assertEqual(before["read_calls"], self.context.stats()["read_calls"])

    def test_retained_handles_and_exact_index_identity(self):
        rows = self.context.select("index", {"kind": "latest"}, TARGET, self.scope)["units"]
        for _ in range(2):
            for row in rows:
                self.context.read(row)
        stats = self.context.stats()
        self.assertLessEqual(stats["resident_files"], 8)
        self.assertLessEqual(stats["resident_datasets"], 8)
        self.assertGreater(stats["file_cache_hits"], 0)
        self.assertGreater(stats["dataset_cache_hits"], 0)
        self.assertEqual(stats["index_build_files"], 18)
        self.assertEqual(stats["index_build_units"], 36)
        changed = json.loads(self.catalog.read_text())
        changed["sources"][0]["version"] += 10
        self.catalog.write_text(json.dumps(changed))
        with NativeContext(LIBRARY, self.config) as other:
            with self.assertRaises(NativeError):
                other.select("index", {"kind": "latest"}, TARGET, self.scope)


if __name__ == "__main__":
    unittest.main()
