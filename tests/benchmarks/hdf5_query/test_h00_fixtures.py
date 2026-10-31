"""H00 generator contracts and formula oracles; no IoTDB or compiled probe needed."""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import time
import unittest

import h5py
import numpy as np

GENERATOR = Path(__file__).resolve().parents[2] / "fixtures" / "hdf5" / "generate_h00.py"
spec = importlib.util.spec_from_file_location("generate_h00", GENERATOR)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class H00FixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="iotdbfs-h00-test-")
        cls.directory = Path(cls.temporary.name) / "first"
        cls.manifest = generator.generate(cls.directory)
        cls.files = {item["id"]: item for item in cls.manifest["files"]}

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_every_supported_selection_matches_independent_digest(self):
        checked = 0
        for entry in self.manifest["files"]:
            if not any(s["support"] for s in entry["selections"]):
                continue
            with h5py.File(self.directory / entry["path"], "r") as file:
                for selection in entry["selections"]:
                    if not selection["support"]:
                        continue
                    slices = tuple(slice(start, start + count) for start, count in
                                   zip(selection["start"], selection["count"]))
                    actual = file[selection["dataset"]][slices]
                    expected = selection["expected"]
                    self.assertEqual(list(actual.shape), expected["shape"])
                    raw = actual.astype(np.dtype(expected["dtype"]), copy=False).tobytes(order="C")
                    self.assertEqual(hashlib.sha256(raw).hexdigest(), expected["sha256"],
                                     (entry["id"], selection["id"]))
                    checked += 1
        self.assertGreater(checked, 100)

    def test_formula_oracle_against_scalar_arithmetic(self):
        # A third implementation: scalar arithmetic, no NumPy coordinate grid
        # and no calls to the writer formula or expected_selection.
        for dtype in ("<f4", ">f8"):
            shape, start, count = (11, 9, 7), [6, 3, 1], [5, 4, 3]
            formula = generator.recipe(shape, fill=-7.25,
                written=[{"start": [0, 0, 0], "count": [8, 6, 4]}])
            reference = np.empty(count, dtype=np.dtype(dtype).newbyteorder("<"))
            for local in np.ndindex(tuple(count)):
                global_index = tuple(start[axis] + local[axis] for axis in range(3))
                if all(global_index[axis] < (8, 6, 4)[axis] for axis in range(3)):
                    value = -3.25 + sum(global_index[axis] * 17 ** axis / 8 for axis in range(3))
                else:
                    value = -7.25
                reference[local] = value
            actual = generator.expected_selection(formula, start, count, dtype)
            self.assertEqual(actual.tobytes(), reference.tobytes())

    def test_numeric_matrix_endianness_and_layout(self):
        for rank in (1, 2, 3):
            for endian in ("le", "be"):
                for bits in (32, 64):
                    for layout in ("contiguous", "chunked"):
                        entry = self.files[f"rank{rank}_{endian}_f{bits}_{layout}"]
                        with h5py.File(self.directory / entry["path"], "r") as file:
                            dset = file["data"]
                            self.assertEqual(len(dset.shape), rank)
                            self.assertEqual(dset.dtype.itemsize * 8, bits)
                            self.assertEqual(dset.id.get_type().get_order(),
                                h5py.h5t.ORDER_LE if endian == "le" else h5py.h5t.ORDER_BE)
                            self.assertEqual(dset.chunks is not None, layout == "chunked")

    def test_filter_pipeline_masks_and_edge_chunks(self):
        expected_filters = {"deflate": {1}, "shuffle": {2}, "fletcher32": {3},
            "deflate_shuffle": {1, 2}, "deflate_fletcher32": {1, 3},
            "shuffle_fletcher32": {2, 3}, "all_filters": {1, 2, 3}}
        for name, filters in expected_filters.items():
            with h5py.File(self.directory / self.files[name]["path"], "r") as file:
                dset = file["data"]
                dcpl = dset.id.get_create_plist()
                actual = {dcpl.get_filter(i)[0] for i in range(dcpl.get_nfilters())}
                self.assertEqual(actual, filters)
                self.assertTrue(all(s % c for s, c in zip(dset.shape, dset.chunks)))
                self.assertEqual(dset.id.get_num_chunks(), 3 * 2 * 2)
        with h5py.File(self.directory / self.files["skipped_all_filters"]["path"], "r") as file:
            mask, raw = file["data"].id.read_direct_chunk((0, 0, 0))
            self.assertEqual(mask, 7)
            self.assertEqual(len(raw), 8 * 6 * 4 * 4)
            self.assertEqual(file["data"].id.read_direct_chunk((8, 0, 0))[0], 0)

    def test_fill_allocation_is_real_not_materialized_oracle(self):
        for layout in ("chunked", "contiguous"):
            for fill in ("default", "explicit", "undefined"):
                entry = self.files[f"{layout}_unallocated_{fill}_fill"]
                with h5py.File(self.directory / entry["path"], "r") as file:
                    dset = file["data"]
                    if layout == "chunked":
                        self.assertEqual(dset.id.get_num_chunks(), 1)
                    else:
                        self.assertEqual(dset.id.get_storage_size(), 0)
                        self.assertIsNone(dset.id.get_offset())
                    status = dset.id.get_create_plist().fill_value_defined()
                    self.assertEqual(status, {"default": h5py.h5d.FILL_VALUE_DEFAULT,
                        "explicit": h5py.h5d.FILL_VALUE_USER_DEFINED,
                        "undefined": h5py.h5d.FILL_VALUE_UNDEFINED}[fill])
                    self.assertEqual(entry["support"], fill != "undefined")
                    if fill == "undefined":
                        self.assertTrue(all(s["expected"] is None for s in entry["selections"]
                                            if not s["support"]))
                        if layout == "chunked":
                            self.assertTrue(entry["selections"][-1]["support"])

    def test_corrupt_fletcher_is_detected_by_independent_hdf5_reader(self):
        entry = self.files["corrupt_fletcher32"]
        self.assertEqual(entry["expected_status"], "corrupt")
        with h5py.File(self.directory / entry["path"], "r") as file:
            with self.assertRaises(OSError):
                file["data"][:]

    def test_userblock_and_hardlink_graph(self):
        signature = b"\x89HDF\r\n\x1a\n"
        for size in (512, 4096):
            for suffix in ("", "_chunked_deflate_shuffle"):
                path = self.directory / self.files[f"userblock_{size}{suffix}"]["path"]
                raw = path.read_bytes()
                self.assertNotEqual(raw[:8], signature)
                self.assertEqual(raw[size:size + 8], signature)
                if suffix:
                    with h5py.File(path, "r") as file:
                        dset = file["data"]
                        self.assertEqual(dset.compression, "gzip")
                        self.assertTrue(dset.shuffle)
                        for index in range(dset.id.get_num_chunks()):
                            info = dset.id.get_chunk_info(index)
                            mask, chunk = dset.id.read_direct_chunk(info.chunk_offset)
                            self.assertEqual(mask, info.filter_mask)
                            self.assertGreaterEqual(info.byte_offset, size)
                            self.assertEqual(raw[info.byte_offset:info.byte_offset + info.size], chunk)
        with h5py.File(self.directory / self.files["hardlink_alias_cycle"]["path"], "r") as file:
            self.assertEqual(file["data"].id, file["alias"].id)
            self.assertEqual(file["group"].id, file["group/self"].id)
            self.assertEqual(file["data"].id, file["group/self/data_alias"].id)

    def test_minimal_slice_binds_dataset_and_excludes_unrelated_payload(self):
        entry = self.files["minimal_slice"]
        self.assertEqual([d["path"] for d in entry["datasets"]], ["/a", "/b"])
        selected = next(s for s in entry["selections"] if s["id"] == "exact_minimal_slice")
        self.assertEqual(selected["start"], [1, 2, 3])
        self.assertEqual(selected["count"], [4, 5, 6])
        self.assertEqual(selected["excluded_datasets"], ["/b"])
        with h5py.File(self.directory / entry["path"], "r") as file:
            self.assertEqual(file["a"].shape, (5, 7, 9))
            self.assertEqual(file["a"].chunks, (2, 3, 4))
            self.assertEqual(file["b"].shape, (11, 5))
            self.assertNotEqual(file["a"][0, 0, 0], file["b"][0, 0])
            ranges = {}
            for name in ("a", "b"):
                dset = file[name]
                ranges[name] = [dset.id.get_chunk_info(i) for i in range(dset.id.get_num_chunks())]
            for a in ranges["a"]:
                for b in ranges["b"]:
                    self.assertFalse(a.byte_offset < b.byte_offset + b.size and
                                     b.byte_offset < a.byte_offset + a.size)

    def test_unsupported_profiles_are_structurally_distinct(self):
        for name in ("scalar", "null_dataspace", "compact", "integer", "vlen",
                     "external_storage", "soft_link", "external_link", "virtual_dataset"):
            entry = self.files[name]
            self.assertFalse(entry["support"])
            self.assertTrue(entry["reason"])
            self.assertTrue(all(s["expected"] is None for s in entry["selections"]))
        def open_file(name):
            return h5py.File(self.directory / self.files[name]["path"], "r")
        with open_file("scalar") as file:
            self.assertEqual(file["data"].shape, ())
        with open_file("null_dataspace") as file:
            self.assertIsNone(file["data"].shape)
        with open_file("compact") as file:
            self.assertEqual(file["data"].id.get_create_plist().get_layout(), h5py.h5d.COMPACT)
        with open_file("integer") as file:
            self.assertEqual(file["data"].dtype.kind, "i")
        with open_file("vlen") as file:
            self.assertIsNotNone(h5py.check_vlen_dtype(file["data"].dtype))
        with open_file("external_storage") as file:
            self.assertEqual(len(file["data"].external), 1)
        with open_file("soft_link") as file:
            self.assertIsInstance(file.get("data", getlink=True), h5py.SoftLink)
        with open_file("external_link") as file:
            self.assertIsInstance(file.get("data", getlink=True), h5py.ExternalLink)
        with open_file("virtual_dataset") as file:
            self.assertTrue(file["data"].is_virtual)

    def test_hashes_sizes_and_bounded_generation(self):
        self.assertEqual(self.manifest["schema_version"], generator.SCHEMA)
        self.assertLess(self.manifest["total_bytes"], 5 * 1024 * 1024)
        for entry in self.manifest["files"]:
            raw = (self.directory / entry["path"]).read_bytes()
            self.assertEqual(entry["size_bytes"], len(raw))
            self.assertEqual(entry["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertFalse((self.directory / "not_present.raw").exists())
        self.assertFalse((self.directory / "not_present.h5").exists())

    def test_repeat_generation_same_toolchain_is_byte_stable(self):
        # Cross an HDF5 timestamp tick so accidental tracked object times cannot
        # make a same-second repeatability check pass by luck.
        time.sleep(1.05)
        second = generator.generate(Path(self.temporary.name) / "second")
        self.assertEqual(self.manifest, second)
        for entry in self.manifest["files"]:
            self.assertEqual((self.directory / entry["path"]).read_bytes(),
                (Path(self.temporary.name) / "second" / entry["path"]).read_bytes())


if __name__ == "__main__":
    unittest.main()
