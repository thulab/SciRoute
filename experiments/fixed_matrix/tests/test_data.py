import ctypes.util
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from experiments.fixed_matrix import data


CATALOG = Path(__file__).parents[1] / "catalog.json"


def spec(fmt="hdf5", **conditions):
    catalog = json.loads(CATALOG.read_text())
    return next(s for s in data.specifications(catalog) if s["format"] == fmt and
                all(s.get(k) == v for k, v in conditions.items()))


try:
    import h5py
    import numpy as np
    HAS_HDF = True
except ImportError:
    HAS_HDF = False


class MatrixDataContractTests(unittest.TestCase):
    def test_primary_corpora_are_unique_and_shared_only_by_case_variables(self):
        catalog = json.loads(CATALOG.read_text())
        specs = data.specifications(catalog)
        self.assertEqual(len(specs), 14)
        self.assertEqual(sum(len(s["group_ids"]) for s in specs), 32)
        large = [s for s in specs if s["N"] == 512]
        self.assertEqual(len(large), 2)
        self.assertTrue(all(len(s["group_ids"]) == 4 for s in large))
        self.assertTrue(all(s["N"] != 1024 and s["V_stored"] != 32 for s in specs))
        self.assertEqual(len(data.specifications(catalog, "full")), 20)
        with self.assertRaises(ValueError):
            data.specifications(catalog, "ad-hoc")

    def test_names_do_not_encode_matches_and_fixed_scope_contains_them(self):
        names = [data.logical_file("hdf5", i) for i in range(512)]
        self.assertEqual(len(set(names)), 512)
        self.assertTrue(all("match" not in name and "f000" not in name for name in names))
        self.assertEqual(sum(data._matches(data.attributes(i, u)) for i in range(512) for u in range(4)), 2)
        for count in (8, 32, 128, 512):
            self.assertEqual(sum(data._matches(data.attributes(i, u)) for i in range(count) for u in range(4)), 2)
        self.assertEqual({data.target_slot(i) for i in range(128)}, {0, 1, 2, 3})

    def test_existing_partial_directory_is_rejected_without_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "partial"
            root.mkdir()
            (root / "keep").write_text("partial artifact")
            with mock.patch.object(data, "_hdf_environment", side_effect=AssertionError("must not start")):
                with self.assertRaises(FileExistsError):
                    data.prepare(spec(N=8, V_stored=1, size_profile="S0"), root)
            self.assertEqual((root / "keep").read_text(), "partial artifact")

    def test_failure_is_retained_and_cannot_be_resumed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "failed"
            with mock.patch.object(data, "_hdf_environment", side_effect=RuntimeError("native unavailable")):
                with self.assertRaisesRegex(RuntimeError, "native unavailable"):
                    data.prepare(spec(N=8, V_stored=1, size_profile="S0"), root)
            self.assertFalse((root / "manifest.json").exists())
            self.assertFalse(json.loads((root / "FAILED.json").read_text())["complete"])
            with self.assertRaises(FileExistsError):
                data.prepare(spec(N=8, V_stored=1, size_profile="S0"), root)

    def test_grib_large_geometry_uses_same_valid_grid_rule(self):
        class FakeCodes:
            def __init__(self):
                self.controls = []
                self.lib = self

            @contextmanager
            def handle(self):
                values = {}
                self.controls.append(values)
                yield values

            def set(self, handle, key, value):
                handle[key] = value

            def codes_set_double_array(self, handle, name, values, count):
                handle["value_count"] = count
                return 0

            def check(self, code, _):
                if code:
                    raise AssertionError(code)

            def encoded(self, _):
                return b"placeholder; native ecCodes tested separately"

        if not HAS_HDF:  # NumPy is shared by the two generators.
            self.skipTest("NumPy unavailable")
        with tempfile.TemporaryDirectory() as temporary:
            for profile in ("S0", "S1", "S2"):
                codes = FakeCodes()
                data._grib_create(Path(temporary) / profile, spec("grib", N=8, V_stored=1, size_profile=profile), 0, codes)
                for slot, controls in enumerate(codes.controls):
                    side = 64 if slot == data.target_slot(0) else data.SIDES[profile]
                    self.assertEqual(controls["Ni"], side)
                    self.assertEqual(controls["Nj"], side)
                    self.assertEqual(controls["value_count"], side * side)
                    self.assertEqual(controls["latitudeOfFirstGridPointInDegrees"], 90.0)
                    self.assertEqual(controls["latitudeOfLastGridPointInDegrees"], 90.0 - (side - 1) * .125)
                    self.assertGreaterEqual(controls["latitudeOfLastGridPointInDegrees"], -90)
                    self.assertEqual(controls["jScansPositively"], 0)
                    self.assertEqual(controls["bitsPerValue"], 16)


@unittest.skipUnless(HAS_HDF, "h5py and NumPy required for native HDF5 validation")
class NativeHdfDataTests(unittest.TestCase):
    def test_reproducible_full_bytes_and_oracles(self):
        with tempfile.TemporaryDirectory() as temporary:
            frozen = spec(N=8, V_stored=1, size_profile="S0")
            first = data.prepare(frozen, Path(temporary) / "first")
            second = data.prepare(frozen, Path(temporary) / "second")
            self.assertEqual(first, second)
            self.assertEqual(len(first["entries"]), 8)
            self.assertEqual(first["size_distribution"]["total_logical_bytes"],
                             sum(e["bytes"] for e in first["entries"]))
            for entry in first["entries"]:
                self.assertEqual(len(entry["units"]), 4)
                for unit in entry["units"]:
                    self.assertEqual(unit["oracle"]["elements"], 4096)
                    self.assertEqual(unit["oracle"]["valid_elements"], 4096)
                    self.assertEqual(unit["schema"]["chunks"], [1, 1, 32, 32])
                    self.assertEqual(unit["allocated_chunks"], 4)

    def test_identical_history_is_full_independent_regular_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "identical"
            manifest = data.prepare(spec(history_scenario="identical"), root)
            self.assertEqual(len(manifest["entries"]), 64)
            identities = set()
            per_path = {}
            for entry in manifest["entries"]:
                path = root / entry["file"]
                metadata = path.stat()
                self.assertEqual(metadata.st_nlink, 1)
                self.assertFalse(path.is_symlink())
                self.assertEqual(metadata.st_mode & 0o222, 0)
                identities.add((metadata.st_dev, metadata.st_ino))
                per_path.setdefault(entry["logical_file"], set()).add(entry["sha256"])
            self.assertEqual(len(identities), 64)
            self.assertTrue(all(len(digests) == 1 for digests in per_path.values()))
            self.assertTrue(all(not item["changed_slots"] for item in manifest["version_changes"]))

    def test_single_unit_changes_are_cumulative_and_other_units_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "changed"
            manifest = data.prepare(spec(history_scenario="single-unit-change"), root)
            self.assertEqual(len(manifest["version_changes"]), 56)
            by_key = {(entry["file_id"], entry["ordinal"]): entry for entry in manifest["entries"]}
            for change in manifest["version_changes"]:
                self.assertEqual(change["changed_slots"], [(change["ordinal"] - 2) % 4])
            with h5py.File(root / by_key[(0, 1)]["file"], "r") as first, \
                    h5py.File(root / by_key[(0, 6)]["file"], "r") as sixth:
                expected = first["/data/u0000"][...]
                expected.reshape(-1)[:8] += 2 / 128
                np.testing.assert_array_equal(sixth["/data/u0000"][...], expected)

    def test_size_only_expands_nonmatched_units(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = data.prepare(spec(N=8, V_stored=1, size_profile="S0"), Path(temporary) / "small")
            large = data.prepare(spec(N=8, V_stored=1, size_profile="S1"), Path(temporary) / "large")
            self.assertGreater(large["size_distribution"]["min"], base["size_distribution"]["max"])
            for first, second in zip(base["entries"], large["entries"]):
                self.assertEqual(first["logical_file"], second["logical_file"])
                for old, new in zip(first["units"], second["units"]):
                    if data._matches(new["attributes"]):
                        self.assertEqual(new, old)
                    else:
                        self.assertEqual(new["schema"]["shape"], [1, 1, 384, 384])

    def test_mask_oracle_comes_from_native_values_and_missing_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "native.h5"
            data._hdf_create(path, spec(N=8, V_stored=1, size_profile="S0"), 0)
            with h5py.File(path, "r+") as output:
                dataset = output["/data/u0000"]
                values = dataset[...]
                values.reshape(-1)[:3] = [np.nan, np.inf, -999]
                dataset[...] = values
                dataset.attrs["missing_value"] = np.float32(-999)
            unit = data._hdf_inspect(path)[0]
            expected_mask = np.ones(4096, dtype="u1")
            expected_mask[:3] = 0
            self.assertEqual(unit["oracle"]["valid_elements"], 4093)
            self.assertEqual(unit["oracle"]["mask_sha256"], hashlib.sha256(expected_mask.tobytes()).hexdigest())
            values.reshape(-1)[:3] = 0
            self.assertEqual(unit["oracle"]["value_sha256"], hashlib.sha256(values.astype("<f4").tobytes()).hexdigest())


@unittest.skipUnless(ctypes.util.find_library("eccodes"), "native libeccodes not installed")
class NativeGribDataTests(unittest.TestCase):
    def test_native_inventory_and_exact_single_message_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "grib"
            manifest = data.prepare(spec("grib", history_scenario="single-unit-change"), root)
            self.assertEqual(len(manifest["entries"]), 64)
            self.assertTrue(all(change["changed_slots"] == [(change["ordinal"] - 2) % 4]
                                for change in manifest["version_changes"]))
            for entry in manifest["entries"]:
                messages = list(data._grib_messages(root / entry["file"]))
                self.assertEqual(len(messages), 4)
                for message, unit in zip(messages, entry["units"]):
                    self.assertEqual(hashlib.sha256(message).hexdigest(), unit["unit_id"])
                    self.assertEqual(unit["oracle"]["elements"], 4096)
                    self.assertEqual(unit["schema"]["bitsPerValue"], 16)


if __name__ == "__main__":
    unittest.main()
