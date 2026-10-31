#!/usr/bin/env python3
"""Tiny fixtures only; real runtime/size acceptance belongs to server preparation."""
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import h5py
import numpy as np

import prepare_data as preparation


class PrepareDataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def generate_scaled(self, name="generated"):
        # The installed local library may differ. This patch is fixture-only;
        # manifest records the real runtime and NEVER passes real-size gates.
        with mock.patch.object(preparation, "assert_runtime_version"):
            return preparation.prepare(self.root / name, preparation.scaled_test_workload())

    def test_runtime_rejected_before_any_output_creation(self):
        with mock.patch.object(h5py.version, "hdf5_version", "1.14.5"):
            with self.assertRaisesRegex(ValueError, "exactly 1.14.4"):
                preparation.prepare(self.root / "new-parent" / "output")
        self.assertFalse((self.root / "new-parent").exists())

    def test_frozen_schema_filters_all_chunks_and_oracles(self):
        manifest = self.generate_scaled()
        self.assertTrue(manifest["synthetic_controlled"])
        self.assertFalse(manifest["all_real_size_gates_passed"])
        for tier in manifest["sources"].values():
            path = self.root / "generated" / tier["file"]
            self.assertEqual(preparation.sha256_file(path), tier["sha256"])
            self.assertEqual(path.stat().st_size, tier["bytes"])
            self.assertEqual(tier["allocated_chunk_count"], tier["expected_chunk_count"])
            self.assertFalse(tier["real_size_gate_passed"])
            with h5py.File(path, "r") as file:
                self.assertEqual(list(file), ["T"])
                dataset = file["T"]
                self.assertEqual(dataset.dtype.str, "<f4")
                self.assertEqual(dataset.chunks, (1, 1, 2, 3))
                self.assertTrue(dataset.shuffle)
                self.assertEqual((dataset.compression, dataset.compression_opts), ("gzip", 1))
                self.assertFalse(dataset.fletcher32)
                self.assertTrue(np.isfinite(dataset[:]).all())
                for oracle in manifest["cases"].values():
                    selections = tuple(slice(o, o + n) for o, n in zip(oracle["origin"], oracle["shape"]))
                    value = dataset[selections]
                    self.assertEqual(hashlib.sha256(value.astype("<f4").tobytes(order="C")).hexdigest(), oracle["value_sha256"])
                    self.assertEqual(hashlib.sha256(bytes([1]) * value.size).hexdigest(), oracle["mask_sha256"])

    def test_prefix_values_and_oracles_identical_across_tiers_and_runs(self):
        first = self.generate_scaled("first")
        second = self.generate_scaled("second")
        self.assertEqual(first["cases"], second["cases"])
        for name in first["sources"]:
            self.assertEqual(first["sources"][name]["sha256"], second["sources"][name]["sha256"])

    def test_existing_output_refused_without_modification(self):
        self.generate_scaled()
        existing = (self.root / "generated" / "manifest.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.generate_scaled()
        self.assertEqual(existing, (self.root / "generated" / "manifest.json").read_bytes())

    def test_failure_has_no_published_manifest_or_staging_leftovers(self):
        with mock.patch.object(preparation, "assert_runtime_version"), mock.patch.object(preparation, "sha256_file", side_effect=OSError("test read failure")):
            with self.assertRaisesRegex(OSError, "test read failure"):
                preparation.prepare(self.root / "failed", preparation.scaled_test_workload())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_production_gates_and_schema_cannot_be_relaxed(self):
        config = preparation.workload()
        self.assertEqual(config["chunks"], [1, 1, 91, 144])
        self.assertEqual([x["time_count"] for x in config["tiers"].values()], [2, 160, 1024])
        malformed = copy.deepcopy(config)
        malformed["tiers"]["large"]["minimum_file_bytes"] = 0
        with self.assertRaisesRegex(ValueError, "frozen production workload"):
            preparation.prepare(self.root / "bad", malformed)
        self.assertFalse((self.root / "bad").exists())

    def test_finite_bit_rewrite_and_nonconstant_generation(self):
        values = preparation.finite_chunk(np.random.Generator(np.random.PCG64(1)), (1, 1, 91, 144))
        self.assertTrue(np.isfinite(values).all())
        self.assertGreater(np.unique(values.view("<u4")).size, values.size * 0.99)
        self.assertEqual(values.nbytes, 91 * 144 * 4)

    def test_actual_size_gates_reject_missing_tiers_without_autotuning(self):
        tiers = preparation.workload()["tiers"]
        for name, tier in tiers.items():
            preparation.validate_size_gate(tier["minimum_file_bytes"], tier)
            with self.assertRaisesRegex(ValueError, "actual file size"):
                preparation.validate_size_gate(tier["minimum_file_bytes"] - 1, tier)
            if tier["maximum_file_bytes"] is not None:
                preparation.validate_size_gate(tier["maximum_file_bytes"], tier)
                with self.assertRaisesRegex(ValueError, "actual file size"):
                    preparation.validate_size_gate(tier["maximum_file_bytes"] + 1, tier)

    def test_dummy_compressible_data_is_not_accepted_as_dense_random_payload(self):
        with mock.patch.object(preparation, "assert_runtime_version"), mock.patch.object(
                preparation, "finite_chunk", side_effect=lambda rng, shape: np.zeros(shape, dtype="<f4")):
            with self.assertRaisesRegex(ValueError, "fully allocated dense random data"):
                preparation.prepare(self.root / "zero", preparation.scaled_test_workload())
        self.assertEqual(list(self.root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
