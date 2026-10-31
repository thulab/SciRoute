"""Data-contract, corruption and native transformation acceptance tests."""
import copy
import ctypes.util
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from common import (TARGET, assert_dimensions, canonical, counts, identity,
                    load_manifest, matches, publication, relative_file,
                    safe_file, seal, validate_spec)
import manager


def spec(format_id="hdf5", n=3, k=1, d=2, depth=2):
    payload = ({"shape": [1, 1, 4, 6], "chunks": [1, 1, 2, 3],
                "gzip_level": 1, "shuffle": True} if format_id == "hdf5" else
               {"Ni": 4, "Nj": 3, "bits_per_value": 16, "packing": "grid_simple"})
    return validate_spec({"protocol": "iotdbfs-multifile-spec/1", "format": format_id,
        "N": n, "K": k, "D": d, "depth": depth, "seed": 42,
        "tree_width": 2, "payload": payload, "max_output_bytes": 16 * 1024**2})


class ContractTests(unittest.TestCase):
    def test_invalid_specs(self):
        for patch in ({"N": 0}, {"N": True}, {"K": 4}, {"D": 0},
                      {"depth": 9}, {"unknown": 0}, {"seed": -1}, {"tree_width": 0},
                      {"N": 4096, "D": 512}, {"max_output_bytes": 1},
                      {"query": {"level": float("nan")}}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                validate_spec({**spec(), **patch})

    def test_unknown_payload_fields(self):
        s = spec()
        s["payload"]["dynamic_plugin"] = "anything"
        with self.assertRaises(ValueError):
            validate_spec(s)

    def test_paths_are_stable_and_depth_only_changes_directories(self):
        s = spec()
        before = relative_file(s, 0)
        after = relative_file({**s, "depth": 8}, 0)
        self.assertEqual(Path(before).name, Path(after).name)
        self.assertEqual(len(Path(after).parts) - 2, 8)

    def test_typed_matching(self):
        self.assertFalse(matches({"level": 500.0}, {"level": 500}))
        self.assertFalse(matches({}, {"level": 500}))
        self.assertTrue(matches({"level": 500}, {"level": 500}))

    def test_no_replacement_and_insufficient_distractors(self):
        s = spec(n=2, k=1, d=2)
        units = [{"unit_id": "a", "attributes": TARGET},
                 {"unit_id": "b", "attributes": {**TARGET, "level": 850}}]
        parent = {"format": "hdf5", "sources": [{"controlled": True, "units": units}]}
        with self.assertRaisesRegex(ValueError, "insufficient pool"):
            manager.grouping(parent, s)

    def test_multiple_matches_in_a_file_rejected(self):
        sources = [{"units": [{"unit_id": str(i), "attributes": TARGET} for i in range(2)]}]
        with self.assertRaises(ValueError):
            assert_dimensions(sources, spec(n=1, k=1, d=2))

    def test_unsafe_manifest_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ("../escape", "/absolute"):
                with self.assertRaises(ValueError):
                    safe_file(directory, name)
            outside = Path(directory).parent
            (Path(directory) / "alias").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(ValueError):
                safe_file(directory, "alias/anything")

    def test_atomic_failure_and_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output"
            with self.assertRaises(RuntimeError):
                with publication(path) as stage:
                    (stage / "partial").write_text("data")
                    raise RuntimeError("injected")
            self.assertFalse(path.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])
            path.mkdir()
            (path / "keep").write_text("original")
            with self.assertRaises(FileExistsError):
                with publication(path):
                    pass
            self.assertEqual((path / "keep").read_text(), "original")

    def test_manifest_edit_invalidates_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / "manifest.json"
            data = seal({"protocol": "iotdbfs-multifile-data/1", "sources": []})
            p.write_bytes(canonical(data))
            load_manifest(directory)
            data["sources"] = [{"file": "different"}]
            p.write_bytes(canonical(data))
            with self.assertRaises(ValueError):
                load_manifest(directory)

    def test_invalid_gradient_does_not_publish_partial_specs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "plan"
            with self.assertRaises(ValueError):
                manager.gradient(spec(), "K", [0, 4], output)
            self.assertFalse(output.exists())


NATIVE = bool(importlib.util.find_spec("numpy") and importlib.util.find_spec("h5py"))


@unittest.skipUnless(NATIVE, "requires pinned HDF5/NumPy environment")
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_hdf5_gradient_endpoints_and_chunk_allocation(self):
        for index, (n, k, d, depth) in enumerate(((1, 0, 1, 1), (3, 1, 3, 4), (2, 2, 2, 8))):
            s = spec(n=n, k=k, d=d, depth=depth)
            m = manager.generate(s, self.root / str(index))
            self.assertEqual((m["actual"]["N"], m["actual"]["K"]), (n, k))
            self.assertEqual(m["actual"]["K_unit"], k)
            self.assertTrue(all(u["allocated_chunks"] == 4 for f in m["sources"] for u in f["units"]))
            manager.verify(self.root / str(index))

    def test_hdf5_merge_then_split_preserves_native_unit_oracles(self):
        original = self.root / "original"
        parent = manager.generate(spec(n=4, k=2, d=3), original)
        s = spec(n=2, k=2, d=6, depth=4)
        s["payload"] = {"policy": "preserve"}
        merged = manager.regroup(original, s, self.root / "merged")
        self.assertEqual(parent["actual"]["unique_unit_count"], merged["actual"]["unique_unit_count"])
        self.assertEqual({u["unit_id"] for f in parent["sources"] for u in f["units"]},
                         {u["unit_id"] for f in merged["sources"] for u in f["units"]})
        split_spec = {**s, "N": 6, "D": 2}
        split = manager.regroup(self.root / "merged", split_spec, self.root / "split")
        self.assertEqual(split["actual"]["matches_per_file"], [1, 1, 0, 0, 0, 0])
        manager.verify(self.root / "split")

    def test_depth_copy_is_byte_identical(self):
        original = self.root / "original"
        parent = manager.generate(spec(), original)
        copied = manager.reorganize(original, self.root / "copy", 8, 2, 123, 16 * 1024**2)
        self.assertEqual([f["sha256"] for f in parent["sources"]], [f["sha256"] for f in copied["sources"]])
        self.assertEqual(parent["spec"]["seed"], copied["spec"]["seed"])
        self.assertEqual(copied["tree"]["seed"], 123)
        manager.verify(self.root / "copy")

    def test_generation_logical_oracles_reproducible(self):
        left = manager.generate(spec(), self.root / "left")
        right = manager.generate(spec(), self.root / "right")
        self.assertEqual([u["unit_id"] for f in left["sources"] for u in f["units"]],
                         [u["unit_id"] for f in right["sources"] for u in f["units"]])

    def test_public_hdf5_tree_copy_and_regroup_rejection(self):
        import h5py
        import numpy as np
        raw = self.root / "public.h5"
        with h5py.File(raw, "x") as f:
            d = f.create_dataset("T", data=np.arange(12, dtype="<f4").reshape(3, 4))
            d.attrs["standard_name"] = np.bytes_("air_temperature")
        inv = self.root / "inventory"
        query = {"standard_name": "air_temperature"}
        parent = manager.inventory("hdf5", [raw], query, inv, ["/T"])
        copied = manager.reorganize(inv, self.root / "copy", 4, 2, 42, 1048576)
        self.assertEqual(parent["sources"][0]["sha256"], copied["sources"][0]["sha256"])
        s = spec(n=1, k=1, d=1)
        s.update(payload={"policy": "preserve"}, query=query)
        with self.assertRaisesRegex(ValueError, "public HDF5"):
            manager.regroup(inv, s, self.root / "invalid")
        self.assertFalse((self.root / "invalid").exists())

    def test_public_hdf5_requires_explicit_data_dataset_roles(self):
        import h5py
        raw = self.root / "public.h5"
        with h5py.File(raw, "x") as f:
            f.create_dataset("T", data=[1., 2.])
        with self.assertRaisesRegex(ValueError, "allowlist"):
            manager.inventory("hdf5", [raw], TARGET, self.root / "invalid")
        self.assertFalse((self.root / "invalid").exists())

    def test_tampered_source_and_extra_candidate_detected(self):
        original = self.root / "original"
        m = manager.generate(spec(), original)
        extra = original / "raw" / "unexpected.h5"
        extra.write_bytes(b"extra candidate")
        with self.assertRaisesRegex(ValueError, "candidate file set"):
            manager.verify(original)
        extra.unlink()
        path = original / m["sources"][0]["file"]
        with path.open("r+b") as stream:
            stream.write(b"corrupt")
        with self.assertRaisesRegex(ValueError, "source bytes changed"):
            manager.verify(original)

    def test_actual_metadata_overhead_budget_failure_is_atomic(self):
        s = spec(n=1, k=0, d=1)
        s["max_output_bytes"] = 96  # logical values fit, HDF5 metadata cannot.
        with self.assertRaisesRegex(ValueError, "actual raw output"):
            manager.generate(s, self.root / "invalid")
        self.assertFalse((self.root / "invalid").exists())

    def test_gradient_collection_and_depth_copy(self):
        base = spec(n=2, k=1, d=2)
        manager.gradient(base, "N", [2, 3], self.root / "plan")
        summary = manager.materialize(self.root / "plan", self.root / "collection", "generate")
        self.assertEqual(manager.verify(self.root / "collection")["datasets"], 2)
        self.assertEqual([d["actual"]["N"] for d in summary["datasets"]], [2, 3])
        for item in summary["datasets"]:
            manager.verify((self.root / "collection" / item["manifest"]).parent)
        original = (self.root / "collection" / summary["datasets"][0]["manifest"]).parent
        manager.gradient(base, "depth", [1, 4, 8], self.root / "depth-plan")
        copied = manager.materialize(self.root / "depth-plan", self.root / "depths", "reorganize", original)
        parent = load_manifest(original)
        for item in copied["datasets"]:
            m = load_manifest((self.root / "depths" / item["manifest"]).parent)
            self.assertEqual([s["sha256"] for s in parent["sources"]], [s["sha256"] for s in m["sources"]])
            self.assertEqual(m["tree"]["depth"], item["value"])
        self.assertTrue(manager.verify(self.root / "depths")["verified"])

    def test_collection_manifest_mismatch_detected(self):
        manager.gradient(spec(n=1), "depth", [1, 4], self.root / "plan")
        manager.materialize(self.root / "plan", self.root / "collection", "generate")
        path = self.root / "collection" / "collection.json"
        value = json.loads(path.read_text())
        value["datasets"][0]["manifest_sha256"] = "0" * 64
        path.write_bytes(canonical(seal(value)))
        with self.assertRaisesRegex(ValueError, "member differs"):
            manager.verify(self.root / "collection")

    def test_gradient_aggregate_budget_failure_is_atomic(self):
        manager.gradient(spec(), "N", [3, 4], self.root / "plan")
        with self.assertRaises(ValueError):
            manager.materialize(self.root / "plan", self.root / "invalid", "generate", max_total_bytes=500)
        self.assertFalse((self.root / "invalid").exists())

    def test_frozen_gradient_plan_tampering_is_detected(self):
        manager.gradient(spec(), "N", [3, 4], self.root / "plan")
        path = self.root / "plan" / "plan.json"
        plan = json.loads(path.read_text())
        plan["jobs"][0]["spec"]["K"] = 0
        path.write_text(json.dumps(plan))
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            manager.materialize(self.root / "plan", self.root / "invalid", "generate")
        self.assertFalse((self.root / "invalid").exists())

    @unittest.skipUnless(ctypes.util.find_library("eccodes"), "requires native ecCodes")
    def test_grib_generation_regrouping_and_byte_oracles(self):
        original = self.root / "original"
        parent = manager.generate(spec("grib", n=4, k=2, d=3), original)
        s = spec("grib", n=2, k=2, d=6, depth=4)
        s["payload"] = {"policy": "preserve"}
        merged = manager.regroup(original, s, self.root / "merged")
        self.assertEqual(merged["actual"]["K_unit"], 2)
        self.assertEqual({u["unit_id"] for f in parent["sources"] for u in f["units"]},
                         {u["unit_id"] for f in merged["sources"] for u in f["units"]})
        copied = manager.reorganize(self.root / "merged", self.root / "copy", 8, 2, 42, 16 * 1024**2)
        self.assertEqual([s["sha256"] for s in merged["sources"]], [s["sha256"] for s in copied["sources"]])
        manager.verify(self.root / "copy")

    @unittest.skipUnless(ctypes.util.find_library("eccodes"), "requires native ecCodes")
    def test_grib_truncation_is_not_eof(self):
        import grib_data
        original = self.root / "original"
        m = manager.generate(spec("grib", n=1, k=0, d=1), original)
        raw = original / m["sources"][0]["file"]
        bad = self.root / "bad.grib"
        bad.write_bytes(raw.read_bytes()[:-1])
        with self.assertRaisesRegex(ValueError, "truncated"):
            grib_data.inspect(bad)


if __name__ == "__main__":
    unittest.main()
