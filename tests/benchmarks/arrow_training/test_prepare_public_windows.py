#!/usr/bin/env python3
"""Focused PB01 split, training-statistics and window identity regressions."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

import prepare_public_windows as pb01


class PublicWindowTests(unittest.TestCase):
    def test_ett_fixed_index_boundaries_and_unused_tail(self) -> None:
        points, rule = pb01.split_points("ETTm1", 69680, Path("unused"))
        self.assertEqual(points, {
            "train": (0, 34560),
            "validation": (34560, 46080),
            "test": (46080, 57600),
        })
        self.assertEqual(rule["unused_tail"], [57600, 69680])

    def test_electricity_uses_year_labels_with_repeated_local_times(self) -> None:
        labels = [
            "2011-01-01 00:15:00", "2012-10-28 02:00:00", "2012-10-28 01:45:00",
            "2013-01-01 00:00:00", "2013-12-31 23:45:00", "2014-01-01 00:00:00",
        ]
        self.assertEqual(pb01.electricity_split_points(labels), {
            "train": (0, 3), "validation": (3, 5), "test": (5, 6),
        })
        self.assertEqual(pb01.electricity_split_points(labels + ["2015-01-01 00:00:00"]), {
            "train": (0, 3), "validation": (3, 5), "test": (5, 6),
        })
        with self.assertRaisesRegex(ValueError, "out of order"):
            pb01.electricity_split_points(labels + ["2013-12-31 23:45:00"])

    def test_training_statistics_exclude_invalid_and_future_values(self) -> None:
        values = np.asarray([
            [1, 7, 0], [3, 7, 0], [100, 7, 0], [9999, 9999, 9999],
        ], dtype=np.float32)
        valid = np.asarray([
            [1, 1, 1], [1, 1, 0], [0, 1, 1], [1, 1, 1],
        ], dtype=np.bool_)
        stats = pb01.training_statistics(values, valid, 3, ["a", "b", "c"])
        self.assertEqual([item["valid_count"] for item in stats], [2, 3, 2])
        self.assertEqual([item["mean"] for item in stats], [2.0, 7.0, 0.0])
        self.assertEqual([item["stddev"] for item in stats], [1.0, 1.0, 1.0])
        self.assertEqual([item["constant_channel"] for item in stats], [False, True, True])

    def test_target_inside_split_and_validation_history_allowed(self) -> None:
        points = {"train": (0, 20), "validation": (20, 30), "test": (30, 40)}
        splits = pb01.window_splits(points, 40, 5, 3, 2)
        self.assertEqual([splits[key]["count"] for key in ("train", "validation", "test")], [7, 4, 4])
        self.assertEqual(splits["train"]["first_anchor"], 5)
        self.assertEqual(splits["validation"]["first_anchor"], 20)
        self.assertEqual(splits["validation"]["input_history_policy"], "preceding_split_allowed")
        manifest = pb01.seal({
            "splits": splits, "sample_id_prefix": "test/source/L5-H3-S2", "entity_id": "e",
            "input_length": 5, "prediction_length": 3,
        })
        first_val = next(pb01.iter_windows(manifest, "validation"))
        self.assertEqual(first_val, {
            "sample_id": "test/source/L5-H3-S2/validation/20", "entity_id": "e",
            "anchor": 20, "input_range": [15, 20], "target_range": [20, 23],
        })
        for split in ("train", "validation", "test"):
            windows = list(pb01.iter_windows(manifest, split))
            self.assertEqual(len(windows), splits[split]["count"])
            self.assertTrue(all(splits[split]["target_interval"][0] <= item["target_range"][0] and
                                item["target_range"][1] <= splits[split]["target_interval"][1]
                                for item in windows))

    def test_artifact_hash_stable_and_detects_edit(self) -> None:
        document = pb01.seal({"channels": ["温度", "load"], "split": [0, 100]})
        self.assertEqual(document, pb01.seal({"split": [0, 100], "channels": ["温度", "load"]}))
        pb01.verify_seal(document)
        document["split"][1] = 101
        with self.assertRaisesRegex(ValueError, "content hash mismatch"):
            pb01.verify_seal(document)

    def test_full_artifact_bindings_and_repeatability(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "canonical"
            manifests = root / "manifests"
            arrow = root / "arrow"
            canonical.mkdir()
            manifests.mkdir()
            arrow.mkdir()
            values = np.lib.format.open_memmap(canonical / "ETTm1.values.npy", mode="w+", dtype="<f4", shape=(69680, 2))
            valid = np.lib.format.open_memmap(canonical / "ETTm1.valid.npy", mode="w+", dtype=np.bool_, shape=(69680, 2))
            values[:] = [2, 4]
            valid[:] = True
            values.flush()
            valid.flush()
            (canonical / "ETTm1.timestamps.txt").write_text("label\n" * 69680)
            (arrow / "test.arrows").write_bytes(b"arrow")
            pb00 = {
                "dataset_id": "ETTm1", "source_sha256": "a" * 64, "shape": [69680, 2], "channels": ["a", "b"],
                "canonical": {path.name: {"sha256": pb01.file_sha256(path)} for path in canonical.iterdir()},
                "arrow": {"path": "test.arrows", "sha256": pb01.file_sha256(arrow / "test.arrows")},
            }
            (manifests / "ETTm1-pb00.json").write_text(json.dumps(pb00))
            output = pb01.prepare_dataset(root, "ETTm1")
            first = [path.read_bytes() for path in output]
            self.assertEqual(first, [path.read_bytes() for path in pb01.prepare_dataset(root, "ETTm1")])
            normalization = json.loads(output[0].read_text())
            windows = json.loads(output[1].read_text())
            pb01.verify_seal(normalization)
            pb01.verify_seal(windows)
            self.assertEqual(windows["normalization_artifact_id"], normalization["artifact_id"])
            self.assertEqual(windows["arrow_sha256"], pb00["arrow"]["sha256"])
            self.assertEqual(windows["splits"]["test"]["target_interval"], [46080, 57600])
            self.assertTrue(next(pb01.iter_windows(windows, "train"))["sample_id"].startswith("pb01/ETTm1/"))
            (arrow / "test.arrows").write_bytes(b"edited")
            with self.assertRaisesRegex(ValueError, "stream hash mismatch"):
                pb01.prepare_dataset(root, "ETTm1")


if __name__ == "__main__":
    unittest.main()
