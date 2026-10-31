#!/usr/bin/env python3
"""Small independent parser, nested-stream and mask regression checks."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

import prepare_public_data as pb
import verify_public_data as oracle


class PublicDataTests(unittest.TestCase):
    def test_nested_fixture_windows_and_three_validity_levels(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            import json
            (root / "fixture-manifest.json").write_text(json.dumps(pb.generate_fixtures(root)))
            checked = oracle.verify_fixture(root / "fixture-manifest.json")
            self.assertEqual(set(checked), {"nested-multibatch-none.arrows", "nested-multibatch-lz4.arrows", "nested-multibatch-zstd.arrows", "nested-target-only-none.arrows", "nested-target-dictionary-delta.arrows", "nested-target-dictionary-replacement.arrows"})

    def test_ett_parser_and_window_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "ETTm1.csv"
            source.write_text("date," + ",".join(pb.ETTM1_CHANNELS) + "\n" +
                              "2020-01-01 00:00:00,1,2,3,4,5,6,7\n" +
                              "2020-01-01 00:15:00,8,9,,11,12,13,14\n" +
                              "2020-01-01 00:30:00,15,16,17,18,19,20,21\n")
            with patch.object(pb, "ETTM1_ROWS", 3):
                audit = pb.parse_source("ETTm1", source, pb.sha256(source), root / "canonical")
            stream = pb.write_dataset_stream("ETTm1", root / "canonical", root / "arrow" / "test.arrows", segment_steps=2, rows_per_batch=1, compression=None)
            audit["arrow"] = stream
            self.assertFalse(np.load(root / "canonical" / "ETTm1.valid.npy")[1, 2])
            import json
            path = root / "manifest.json"
            path.write_text(json.dumps(audit))
            self.assertGreater(oracle.verify_dataset(path, root)["windows_checked"], 0)

    def test_electricity_header_decimal_comma_and_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "LD2011_2014.txt"
            labels = [f"MT_{i:03d}" for i in range(1, 371)]
            source.write_text(";" + ";".join(labels) + "\n" +
                              "2011-01-01 00:15:00;" + ";".join(["1,25", "0", *(["2"] * 368)]) + "\n" +
                              "2011-01-01 00:30:00;" + ";".join(["", "3,5", *(["2"] * 368)]) + "\n")
            with patch.object(pb, "ELECTRICITY_ROWS", 2):
                audit = pb.parse_source("UCI_Electricity_370", source, pb.sha256(source), root / "canonical")
            values = np.load(root / "canonical" / "UCI_Electricity_370.values.npy")
            valid = np.load(root / "canonical" / "UCI_Electricity_370.valid.npy")
            self.assertEqual(audit["shape"], [2, 370])
            self.assertEqual(values[0, 0], np.float32(1.25))
            self.assertTrue(valid[0, 1])
            self.assertEqual(values[0, 1], 0)
            self.assertFalse(valid[1, 0])
            self.assertEqual(values[1, 1], np.float32(3.5))

    def test_source_hash_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "ETTm1.csv"
            source.write_text("different")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                pb.parse_source("ETTm1", source, "0" * 64, Path(temp) / "canonical")


if __name__ == "__main__":
    unittest.main()
