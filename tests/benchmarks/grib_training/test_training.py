"""Numerical and trace regression tests for the shared ERA5 trainer."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import torch

import dataset as training_data
from dataset import (FrozenERA5Dataset, fit_training_statistics,
                     load_trace, make_trace, save_trace)
from local_reader import Sample
from model import RegionalCNN, cosine_latitude_mse
from sample_contract import CHANNELS, PATCH, PROTOCOL, VIEWS
from train import _validate_contract


class FakeReader:
    def __init__(self):
        self.reads = 0

    def profile(self):
        return {"read_calls": self.reads}

    def sample(self, row, ordinals):
        self.reads += 1
        values = np.asarray(ordinals, dtype=np.float32)[None, :, None, None]
        x = np.broadcast_to(values, (2, len(ordinals), PATCH, PATCH)).copy()
        y = np.broadcast_to(values, (1, len(ordinals), PATCH, PATCH)).copy()
        return Sample(row["sample_id"], x, y,
                      np.ones_like(x, dtype=np.bool_),
                      np.ones_like(y, dtype=np.bool_))


class TrainingTest(unittest.TestCase):
    def test_frozen_contract_accepts_json_tuple_to_list_round_trip(self):
        expected = {"snapshot_fingerprint": "abc",
                    "splits": {"train": ("start", "end")},
                    "views": {"V0": {"samples": ["one"]}}}
        catalog = type("CatalogStub", (), {"freeze": lambda self: expected})()
        restored = json.loads(json.dumps(expected))
        _validate_contract(catalog, restored, "V0")
        restored["views"]["V0"]["samples"][0] = "changed"
        with self.assertRaises(ValueError):
            _validate_contract(catalog, restored, "V0")

    def test_statistics_count_unique_spatial_union_once(self):
        start = datetime(2018, 1, 1, tzinfo=timezone.utc)
        times = [start + index * timedelta(hours=6) for index in range(84)]
        catalog = type("CatalogStub", (), {
            "tier": "d1", "times": times, "fingerprint": "source",
        })()

        class FieldReader:
            def __init__(self, source):
                self.catalog = source
                self.calls = 0

            def _decode(self, when, ordinal):
                self.calls += 1
                time_index = int((when - start) / timedelta(hours=6))
                return np.broadcast_to(
                    np.arange(721, dtype=np.float32)[:, None] + time_index,
                    (721, 1440))

        reader = FieldReader(catalog)
        with patch.object(training_data, "CHANNELS", CHANNELS[:1]):
            statistics = fit_training_statistics(catalog, "east_asia", reader)
        self.assertEqual(reader.calls, 84)
        self.assertEqual(statistics["count"], [84 * 176 * 176])
        self.assertEqual(statistics["fit_domain"]["unique_grid_points_per_time"],
                         176 * 176)
        self.assertAlmostEqual(statistics["mean"][0], 41.5 + (156 + 331) / 2)
        expected_variance = ((84 ** 2 - 1) + (176 ** 2 - 1)) / 12
        self.assertAlmostEqual(statistics["stddev"][0] ** 2,
                               expected_variance, places=5)

    def test_model_shape_reflection_and_backward(self):
        model = RegionalCNN(8)
        self.assertEqual(len([m for m in model.modules()
                              if isinstance(m, torch.nn.Conv2d)]), 4)
        for layer in (model.network[0], model.network[2], model.network[4]):
            self.assertEqual(layer.padding_mode, "reflect")
            self.assertEqual(layer.kernel_size, (3, 3))
        x = torch.randn(2, 2, 8, 16, 16, requires_grad=True)
        prediction = model(x)
        self.assertEqual(tuple(prediction.shape), (2, 1, 8, 16, 16))
        loss = cosine_latitude_mse(
            prediction, torch.zeros_like(prediction),
            torch.ones_like(prediction, dtype=torch.bool), torch.tensor([156, 172]))
        loss.backward()
        self.assertTrue(torch.isfinite(x.grad).all())

    def test_area_weight_and_channel_average(self):
        target = torch.zeros(1, 1, 2, 2, 2)
        prediction = torch.tensor([[[[[1., 1.], [3., 3.]],
                                      [[2., 2.], [4., 4.]]]]])
        mask = torch.ones_like(target, dtype=torch.bool)
        mask[0, 0, 0, 0, 0] = False
        row = torch.tensor([156])
        result = cosine_latitude_mse(prediction, target, mask, row)
        lat0 = np.cos(np.deg2rad(90 - 156 * 0.25))
        lat1 = np.cos(np.deg2rad(90 - 157 * 0.25))
        channel0 = (lat0 + 18 * lat1) / (lat0 + 2 * lat1)
        channel1 = (8 * lat0 + 32 * lat1) / (2 * lat0 + 2 * lat1)
        self.assertAlmostEqual(float(result), (channel0 + channel1) / 2, places=5)

    def test_trace_replay_and_subset_standardization(self):
        rows = [{"sample_id": f"sample-{index}", "split": "train",
                 "view_fingerprint": "v", "row": 156, "col": 400}
                for index in range(6)]
        contract = {"protocol": PROTOCOL,
                    "tier": "d1", "snapshot_fingerprint": "snapshot",
                    "views": {"V2": {"spec": VIEWS["V2"],
                                      "samples": rows}}}
        trace = make_trace(contract, "V2", 17, 2, 2)
        self.assertEqual(len(set(trace["sample_ids"])), 4)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.json"
            save_trace(path, trace)
            self.assertEqual(load_trace(path, contract, "V2", 2, 2), trace)
            modified = json.loads(path.read_text())
            modified["sample_ids"][0] = "sample-not-in-train"
            path.write_text(json.dumps(modified))
            with self.assertRaises(ValueError):
                load_trace(path, contract, "V2", 2, 2)
        stats = {"region": "east_asia", "mean": [float(i) - 1 for i in range(20)],
                 "stddev": [2.0] * 20}
        reader = FakeReader()
        dataset = FrozenERA5Dataset(contract, "V2", trace, stats, reader)
        sample = dataset[0]
        self.assertEqual(sample.x.shape, (2, 8, PATCH, PATCH))
        self.assertTrue(np.array_equal(sample.x, np.full_like(sample.x, 0.5)))
        self.assertEqual(sample.reader_profile_delta["read_calls"], 1)


if __name__ == "__main__":
    unittest.main()
