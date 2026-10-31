"""Z1 regression tests for immutable identity, patch order, and byte accounting."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import numpy as np

try:
    import zarr
    from numcodecs import Blosc
    import zarr_backend as backend
except ImportError:
    zarr = None
    Blosc = None
    backend = None

from sample_contract import CHANNELS, HEIGHT, PATCH, WIDTH, iso


@unittest.skipIf(zarr is None, "zarr and numcodecs are required")
class ZarrBackendTest(unittest.TestCase):
    def _store(self, destination: Path) -> list[str]:
        times = [iso(datetime(2018, 1, 1, tzinfo=timezone.utc)
                     + timedelta(hours=6 * index)) for index in range(8)]
        root = zarr.open_group(str(destination), mode="w")
        shape = (8, len(CHANNELS), HEIGHT, WIDTH)
        codec = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
        values = root.create_dataset(
            "values", shape=shape, chunks=backend.CHUNKS, dtype="<f4",
            compressor=codec, fill_value=np.nan,
        )
        mask = root.create_dataset(
            "mask", shape=shape, chunks=backend.CHUNKS, dtype="bool",
            compressor=codec, fill_value=True,
        )
        root.attrs.update({
            "protocol": backend.PROTOCOL,
            "snapshot_fingerprint": "frozen-source-content",
            "tier": "d0", "channels": [asdict(channel) for channel in CHANNELS],
            "utc_times": times,
            "layout": ["time", "channel", "y", "x"],
            "chunks": list(backend.CHUNKS),
        })
        base = np.arange(PATCH * PATCH, dtype=np.float32).reshape(PATCH, PATCH)
        for time_index in range(3):
            values[time_index, 0, 156:284, 400:528] = base + 100000 * time_index
            values[time_index, 7, 156:284, 400:528] = (
                base + 700000 + 100000 * time_index)
        mask[1, 0, 156, 400] = False
        return times

    def test_patch_values_mask_and_real_encoded_byte_counters(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / "control.zarr"
            times = self._store(destination)
            reader = backend.ZarrReader(destination,
                                        expected_fingerprint="frozen-source-content")
            row = {
                "sample_id": "sample-identity", "split": "train",
                "row": 156, "col": 400,
                "input_times": times[:2], "target_time": times[2],
            }
            before = reader.metrics()
            sample = reader.sample(row, [0])
            after = reader.metrics()
            base = np.arange(PATCH * PATCH, dtype=np.float32).reshape(PATCH, PATCH)
            self.assertEqual(sample.sample_id, "sample-identity")
            self.assertEqual(sample.x.shape, (2, 1, PATCH, PATCH))
            self.assertEqual(sample.y.shape, (1, 1, PATCH, PATCH))
            np.testing.assert_array_equal(sample.x[0, 0], base)
            np.testing.assert_array_equal(sample.x[1, 0], base + 100000)
            np.testing.assert_array_equal(sample.y[0, 0], base + 200000)
            self.assertFalse(sample.x_mask[1, 0, 0, 0])
            self.assertEqual(np.count_nonzero(~sample.x_mask), 1)
            self.assertTrue(sample.y_mask.all())
            self.assertGreater(after["read_calls"] - before["read_calls"], 0)
            self.assertGreater(after["read_bytes"] - before["read_bytes"], 0)
            self.assertEqual(after["logical_bytes"] - before["logical_bytes"],
                             3 * PATCH * PATCH * (4 + 1))
            self.assertEqual(after["chunks_requested"] - before["chunks_requested"],
                             2 * 3 * 4)
            reordered = reader.sample(row, [7, 0])
            np.testing.assert_array_equal(reordered.x[0, 0], base + 700000)
            np.testing.assert_array_equal(reordered.x[0, 1], base)

    def test_identity_and_split_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / "control.zarr"
            times = self._store(destination)
            with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
                backend.ZarrReader(destination, expected_fingerprint="other")
            reader = backend.ZarrReader(destination)
            row = {
                "sample_id": "bad", "split": "validation",
                "row": 156, "col": 400,
                "input_times": times[:2], "target_time": times[2],
            }
            with self.assertRaisesRegex(ValueError, "crosses its frozen split"):
                reader.sample(row, [0])
            self.assertEqual(reader.metrics()["read_calls"], 0)

    def test_conversion_is_bounded_and_publishes_only_complete_store(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            source = directory / "source.grib"
            source.write_bytes(b"synthetic-source")
            source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
            when = datetime(2018, 1, 1, tzinfo=timezone.utc)
            channel = CHANNELS[0]

            class FakeCatalog:
                tier = "d0"
                times = [when]
                sources = [{"kind": "single-levels", "sha256": source_sha,
                            "bytes": source.stat().st_size}]
                paths = {"single-levels": source}
                rows = {when: {0: {"length": source.stat().st_size}}}
                fingerprint = "fake-fingerprint"

            field = np.full((HEIGHT, WIDTH), 12.5, dtype=np.float32)
            calls = []

            class FakeReader:
                def __init__(self, catalog, max_cache_bytes):
                    self.max_cache_bytes = max_cache_bytes

                def _decode(self, at, ordinal):
                    calls.append((at, ordinal))
                    return field

            destination = directory / "converted.zarr"
            with (mock.patch.object(backend, "CHANNELS", (channel,)),
                  mock.patch.object(backend, "LocalGribReader", FakeReader)):
                report = backend.convert_grib_to_zarr(FakeCatalog(), destination)
            self.assertEqual(calls, [(when, 0)])
            self.assertEqual(report["decoded_fields"], 1)
            self.assertEqual(report["source_message_bytes_read"], len(b"synthetic-source"))
            self.assertGreater(report["value_chunk_files"], 0)
            root = zarr.open_group(str(destination), mode="r")
            self.assertEqual(root.attrs["snapshot_fingerprint"], "fake-fingerprint")
            self.assertEqual(float(root["values"][0, 0, 0, 0]), 12.5)
            self.assertTrue(bool(root["mask"][0, 0, 0, 0]))
            with self.assertRaises(FileExistsError):
                backend.convert_grib_to_zarr(FakeCatalog(), destination)


if __name__ == "__main__":
    unittest.main()
