"""Focused correctness and cache checks for the independent local A/R1 reader."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_public_data import SCHEMA, _target_from_matrix
from public_backends import LocalArrowWindowReader


def _stream(path: Path) -> tuple[np.ndarray, np.ndarray, list[dict]]:
    values = np.arange(48, dtype=np.float32).reshape(12, 4)
    valid = np.ones_like(values, dtype=np.bool_)
    valid[2, 1] = False
    valid[8, 3] = False
    values[~valid] = 0
    rows = []
    with path.open("wb") as handle, ipc.new_stream(handle, SCHEMA) as writer:
        for batch_number, (start, end) in enumerate(((0, 4), (4, 8), (8, 12))):
            target = _target_from_matrix(values[start:end], valid[start:end])
            batch = pa.record_batch(
                [
                    pa.array(["tiny"]), pa.array(["tiny"]), pa.array(["tiny"]),
                    pa.array([batch_number], type=pa.int32()),
                    pa.array([start], type=pa.int64()),
                    pa.array([end], type=pa.int64()),
                    target,
                ],
                schema=SCHEMA,
            )
            writer.write_batch(batch)
            rows.append({"batch": batch_number, "row_in_batch": 0,
                         "start_step": start, "end_step": end, "channels": 4})
    return values, valid, rows


class LocalArrowWindowReaderTest(unittest.TestCase):
    def test_manifest_index_and_cross_batch_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tiny.arrows"
            expected, valid, rows = _stream(path)
            with LocalArrowWindowReader(path, row_index=rows) as reader:
                self.assertEqual((reader.total_steps, reader.channel_count), (12, 4))
                values, mask = reader.read_window(2, 8, [3, 1, 0])
                np.testing.assert_array_equal(values, expected[2:10, [3, 1, 0]])
                np.testing.assert_array_equal(mask, valid[2:10, [3, 1, 0]])
                self.assertEqual(reader.metrics["batch_decodes"], 3)
                reader.read_window(4, 3, [0])
                self.assertGreaterEqual(reader.metrics["cache_hits"], 1)
                self.assertEqual(reader.metrics["batch_decodes"], 3)

    def test_index_fallback_and_zero_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tiny.arrows"
            expected, valid, _ = _stream(path)
            with LocalArrowWindowReader(path, max_cache_bytes=0) as reader:
                first, mask = reader.read_window(3, 6, [2, 1])
                np.testing.assert_array_equal(first, expected[3:9, [2, 1]])
                np.testing.assert_array_equal(mask, valid[3:9, [2, 1]])
                self.assertEqual(reader.metrics["cache_entries"], 0)
                self.assertEqual(reader.metrics["cache_bytes"], 0)

    def test_decoded_cache_evicts_at_byte_limit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tiny.arrows"
            _, _, rows = _stream(path)
            with path.open("rb") as handle:
                batch_bytes = next(iter(ipc.open_stream(handle))).nbytes
            with LocalArrowWindowReader(
                path, row_index=rows, max_cache_bytes=batch_bytes
            ) as reader:
                reader.read_window(0, 2, [0])
                reader.read_window(4, 2, [0])
                self.assertEqual(reader.metrics["cache_entries"], 1)
                self.assertEqual(reader.metrics["cache_evictions"], 1)
                self.assertLessEqual(reader.metrics["cache_bytes"], batch_bytes)
                reader.read_window(0, 2, [0])
                self.assertEqual(reader.metrics["batch_decodes"], 3)

    def test_bad_manifest_and_range_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tiny.arrows"
            _, _, rows = _stream(path)
            bad = [dict(row) for row in rows]
            bad[1]["start_step"] = 5
            with self.assertRaisesRegex(ValueError, "contiguous steps"):
                LocalArrowWindowReader(path, row_index=bad)
            with LocalArrowWindowReader(path, row_index=rows) as reader:
                with self.assertRaisesRegex(ValueError, "outside"):
                    reader.read_window(11, 2, [0])
                with self.assertRaisesRegex(ValueError, "channel"):
                    reader.read_window(0, 1, [4])
            with self.assertRaisesRegex(ValueError, "closed"):
                reader.read_window(0, 1, [0])


if __name__ == "__main__":
    unittest.main()
