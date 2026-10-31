"""Client contract tests; run with the public baseline NumPy/PyArrow environment."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from public_iotdbfs import IoTDBFSWindowReader, WindowArrays, compact_snapshot


class PreparedClientTests(unittest.TestCase):
    def reader(self, mount, mode="prepared_compact"):
        pb = {"dataset_id": "fixture", "channels": ["a", "b"],
              "arrow": {"sha256": "sha", "path": "fixture.arrow", "rows": [
                  {"start_step": 0, "end_step": 8192, "batch": 0, "row_in_batch": 0},
                  {"start_step": 8192, "end_step": 9000, "batch": 0, "row_in_batch": 1}]}}
        norm = {"artifact_id": "normalization", "statistics": [
            {"mean": 1, "stddev": 2}, {"mean": 3, "stddev": 4}]}
        windows = {"normalization_artifact_id": "normalization", "arrow_sha256": "sha",
                   "input_length": 96, "prediction_length": 24, "stride": 1,
                   "splits": {"train": {"target_interval": [0, 9000], "first_anchor": 96,
                                        "stop_anchor_exclusive": 8977, "stride": 1}}}
        with patch("public_iotdbfs.verify_seal"):
            return IoTDBFSWindowReader(socket_path=mount / "socket", mount=mount, pb00=pb,
                normalization=norm, windows=windows, split="train", channels=(0, 1),
                source_version_exact=7, registration_mode=mode)

    def test_shared_table_roundtrip_preserves_short_tail_and_exception(self):
        reader = self.reader(Path("/tmp"))
        original = copy.deepcopy(reader.snapshot)
        normalized = compact_snapshot(original)
        self.assertEqual(len(normalized["channel_tables"]), 1)
        self.assertEqual(original, reader.snapshot)
        self.assertEqual(normalized["rows"][1]["declared_length"], 808)
        tables = {t["id"]: t["channels"] for t in normalized["channel_tables"]}
        for before, after in zip(original["rows"], normalized["rows"]):
            restored = [dict(c, length=c.get("length", after["declared_length"]))
                        for c in tables[after["channel_table_id"]]]
            self.assertEqual(restored, before["channels"])
        original["rows"][1]["channels"][1]["length"] = 99
        changed = compact_snapshot(original)
        self.assertEqual(len(changed["channel_tables"]), 2)
        self.assertEqual(changed["channel_tables"][1]["channels"][1]["length"], 99)

    def test_explicit_step_major_mapping_survives_compaction(self):
        reader = self.reader(Path("/tmp"))
        self.assertTrue(all(r["value_layout"] == "channel_major" for r in reader.snapshot["rows"]))
        reader.pb00["arrow"]["value_layout"] = "step_major"
        snapshot = compact_snapshot(reader._snapshot())
        self.assertTrue(all(r["value_layout"] == "step_major" for r in snapshot["rows"]))
        self.assertEqual(len(snapshot["channel_tables"]), 1)

    def test_prepare_is_lazy_once_and_result_routes_are_released(self):
        with tempfile.TemporaryDirectory() as directory:
            mount = Path(directory)
            path = mount / "result.ipc"
            with pa.OSFile(str(path), "wb") as sink:
                table = pa.table({"test": [1]})
                with ipc.new_stream(sink, table.schema) as writer:
                    writer.write_table(table)
            reader = self.reader(mount)
            calls = []
            def control(socket, request, timing=None):
                calls.append(request)
                if request["kind"] == "window_prepare":
                    return {"kind": "prepared_window", "prepared_id": "id"}
                if request["kind"] == "window_execute":
                    return {"kind": "published_route", "path": str(path)}
                return {"released": True}
            arrays = WindowArrays(*(np.zeros((2, 1, 2)) for _ in range(4)))
            with patch("public_iotdbfs.control", side_effect=control), \
                 patch.object(reader, "_decode", return_value=arrays):
                self.assertIsNone(reader.prepared_id)
                reader.read_windows([96, 100])
                reader.read_windows([101, 102])
                reader.close()
            self.assertEqual([c["kind"] for c in calls], ["window_prepare", "window_execute",
                "window_result_release", "window_execute", "window_result_release", "window_release"])
            for request in calls:
                if request["kind"] == "window_execute":
                    self.assertEqual(set(request), {"kind", "prepared_id", "request"})
            self.assertEqual(len(reader.metrics["preparation_timings"]), 1)
            self.assertEqual(len(reader.metrics["release_timings"]), 2)
            with self.assertRaisesRegex(ValueError, "closed"):
                reader.read_windows([100])


if __name__ == "__main__":
    unittest.main()
