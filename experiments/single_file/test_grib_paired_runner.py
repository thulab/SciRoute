import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa

import grib_paired_runner as run


class ProfileContracts(unittest.TestCase):
    def fixture(self, view="values"):
        case = {"view": view, "oracle": {"rows": 1, "message_lengths": [66606]}}
        profile = {"counters": {"typed_cursor_next_calls": 2, "typed_ipc_batches": 1,
                               "typed_ipc_rows": 1, "typed_prepared_reuse": 1},
                   "iotdb_delta": {"metadata_query_calls": 0, "layout_calls": 0,
                                   "payload_requested_bytes": 66606,
                                   "payload_returned_bytes": 66606, "payload_calls": 1}}
        return case, profile

    def test_binary_miss_reads_and_warm_hit_skips_payload(self):
        case, profile = self.fixture("binary")
        run.check_profile(profile, case, 0, True)
        with self.assertRaisesRegex(ValueError, "pruning"):
            run.check_profile(profile, case, 99, True)
        for key in ("payload_requested_bytes", "payload_returned_bytes", "payload_calls"):
            profile["iotdb_delta"][key] = 0
        run.check_profile(profile, case, 99, True)
        run.check_profile(profile, case, 99, False)
        with self.assertRaisesRegex(ValueError, "pruning"):
            run.check_profile(profile, case, 0, True)

    def test_warm_numerical_reads_reuse_data_but_need_a_fresh_cursor(self):
        case, profile = self.fixture()
        run.check_profile(profile, case, 0, True)
        with self.assertRaisesRegex(ValueError, "pruning"):
            run.check_profile(profile, case, 1, True)
        for key in ("payload_requested_bytes", "payload_returned_bytes", "payload_calls"):
            profile["iotdb_delta"][key] = 0
        run.check_profile(profile, case, 1, True)
        profile["counters"]["typed_cursor_next_calls"] = 0
        with self.assertRaisesRegex(ValueError, "cursor"):
            run.check_profile(profile, case, 1, True)

    def test_bound_read_cannot_repeat_planning_sql(self):
        case, profile = self.fixture()
        profile["iotdb_delta"]["metadata_query_calls"] = 1
        with self.assertRaisesRegex(ValueError, "planning"):
            run.check_profile(profile, case, 0, True)


class IpcContracts(unittest.TestCase):
    def test_values_are_owned_and_exact_provenance_is_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.ipc"
            arrays = [np.array([0.], dtype="<f8"), np.array([0.], dtype="<f8"),
                      np.array([3.], dtype="<f4")]
            schema = pa.schema([("lat", pa.float64()), ("lon", pa.float64()), ("value", pa.float32())])
            batch = pa.record_batch(arrays, schema=schema)
            metadata = {"iotdbfs.sources": "1", "iotdbfs.source.0.path": "/x.grib2",
                        "iotdbfs.source.0.version": "7", "iotdbfs.source.0.format_id": "grib",
                        "iotdbfs.domain.present": "1", "iotdbfs.domain.dimension.0.shape": "1",
                        "iotdbfs.domain.dimension.1.shape": "1"}
            with pa.OSFile(str(path), "wb") as file:
                with pa.ipc.new_stream(file, schema) as writer:
                    writer.write_batch(batch, custom_metadata=metadata)
            digest = lambda data: hashlib.sha256(data).hexdigest()
            case = {"view": "values", "oracle": {"rows": 1, "messages": [{"shape": [1, 1]}],
                    "latitude_sha256": digest(arrays[0].tobytes()),
                    "longitude_sha256": digest(arrays[1].tobytes()),
                    "value_sha256": digest(arrays[2].tobytes()), "mask_sha256": digest(b"\1")}}
            with patch.object(run.base, "control", return_value=({"path": str(path)}, 1)):
                row = run.query({"socket": "unused"}, "/x.grib2", 7, case, "binding")
                self.assertEqual(row["rows"], 1)
                self.assertGreater(row["total_ns"], row["registration_ns"])
                with self.assertRaisesRegex(ValueError, "provenance"):
                    run.query({"socket": "unused"}, "/x.grib2", 8, case, "binding")
            with pa.memory_map(str(path), "r") as file:
                reader = pa.ipc.open_stream(file)
                owned, _, _, _ = run.extract(reader, "values")
                reader.close()
            path.unlink()
            self.assertTrue(owned["values"].flags.owndata)
            self.assertEqual(owned["values"].tolist(), [3.])


if __name__ == "__main__":
    unittest.main()
