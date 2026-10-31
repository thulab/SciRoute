import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch, Mock

import runner


class AccountingTests(unittest.TestCase):
    def test_prefix_cost_includes_first_query_and_one_binding(self):
        observations = [{"total_ns": x} for x in [100, 20, 30]]
        value = runner.summary(1000, 40, observations, [1, 3])
        self.assertEqual(value["query_sum_ns_by_q"], {"1": 100, "3": 150})
        self.assertEqual(value["lifecycle_ns_by_q"], {"1": 1140, "3": 1190})
        self.assertEqual(value["first_query_ns"], 100)
        self.assertEqual(value["warm_p50_ns"], 25)

    def test_cannot_extrapolate_missing_queries(self):
        with self.assertRaisesRegex(ValueError, "not enough"):
            runner.summary(1, 2, [{"total_ns": 3}], [1, 100])

    def test_exact_version_and_half_open_four_dimensional_request(self):
        case = {"origin": [1, 0, 88, 140], "shape": [1, 1, 8, 12]}
        value = runner.query_request("/single-file-X/small.h5", 9, case)
        self.assertEqual(value["request"]["sources"]["versions"], {"kind": "exact", "exact": 9})
        slices = value["request"]["subset"]["slices"]
        self.assertEqual([x["upper"]["value"]["value"] for x in slices], [2, 1, 96, 152])

    def test_bad_shape_or_version_descriptor_is_not_accepted(self):
        case = {"origin": [1, 0, 88, 140], "shape": [1, 1, 8, 12], "rows": 96}
        with self.assertRaisesRegex(ValueError, "descriptor"):
            runner.validate_descriptor({b"iotdbfs.result_descriptors": b"1"}, "/x", 9,
                                       [2, 1, 728, 1152], case)

    def test_copy_refuses_existing_output_and_returns_same_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            src, dst = root / "src", root / "dst"
            src.write_bytes(b"x" * (1 << 20) + b"tail")
            result = runner.checked_copy(src, dst)
            self.assertEqual(runner.sha_file(src), runner.sha_file(dst))
            self.assertGreater(result["total_ns"], 0)
            self.assertEqual(result["total_ns"], sum(result[k] for k in ["open_ns", "copy_ns", "file_fsync_ns", "close_ns"]))
            with self.assertRaisesRegex(ValueError, "already exists"):
                runner.checked_copy(src, dst)

    def test_short_control_frame_is_rejected(self):
        stream = Mock()
        stream.recv.side_effect = [b"abc", b""]
        with self.assertRaisesRegex(ValueError, "short control"):
            runner.receive(stream, 4)

    def test_native_same_subset_hash_cannot_hide_wrong_size_tier(self):
        case = {"origin": [1, 0, 0, 0], "shape": [1, 1, 1, 1], "rows": 1,
                "dtype": "<f4", "value_sha256": "good", "mask_sha256": "goodmask"}
        row = dict(case, runtime_hdf5="1.14.4", library_threadsafe=True,
                   source_shape=[2, 1, 728, 1152], total_ns=1)
        returned = subprocess.CompletedProcess([], 0, json.dumps(row), "")
        with patch.object(runner.subprocess, "run", return_value=returned):
            with self.assertRaisesRegex(ValueError, "full source shape"):
                runner.native_wave("unused", "unused", {"shape": [160, 1, 728, 1152]}, case, 1, {})

    def test_unmount_failure_still_reaps_owned_daemon(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        original_exists = Path.exists
        def exists(path):
            return True if str(path).startswith("/tmp/sf-") else original_exists(path)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(runner.subprocess, "Popen", return_value=process), \
                 patch.object(runner.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["fusermount"])), \
                 patch.object(runner.os.path, "ismount", return_value=True), \
                 patch.object(Path, "exists", exists):
                with self.assertRaises(subprocess.CalledProcessError):
                    with runner.private_mount("unused", Path(directory) / "owned", {}, Path(directory) / "lock"):
                        pass
        process.wait.assert_called_once()

    def test_short_writes_are_retried(self):
        original_open = Path.open
        class Partial:
            def __init__(self, file): self.file = file
            def write(self, value): return self.file.write(value[:7])
            def flush(self): return self.file.flush()
            def fileno(self): return self.file.fileno()
            def close(self): return self.file.close()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            src, dst = root / "src", root / "dst"
            src.write_bytes(bytes(range(256)))
            def opened(path, *args, **kwargs):
                file = original_open(path, *args, **kwargs)
                return Partial(file) if path == dst and args and args[0] == "xb" else file
            with patch.object(Path, "open", opened):
                runner.checked_copy(src, dst)
            self.assertEqual(src.read_bytes(), dst.read_bytes())


class IpcContentTests(unittest.TestCase):
    def test_bad_value_null_and_source_descriptor_fail(self):
        import numpy as np
        import pyarrow as pa
        case = {"origin": [1, 0, 0, 0], "shape": [1, 1, 1, 2], "rows": 2, "dtype": "<f4",
                "value_sha256": hashlib.sha256(np.array([1, 2], dtype="<f4").tobytes()).hexdigest(),
                "mask_sha256": hashlib.sha256(bytes([1, 1])).hexdigest()}
        full = [2, 1, 4, 6]
        fields = {"protocol": "iotdbfs-hdf5-subarray/1", "source.path": "/source", "source.version": "9",
                  "dataset.path": "/T", "dataset.rank": "4", "output.value_type": "float32",
                  "order": "C", "selection.row_count": "2"}
        for axis in range(4):
            fields.update({f"axis.{axis}.full_extent": str(full[axis]),
                           f"axis.{axis}.selection_origin": str(case["origin"][axis]),
                           f"axis.{axis}.selection_extent": str(case["shape"][axis])})
        for mode in ("good", "wrong-value", "null", "wrong-version", "wrong-full-shape"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "result.ipc"
                values = [1, 3] if mode == "wrong-value" else [1, None] if mode == "null" else [1, 2]
                current = fields.copy()
                if mode == "wrong-version": current["source.version"] = "10"
                if mode == "wrong-full-shape": current["axis.0.full_extent"] = "160"
                metadata = {(b"iotdbfs.result_descriptor.0." + k.encode()): v.encode() for k,v in current.items()}
                metadata[b"iotdbfs.result_descriptors"] = b"1"
                schema = pa.schema([("value", pa.float32())], metadata=metadata)
                with pa.OSFile(str(path), "wb") as file, pa.ipc.new_stream(file, schema) as stream:
                    stream.write_batch(pa.record_batch([pa.array(values, type=pa.float32())], schema=schema))
                context = {"mount": Path(directory), "socket": "unused"}
                with patch.object(runner, "control", return_value=({"ok": True, "path": str(path)}, 1)):
                    if mode == "good":
                        self.assertEqual(runner.query_iotdb(context, "/source", 9, {"shape": full}, case)["rows"], 2)
                    else:
                        with self.assertRaises(ValueError):
                            runner.query_iotdb(context, "/source", 9, {"shape": full}, case)


if __name__ == "__main__":
    unittest.main()
