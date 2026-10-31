"""Regression checks for the pilot's IPC ownership and protocol diagnostics."""
import json
from pathlib import Path
import socket
import struct
import tempfile
import threading
import time
import unittest

import numpy as np
import pyarrow as pa

import small_runner as runner


class RunnerTests(unittest.TestCase):
    def test_profile_waits_for_delayed_complete_line_without_losing_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"profile.jsonl"
            previous = b'{"sequence":1}\n'
            path.write_bytes(previous + b'{"sequence":')
            def finish():
                time.sleep(.02)
                with path.open("ab") as stream:
                    stream.write(b'2,"ok":true}\n')
            thread = threading.Thread(target=finish)
            thread.start()
            try:
                profiles, offset = runner.wait_profiles(path, len(previous), timeout=1)
                self.assertEqual(profiles, [{"sequence": 2, "ok": True}])
                self.assertEqual(offset, path.stat().st_size)
                with self.assertRaisesRegex(ValueError, "profile sink did not finish"):
                    runner.wait_profiles(path, offset, timeout=.01)
            finally:
                thread.join(timeout=1)

    def test_nullable_float_ipc_returns_owned_values_and_boolean_mask(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "result.ipc"
            schema = pa.schema([("value", pa.float32())])
            with pa.OSFile(str(path), "wb") as sink:
                with pa.ipc.new_stream(sink, schema) as writer:
                    writer.write_batch(pa.record_batch([pa.array([1., None, 3.], type=pa.float32())], schema=schema),
                                       custom_metadata={"iotdbfs.sources": "1"})
            batches, size = runner.ipc({"mount": root}, {"path": str(path)}, True)
            self.assertEqual(size, path.stat().st_size)
            self.assertEqual(batches[0]["meta"][b"iotdbfs.sources"], b"1")
            np.testing.assert_array_equal(batches[0]["mask"], [1, 0, 1])
            self.assertTrue(batches[0]["array"].flags.owndata)
            self.assertTrue(batches[0]["mask"].flags.owndata)
            path.unlink()
            np.testing.assert_array_equal(batches[0]["array"][[0, 2]], [1., 3.])

    def test_registration_error_preserves_code_phase_and_message(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            path = Path(directory) / "s"
            response = {"ok": False, "code": 1, "phase": 1,
                        "message": "HDF5 subset requires one exact source path and version"}
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(str(path))
                server.listen(1)

                def reply():
                    with server.accept()[0] as stream:
                        length, = struct.unpack(">I", runner.sf.receive(stream, 4))
                        runner.sf.receive(stream, length)
                        body = json.dumps(response).encode()
                        stream.sendall(struct.pack(">I", len(body)) + body)

                thread = threading.Thread(target=reply)
                thread.start()
                try:
                    with self.assertRaisesRegex(ValueError, "one exact source path and version") as error:
                        runner.query_control(path, {"kind": "typed"})
                    self.assertIn('"code": 1', str(error.exception))
                    self.assertIn('"phase": 1', str(error.exception))
                finally:
                    thread.join(timeout=5)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
