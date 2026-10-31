"""Live A11 mount smoke test using an isolated, temporary Arrow source."""

import argparse
import errno
import hashlib
import io
import json
import os
import socket
import struct
import subprocess
import sys
import uuid

import pyarrow as pa


def register(socket_path, declaration):
    payload = json.dumps(declaration, separators=(",", ":")).encode()
    with socket.socket(socket.AF_UNIX) as connection:
        connection.connect(socket_path)
        connection.sendall(struct.pack(">I", len(payload)) + payload)
        header = connection.recv(4)
        assert len(header) == 4, "registration response header is incomplete"
        remaining = struct.unpack(">I", header)[0]
        response = bytearray()
        while len(response) < remaining:
            chunk = connection.recv(remaining - len(response))
            assert chunk, "registration response is incomplete"
            response.extend(chunk)
    result = json.loads(response)
    assert result.get("ok") in (True, "true"), result
    return result["path"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount", required=True)
    parser.add_argument("--socket", required=True)
    args = parser.parse_args()

    logical_path = "/a11-smoke-" + uuid.uuid4().hex + ".arrow"
    source_path = args.mount.rstrip("/") + logical_path
    values = pa.array(
        [[[1.0, 2.0, 3.0], [10.0, 20.0, 30.0]]],
        type=pa.list_(pa.list_(pa.float32())),
    )
    batch = pa.record_batch([values], names=["target"])
    buffer = io.BytesIO()
    with pa.ipc.new_stream(buffer, batch.schema) as writer:
        writer.write_batch(batch)

    try:
        with open(source_path, "wb") as output:
            output.write(buffer.getvalue())
        with open(source_path, "rb") as source:
            assert source.read() == buffer.getvalue(), "stored Arrow bytes differ"
        assert os.getxattr(source_path, "user.arrow.container") == b"stream"
        selection = register(args.socket, {"kind": "typed", "request": {
            "sources": {"paths": [{"kind": "exact", "path": logical_path}],
                "versions": {"kind": "latest"}}}})
        assert "&result=" in selection
        with pa.memory_map(selection, "r") as selected:
            catalog = pa.ipc.open_stream(selected).read_all()
        assert catalog.num_rows == 1
        assert catalog.column("source.format").to_pylist() == ["arrow"]
        version = catalog.column("source.version").to_pylist()[0]
        physical_declaration = {
            "kind": "typed",
            "request": {
                "sources": {
                    "paths": [{"kind": "exact", "path": logical_path}],
                    "versions": {"kind": "exact", "exact": version},
                },
                "subset": {
                    "projection": ["index", "value.0", "value.1"],
                    "physical_array": {
                        "field_id": "target",
                        "record_batch_index": 0,
                        "row_index": 0,
                        "values": [
                            {"child_indices": [0]},
                            {"child_indices": [1]},
                        ],
                    },
                },
            },
        }
        route = register(args.socket, physical_declaration)
        assert "&result=" in route
        size = os.stat(route).st_size
        with pa.memory_map(route, "r") as mapped:
            mapped_table = pa.ipc.open_stream(mapped).read_all()
            assert mapped_table.column("value.0").to_pylist() == [1.0, 2.0, 3.0]
        with open(route, "rb") as first, open(route, "rb") as second:
            stream = first.read()
            assert size == len(stream)
            assert second.read(32) == stream[:32]
            second.seek(size - 16)
            assert second.read() == stream[-16:]
            second.seek(size)
            assert second.read() == b""
            table = pa.ipc.open_stream(io.BytesIO(stream)).read_all()
            assert table.column("index").to_pylist() == [0, 1, 2]
            assert table.column("value.0").to_pylist() == [1.0, 2.0, 3.0]
            assert table.column("value.1").to_pylist() == [10.0, 20.0, 30.0]
            client = """import hashlib,json,sys
sys.path.insert(0, sys.argv[3])
from a11_mount_smoke import register
route = register(sys.argv[1], json.loads(sys.argv[2]))
with open(route, 'rb') as result:
    print(hashlib.sha256(result.read()).hexdigest())
"""
            child_hash = subprocess.check_output([sys.executable, "-c", client,
                args.socket, json.dumps(physical_declaration),
                os.path.dirname(__file__)], text=True).strip()
            assert child_hash == hashlib.sha256(stream).hexdigest()

            window_declaration = {
                "kind": "window",
                "snapshot": {
                    "dataset_id": "a11-smoke",
                    "selector": {"paths": [{"kind": "exact", "path": logical_path}],
                        "versions": {"kind": "exact", "exact": version},
                        "formats": {"adapters": "listed", "adapter_ids": ["arrow"],
                            "include_byte_passthrough": False}},
                    "shards": [{"filename": "a11.arrow", "path": logical_path,
                        "version": version}],
                    "rows": [{"logical_row": 0, "shard_index": 0,
                        "batch_index": 0, "row_index": 0,
                        "series_id": "series", "steps": {"begin": 0, "end": 3},
                        "declared_n_channels": 2, "declared_length": 3,
                        "channels": [
                            {"field_id": "target", "original_ordinal": 0,
                                "physical_ordinal": 0, "label": "x", "length": 3},
                            {"field_id": "target", "original_ordinal": 1,
                                "physical_ordinal": 1, "label": "y", "length": 3},
                        ]}],
                },
                "view": {"view_id": "a11-view", "experiment_id": "smoke",
                    "input_channels": [{"field_id": "target", "ordinal": 0,
                        "label": "x"}],
                    "target_channels": [{"field_id": "target", "ordinal": 1,
                        "label": "y"}],
                    "input_length": 2, "gap": 0, "target_length": 1,
                    "stride": 1,
                    "split": {"split_id": "train",
                        "permitted_input": {"begin": 0, "end": 3},
                        "permitted_target": {"begin": 0, "end": 3}}},
                "request": {"coordinates": [{"series_id": "series",
                    "anchor": 2}], "max_windows": 1},
            }
            window_route = register(args.socket, window_declaration)
            second_window_route = register(args.socket,
                window_declaration)
            with pa.memory_map(window_route, "r") as window_file:
                window_table = pa.ipc.open_stream(window_file).read_all()
            assert window_table.num_rows == 3
            assert window_table.column("role").to_pylist() == [
                "input", "input", "target"]
            assert window_table.column("input.0").to_pylist() == [
                1.0, 2.0, None]
            assert window_table.column("target.0").to_pylist() == [
                None, None, 30.0]

            os.unlink(source_path)
            first.seek(0)
            assert first.read(32) == stream[:32]
            try:
                os.stat(second_window_route)
            except OSError as error:
                assert error.errno in (errno.ESTALE, errno.ENOENT), error
            else:
                raise AssertionError("deleted source still authorized a window")
        print(json.dumps({"status": "ok", "bytes": size,
            "rows": table.num_rows, "window_rows": window_table.num_rows,
            "route": route}))
    finally:
        if os.path.exists(source_path):
            os.unlink(source_path)


if __name__ == "__main__":
    main()
