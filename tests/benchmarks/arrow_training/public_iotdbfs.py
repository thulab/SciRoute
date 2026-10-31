"""Bounded batch client for the public IoTDB-FS typed-window route."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import socket
import struct
import time
from typing import Sequence

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_public_windows import verify_seal


FRAME_LIMIT = 1024 * 1024


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise ConnectionError("registration socket closed before response")
        result.extend(chunk)
    return bytes(result)


def control(socket_path: Path, declaration: dict, timing: dict | None = None) -> dict:
    start = time.perf_counter()
    payload = json.dumps(declaration, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    serialized = time.perf_counter()
    if not payload or len(payload) > FRAME_LIMIT:
        raise ValueError(f"query declaration exceeds socket frame limit: {len(payload)}")
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(300)
        connection.connect(str(socket_path))
        connection.sendall(struct.pack(">I", len(payload)) + payload)
        length = struct.unpack(">I", _recv_exact(connection, 4))[0]
        if not 0 < length <= FRAME_LIMIT:
            raise ValueError("invalid query registration response length")
        response_bytes = _recv_exact(connection, length)
    received = time.perf_counter()
    response = json.loads(response_bytes)
    decoded = time.perf_counter()
    if timing is not None:
        timing.update(request_json_bytes=len(payload),
                      request_encode_seconds=serialized - start,
                      socket_roundtrip_seconds=received - serialized,
                      response_decode_seconds=decoded - received)
    if response.get("ok") not in (True, "true"):
        raise RuntimeError(f"query registration failed: {response}")
    return response


def register(socket_path: Path, declaration: dict, timing: dict | None = None) -> Path:
    return Path(control(socket_path, declaration, timing)["path"])


def compact_snapshot(snapshot: dict) -> dict:
    """Share identical channel mappings; inherit only explicitly declared lengths."""
    tables, by_mapping, rows = [], {}, []
    for row in snapshot["rows"]:
        channels = []
        for channel in row["channels"]:
            item = dict(channel)
            if "declared_length" in row and item.get("length") == row["declared_length"]:
                item.pop("length")
            channels.append(item)
        key = json.dumps(channels, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        if key not in by_mapping:
            table_id = f"channels-{len(tables)}"
            by_mapping[key] = table_id
            tables.append({"id": table_id, "channels": channels})
        normalized = {k: v for k, v in row.items() if k != "channels"}
        normalized["channel_table_id"] = by_mapping[key]
        rows.append(normalized)
    return dict(snapshot, rows=rows, channel_tables=tables)


def source_version(socket_path: Path, logical_path: str) -> int:
    route = register(socket_path, {"kind": "typed", "request": {"sources": {
        "paths": [{"kind": "exact", "path": logical_path}],
        "versions": {"kind": "latest"},
    }}})
    with pa.memory_map(str(route), "r") as mapped:
        table = ipc.open_stream(mapped).read_all()
    if table.num_rows != 1 or table.column("source.format").to_pylist() != ["arrow"]:
        raise ValueError("IoTDB-FS source catalog does not resolve one Arrow source")
    return int(table.column("source.version")[0].as_py())


@dataclass
class WindowArrays:
    input_values: np.ndarray
    input_valid: np.ndarray
    target_values: np.ndarray
    target_valid: np.ndarray



def convert_window_columns(table: pa.Table, n: int, length: int, horizon: int,
                           width: int, mode: str = "buffers") -> WindowArrays:
    """Convert validated flat IPC rows into owned, contiguous training arrays.

    sample is the unchanged pre-optimization loop, kept as an explicit ablation
    control. batch converts each role/channel once across all samples. Only one
    column's temporary values and validity are retained at a time. buffers reads
    Float32 buffers and validity bitmaps directly, avoiding compute kernels for
    fill_null/is_valid and preserving valid NaNs independently of nulls.
    """
    if mode not in ("sample", "batch", "buffers"):
        raise ValueError("unknown conversion mode")
    input_values = np.zeros((n, length, width), dtype=np.float32)
    input_valid = np.zeros((n, length, width), dtype=np.bool_)
    target_values = np.zeros((n, horizon, width), dtype=np.float32)
    target_valid = np.zeros((n, horizon, width), dtype=np.bool_)
    if mode == "sample":
        for sample_index in range(n):
            offset = sample_index * (length + horizon)
            for channel in range(width):
                in_col = table.column(f"input.{channel}").slice(offset, length).combine_chunks()
                out_col = table.column(f"target.{channel}").slice(offset + length, horizon).combine_chunks()
                input_values[sample_index, :, channel] = in_col.fill_null(0).to_numpy(zero_copy_only=False)
                input_valid[sample_index, :, channel] = in_col.is_valid().to_numpy(zero_copy_only=False)
                target_values[sample_index, :, channel] = out_col.fill_null(0).to_numpy(zero_copy_only=False)
                target_valid[sample_index, :, channel] = out_col.is_valid().to_numpy(zero_copy_only=False)
    else:
        # Resolve names once per table. Do not cache indices across changed IPC
        # schemas, or silently accept ambiguous names via a dict overwrite.
        positions = {}
        for i, name in enumerate(table.column_names):
            positions[name] = i if name not in positions else None
        total = length + horizon
        for role, begin, end, values, valid in (
                ("input", 0, length, input_values, input_valid),
                ("target", length, total, target_values, target_valid)):
            for channel in range(width):
                name = f"{role}.{channel}"
                index = positions.get(name)
                if index is None:
                    raise ValueError(f"missing or ambiguous window column: {name}")
                column = table.column(index)
                if mode == "batch":
                    values[:, :, channel] = column.fill_null(0).to_numpy(
                        zero_copy_only=False).reshape(n, total)[:, begin:end]
                    valid[:, :, channel] = column.is_valid().to_numpy(
                        zero_copy_only=False).reshape(n, total)[:, begin:end]
                    continue
                if column.type != pa.float32() or len(column) != n * total:
                    raise ValueError("window column must contain the declared Float32 rows")
                # The usual IPC table has a single chunk. Combine only when
                # necessary; Arrow handles unrelated chunk boundaries/offsets.
                array = column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()
                if not len(array):
                    continue
                bitmap, data = array.buffers()
                raw = np.frombuffer(data, dtype=np.float32, count=len(array),
                                    offset=array.offset * 4)
                values[:, :, channel] = raw.reshape(n, total)[:, begin:end]
                if array.null_count == 0:
                    valid[:, :, channel] = True
                else:
                    bit_offset = array.offset % 8
                    packed = np.frombuffer(bitmap, dtype=np.uint8,
                                           offset=array.offset // 8,
                                           count=(bit_offset + len(array) + 7) // 8)
                    bits = np.unpackbits(packed, bitorder="little",
                                         count=bit_offset + len(array))[bit_offset:]
                    valid[:, :, channel] = bits.reshape(n, total)[:, begin:end]
            if mode == "buffers":
                # Never use isnan to identify nulls: observed NaNs are valid.
                # Copy into independent dense outputs before the mmap closes.
                np.copyto(values, np.float32(0), where=~valid)
    return WindowArrays(input_values, input_valid, target_values, target_valid)


class IoTDBFSWindowReader:
    def __init__(self, *, socket_path: Path, mount: Path, pb00: dict,
                 normalization: dict, windows: dict, split: str,
                 channels: Sequence[int], source_version_exact: int | None = None,
                 registration_mode: str = "prepared_compact",
                 conversion_mode: str = "buffers"):
        verify_seal(normalization)
        verify_seal(windows)
        if windows["normalization_artifact_id"] != normalization["artifact_id"]:
            raise ValueError("window/normalization artifact mismatch")
        if windows["arrow_sha256"] != pb00["arrow"]["sha256"]:
            raise ValueError("window/Arrow source mismatch")
        self.socket_path = Path(socket_path)
        self.mount = Path(mount).resolve()
        self.pb00 = pb00
        self.normalization = normalization
        self.windows = windows
        self.split = split
        self.channels = tuple(int(channel) for channel in channels)
        if not self.channels or len(set(self.channels)) != len(self.channels):
            raise ValueError("channels must be nonempty and unique")
        if any(channel < 0 or channel >= len(pb00["channels"]) for channel in self.channels):
            raise ValueError("channel ordinal out of range")
        self.logical_path = "/" + pb00["arrow"]["path"]
        self.version = (source_version_exact if source_version_exact is not None
                        else source_version(self.socket_path, self.logical_path))
        self.snapshot = self._snapshot()
        self.view = self._view()
        if registration_mode not in ("full", "prepared", "prepared_compact"):
            raise ValueError("unknown registration mode")
        if conversion_mode not in ("sample", "batch", "buffers"):
            raise ValueError("unknown conversion mode")
        self.conversion_mode = conversion_mode
        self.registration_mode = registration_mode
        self.prepared_id = None
        self.closed = False
        self.metrics = {"requests": 0, "ipc_bytes": 0, "samples": 0,
                        "registration_mode": registration_mode,
                        "conversion_mode": conversion_mode,
                        "ipc_validation_seconds": 0.0, "ipc_array_seconds": 0.0,
                        "preparation_seconds": 0.0, "preparation_timings": [],
                        "result_release_seconds": 0.0, "release_timings": [],
                        "registration_seconds": 0.0,
                        "materialize_seconds": 0.0,
                        "ipc_read_seconds": 0.0,
                        "ipc_decode_seconds": 0.0, "request_timings": []}

    def _snapshot(self) -> dict:
        n = len(self.pb00["channels"])
        rows = []
        for logical_row, row in enumerate(self.pb00["arrow"]["rows"]):
            length = row["end_step"] - row["start_step"]
            rows.append({
                "logical_row": logical_row,
                "shard_index": 0,
                "batch_index": row["batch"],
                "row_index": row["row_in_batch"],
                "series_id": self.pb00["dataset_id"],
                "entity_id": self.pb00["dataset_id"],
                "steps": {"begin": row["start_step"], "end": row["end_step"]},
                "value_layout": self.pb00["arrow"].get("value_layout", "channel_major"),
                "declared_n_channels": n,
                "declared_length": length,
                "channels": [{
                    "field_id": "target", "original_ordinal": ordinal,
                    "physical_ordinal": ordinal, "label": label, "length": length,
                } for ordinal, label in enumerate(self.pb00["channels"])],
            })
        return {
            "dataset_id": self.pb00["dataset_id"],
            "selector": {
                "paths": [{"kind": "exact", "path": self.logical_path}],
                "versions": {"kind": "exact", "exact": self.version},
                "formats": {"adapters": "listed", "adapter_ids": ["arrow"],
                            "include_byte_passthrough": False},
            },
            "shards": [{"filename": self.pb00["arrow"]["path"],
                        "path": self.logical_path, "version": self.version}],
            "rows": rows,
        }

    def _view(self) -> dict:
        split = self.windows["splits"][self.split]
        stats = self.normalization["statistics"]
        channel_refs = [{"field_id": "target", "ordinal": i,
                         "label": self.pb00["channels"][i]} for i in self.channels]
        permitted_target = split["target_interval"]
        return {
            "view_id": f"{self.pb00['dataset_id']}-public-ab-{self.split}",
            "experiment_id": "public-ab",
            "input_channels": channel_refs,
            "target_channels": channel_refs,
            "input_length": self.windows["input_length"],
            "gap": 0,
            "target_length": self.windows["prediction_length"],
            "stride": self.windows["stride"],
            "split": {"split_id": self.split,
                      "permitted_input": {"begin": 0, "end": permitted_target[1]},
                      "permitted_target": {"begin": permitted_target[0],
                                           "end": permitted_target[1]}},
            "transform": {"kind": "standardize",
                          "artifact_id": self.normalization["artifact_id"],
                          "statistics": [{"field_id": "target", "ordinal": i,
                                          "mean": stats[i]["mean"],
                                          "stddev": stats[i]["stddev"]}
                                         for i in self.channels]},
        }

    def _prepare(self) -> None:
        if self.prepared_id is not None:
            return
        start = time.perf_counter()
        snapshot = (compact_snapshot(self.snapshot)
                    if self.registration_mode == "prepared_compact" else self.snapshot)
        normalized = time.perf_counter()
        timing = {}
        response = control(self.socket_path, {"kind": "window_prepare",
            "snapshot": snapshot, "view": self.view}, timing)
        if response.get("kind") != "prepared_window" or not response.get("prepared_id"):
            raise ValueError("server did not return a prepared window ID")
        self.prepared_id = response["prepared_id"]
        elapsed = time.perf_counter() - start
        self.metrics["preparation_seconds"] += elapsed
        self.metrics["preparation_timings"].append({**timing,
            "prepared_id": self.prepared_id, "registration_seconds": elapsed,
            "normalize_seconds": normalized - start})

    def close(self) -> None:
        if self.prepared_id is not None:
            control(self.socket_path, {"kind": "window_release", "prepared_id": self.prepared_id})
            self.prepared_id = None
        self.closed = True

    def read_windows(self, anchors: Sequence[int]) -> WindowArrays:
        if self.closed:
            raise ValueError("window reader is closed")
        selected = tuple(int(anchor) for anchor in anchors)
        if not selected:
            raise ValueError("an empty query batch is not useful")
        split = self.windows["splits"][self.split]
        if any(anchor < split["first_anchor"] or
               anchor >= split["stop_anchor_exclusive"] or
               (anchor - split["first_anchor"]) % split["stride"] for anchor in selected):
            raise ValueError("anchor is outside the selected split/window manifest")
        window_request = {"coordinates": [{"series_id": self.pb00["dataset_id"],
                                           "anchor": anchor} for anchor in selected],
                          "max_windows": len(selected)}
        if self.registration_mode == "full":
            request = {"kind": "window", "snapshot": self.snapshot, "view": self.view,
                       "request": window_request}
        else:
            # Lazy preparation is inside the first Trainer fetch/train timer.
            self._prepare()
            request = {"kind": "window_execute", "prepared_id": self.prepared_id,
                       "request": window_request}
        start = time.perf_counter()
        transport_timing = {}
        route = register(self.socket_path, request, transport_timing)
        registered = time.perf_counter()
        if not route.is_relative_to(self.mount):
            raise ValueError("query route escaped expected mount")
        self.metrics["requests"] += 1
        try:
            ipc_bytes = route.stat().st_size
            self.metrics["ipc_bytes"] += ipc_bytes
            materialized = time.perf_counter()
            with pa.memory_map(str(route), "r") as mapped:
                table = ipc.open_stream(mapped).read_all()
            read = time.perf_counter()
            self._decode_detail = {}
            arrays = self._decode(table, selected)
            decoded = time.perf_counter()
        finally:
            # Returned arrays own their bytes; closed mappings no longer need
            # the route. Retire it so a long epoch cannot fill the registry.
            if self.prepared_id is not None:
                release_start = time.perf_counter()
                release_timing = {}
                control(self.socket_path, {"kind": "window_result_release",
                    "prepared_id": self.prepared_id, "path": str(route)}, release_timing)
                elapsed = time.perf_counter() - release_start
                self.metrics["result_release_seconds"] += elapsed
                self.metrics["release_timings"].append({**release_timing,
                    "prepared_id": self.prepared_id, "path": str(route),
                    "registration_seconds": elapsed})
        self.metrics["samples"] += len(selected)
        self.metrics["registration_seconds"] += registered - start
        self.metrics["materialize_seconds"] += materialized - registered
        self.metrics["ipc_read_seconds"] += read - materialized
        self.metrics["ipc_decode_seconds"] += decoded - read
        for key, elapsed in self._decode_detail.items():
            self.metrics[key] += elapsed
        self.metrics["request_timings"].append({
            **transport_timing, **self._decode_detail,
            "path": str(route), "anchors": list(selected), "ipc_bytes": ipc_bytes,
            "registration_seconds": registered - start,
            "stat_seconds": materialized - registered,
            "ipc_read_seconds": read - materialized,
            "ipc_decode_seconds": decoded - read,
        })
        return arrays

    def _decode(self, table: pa.Table, anchors: Sequence[int]) -> WindowArrays:
        started = time.perf_counter()
        metadata = table.schema.metadata or {}
        if metadata.get(b"iotdbfs.protocol") != b"iotdbfs-window-flat/3":
            raise ValueError("unexpected IoTDB-FS window IPC protocol")
        if metadata.get(b"iotdbfs.transform.kind") != b"standardize" or \
                metadata.get(b"iotdbfs.transform.artifact_id") != \
                self.normalization["artifact_id"].encode():
            raise ValueError("IoTDB-FS window transform differs from frozen artifact")
        n, length, horizon, width = (len(anchors), self.windows["input_length"],
                                     self.windows["prediction_length"], len(self.channels))
        if table.num_rows != n * (length + horizon):
            raise ValueError("IoTDB-FS window result has the wrong number of rows")
        sample_indices = table.column("sample_index").to_numpy()
        roles = table.column("role").to_pylist()
        steps = table.column("step").to_numpy()
        descriptors = table.column("descriptor_json")
        for sample_index, anchor in enumerate(anchors):
            offset = sample_index * (length + horizon)
            if not np.all(sample_indices[offset:offset + length + horizon] == sample_index):
                raise ValueError("IoTDB-FS changed requested sample order")
            if roles[offset:offset + length] != ["input"] * length or \
                    roles[offset + length:offset + length + horizon] != ["target"] * horizon:
                raise ValueError("IoTDB-FS window roles are out of order")
            if not np.array_equal(steps[offset:offset + length],
                                  np.arange(anchor - length, anchor, dtype=np.uint64)) or \
                    not np.array_equal(steps[offset + length:offset + length + horizon],
                                       np.arange(anchor, anchor + horizon, dtype=np.uint64)):
                raise ValueError("IoTDB-FS returned unexpected step positions")
            descriptor = json.loads(descriptors[offset].as_py())
            if descriptor["anchor"] != anchor or descriptor["series_id"] != self.pb00["dataset_id"]:
                raise ValueError("IoTDB-FS returned another sample")

        validated = time.perf_counter()
        arrays = convert_window_columns(table, n, length, horizon, width, self.conversion_mode)
        converted = time.perf_counter()
        self._decode_detail = {"ipc_validation_seconds": validated - started,
                               "ipc_array_seconds": converted - validated}
        return arrays
