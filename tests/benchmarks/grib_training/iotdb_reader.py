"""Exact-version GRIB typed-query reader for the EG01 D0 oracle."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import socket
import struct
import time

import numpy as np
import pyarrow as pa

from local_reader import Sample
from sample_contract import CHANNELS, PATCH, Catalog, parse_iso


FIELDS = [
    "grib.message.offset",
    "grib.message.param_id",
    "grib.message.level",
    "grib.message.validity_date",
    "grib.message.validity_time",
    "grib.data.value",
]


def _compare(field: str, value: int) -> dict:
    return {"kind": "compare", "field": field, "operator": "eq",
            "value": {"type": "int64", "value": value}}


def _closed_slice(field: str, lower: float, upper: float) -> dict:
    return {
        "dimension": field,
        "lower": {"kind": "inclusive", "value": {"type": "float64", "value": lower}},
        "upper": {"kind": "inclusive", "value": {"type": "float64", "value": upper}},
    }


def typed_request(source: dict, when: datetime, kind: str, row: int, col: int,
                  channel_ordinals: list[int] | None = None) -> dict:
    wanted = set(range(len(CHANNELS)) if channel_ordinals is None
                 else channel_ordinals)
    channels = [channel for channel in CHANNELS
                if channel.source == kind and channel.ordinal in wanted]
    if not channels:
        raise ValueError(f"no requested channels belong to {kind}")
    parameters = []
    for channel in channels:
        selected = _compare("grib.message.param_id", channel.param_id)
        if kind == "pressure-levels":
            selected = {"kind": "and", "operands": [
                selected, _compare("grib.message.level", channel.level)]}
        parameters.append(selected)
    predicate = {
        "kind": "and",
        "operands": [
            _compare("grib.message.validity_date", int(when.strftime("%Y%m%d"))),
            _compare("grib.message.validity_time", int(when.strftime("%H%M"))),
            {"kind": "or", "operands": parameters},
        ],
    }
    lat_first = 90.0 - row * 0.25
    lat_last = 90.0 - (row + PATCH - 1) * 0.25
    lon_first = col * 0.25
    lon_last = (col + PATCH - 1) * 0.25
    return {
        "kind": "typed",
        "request": {
            "sources": {
                "paths": [{"kind": "exact", "path": source["path"]}],
                "versions": {"kind": "exact", "exact": source["version"]},
            },
            "subset": {
                "projection": FIELDS,
                "predicate": predicate,
                "slices": [
                    _closed_slice("grib.grid.latitude", lat_last, lat_first),
                    _closed_slice("grib.grid.longitude", lon_first, lon_last),
                ],
            },
        },
    }


def submit(socket_path: Path, request: dict) -> dict:
    payload = json.dumps(request, separators=(",", ":")).encode("utf-8")
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(300)
        connection.connect(str(socket_path))
        connection.sendall(struct.pack(">I", len(payload)) + payload)

        def receive(size: int) -> bytes:
            result = bytearray()
            while len(result) < size:
                part = connection.recv(size - len(result))
                if not part:
                    raise RuntimeError("query registration response ended early")
                result.extend(part)
            return bytes(result)

        (length,) = struct.unpack(">I", receive(4))
        if not 0 < length <= 1 << 20:
            raise RuntimeError("query registration response exceeds limit")
        response = json.loads(receive(length))
    if not response.get("ok"):
        raise RuntimeError(f"typed query failed: {response}")
    return response


class IoTDBGribReader:
    def __init__(self, catalog: Catalog, bindings: dict, socket_path: Path):
        self.catalog = catalog
        self.bindings = bindings
        self.socket_path = socket_path
        self.query_count = 0
        self.ipc_bytes = 0
        self.query_ns = 0
        # The mount eagerly materializes typed routes before submit returns;
        # this is registration + authorization + query + encode/publish, not
        # just socket framing time.
        self.registration_ns = 0
        self.ipc_read_ns = 0
        self.array_convert_ns = 0
        self.sample_ns = 0
        expected = {item["kind"]: item for item in catalog.sources}
        for kind in ("single-levels", "pressure-levels"):
            source = bindings.get(kind)
            if (not source or not isinstance(source.get("version"), int)
                    or source["version"] <= 0 or not source.get("path")
                    or source.get("sha256") != expected[kind]["sha256"]):
                raise ValueError(f"{kind} requires an exact version and matching SHA-256")

    def _fields(self, when: datetime, kind: str, row: int, col: int,
                channel_ordinals: list[int]) -> dict[int, np.ndarray]:
        source = self.bindings[kind]
        started = time.perf_counter_ns()
        response = submit(self.socket_path, typed_request(
            source, when, kind, row, col, channel_ordinals))
        registered = time.perf_counter_ns()
        self.registration_ns += registered - started
        route = Path(response["path"])
        with pa.memory_map(str(route), "r") as mapped:
            table = pa.ipc.open_stream(mapped).read_all()
        read_done = time.perf_counter_ns()
        self.ipc_read_ns += read_done - registered
        self.query_count += 1
        self.ipc_bytes += route.stat().st_size
        convert_started = time.perf_counter_ns()
        if table.column_names != FIELDS:
            raise ValueError(f"unexpected GRIB query columns: {table.column_names}")
        if any(table.column(field).null_count for field in FIELDS
               if field != "grib.message.level"):
            raise ValueError("GRIB query returned null message metadata or value")
        offsets = table.column(FIELDS[0]).to_numpy(zero_copy_only=False)
        param_ids = table.column(FIELDS[1]).to_numpy(zero_copy_only=False)
        levels = table.column(FIELDS[2]).to_numpy(zero_copy_only=False)
        dates = table.column(FIELDS[3]).to_numpy(zero_copy_only=False)
        clocks = table.column(FIELDS[4]).to_numpy(zero_copy_only=False)
        values = table.column(FIELDS[5]).to_numpy(zero_copy_only=False)
        expected = {
            int(self.catalog.rows[when][channel.ordinal]["offset"]): channel
            for channel in CHANNELS if channel.source == kind
            and channel.ordinal in channel_ordinals
        }
        actual: dict[int, np.ndarray] = {}
        if len(values) != len(expected) * PATCH * PATCH:
            raise ValueError("GRIB query returned the wrong number of patch values")
        for offset in np.unique(offsets):
            physical_offset = int(offset)
            channel = expected.get(physical_offset)
            if channel is None:
                raise ValueError(f"GRIB query returned undeclared offset {physical_offset}")
            selected = offsets == offset
            if (int(selected.sum()) != PATCH * PATCH
                    or not np.all(param_ids[selected] == channel.param_id)
                    or (kind == "pressure-levels"
                        and not np.all(levels[selected] == channel.level))
                    or not np.all(dates[selected] == int(when.strftime("%Y%m%d")))
                    or not np.all(clocks[selected] == int(when.strftime("%H%M")))):
                raise ValueError("GRIB query returned inconsistent message metadata")
            actual[channel.ordinal] = np.asarray(
                values[selected].reshape(PATCH, PATCH), dtype=np.float32)
        if set(actual) != {channel.ordinal for channel in CHANNELS
                           if channel.source == kind
                           and channel.ordinal in channel_ordinals}:
            raise ValueError("GRIB query missed a channel")
        self.array_convert_ns += time.perf_counter_ns() - convert_started
        self.query_ns += time.perf_counter_ns() - started
        return actual

    def sample(self, row: dict, channel_ordinals: list[int]) -> Sample:
        sample_started = time.perf_counter_ns()
        times = [parse_iso(value) for value in row["input_times"]]
        target = parse_iso(row["target_time"])
        x = np.empty((2, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        y = np.empty((1, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        for time_index, when in enumerate([*times, target]):
            found = {}
            for kind in ("single-levels", "pressure-levels"):
                selected = [ordinal for ordinal in channel_ordinals
                            if CHANNELS[ordinal].source == kind]
                if selected:
                    found.update(self._fields(when, kind, row["row"], row["col"],
                                              selected))
            for channel_index, ordinal in enumerate(channel_ordinals):
                if time_index < 2:
                    x[time_index, channel_index] = found[ordinal]
                else:
                    y[0, channel_index] = found[ordinal]
        result = Sample(
            row["sample_id"], x, y,
            np.ones_like(x, dtype=np.bool_), np.ones_like(y, dtype=np.bool_),
        )
        self.sample_ns += time.perf_counter_ns() - sample_started
        return result

    def profile(self) -> dict[str, int]:
        return {
            "registration_ns": self.registration_ns,
            "ipc_read_ns": self.ipc_read_ns,
            "array_convert_ns": self.array_convert_ns,
            "query_ns": self.query_ns,
            "query_count": self.query_count,
            "ipc_bytes": self.ipc_bytes,
            "sample_ns": self.sample_ns,
        }
