"""Indexed, byte-bounded local ecCodes reader for ERA5 regional samples."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
import hashlib
from pathlib import Path
import time

import eccodes as ec
import numpy as np

from sample_contract import CHANNELS, PATCH, Catalog, message_time, parse_iso


@dataclass
class Sample:
    sample_id: str
    x: np.ndarray
    y: np.ndarray
    x_mask: np.ndarray
    y_mask: np.ndarray

    def digest(self) -> str:
        h = hashlib.sha256()
        for item in (self.x, self.y, self.x_mask, self.y_mask):
            h.update(np.ascontiguousarray(item).tobytes())
        return h.hexdigest()


class LocalGribReader:
    def __init__(self, catalog: Catalog, max_cache_bytes: int = 256 << 20):
        if max_cache_bytes < 0:
            raise ValueError("max_cache_bytes must be nonnegative")
        self.catalog = catalog
        self.max_cache_bytes = max_cache_bytes
        self.cache: OrderedDict[tuple[datetime, int], np.ndarray] = OrderedDict()
        self.cache_bytes = 0
        self.decoded_messages = 0
        self.cache_hits = 0
        self.encoded_bytes = 0
        self.source_read_ns = 0
        self.decode_ns = 0
        self.sample_ns = 0

    def _decode(self, when: datetime, ordinal: int) -> np.ndarray:
        key = when, ordinal
        if key in self.cache:
            self.cache_hits += 1
            self.cache.move_to_end(key)
            return self.cache[key]
        channel = CHANNELS[ordinal]
        row = self.catalog.rows[when][ordinal]
        path: Path = self.catalog.paths[channel.source]
        read_started = time.perf_counter_ns()
        with path.open("rb") as stream:
            stream.seek(int(row["offset"]))
            encoded = stream.read(int(row["length"]))
        self.source_read_ns += time.perf_counter_ns() - read_started
        self.encoded_bytes += len(encoded)
        if len(encoded) != row["length"] or encoded[:4] != b"GRIB":
            raise ValueError(f"GRIB source bytes changed for channel {ordinal}")
        decode_started = time.perf_counter_ns()
        handle = ec.codes_new_from_message(encoded)
        try:
            if (int(ec.codes_get(handle, "paramId")) != channel.param_id
                    or (int(ec.codes_get(handle, "level")) if channel.source == "pressure-levels"
                        else 0) != channel.level
                    or message_time({
                        "validityDate": ec.codes_get(handle, "validityDate"),
                        "validityTime": ec.codes_get(handle, "validityTime"),
                    }) != when
                    or int(ec.codes_get(handle, "numberOfMissing")) != 0):
                raise ValueError(f"manifest/message mismatch for channel {ordinal}")
            values = np.asarray(ec.codes_get_values(handle), dtype=np.float32)
        finally:
            ec.codes_release(handle)
        if values.size != 721 * 1440:
            raise ValueError("unexpected GRIB grid size")
        field = values.reshape(721, 1440)
        if not np.isfinite(field).all():
            raise ValueError("non-finite ERA5 value")
        self.decode_ns += time.perf_counter_ns() - decode_started
        self.decoded_messages += 1
        if field.nbytes <= self.max_cache_bytes:
            while self.cache and self.cache_bytes + field.nbytes > self.max_cache_bytes:
                _, evicted = self.cache.popitem(last=False)
                self.cache_bytes -= evicted.nbytes
            self.cache[key] = field
            self.cache_bytes += field.nbytes
        return field

    def sample(self, row: dict, channel_ordinals: list[int]) -> Sample:
        sample_started = time.perf_counter_ns()
        times = [parse_iso(value) for value in row["input_times"]]
        target = parse_iso(row["target_time"])
        origin = int(row["row"]), int(row["col"])
        y0, x0 = origin
        if y0 < 0 or x0 < 0 or y0 + PATCH > 721 or x0 + PATCH > 1440:
            raise ValueError("patch exceeds ERA5 grid")
        x = np.empty((2, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        y = np.empty((1, len(channel_ordinals), PATCH, PATCH), dtype=np.float32)
        for channel_index, ordinal in enumerate(channel_ordinals):
            for time_index, when in enumerate(times):
                x[time_index, channel_index] = self._decode(when, ordinal)[
                    y0:y0 + PATCH, x0:x0 + PATCH]
            y[0, channel_index] = self._decode(target, ordinal)[
                y0:y0 + PATCH, x0:x0 + PATCH]
        result = Sample(
            row["sample_id"], x, y,
            np.ones_like(x, dtype=np.bool_), np.ones_like(y, dtype=np.bool_),
        )
        self.sample_ns += time.perf_counter_ns() - sample_started
        return result

    def profile(self) -> dict[str, int]:
        return {
            "source_read_ns": self.source_read_ns,
            "decode_ns": self.decode_ns,
            "encoded_bytes": self.encoded_bytes,
            "cache_hits": self.cache_hits,
            "decoded_messages": self.decoded_messages,
            "cache_bytes": self.cache_bytes,
            "sample_ns": self.sample_ns,
        }
