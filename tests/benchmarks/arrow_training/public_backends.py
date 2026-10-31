"""Independent, bounded local Arrow reader for the public training baseline.

The public stream stores one segment per row as target[channel][step].  A
manifest row index avoids decompressing all record batches at startup; when no
index is supplied, the same index is built by a single sequential batch pass.
Neither path converts a file or a batch to Python lists.  Windows decode only
their touched batches, which are retained in a byte-bounded LRU cache.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Sequence

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc


@dataclass(frozen=True)
class _Row:
    batch: int
    row_in_batch: int
    start: int
    end: int


class LocalArrowWindowReader:
    """Read [time, selected channel] windows from one immutable IPC stream.

    ``row_index`` is the ``manifest['arrow']['rows']`` produced by PB00.  Its
    batch/row/start/end declarations are verified when a batch is first read.
    ``max_cache_bytes=0`` disables retention without changing read semantics.
    This reader owns its memory map and should be closed after use.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        row_index: Sequence[dict] | None = None,
        max_cache_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        if max_cache_bytes < 0:
            raise ValueError("max_cache_bytes must be nonnegative")
        self.path = Path(path)
        self.max_cache_bytes = max_cache_bytes
        self._lock = RLock()
        self._source = pa.memory_map(str(self.path), "r")
        self._cache: OrderedDict[int, pa.RecordBatch] = OrderedDict()
        self._cache_bytes = 0
        self._closed = False
        self._metrics = {
            "read_calls": 0,
            "batch_decodes": 0,
            "cache_hits": 0,
            "cache_evictions": 0,
            "decoded_bytes": 0,
        }
        try:
            schema_message = ipc.read_message(self._source)
            if schema_message.type != "schema":
                raise ValueError("Arrow stream does not begin with a schema")
            self.schema = ipc.read_schema(schema_message)
            required = {
                "start_step": pa.int64(),
                "end_step": pa.int64(),
                "target": pa.list_(pa.list_(pa.float32())),
            }
            for name, expected in required.items():
                if self.schema.get_field_index(name) < 0 or self.schema.field(name).type != expected:
                    raise ValueError(f"Arrow stream needs {name}: {expected}")

            offsets: list[int] = []
            discovered_rows: list[_Row] = []
            discovered_channels: int | None = None
            while True:
                offset = self._source.tell()
                try:
                    message = ipc.read_message(self._source)
                except EOFError:
                    break
                if message.type != "record batch":
                    raise ValueError(f"unsupported public Arrow stream message: {message.type}")
                batch_number = len(offsets)
                offsets.append(offset)
                if row_index is None:
                    batch = ipc.read_record_batch(message, self.schema)
                    discovered_rows.extend(self._rows_from_batch(batch_number, batch))
                    for row_in_batch in range(batch.num_rows):
                        target = batch.column("target")[row_in_batch]
                        if target.is_valid:
                            width = len(target.values)
                            if discovered_channels is None:
                                discovered_channels = width
                            elif width != discovered_channels:
                                raise ValueError("channel count changes between Arrow rows")
            self._batch_offsets = tuple(offsets)
            if not offsets:
                raise ValueError("public Arrow stream has no record batches")
            if row_index is None:
                rows = discovered_rows
                if discovered_channels is None:
                    raise ValueError("cannot infer channels from all-null Arrow rows")
                channel_count = discovered_channels
            else:
                rows = [
                    _Row(int(row["batch"]), int(row["row_in_batch"]),
                         int(row["start_step"]), int(row["end_step"]))
                    for row in row_index
                ]
                channel_counts = {int(row["channels"]) for row in row_index}
                if len(channel_counts) != 1:
                    raise ValueError("manifest rows must declare one channel count")
                channel_count = channel_counts.pop()
            self._validate_index(rows, channel_count, len(offsets))
            self._rows = tuple(rows)
            self._row_starts = tuple(row.start for row in rows)
            self._batch_row_counts = tuple(
                sum(row.batch == batch for row in rows) for batch in range(len(offsets))
            )
            self.total_steps = rows[-1].end
            self.channel_count = channel_count
            self.indexed_batches = len(offsets)
            self.indexed_rows = len(rows)
        except BaseException:
            self._source.close()
            raise

    @staticmethod
    def _rows_from_batch(batch_number: int, batch: pa.RecordBatch) -> list[_Row]:
        starts = batch.column("start_step").to_numpy(zero_copy_only=False)
        ends = batch.column("end_step").to_numpy(zero_copy_only=False)
        return [
            _Row(batch_number, i, int(starts[i]), int(ends[i]))
            for i in range(batch.num_rows)
        ]

    @staticmethod
    def _validate_index(rows: list[_Row], channels: int, batches: int) -> None:
        if not rows or channels <= 0:
            raise ValueError("public Arrow index must contain rows and channels")
        prior_end = 0
        prior_batch = -1
        prior_row = -1
        for row in rows:
            if row.start != prior_end or row.end <= row.start:
                raise ValueError("public Arrow rows must cover contiguous steps from zero")
            if row.batch < 0 or row.batch >= batches:
                raise ValueError("manifest batch index is out of range")
            if row.batch == prior_batch:
                if row.row_in_batch != prior_row + 1:
                    raise ValueError("manifest row-in-batch indices are not contiguous")
            elif row.batch == prior_batch + 1:
                if row.row_in_batch != 0:
                    raise ValueError("first manifest row in a batch must be zero")
            else:
                raise ValueError("manifest batch indices are not contiguous")
            prior_end, prior_batch, prior_row = row.end, row.batch, row.row_in_batch
        if prior_batch != batches - 1:
            raise ValueError("manifest does not cover every Arrow record batch")

    def _get_batch(self, batch_number: int) -> pa.RecordBatch:
        cached = self._cache.get(batch_number)
        if cached is not None:
            self._cache.move_to_end(batch_number)
            self._metrics["cache_hits"] += 1
            return cached
        self._source.seek(self._batch_offsets[batch_number])
        message = ipc.read_message(self._source)
        if message.type != "record batch":
            raise ValueError("indexed Arrow message is no longer a record batch")
        batch = ipc.read_record_batch(message, self.schema)
        if batch.num_rows != self._batch_row_counts[batch_number]:
            raise ValueError("manifest row count differs from decoded Arrow batch")
        self._metrics["batch_decodes"] += 1
        self._metrics["decoded_bytes"] += batch.nbytes
        if batch.nbytes <= self.max_cache_bytes:
            while self._cache and self._cache_bytes + batch.nbytes > self.max_cache_bytes:
                _, victim = self._cache.popitem(last=False)
                self._cache_bytes -= victim.nbytes
                self._metrics["cache_evictions"] += 1
            self._cache[batch_number] = batch
            self._cache_bytes += batch.nbytes
        return batch

    def read_window(
        self, start: int, length: int, channels: Sequence[int]
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return Float32 values and validity mask in caller's channel order."""
        with self._lock:
            if self._closed:
                raise ValueError("Arrow reader is closed")
            selected = tuple(int(channel) for channel in channels)
            if length <= 0 or start < 0 or start + length > self.total_steps:
                raise ValueError("window is outside the public Arrow step range")
            if not selected or any(channel < 0 or channel >= self.channel_count for channel in selected):
                raise ValueError("channel selection is empty or out of range")
            values = np.zeros((length, len(selected)), dtype=np.float32)
            mask = np.zeros((length, len(selected)), dtype=np.bool_)
            cursor = start
            while cursor < start + length:
                row_number = bisect_right(self._row_starts, cursor) - 1
                row = self._rows[row_number]
                take = min(row.end - cursor, start + length - cursor)
                batch = self._get_batch(row.batch)
                if row.row_in_batch >= batch.num_rows:
                    raise ValueError("manifest row index exceeds decoded Arrow batch")
                actual_start = batch.column("start_step")[row.row_in_batch].as_py()
                actual_end = batch.column("end_step")[row.row_in_batch].as_py()
                if (actual_start, actual_end) != (row.start, row.end):
                    raise ValueError("manifest step range differs from decoded Arrow row")
                target = batch.column("target")[row.row_in_batch]
                if target.is_valid:
                    if len(target.values) != self.channel_count:
                        raise ValueError("Arrow target channel count differs from index")
                    for output_channel, source_channel in enumerate(selected):
                        child = target.values[source_channel]
                        if not child.is_valid:
                            continue
                        if len(child.values) != row.end - row.start:
                            raise ValueError("Arrow channel length differs from declared steps")
                        leaf = child.values.slice(cursor - row.start, take)
                        destination = slice(cursor - start, cursor - start + take)
                        valid = leaf.is_valid().to_numpy(zero_copy_only=False)
                        mask[destination, output_channel] = valid
                        values[destination, output_channel] = leaf.fill_null(0).to_numpy(
                            zero_copy_only=False
                        )
                cursor += take
            self._metrics["read_calls"] += 1
            return values, mask

    @property
    def metrics(self) -> dict[str, int]:
        with self._lock:
            return {**self._metrics, "cache_bytes": self._cache_bytes,
                    "cache_entries": len(self._cache)}

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._cache.clear()
                self._cache_bytes = 0
                self._source.close()
                self._closed = True

    def __enter__(self) -> LocalArrowWindowReader:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
