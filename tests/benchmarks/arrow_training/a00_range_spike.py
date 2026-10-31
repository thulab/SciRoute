"""A00 IPC stream range-read spike for target: list<list<float32>>.

This is an experiment, not the production decoder. It indexes IPC message
metadata, then reads only the selected row/channel offsets, bitmap bytes and
value tiles (or whole compressed buffers). It compares every result with
PyArrow and records exact physical reads. No private dataset code is used.
"""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.ipc as ipc


def unpack(fmt: str, data: bytes, offset: int):
    return struct.unpack_from("<" + fmt, data, offset)[0]


class FlatTable:
    """Read only the Arrow IPC FlatBuffer fields needed by this spike."""

    def __init__(self, data: bytes, offset: int):
        self.data = data
        self.offset = offset
        self.vtable = offset - unpack("i", data, offset)
        self.vtable_size = unpack("H", data, self.vtable)

    def field(self, index: int):
        slot = 4 + 2 * index
        if slot + 2 > self.vtable_size:
            return None
        relative = unpack("H", self.data, self.vtable + slot)
        return self.offset + relative if relative else None

    def scalar(self, index: int, fmt: str, default=0):
        at = self.field(index)
        return default if at is None else unpack(fmt, self.data, at)

    def table(self, index: int):
        at = self.field(index)
        return None if at is None else FlatTable(self.data, at + unpack("I", self.data, at))

    def vector(self, index: int):
        at = self.field(index)
        if at is None:
            return 0, 0
        vector = at + unpack("I", self.data, at)
        return vector + 4, unpack("I", self.data, vector)


class RecordingRangeReader:
    def __init__(self, path: Path):
        self.path = path
        self.size = path.stat().st_size
        self.fd = os.open(path, os.O_RDONLY)
        self.reads = []

    def close(self):
        os.close(self.fd)

    def read(self, offset: int, length: int, kind: str) -> bytes:
        if offset < 0 or length < 0 or offset + length > self.size:
            raise ValueError(f"Out-of-range {kind} read: {offset}+{length}")
        data = os.pread(self.fd, length, offset)
        if len(data) != length:
            raise OSError(f"Short {kind} read")
        self.reads.append({"kind": kind, "offset": offset, "length": length})
        return data


class RecordingSequentialReader:
    """Expose normal stream reads to PyArrow and count bytes actually returned."""

    def __init__(self, path: Path):
        self.file = path.open("rb")
        self.reads = []

    def read(self, length=-1):
        offset = self.file.tell()
        data = self.file.read(length)
        self.reads.append({"offset": offset, "length": len(data)})
        return data

    def seek(self, offset, whence=0):
        return self.file.seek(offset, whence)

    def tell(self):
        return self.file.tell()

    def close(self):
        self.file.close()

    @property
    def closed(self):
        return self.file.closed

    def readable(self):
        return True

    def seekable(self):
        return True


@dataclass(frozen=True)
class BufferSpan:
    offset: int
    length: int


@dataclass
class Batch:
    message_offset: int
    body_offset: int
    body_length: int
    rows: int
    buffers: list[BufferSpan]
    codec: str | None
    dictionary_epoch: dict[int, int]


def count_buffers(data_type) -> int:
    if pa.types.is_dictionary(data_type):
        return 2  # dictionary indices; values live in separate messages
    if pa.types.is_list(data_type):
        return 2 + count_buffers(data_type.value_type)
    if pa.types.is_string(data_type) or pa.types.is_binary(data_type):
        return 3
    if pa.types.is_integer(data_type) or pa.types.is_floating(data_type):
        return 2
    raise ValueError(f"A00 spike does not map buffers for {data_type}")


def target_buffer_start(schema: pa.Schema) -> int:
    before = 0
    expected = pa.list_(pa.list_(pa.float32()))
    for field in schema:
        if field.name == "target":
            if field.type != expected:
                raise ValueError(f"Expected target: {expected}; got {field.type}")
            return before
        before += count_buffers(field.type)
    raise ValueError("target field absent")


def index_stream(reader: RecordingRangeReader):
    offset = 0
    schema = None
    messages = []
    batches = []
    dictionary_epoch = {}
    while offset + 8 <= reader.size:
        frame = reader.read(offset, 8, "frame")
        if unpack("I", frame, 0) != 0xFFFFFFFF:
            raise ValueError("Expected modern IPC continuation marker")
        metadata_length = unpack("i", frame, 4)
        if metadata_length == 0:
            if offset + 8 != reader.size:
                raise ValueError("Trailing bytes after IPC EOS")
            break
        if metadata_length < 0:
            raise ValueError("Negative IPC metadata length")
        metadata = reader.read(offset + 8, metadata_length, "message_metadata")
        message = FlatTable(metadata, unpack("I", metadata, 0))
        header_type = message.scalar(1, "B")
        body_length = message.scalar(3, "q")
        body_offset = offset + 8 + metadata_length
        if body_length < 0 or body_offset + body_length > reader.size:
            raise ValueError("Invalid IPC body span")
        record = {
            "type": {1: "schema", 2: "dictionary", 3: "record_batch"}.get(
                header_type, str(header_type)
            ),
            "message_offset": offset,
            "metadata_length": metadata_length,
            "body_offset": body_offset,
            "body_length": body_length,
        }
        header = message.table(2)
        if header_type == 1:
            if schema is not None or body_length:
                raise ValueError("Unexpected schema message")
            schema = ipc.read_schema(pa.BufferReader(frame + metadata))
        elif header_type == 2:
            dictionary_id = header.scalar(0, "q")
            dictionary_epoch[dictionary_id] = dictionary_epoch.get(dictionary_id, 0) + 1
            record["dictionary_id"] = dictionary_id
            record["is_delta"] = bool(header.scalar(2, "B"))
        elif header_type == 3:
            if schema is None:
                raise ValueError("Record batch before schema")
            rows = header.scalar(0, "q")
            vector_at, count = header.vector(2)
            buffers = []
            for index in range(count):
                buf_offset, buf_length = struct.unpack_from(
                    "<qq", metadata, vector_at + 16 * index
                )
                if buf_offset < 0 or buf_length < 0 or buf_offset + buf_length > body_length:
                    raise ValueError("Invalid RecordBatch buffer span")
                buffers.append(BufferSpan(buf_offset, buf_length))
            compression = header.table(3)
            codec = None
            if compression is not None:
                if compression.scalar(1, "B") != 0:
                    raise ValueError("Unsupported IPC compression method")
                codec = ("lz4_frame", "zstd")[compression.scalar(0, "B")]
            batches.append(
                Batch(
                    offset,
                    body_offset,
                    body_length,
                    rows,
                    buffers,
                    codec,
                    dictionary_epoch.copy(),
                )
            )
            record["rows"] = rows
            record["codec"] = codec
            record["buffer_count"] = count
            record["dictionary_epoch"] = dictionary_epoch.copy()
        else:
            raise ValueError(f"Unsupported IPC message header {header_type}")
        messages.append(record)
        offset = body_offset + body_length
    if schema is None or not batches:
        raise ValueError("Expected schema and at least one record batch")
    return schema, batches, messages


class SelectedWindowReader:
    def __init__(self, reader: RecordingRangeReader, schema: pa.Schema, batches: list[Batch]):
        self.reader = reader
        self.batches = batches
        self.target_start = target_buffer_start(schema)
        self.buffer_cache = {}
        self.tile_cache = {}
        self.decompressions = 0
        self.decoded_bytes = 0

    def compressed_buffer(self, batch_index: int, buffer_index: int) -> bytes:
        key = batch_index, buffer_index
        if key not in self.buffer_cache:
            batch = self.batches[batch_index]
            span = batch.buffers[buffer_index]
            raw = self.reader.read(
                batch.body_offset + span.offset, span.length, "compressed_buffer"
            )
            if len(raw) < 8:
                raise ValueError("Missing compression prefix")
            original_length = unpack("q", raw, 0)
            if original_length == -1:
                decoded = raw[8:]
            elif original_length >= 0:
                decoded = pa.Codec(batch.codec).decompress(
                    raw[8:], decompressed_size=original_length
                ).to_pybytes()
                self.decompressions += 1
            else:
                raise ValueError("Invalid compression length")
            self.decoded_bytes += len(decoded)
            self.buffer_cache[key] = decoded
        return self.buffer_cache[key]

    def segment(self, batch_index: int, buffer_index: int, start: int, length: int, kind: str):
        batch = self.batches[batch_index]
        span = batch.buffers[buffer_index]
        if start < 0 or length < 0:
            raise ValueError("Negative buffer subrange")
        if batch.codec is not None:
            decoded = self.compressed_buffer(batch_index, buffer_index)
            if start + length > len(decoded):
                raise ValueError("Decoded buffer subrange out of bounds")
            return decoded[start : start + length]
        if start + length > span.length:
            raise ValueError("Raw buffer subrange out of bounds")
        return self.reader.read(batch.body_offset + span.offset + start, length, kind)

    def valid(self, batch_index: int, buffer_index: int, index: int) -> bool:
        span = self.batches[batch_index].buffers[buffer_index]
        if span.length == 0:
            return True
        bitmap_byte = self.segment(
            batch_index, buffer_index, index // 8, 1, "validity_byte"
        )[0]
        return bool(bitmap_byte & (1 << (index % 8)))

    def value_bytes(self, batch_index: int, start: int, end: int) -> bytes:
        buffer_index = self.target_start + 5
        batch = self.batches[batch_index]
        if batch.codec is not None:
            return self.segment(
                batch_index, buffer_index, start * 4, (end - start) * 4, "values"
            )
        # Stable, window-independent 64-float tiles are shared by overlaps.
        span = batch.buffers[buffer_index]
        total = span.length // 4
        parts = []
        cursor = start
        while cursor < end:
            tile_start = cursor // 64 * 64
            key = batch_index, buffer_index, tile_start
            if key not in self.tile_cache:
                tile_count = min(64, total - tile_start)
                self.tile_cache[key] = self.segment(
                    batch_index, buffer_index, tile_start * 4, tile_count * 4, "value_tile"
                )
            tile_end = min(end, tile_start + len(self.tile_cache[key]) // 4)
            parts.append(
                self.tile_cache[key][(cursor - tile_start) * 4 : (tile_end - tile_start) * 4]
            )
            cursor = tile_end
        return b"".join(parts)

    def window(self, batch_index: int, row: int, channel: int, start: int, end: int):
        batch = self.batches[batch_index]
        if not (0 <= row < batch.rows and 0 <= start <= end):
            raise ValueError("Invalid row or step interval")
        base = self.target_start
        if not self.valid(batch_index, base, row):
            raise ValueError("Selected row is null")
        outer = self.segment(batch_index, base + 1, row * 4, 8, "outer_offsets")
        outer_start, outer_end = struct.unpack("<ii", outer)
        if not (0 <= channel < outer_end - outer_start):
            raise ValueError("Channel ordinal out of bounds")
        inner_row = outer_start + channel
        if not self.valid(batch_index, base + 2, inner_row):
            raise ValueError("Selected channel is null")
        inner = self.segment(
            batch_index, base + 3, inner_row * 4, 8, "inner_offsets"
        )
        inner_start, inner_end = struct.unpack("<ii", inner)
        if end > inner_end - inner_start:
            raise ValueError("Step range exceeds channel")
        raw_values = self.value_bytes(batch_index, inner_start + start, inner_start + end)
        floats = struct.unpack("<" + "f" * (end - start), raw_values)
        mask = [
            self.valid(batch_index, base + 4, inner_start + step)
            for step in range(start, end)
        ]
        return [value if present else None for value, present in zip(floats, mask)], mask


def oracle_window(path: Path, batch_index: int, row: int, channel: int, start: int, end: int):
    stream = ipc.open_stream(path)
    for index, batch in enumerate(stream):
        if index == batch_index:
            target = batch.column(batch.schema.get_field_index("target"))
            values = target[row].as_py()[channel][start:end]
            return values, [value is not None for value in values]
    raise ValueError("Batch index out of bounds")


def batch_reader_window(path: Path, batch_index: int, row: int, channel: int, start: int, end: int):
    source = RecordingSequentialReader(path)
    try:
        stream = ipc.open_stream(source)
        for index, batch in enumerate(stream):
            if index == batch_index:
                target = batch.column(batch.schema.get_field_index("target"))
                values = target[row].as_py()[channel][start:end]
                return values, source.reads.copy()
        raise ValueError("Batch index out of bounds")
    finally:
        source.close()


def run_case(path: Path, batch_index: int, row: int, channel: int, start: int, end: int):
    reader = RecordingRangeReader(path)
    try:
        schema, batches, messages = index_stream(reader)
        index_reads = reader.reads.copy()
        selected = SelectedWindowReader(reader, schema, batches)
        values, mask = selected.window(batch_index, row, channel, start, end)
        expected_values, expected_mask = oracle_window(
            path, batch_index, row, channel, start, end
        )
        if values != expected_values or mask != expected_mask:
            raise AssertionError("Range result differs from PyArrow oracle")
        batch_values, batch_reads = batch_reader_window(
            path, batch_index, row, channel, start, end
        )
        if batch_values != expected_values:
            raise AssertionError("Sequential batch reader differs from oracle")
        first_reads = reader.reads[len(index_reads) :].copy()
        selected.window(batch_index, row, channel, min(start + 1, end), end)
        warm_reads = reader.reads[len(index_reads) + len(first_reads) :].copy()
        batch = batches[batch_index]
        result = {
            "source": str(path),
            "size_bytes": reader.size,
            "request": {
                "batch": batch_index,
                "row": row,
                "channel": channel,
                "steps": [start, end],
            },
            "schema": str(schema),
            "messages": messages,
            "selected_batch": {
                "body_offset": batch.body_offset,
                "body_length": batch.body_length,
                "codec": batch.codec,
                "dictionary_epoch": batch.dictionary_epoch,
            },
            "result": {"values": values, "mask": mask},
            "index_reads": index_reads,
            "first_window_reads": first_reads,
            "warm_overlap_reads": warm_reads,
            "batch_reader_reads": batch_reads,
            "bytes": {
                "index": sum(item["length"] for item in index_reads),
                "first_window": sum(item["length"] for item in first_reads),
                "warm_overlap": sum(item["length"] for item in warm_reads),
                "batch_reader": sum(item["length"] for item in batch_reads),
                "requested_values": 4 * (end - start),
                "full_stream": reader.size,
            },
            "decompressions": selected.decompressions,
            "decoded_buffer_bytes": selected.decoded_bytes,
            "oracle_equal": True,
        }
        return result
    finally:
        reader.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stream", type=Path)
    parser.add_argument("--batch", type=int, default=0)
    parser.add_argument("--row", type=int, default=0)
    parser.add_argument("--channel", type=int, default=0)
    parser.add_argument("--start", type=int, default=2)
    parser.add_argument("--end", type=int, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_case(
        args.stream, args.batch, args.row, args.channel, args.start, args.end
    )
    encoded = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(json.dumps({
        "source": report["source"],
        "codec": report["selected_batch"]["codec"],
        "bytes": report["bytes"],
        "decompressions": report["decompressions"],
        "oracle_equal": report["oracle_equal"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
