#!/usr/bin/env python3
"""Index the exact original training Arrow stream, without rewriting its bytes.

The manifest describes each complete physical record batch and every time row
inside it. It is consumed by the same prepared reader as derived one-row blocks.
Index construction and source hashing are measured separately from query time.
"""
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
import struct
import time

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_arrow_query_variants import sha256_file


def _field(metadata: memoryview, table: int, slot: int) -> int | None:
    """Resolve a FlatBuffer table field, checking all accessed byte positions."""
    def scalar(fmt, offset):
        length = struct.calcsize(fmt)
        if offset < 0 or offset + length > len(metadata):
            raise ValueError('truncated Arrow IPC metadata')
        return struct.unpack_from(fmt, metadata, offset)[0]

    vtable = table - scalar('<i', table)
    size = scalar('<H', vtable)
    offset = 4 + 2*slot
    if offset + 2 > size:
        return None
    relative = scalar('<H', vtable + offset)
    if not relative:
        return None
    position = table + relative
    if position < 0 or position >= len(metadata):
        raise ValueError('invalid Arrow IPC field position')
    return position


def _require_uncompressed(message):
    # Message.header (slot 2) is RecordBatch; its optional compression is slot 3.
    # Presence matters, including buffers marked individually "not compressed".
    metadata = memoryview(message.metadata)
    root = struct.unpack_from('<I', metadata, 0)[0]
    header_field = _field(metadata, root, 2)
    if header_field is None or header_field + 4 > len(metadata):
        raise ValueError('missing Arrow record batch header')
    header = header_field + struct.unpack_from('<I', metadata, header_field)[0]
    if _field(metadata, header, 3) is not None:
        raise ValueError('original control must use uncompressed Arrow IPC')


def index_original(source: Path, *, source_version: int, source_logical_path: str,
                   mounted: str | None = None) -> dict:
    """Build an external manifest for an immutable, uncompressed original source."""
    started = time.perf_counter()
    source = Path(source).resolve()
    if source_version <= 0 or not source_logical_path.startswith('/'):
        raise ValueError('original source requires an exact version and logical path')
    before = source.stat()
    batches, rows = [], []
    cursor, channels, identities = 0, None, None
    with pa.OSFile(str(source), 'rb') as stream:
        schema = ipc.read_schema(ipc.read_message(stream))
        if schema.field('target').type != pa.list_(pa.list_(pa.float32())):
            raise ValueError('original target must be list<list<float32>>')
        while True:
            offset = stream.tell()
            try:
                message = ipc.read_message(stream)
            except EOFError:
                break
            if message is None:
                break
            if message.type != 'record batch':
                raise ValueError('original stream contains a non-record-batch message')
            _require_uncompressed(message)
            length = stream.tell() - offset
            batch = ipc.read_record_batch(message, schema)
            if not batch.num_rows:
                raise ValueError('original stream contains an empty batch')
            batch_rows = []
            for row_index in range(batch.num_rows):
                start = batch.column('start_step')[row_index].as_py()
                end = batch.column('end_step')[row_index].as_py()
                if start != cursor or end is None or end <= start:
                    raise ValueError('original rows must contiguously cover steps from zero')
                identity = {name: batch.column(name)[row_index].as_py()
                            for name in ('dataset_id', 'entity_id', 'series_id')}
                if identities is not None and identity != identities:
                    raise ValueError('original source must contain one series')
                identities = identity
                target = batch.column('target')[row_index]
                if not target.is_valid:
                    raise ValueError('original parent List is null')
                children = target.values
                if not len(children) or children.null_count:
                    raise ValueError('original channels must be non-null Lists')
                if channels is not None and channels != len(children):
                    raise ValueError('original channel count changes')
                channels = len(children)
                if not np.all(np.diff(children.offsets.to_numpy()) == end-start):
                    raise ValueError('original channel length does not match step coverage')
                row = dict(batch=len(batches), row_in_batch=row_index,
                           start_step=start, end_step=end, channels=channels)
                rows.append(row)
                batch_rows.append(row)
                cursor = end
            batches.append(dict(batch=len(batches), start_step=batch_rows[0]['start_step'],
                                end_step=cursor, channels=channels, offset=offset,
                                length=length, rows=batch_rows))
        if stream.tell() != before.st_size or not batches:
            raise ValueError('original stream is empty or contains trailing bytes')
    index_seconds = time.perf_counter()-started
    hash_started = time.perf_counter()
    digest = sha256_file(source)
    after = source.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError('original source changed while indexing')
    result = dict(protocol='iotdbfs-original-training-index/1', artifact_kind='original_index',
                  layout='channel_major', codec='none', block_steps=None,
                  local=str(source), path=str(source), logical=source_logical_path,
                  version=source_version, bytes=before.st_size, sha256=digest,
                  source_path=str(source), source_logical_path=source_logical_path,
                  source_version=source_version, source_sha256=digest,
                  source_bytes=before.st_size,
                  source=dict(path=source_logical_path, version=source_version, sha256=digest),
                  total_steps=cursor, steps=cursor, channels=channels, identities=identities,
                  batches=batches, rows=rows,
                  schema_ipc_base64=base64.b64encode(schema.serialize().to_pybytes()).decode(),
                  index_seconds=index_seconds, hash_seconds=time.perf_counter()-hash_started,
                  preparation_seconds=time.perf_counter()-started)
    if mounted is not None:
        result['mounted'] = str(mounted)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-version', type=int, required=True)
    parser.add_argument('--source-logical-path', required=True)
    parser.add_argument('--mounted')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = index_original(args.source, source_version=args.source_version,
                              source_logical_path=args.source_logical_path, mounted=args.mounted)
    with args.output.open('x') as output:
        json.dump(manifest, output, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
