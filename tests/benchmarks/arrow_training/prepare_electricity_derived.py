#!/usr/bin/env python3
"""Build immutable, reusable temporal-block Arrow artifacts; never modify sources.

channel_major is compatible with the production training List binding. step_major
is a flat List per block for the experimental reader (not a training binding).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_arrow_query_variants import sha256_file, source_info

PROTOCOL = "iotdbfs-electricity-derived/1"
LAYOUTS = ("channel_major", "step_major")
CODECS = ("none", "lz4", "zstd")


def logical_digest(values: np.ndarray, valid: np.ndarray) -> str:
    digest = hashlib.sha256()
    digest.update(np.asarray(values.shape, dtype="<u8").tobytes())
    # Null payload bits are unspecified; validity and every valid Float32 bit count.
    digest.update(np.where(valid, values, np.float32(0)).astype("<f4", copy=False).tobytes())
    digest.update(valid.astype(np.uint8, copy=False).tobytes())
    return digest.hexdigest()


def load_source(source: Path) -> dict:
    load_started = time.perf_counter()
    matrices, masks = [], []
    identities = None
    expected_start = 0
    with source.open("rb") as handle, ipc.open_stream(handle) as reader:
        schema = reader.schema
        target_type = schema.field("target").type
        if target_type != pa.list_(pa.list_(pa.float32())):
            raise ValueError("source target must be list<list<float32>>")
        for batch in reader:
            for row in range(batch.num_rows):
                start = batch.column("start_step")[row].as_py()
                end = batch.column("end_step")[row].as_py()
                if start != expected_start or end <= start:
                    raise ValueError("source rows must cover contiguous steps starting at zero")
                current = {name: batch.column(name)[row].as_py()
                           for name in ("dataset_id", "entity_id", "series_id")}
                if identities is not None and identities != current:
                    raise ValueError("source must describe exactly one series")
                identities = current
                target = batch.column("target")[row]
                if not target.is_valid or not len(target.values):
                    raise ValueError("null or empty parent List is not a dense series")
                channels, channel_masks = [], []
                for child in target.values:
                    if not child.is_valid or len(child.values) != end - start:
                        raise ValueError("null or ragged channel List is not a dense series")
                    leaf = child.values
                    channels.append(leaf.fill_null(np.float32(0)).to_numpy(zero_copy_only=False))
                    channel_masks.append(leaf.is_valid().to_numpy(zero_copy_only=False))
                matrix = np.stack(channels, axis=1)
                if matrices and matrices[0].shape[1] != matrix.shape[1]:
                    raise ValueError("source channel count changes between rows")
                matrices.append(matrix)
                masks.append(np.stack(channel_masks, axis=1))
                expected_start = end
    if not matrices:
        raise ValueError("source is empty")
    values = np.concatenate(matrices)
    valid = np.concatenate(masks)
    return dict(values=values, valid=valid, schema=schema, identities=identities,
                logical_sha256=logical_digest(values, valid),
                source_decode_seconds=time.perf_counter() - load_started)


def target_array(values: np.ndarray, valid: np.ndarray, layout: str) -> pa.Array:
    steps, channels = values.shape
    flat = values.T.reshape(-1) if layout == "channel_major" else values.reshape(-1)
    flat_valid = valid.T.reshape(-1) if layout == "channel_major" else valid.reshape(-1)
    leaf = pa.array(flat, mask=~flat_valid, type=pa.float32())
    if layout == "channel_major":
        children = pa.ListArray.from_arrays(pa.array(np.arange(channels + 1) * steps,
                                                     type=pa.int32()), leaf)
        return pa.ListArray.from_arrays(pa.array([0, channels], type=pa.int32()), children)
    return pa.ListArray.from_arrays(pa.array([0, steps * channels], type=pa.int32()), leaf)


def derived_schema(source_schema: pa.Schema, layout: str, block_steps: int) -> pa.Schema:
    fields = [pa.field(field.name, pa.list_(pa.float32()), nullable=field.nullable)
              if field.name == "target" and layout == "step_major" else field
              for field in source_schema]
    metadata = dict(source_schema.metadata or {})
    metadata.update({b"iotdbfs.derived.protocol": PROTOCOL.encode(),
                     b"iotdbfs.derived.layout": layout.encode(),
                     b"iotdbfs.derived.block_steps": str(block_steps).encode()})
    return pa.schema(fields, metadata=metadata)


def scan_and_verify(path: Path, data: dict, layout: str, block_steps: int) -> tuple[list[dict], bytes]:
    """Read complete message ranges and compare inverse layout with source bits."""
    batches = []
    with pa.OSFile(str(path), "rb") as stream:
        schema = ipc.read_schema(ipc.read_message(stream))
        expected_schema = derived_schema(data["schema"], layout, block_steps)
        if not schema.equals(expected_schema, check_metadata=True):
            raise ValueError("derived schema differs")
        cursor = 0
        while True:
            offset = stream.tell()
            try:
                message = ipc.read_message(stream)
            except EOFError:
                break
            if message is None:
                break
            length = stream.tell() - offset
            if message.type != "record batch":
                raise ValueError("unexpected non-batch message")
            batch = ipc.read_record_batch(message, schema)
            if batch.num_rows != 1:
                raise ValueError("derived batch must contain one block")
            start = batch.column("start_step")[0].as_py()
            end = batch.column("end_step")[0].as_py()
            if start != cursor or end != min(cursor + block_steps, len(data["values"])):
                raise ValueError("derived block coverage differs")
            for name, identity in data["identities"].items():
                if batch.column(name)[0].as_py() != identity:
                    raise ValueError("derived series identity differs")
            if batch.column("segment_index")[0].as_py() != len(batches):
                raise ValueError("derived segment index differs")
            target = batch.column("target")[0]
            if not target.is_valid:
                raise ValueError("derived parent List became null")
            if layout == "channel_major":
                children = target.values
                if children.null_count or len(children) != data["values"].shape[1]:
                    raise ValueError("derived channel coverage differs")
                if any(len(child.values) != end - start for child in children):
                    raise ValueError("derived channel length differs")
                leaf = children.values
            else:
                leaf = target.values
            values = leaf.fill_null(np.float32(0)).to_numpy(zero_copy_only=False)
            valid = leaf.is_valid().to_numpy(zero_copy_only=False)
            channels = data["values"].shape[1]
            shape = (channels, end - start) if layout == "channel_major" else (end - start, channels)
            values, valid = values.reshape(shape), valid.reshape(shape)
            if layout == "channel_major":
                values, valid = values.T, valid.T
            if logical_digest(values, valid) != logical_digest(data["values"][start:end], data["valid"][start:end]):
                raise ValueError("derived values or validity differ from source")
            batches.append(dict(batch=len(batches), row_in_batch=0, start_step=start,
                                end_step=end, channels=channels, offset=offset, length=length))
            cursor = end
        if cursor != len(data["values"]) or stream.tell() != path.stat().st_size:
            raise ValueError("derived coverage incomplete or trailing bytes present")
    return batches, schema.serialize().to_pybytes()


def _publish_json(value: dict, destination: Path) -> None:
    fd, name = tempfile.mkstemp(prefix=".derived-manifest-", dir=destination.parent)
    temporary = Path(name)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w") as handle:
            json.dump(value, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def build_variant(source: Path, destination: Path, *, layout: str, block_steps: int,
                  codec: str, source_version: int | None = None, source_logical_path: str | None = None,
                  data: dict | None = None) -> dict:
    if layout not in LAYOUTS or codec not in CODECS or block_steps <= 0:
        raise ValueError("invalid derived layout, codec, or block steps")
    source, destination = Path(source), Path(destination)
    started = time.perf_counter()
    source_hash = sha256_file(source)
    data = load_source(source) if data is None else data
    source_validation_seconds = time.perf_counter() - started
    identity = dict(protocol=PROTOCOL, source_path=str(source.resolve()),
                    source_logical_path=source_logical_path, source_version=source_version,
                    source_sha256=source_hash, source_bytes=source.stat().st_size,
                    logical_sha256=data["logical_sha256"], layout=layout,
                    block_steps=block_steps, codec=codec)
    sidecar = destination.with_suffix(destination.suffix + ".json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or sidecar.exists():
        if not destination.exists() or not sidecar.exists():
            raise ValueError("incomplete existing derived artifact; use a new destination")
        existing = json.loads(sidecar.read_text())
        if any(existing.get(key) != value for key, value in identity.items()):
            raise ValueError("existing derived artifact identity differs")
        if existing["bytes"] != destination.stat().st_size or existing["sha256"] != sha256_file(destination):
            raise ValueError("existing derived artifact bytes differ")
        batches, schema_bytes = scan_and_verify(destination, data, layout, block_steps)
        rows = [{key: b[key] for key in ("batch", "row_in_batch", "start_step", "end_step", "channels")} for b in batches]
        if (batches != existing["batches"] or rows != existing["rows"] or
                existing["channels"] != data["values"].shape[1] or
                existing["total_steps"] != len(data["values"]) or
                base64.b64encode(schema_bytes).decode() != existing["schema_ipc_base64"]):
            raise ValueError("existing derived sidecar differs")
        return existing
    fd, name = tempfile.mkstemp(prefix=".derived-", suffix=".pending", dir=destination.parent)
    temporary = Path(name)
    schema = derived_schema(data["schema"], layout, block_steps)
    build_started = time.perf_counter()
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "wb") as handle:
            options = ipc.IpcWriteOptions(compression=None if codec == "none" else codec)
            with ipc.new_stream(handle, schema, options=options) as writer:
                for segment, start in enumerate(range(0, len(data["values"]), block_steps)):
                    end = min(start + block_steps, len(data["values"]))
                    scalars = dict(data["identities"], segment_index=segment, start_step=start, end_step=end)
                    target = target_array(data["values"][start:end], data["valid"][start:end], layout)
                    arrays = [target if field.name == "target" else pa.array([scalars[field.name]], type=field.type)
                              for field in schema]
                    writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
            handle.flush()
            os.fsync(handle.fileno())
        build_seconds = time.perf_counter() - build_started
        verify_started = time.perf_counter()
        batches, schema_bytes = scan_and_verify(temporary, data, layout, block_steps)
        result = dict(identity, path=str(destination.resolve()), local=str(destination.resolve()),
                      total_steps=len(data["values"]),
                      source=dict(sha256=source_hash, version=source_version,
                                  path=source_logical_path or str(source.resolve())),
                      rows=[{key: b[key] for key in ("batch", "row_in_batch", "start_step", "end_step", "channels")}
                            for b in batches], bytes=temporary.stat().st_size,
                      sha256=sha256_file(temporary), steps=len(data["values"]),
                      channels=data["values"].shape[1], identities=data["identities"],
                      batches=batches, schema_ipc_base64=base64.b64encode(schema_bytes).decode(),
                      source_validation_seconds=source_validation_seconds,
                      source_decode_seconds=data["source_decode_seconds"], build_seconds=build_seconds,
                      verification_seconds=time.perf_counter() - verify_started)
        os.link(temporary, destination)
        _publish_json(result, sidecar)
        return result
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--layouts", nargs="+", choices=LAYOUTS, default=list(LAYOUTS))
    parser.add_argument("--block-steps", nargs="+", type=int, default=[128, 256, 512, 8192])
    parser.add_argument("--codecs", nargs="+", choices=CODECS, default=["none", "lz4", "zstd"])
    parser.add_argument("--source-version", type=int)
    parser.add_argument("--source-logical-path")
    args = parser.parse_args()
    source, _ = source_info(args.work_dir, "UCI_Electricity_370")
    data = load_source(source)
    for layout in args.layouts:
        for block_steps in args.block_steps:
            for codec in args.codecs:
                path = args.output_dir / f"Electricity-{layout}-{block_steps}-{codec}.arrows"
                result = build_variant(source, path, layout=layout, block_steps=block_steps, codec=codec,
                                       source_version=args.source_version, source_logical_path=args.source_logical_path,
                                       data=data)
                print(json.dumps({key: result[key] for key in ("path", "layout", "block_steps", "codec", "bytes", "build_seconds")}), flush=True)


if __name__ == "__main__":
    main()
