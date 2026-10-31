#!/usr/bin/env python3
"""PB00: turn audited public time-series text into nested Arrow IPC streams.

No TimeBench/TimerLathe code is imported. Raw files and generated data stay in
the experiment work directory; only this converter and its fixture recipe live
in the repository.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import zipfile

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc


ETTM1_SHA256 = "6ce1759b1a18e3328421d5d75fadcb316c449fcd7cec32820c8dafda71986c9e"
ETTM1_CHANNELS = ("HUFL", "HULL", "MUFL", "MULL", "LUFL", "LULL", "OT")
ETTM1_ROWS = 69680
ELECTRICITY_ROWS = 140256
ELECTRICITY_CHANNELS = 370
ELECTRICITY_ZIP_SHA256 = "f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1"
ELECTRICITY_TXT_SHA256 = "d51565f2cb5a6b768d06ba1bbd3c084c6e2f3aab07f00c6f2dcb80e90175124b"
ELECTRICITY_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00321/LD2011_2014.txt.zip"
ELECTRICITY_MIRROR_URL = "https://sagemaker-example-files-prod-us-east-1.s3.amazonaws.com/datasets/timeseries/uci_electricity/LD2011_2014.txt.zip"
TARGET_TYPE = pa.list_(pa.list_(pa.float32()))
SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("segment_index", pa.int32(), nullable=False),
        pa.field("start_step", pa.int64(), nullable=False),
        pa.field("end_step", pa.int64(), nullable=False),
        pa.field("target", TARGET_TYPE),
    ],
    metadata={b"pb00.layout": b"target[channel][step]", b"pb00.version": b"1"},
)
TARGET_ONLY_SCHEMA = pa.schema(
    [pa.field("target", TARGET_TYPE)],
    metadata={b"pb00.layout": b"target[channel][step]", b"pb00.version": b"1"},
)
DICT_SCHEMA = pa.schema(
    [pa.field("target", TARGET_TYPE), pa.field("role", pa.dictionary(pa.int8(), pa.string()))],
    metadata={b"pb00.layout": b"target[channel][step]", b"pb00.version": b"1"},
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_sha(path: Path, expected: str) -> str:
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch for {path}: {actual}, expected {expected}")
    return actual


def electricity_txt_from_zip(archive: Path, destination: Path, expected_zip_sha: str) -> str:
    if expected_zip_sha != ELECTRICITY_ZIP_SHA256:
        raise ValueError("UCI Electricity archive hash differs from pinned original")
    require_sha(archive, expected_zip_sha)
    with zipfile.ZipFile(archive) as bundle:
        matches = [name for name in bundle.namelist() if name.endswith("/LD2011_2014.txt") or name == "LD2011_2014.txt"]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one LD2011_2014.txt in {archive}, got {matches}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with bundle.open(matches[0]) as source, destination.open("wb") as target:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                target.write(block)
    return require_sha(destination, ELECTRICITY_TXT_SHA256)


def parse_source(dataset: str, source: Path, expected_sha: str, out: Path) -> dict:
    """Parse text independently; preserve input order, labels, zeros and masks."""
    require_sha(source, expected_sha)
    if dataset == "ETTm1":
        count, channels, delimiter, has_header = ETTM1_ROWS, ETTM1_CHANNELS, ",", True
    elif dataset == "UCI_Electricity_370":
        count, channels, delimiter, has_header = ELECTRICITY_ROWS, tuple(f"MT_{i:03d}" for i in range(1, ELECTRICITY_CHANNELS + 1)), ";", True
    else:
        raise ValueError(f"Unknown dataset: {dataset}")
    out.mkdir(parents=True, exist_ok=True)
    values_path = out / f"{dataset}.values.npy"
    valid_path = out / f"{dataset}.valid.npy"
    dates_path = out / f"{dataset}.timestamps.txt"
    values = np.lib.format.open_memmap(values_path, mode="w+", dtype="<f4", shape=(count, len(channels)))
    valid = np.lib.format.open_memmap(valid_path, mode="w+", dtype=np.bool_, shape=(count, len(channels)))
    with source.open("r", encoding="utf-8-sig", newline="") as handle, dates_path.open("w", encoding="utf-8", newline="\n") as dates:
        reader = csv.reader(handle, delimiter=delimiter)
        if has_header:
            header = next(reader)
            if header[0] not in ("", "date") or header[1:] != list(channels):
                raise ValueError(f"Unexpected {dataset} channel order: {header}")
        observed_count = 0
        missing_count = 0
        zero_count = 0
        first_timestamp = None
        last_timestamp = None
        for row_index, row in enumerate(reader):
            if row_index >= count or len(row) != len(channels) + 1:
                raise ValueError(f"Invalid source shape at row {row_index}: {len(row)} fields")
            dates.write(row[0] + "\n")
            if first_timestamp is None:
                first_timestamp = row[0]
            last_timestamp = row[0]
            numbers = np.fromiter(
                (float(token.replace(",", ".")) if token.strip() else math.nan for token in row[1:]),
                dtype=np.float32,
                count=len(channels),
            )
            observed = np.isfinite(numbers)
            if np.isinf(numbers).any():
                raise ValueError(f"Infinite value at row {row_index}")
            values[row_index] = np.where(observed, numbers, np.float32(0))
            valid[row_index] = observed
            missing_count += int(np.count_nonzero(~observed))
            zero_count += int(np.count_nonzero(observed & (numbers == 0)))
            observed_count += 1
        if observed_count != count:
            raise ValueError(f"Expected {count} rows, got {observed_count}")
    values.flush()
    valid.flush()
    result = {
        "dataset_id": dataset,
        "source_file": source.name,
        "source_sha256": expected_sha,
        "source_url": "https://raw.githubusercontent.com/zhouhaoyi/ETDataset/main/ETT-small/ETTm1.csv" if dataset == "ETTm1" else ELECTRICITY_URL,
        "source_license": "CC BY-ND 4.0" if dataset == "ETTm1" else "CC BY 4.0",
        "rows": count,
        "channels": list(channels),
        "shape": [count, len(channels)],
        "first_timestamp_label": first_timestamp,
        "last_timestamp_label": last_timestamp,
        "null_values": missing_count,
        "valid_zeros": zero_count,
        "parser": "prepare_public_data.py/v1; CSV stdlib; decimal comma for UCI; float32; empty/NaN=NULL; zero retained",
        "canonical": {p.name: {"sha256": sha256(p), "bytes": p.stat().st_size} for p in (values_path, valid_path, dates_path)},
    }
    if dataset == "UCI_Electricity_370":
        result["source_archive"] = {"url": ELECTRICITY_URL, "verified_mirror_url": ELECTRICITY_MIRROR_URL, "sha256": ELECTRICITY_ZIP_SHA256, "bytes": 261335609}
    return result


def _target_from_matrix(values: np.ndarray, valid: np.ndarray) -> pa.ListArray:
    """Build one [channel][step] row without materializing Python float lists."""
    steps, channels = values.shape
    flat = np.ascontiguousarray(values.T).ravel()
    mask = ~np.ascontiguousarray(valid.T).ravel()
    leaf = pa.array(flat, mask=mask, type=pa.float32())
    inner = pa.ListArray.from_arrays(pa.array(np.arange(channels + 1, dtype=np.int32) * steps), leaf)
    return pa.ListArray.from_arrays(pa.array([0, channels], type=pa.int32()), inner)


def write_dataset_stream(dataset: str, canonical: Path, destination: Path, *, segment_steps: int, rows_per_batch: int, compression: str | None) -> dict:
    if segment_steps <= 0 or rows_per_batch <= 0:
        raise ValueError("segment_steps and rows_per_batch must be positive")
    values = np.load(canonical / f"{dataset}.values.npy", mmap_mode="r")
    valid = np.load(canonical / f"{dataset}.valid.npy", mmap_mode="r")
    if values.shape != valid.shape or values.dtype != np.dtype("float32"):
        raise ValueError("Canonical values/mask mismatch")
    destination.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    batches = []
    pending = []
    options = ipc.IpcWriteOptions(compression=compression)
    with destination.open("wb") as handle, ipc.new_stream(handle, SCHEMA, options=options) as writer:
        for segment_index, start in enumerate(range(0, values.shape[0], segment_steps)):
            end = min(start + segment_steps, values.shape[0])
            target = _target_from_matrix(values[start:end], valid[start:end])
            pending.append((segment_index, start, end, target))
            rows.append({"batch": len(batches), "row_in_batch": len(pending) - 1, "series_id": dataset, "segment_index": segment_index, "start_step": start, "end_step": end, "channels": values.shape[1], "continuation": segment_index > 0})
            if len(pending) == rows_per_batch:
                _flush(writer, dataset, pending)
                batches.append(len(pending))
                pending.clear()
        if pending:
            _flush(writer, dataset, pending)
            batches.append(len(pending))
    return {"path": destination.name, "sha256": sha256(destination), "bytes": destination.stat().st_size, "format": "Arrow IPC stream", "compression": compression or "none", "schema": str(SCHEMA), "segment_steps": segment_steps, "rows_per_batch": rows_per_batch, "batch_row_counts": batches, "rows": rows}


def _flush(writer: ipc.RecordBatchStreamWriter, dataset: str, pending: list) -> None:
    writer.write_batch(pa.record_batch(
        [
            pa.array([dataset] * len(pending), type=pa.string()),
            pa.array([dataset] * len(pending), type=pa.string()),
            pa.array([dataset] * len(pending), type=pa.string()),
            pa.array([row[0] for row in pending], type=pa.int32()),
            pa.array([row[1] for row in pending], type=pa.int64()),
            pa.array([row[2] for row in pending], type=pa.int64()),
            pa.concat_arrays([row[3] for row in pending]),
        ],
        schema=SCHEMA,
    ))


FIXTURE_ROWS = [
    [[float(c * 1000 + s) if s not in (3, 11) else None for s in range(17)] for c in range(3)],
    [[float(c * 1000 + s) for s in range(length)] if c != 1 else None for c, length in enumerate((9, 6, 11))],
    None,
    [[], [None, 1001.0, 1002.0, 1003.0, 1004.0], [2000.0, 2001.0, 2002.0, 2003.0]],
    [[float(c * 1000 + s) for s in range(13)] for c in range(3)],
]


def generate_fixtures(out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    generated = {}
    for compression in (None, "lz4", "zstd"):
        filename = f"nested-multibatch-{compression or 'none'}.arrows"
        path = out / filename
        options = ipc.IpcWriteOptions(compression=compression)
        with path.open("wb") as handle, ipc.new_stream(handle, SCHEMA, options=options) as writer:
            for base, rows in ((0, FIXTURE_ROWS[:2]), (2, FIXTURE_ROWS[2:])):
                writer.write_batch(pa.record_batch(
                    [
                        pa.array(["fixture"] * len(rows), type=pa.string()),
                        pa.array([f"entity-{base + i}" for i in range(len(rows))], type=pa.string()),
                        pa.array([f"series-{base + i}" for i in range(len(rows))], type=pa.string()),
                        pa.array(list(range(base, base + len(rows))), type=pa.int32()),
                        pa.array([0] * len(rows), type=pa.int64()),
                        pa.array([max((len(channel) for channel in row if channel is not None), default=0) if row is not None else 0 for row in rows], type=pa.int64()),
                        pa.array(rows, type=TARGET_TYPE),
                    ],
                    schema=SCHEMA,
                ))
        generated[filename] = {"sha256": sha256(path), "bytes": path.stat().st_size, "compression": compression or "none", "batches": [2, 3], "rows": len(FIXTURE_ROWS)}
    filename = "nested-target-only-none.arrows"
    path = out / filename
    with path.open("wb") as handle, ipc.new_stream(handle, TARGET_ONLY_SCHEMA) as writer:
        for rows in (FIXTURE_ROWS[:2], FIXTURE_ROWS[2:]):
            writer.write_batch(pa.record_batch([pa.array(rows, type=TARGET_TYPE)], schema=TARGET_ONLY_SCHEMA))
    generated[filename] = {"sha256": sha256(path), "bytes": path.stat().st_size, "compression": "none", "batches": [2, 3], "rows": len(FIXTURE_ROWS), "target_only": True}
    for mode in ("delta", "replacement"):
        filename = f"nested-target-dictionary-{mode}.arrows"
        path = out / filename
        options = ipc.IpcWriteOptions(emit_dictionary_deltas=(mode == "delta"))
        with path.open("wb") as handle, ipc.new_stream(handle, DICT_SCHEMA, options=options) as writer:
            roles = (
                pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()), pa.array(["train", "eval"])),
                pa.DictionaryArray.from_arrays(pa.array([2, 1, 0], type=pa.int8()), pa.array(["train", "eval", "test"] if mode == "delta" else ["test", "eval", "train"])),
            )
            for rows, role in zip((FIXTURE_ROWS[:2], FIXTURE_ROWS[2:]), roles):
                writer.write_batch(pa.record_batch([pa.array(rows, type=TARGET_TYPE), role], schema=DICT_SCHEMA))
        generated[filename] = {"sha256": sha256(path), "bytes": path.stat().st_size, "compression": "none", "batches": [2, 3], "rows": len(FIXTURE_ROWS), "dictionary_mode": mode, "role_values": [["train", "eval"], ["test", "eval", "train"] if mode == "delta" else ["train", "eval", "test"]]}
    return {"fixture_recipe": "prepare_public_data.py/v1", "pyarrow": pa.__version__, "schema": str(SCHEMA), "target_only_schema": str(TARGET_ONLY_SCHEMA), "dictionary_schema": str(DICT_SCHEMA), "rows": FIXTURE_ROWS, "files": generated}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fixtures = commands.add_parser("fixtures")
    fixtures.add_argument("--out", type=Path, required=True)
    extract = commands.add_parser("extract-electricity")
    extract.add_argument("--archive", type=Path, required=True)
    extract.add_argument("--archive-sha256", required=True)
    extract.add_argument("--out", type=Path, required=True)
    convert = commands.add_parser("convert")
    convert.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    convert.add_argument("--source", type=Path, required=True)
    convert.add_argument("--source-sha256", required=True)
    convert.add_argument("--work-dir", type=Path, required=True)
    convert.add_argument("--segment-steps", type=int, default=8192)
    convert.add_argument("--rows-per-batch", type=int, default=2)
    convert.add_argument("--compression", choices=("none", "lz4", "zstd"), default="none")
    args = parser.parse_args()
    if args.command == "fixtures":
        result = generate_fixtures(args.out)
        manifest = args.out / "fixture-manifest.json"
    elif args.command == "extract-electricity":
        result = {
            "source_url": ELECTRICITY_URL,
            "verified_mirror_url": ELECTRICITY_MIRROR_URL,
            "archive_sha256": args.archive_sha256,
            "archive_bytes": args.archive.stat().st_size,
            "txt_sha256": electricity_txt_from_zip(args.archive, args.out, args.archive_sha256),
            "txt_bytes": args.out.stat().st_size,
        }
        manifest = args.out.parent / "electricity-source.json"
    else:
        source_sha = ETTM1_SHA256 if args.dataset == "ETTm1" else ELECTRICITY_TXT_SHA256
        if args.source_sha256 != source_sha:
            raise ValueError(f"{args.dataset} source hash differs from pinned original")
        canonical = args.work_dir / "canonical"
        result = parse_source(args.dataset, args.source, source_sha, canonical)
        stream = args.work_dir / "arrow" / f"{args.dataset}-{args.compression}-{args.segment_steps}.arrows"
        result["arrow"] = write_dataset_stream(args.dataset, canonical, stream, segment_steps=args.segment_steps, rows_per_batch=args.rows_per_batch, compression=None if args.compression == "none" else args.compression)
        manifest = args.work_dir / "manifests" / f"{args.dataset}-pb00.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(manifest)


if __name__ == "__main__":
    main()
