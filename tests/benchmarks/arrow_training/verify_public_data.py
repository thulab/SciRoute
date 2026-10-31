#!/usr/bin/env python3
"""Independent NumPy/PyArrow window oracle for PB00 public data and fixtures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from prepare_public_data import DICT_SCHEMA, FIXTURE_ROWS, SCHEMA, TARGET_ONLY_SCHEMA, sha256


def _slice(row: pa.ListScalar, channel: int, start: int, end: int) -> tuple[np.ndarray, np.ndarray] | None:
    if not row.is_valid:
        return None
    child = row.values[channel]
    if not child.is_valid:
        return None
    items = child.values.slice(start, end - start).to_pylist()
    return (
        np.asarray([0 if x is None else x for x in items], dtype=np.float32),
        np.asarray([x is not None for x in items], dtype=np.bool_),
    )


def _compare(actual: tuple[np.ndarray, np.ndarray] | None, expected: list[float | None] | None, where: str) -> None:
    if expected is None:
        assert actual is None, where
        return
    assert actual is not None, where
    exp_values = np.asarray([0 if x is None else x for x in expected], dtype=np.float32)
    exp_valid = np.asarray([x is not None for x in expected], dtype=np.bool_)
    assert np.array_equal(actual[0], exp_values), f"value mismatch: {where}"
    assert np.array_equal(actual[1], exp_valid), f"mask mismatch: {where}"


def verify_fixture(manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text())
    if manifest["rows"] != FIXTURE_ROWS:
        raise AssertionError("Fixture recipe differs from manifest")
    counts = {}
    for filename, item in manifest["files"].items():
        path = manifest_path.parent / filename
        assert sha256(path) == item["sha256"]
        with path.open("rb") as handle:
            reader = ipc.open_stream(handle)
            expected_schema = DICT_SCHEMA if item.get("dictionary_mode") else TARGET_ONLY_SCHEMA if item.get("target_only") else SCHEMA
            assert reader.schema == expected_schema
            batches = list(reader)
        assert [len(batch) for batch in batches] == [2, 3]
        if item.get("dictionary_mode"):
            assert [batch.column("role").to_pylist() for batch in batches] == item["role_values"]
        rows = [batch.column("target")[row] for batch in batches for row in range(len(batch))]
        checked = 0
        for index, expected_row in enumerate(FIXTURE_ROWS):
            for channel in range(3):
                series = expected_row[channel] if expected_row is not None else None
                length = len(series) if series is not None else 0
                windows = ((0, 0),) if series is None else ((start, end) for start in range(length + 1) for end in range(start, length + 1))
                for start, end in windows:
                    _compare(_slice(rows[index], channel, start, end), None if series is None else series[start:end], f"{filename} row={index} channel={channel} [{start}:{end}]")
                    checked += 1
        # The nulls exercise the top row, inner channel, and float leaf separately.
        assert not rows[2].is_valid
        assert not rows[1].values[1].is_valid
        assert rows[0].values[0].values.null_count == 2
        projected = [_slice(rows[0], channel, 3, 11) for channel in (2, 0)]
        assert all(item is not None for item in projected)
        assert np.array_equal(
            np.stack([item[0] for item in projected], axis=1),
            np.stack([
                np.asarray([0 if value is None else value for value in FIXTURE_ROWS[0][channel][3:11]], dtype=np.float32)
                for channel in (2, 0)
            ], axis=1),
        )
        counts[filename] = {"windows_checked": checked, "batches": 2, "rows": 5}
    return counts


def verify_dataset(manifest_path: Path, work_dir: Path) -> dict:
    manifest = json.loads(manifest_path.read_text())
    dataset = manifest["dataset_id"]
    for filename, meta in manifest["canonical"].items():
        assert sha256(work_dir / "canonical" / filename) == meta["sha256"]
    values = np.load(work_dir / "canonical" / f"{dataset}.values.npy", mmap_mode="r")
    valid = np.load(work_dir / "canonical" / f"{dataset}.valid.npy", mmap_mode="r")
    assert list(values.shape) == manifest["shape"] == list(valid.shape)
    arrow = manifest["arrow"]
    path = work_dir / "arrow" / arrow["path"]
    assert sha256(path) == arrow["sha256"]
    checked = 0
    row_index = 0
    batch_counts = []
    sampled = []
    with path.open("rb") as handle:
        reader = ipc.open_stream(handle)
        assert reader.schema == SCHEMA
        for batch_index, batch in enumerate(reader):
            batch_counts.append(len(batch))
            for i in range(len(batch)):
                row = arrow["rows"][row_index]
                assert row["batch"] == batch_index and row["row_in_batch"] == i
                start_step, end_step = row["start_step"], row["end_step"]
                assert int(batch.column("start_step")[i].as_py()) == start_step
                assert int(batch.column("end_step")[i].as_py()) == end_step
                target = batch.column("target")[i]
                assert target.is_valid and len(target.values) == values.shape[1]
                length = end_step - start_step
                channels = sorted(set((0, min(2, values.shape[1] - 1), values.shape[1] - 1)))
                starts = sorted(set((0, min(3, length), max(0, length - 7))))
                for channel in channels:
                    for local_start in starts:
                        local_end = min(length, local_start + 7)
                        actual = _slice(target, channel, local_start, local_end)
                        expected_values = values[start_step + local_start:start_step + local_end, channel]
                        expected_valid = valid[start_step + local_start:start_step + local_end, channel]
                        assert actual is not None
                        assert np.array_equal(actual[0], expected_values), (row_index, channel, local_start)
                        assert np.array_equal(actual[1], expected_valid), (row_index, channel, local_start)
                        checked += 1
                if length >= 11 and values.shape[1] >= 3:
                    projected = [_slice(target, channel, 3, 11) for channel in (2, 0)]
                    assert all(item is not None for item in projected)
                    assert np.array_equal(np.stack([item[0] for item in projected], axis=1), values[start_step + 3:start_step + 11, (2, 0)])
                    assert np.array_equal(np.stack([item[1] for item in projected], axis=1), valid[start_step + 3:start_step + 11, (2, 0)])
                    checked += 1
                if row_index in (0, len(arrow["rows"]) // 2, len(arrow["rows"]) - 1):
                    sampled.append(row_index)
                row_index += 1
    assert batch_counts == arrow["batch_row_counts"] and row_index == len(arrow["rows"])
    # Reopen only the adjacent records around one explicit continuation boundary.
    first, second = arrow["rows"][:2]
    assert second["continuation"] and first["end_step"] == second["start_step"]
    with path.open("rb") as handle:
        rows = [batch.column("target")[i] for batch in ipc.open_stream(handle) for i in range(len(batch))]
    for channel in (0, values.shape[1] - 1):
        tail = min(3, first["end_step"] - first["start_step"])
        head = min(4, second["end_step"] - second["start_step"])
        left = _slice(rows[0], channel, first["end_step"] - first["start_step"] - tail, first["end_step"] - first["start_step"])
        right = _slice(rows[1], channel, 0, head)
        assert left is not None and right is not None
        together = np.concatenate((left[0], right[0]))
        mask = np.concatenate((left[1], right[1]))
        assert np.array_equal(together, values[first["end_step"] - tail:second["start_step"] + head, channel])
        assert np.array_equal(mask, valid[first["end_step"] - tail:second["start_step"] + head, channel])
        checked += 1
    return {"dataset_id": dataset, "windows_checked": checked, "batches": batch_counts, "rows": row_index, "sampled_rows": sampled, "continuation_channels_checked": [0, values.shape[1] - 1]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--dataset-manifest", type=Path, action="append", default=[])
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    result = {"fixtures": verify_fixture(args.fixture_manifest), "datasets": [verify_dataset(path, args.work_dir) for path in args.dataset_manifest]}
    report = args.work_dir / "manifests" / "pb00-window-oracle.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2) + "\n")
    print(report)


if __name__ == "__main__":
    main()
