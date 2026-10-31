#!/usr/bin/env python3
"""Exhaustive PB00/PB01 data check for the two pinned public Arrow streams."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow.ipc as ipc

from prepare_public_data import (
    ELECTRICITY_TXT_SHA256,
    ELECTRICITY_ZIP_SHA256,
    ETTM1_SHA256,
    SCHEMA,
    sha256,
)
from verify_public_data import verify_fixture


PINNED = {
    "ETTm1": ("ETTm1.csv", ETTM1_SHA256),
    "UCI_Electricity_370": ("LD2011_2014.txt", ELECTRICITY_TXT_SHA256),
}


def require_hash(path: Path, expected: str) -> None:
    actual = sha256(path)
    if actual != expected:
        raise AssertionError(f"{path}: SHA-256 {actual} != {expected}")


def check_dataset(work_dir: Path, dataset: str) -> dict:
    manifest_path = work_dir / "manifests" / f"{dataset}-pb00.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_name, source_hash = PINNED[dataset]
    assert manifest["dataset_id"] == dataset
    assert manifest["source_sha256"] == source_hash
    require_hash(work_dir / "raw" / source_name, source_hash)
    if dataset == "UCI_Electricity_370":
        assert manifest["source_archive"]["sha256"] == ELECTRICITY_ZIP_SHA256
        require_hash(work_dir / "raw" / "LD2011_2014.txt.zip", ELECTRICITY_ZIP_SHA256)

    for name, info in manifest["canonical"].items():
        path = work_dir / "canonical" / name
        assert path.stat().st_size == info["bytes"]
        require_hash(path, info["sha256"])
    values = np.load(work_dir / "canonical" / f"{dataset}.values.npy", mmap_mode="r")
    valid = np.load(work_dir / "canonical" / f"{dataset}.valid.npy", mmap_mode="r")
    assert list(values.shape) == list(valid.shape) == manifest["shape"]
    assert values.dtype == np.dtype("float32") and valid.dtype == np.dtype("bool")
    assert len(manifest["channels"]) == values.shape[1]

    arrow_info = manifest["arrow"]
    arrow_path = work_dir / "arrow" / arrow_info["path"]
    assert arrow_path.stat().st_size == arrow_info["bytes"]
    require_hash(arrow_path, arrow_info["sha256"])
    expected_rows = arrow_info["rows"]
    assert sum(arrow_info["batch_row_counts"]) == len(expected_rows)
    rows_checked = 0
    cells_checked = 0
    boundary_windows_checked = 0
    prior_tail = None
    selected = tuple(sorted({0, values.shape[1] // 2, values.shape[1] - 1}))
    with arrow_path.open("rb") as handle:
        reader = ipc.open_stream(handle)
        assert reader.schema == SCHEMA
        batch_counts = []
        for batch_index, batch in enumerate(reader):
            batch_counts.append(len(batch))
            for row_index in range(len(batch)):
                row = expected_rows[rows_checked]
                start, end = row["start_step"], row["end_step"]
                assert row["batch"] == batch_index and row["row_in_batch"] == row_index
                assert row["segment_index"] == rows_checked
                assert start == (0 if rows_checked == 0 else expected_rows[rows_checked - 1]["end_step"])
                assert 0 < end - start <= arrow_info["segment_steps"]
                assert row["series_id"] == dataset and row["channels"] == values.shape[1]
                assert row["continuation"] == (rows_checked > 0)
                for name in ("dataset_id", "entity_id", "series_id"):
                    assert batch.column(name)[row_index].as_py() == dataset
                assert batch.column("segment_index")[row_index].as_py() == rows_checked
                assert batch.column("start_step")[row_index].as_py() == start
                assert batch.column("end_step")[row_index].as_py() == end

                target = batch.column("target")[row_index]
                assert target.is_valid and len(target.values) == values.shape[1]
                current = {}
                for channel in range(values.shape[1]):
                    child = target.values[channel]
                    assert child.is_valid and len(child.values) == end - start
                    leaf = child.values
                    leaf_valid = leaf.is_valid().to_numpy(zero_copy_only=False)
                    leaf_values = leaf.fill_null(0).to_numpy(zero_copy_only=False)
                    np.testing.assert_array_equal(leaf_valid, valid[start:end, channel])
                    np.testing.assert_array_equal(leaf_values, values[start:end, channel])
                    cells_checked += end - start
                    if channel in selected:
                        current[channel] = (leaf_values, leaf_valid)

                if prior_tail is not None:
                    # A 96-history/24-target sample anchored 12 steps before the
                    # segment boundary must stitch 108 left and 12 right steps.
                    assert start >= 108 and end - start >= 12
                    for channel in selected:
                        left_values, left_valid = prior_tail[channel]
                        right_values, right_valid = current[channel]
                        np.testing.assert_array_equal(
                            np.concatenate((left_values[-108:], right_values[:12])),
                            values[start - 108:start + 12, channel],
                        )
                        np.testing.assert_array_equal(
                            np.concatenate((left_valid[-108:], right_valid[:12])),
                            valid[start - 108:start + 12, channel],
                        )
                        boundary_windows_checked += 1
                prior_tail = current
                rows_checked += 1
        assert batch_counts == arrow_info["batch_row_counts"]
    assert rows_checked == len(expected_rows) and expected_rows[-1]["end_step"] == values.shape[0]
    return {
        "dataset": dataset,
        "source_sha256": source_hash,
        "arrow_sha256": arrow_info["sha256"],
        "shape": list(values.shape),
        "batches_checked": len(batch_counts),
        "segments_checked": rows_checked,
        "cells_checked": cells_checked,
        "boundary_windows_checked": boundary_windows_checked,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = {
        "scope": "PB00 source/fixture integrity and PB01 full stream/NumPy equivalence plus intra-stream segment boundaries",
        "fixtures": verify_fixture(args.work_dir / "arrow" / "fixtures" / "fixture-manifest.json"),
        "datasets": [check_dataset(args.work_dir, dataset) for dataset in PINNED],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
