#!/usr/bin/env python3
"""PB01 train-only statistics and deterministic public window manifests.

The manifest describes ranges of eligible anchors. It does not materialize
windows, so local Arrow and IoTDB-FS readers can consume exactly the same IDs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Iterator

import numpy as np


PROTOCOL = "iotdbfs-public-windows/1"
ETTM1_TRAIN_END = 12 * 30 * 24 * 4
ETTM1_VAL_END = 16 * 30 * 24 * 4
ETTM1_TEST_END = 20 * 30 * 24 * 4
_CHUNK_STEPS = 4096


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def content_sha256(document: dict) -> str:
    """Hash the JSON content excluding its own artifact_id field."""
    content = {key: value for key, value in document.items() if key != "artifact_id"}
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def seal(document: dict) -> dict:
    result = dict(document)
    result["artifact_id"] = "sha256:" + content_sha256(result)
    return result


def verify_seal(document: dict) -> None:
    if document.get("artifact_id") != "sha256:" + content_sha256(document):
        raise ValueError("artifact content hash mismatch")


def _write_json(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, ensure_ascii=False, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _verify_pb00(work_dir: Path, dataset_id: str) -> tuple[dict, np.ndarray, np.ndarray]:
    manifest_path = work_dir / "manifests" / f"{dataset_id}-pb00.json"
    pb00 = json.loads(manifest_path.read_text(encoding="utf-8"))
    if pb00["dataset_id"] != dataset_id:
        raise ValueError("PB00 dataset identity mismatch")
    for filename, metadata in pb00["canonical"].items():
        if file_sha256(work_dir / "canonical" / filename) != metadata["sha256"]:
            raise ValueError(f"PB00 canonical hash mismatch: {filename}")
    arrow_path = work_dir / "arrow" / pb00["arrow"]["path"]
    if file_sha256(arrow_path) != pb00["arrow"]["sha256"]:
        raise ValueError("PB00 Arrow stream hash mismatch")
    values = np.load(work_dir / "canonical" / f"{dataset_id}.values.npy", mmap_mode="r")
    valid = np.load(work_dir / "canonical" / f"{dataset_id}.valid.npy", mmap_mode="r")
    if values.shape != valid.shape or list(values.shape) != pb00["shape"]:
        raise ValueError("PB00 canonical shape mismatch")
    if values.dtype != np.dtype("float32") or valid.dtype != np.dtype("bool"):
        raise ValueError("PB00 canonical dtype mismatch")
    if len(pb00["channels"]) != values.shape[1] or len(set(pb00["channels"])) != values.shape[1]:
        raise ValueError("PB00 channel identity mismatch")
    return pb00, values, valid


def electricity_split_points(labels: list[str]) -> dict[str, tuple[int, int]]:
    """Split by source calendar-year labels; do not invent a UTC axis."""
    # The pinned source ends with one boundary observation at 2015-01-01 00:00.
    # It is outside the specified 2014 test interval, so leave it unused.
    eligible = labels[:-1] if labels and labels[-1] == "2015-01-01 00:00:00" else labels
    years = []
    for label in eligible:
        if len(label) < 4 or not label[:4].isdigit():
            raise ValueError(f"invalid Electricity source timestamp label: {label!r}")
        years.append(int(label[:4]))
    if any(year not in (2011, 2012, 2013, 2014) for year in years):
        raise ValueError("Electricity source has a year outside 2011–2014")
    if any(left > right for left, right in zip(years, years[1:])):
        raise ValueError("Electricity source year labels are out of order")
    if not years or years[0] != 2011 or years[-1] != 2014:
        raise ValueError("Electricity source must span 2011 through 2014")
    val = next((i for i, year in enumerate(years) if year == 2013), None)
    test = next((i for i, year in enumerate(years) if year == 2014), None)
    if val is None or test is None or val == 0 or test == val:
        raise ValueError("Electricity source has an empty split")
    return {"train": (0, val), "validation": (val, test), "test": (test, len(years))}


def split_points(dataset_id: str, rows: int, timestamp_path: Path) -> tuple[dict[str, tuple[int, int]], dict]:
    if dataset_id == "ETTm1":
        if rows != 69680:
            raise ValueError(f"ETTm1 requires the audited 69,680 rows, got {rows}")
        return (
            {"train": (0, ETTM1_TRAIN_END), "validation": (ETTM1_TRAIN_END, ETTM1_VAL_END), "test": (ETTM1_VAL_END, ETTM1_TEST_END)},
            {"kind": "30-day-index-months", "unused_tail": [ETTM1_TEST_END, rows]},
        )
    if dataset_id == "UCI_Electricity_370":
        labels = timestamp_path.read_text(encoding="utf-8").splitlines()
        if len(labels) != rows:
            raise ValueError("Electricity timestamp label count mismatch")
        points = electricity_split_points(labels)
        return points, {"kind": "source-calendar-year-labels", "year_assignment": {"train": [2011, 2012], "validation": [2013], "test": [2014]}, "unused_tail": [points["test"][1], rows]}
    raise ValueError(f"unsupported public dataset: {dataset_id}")


def training_statistics(values: np.ndarray, valid: np.ndarray, train_end: int, channels: list[str]) -> list[dict]:
    """Population mean/std over valid training observations, using float64."""
    if train_end <= 0 or train_end > values.shape[0] or values.shape != valid.shape:
        raise ValueError("invalid training range or canonical shape")
    if len(channels) != values.shape[1]:
        raise ValueError("channel count mismatch")
    counts = np.zeros(values.shape[1], dtype=np.int64)
    sums = np.zeros(values.shape[1], dtype=np.float64)
    for start in range(0, train_end, _CHUNK_STEPS):
        stop = min(start + _CHUNK_STEPS, train_end)
        observed = valid[start:stop]
        data = np.asarray(values[start:stop], dtype=np.float64)
        counts += np.count_nonzero(observed, axis=0)
        sums += np.sum(data, axis=0, where=observed)
    if np.any(counts == 0):
        raise ValueError("a training channel has no valid observations")
    means = sums / counts
    squared = np.zeros(values.shape[1], dtype=np.float64)
    for start in range(0, train_end, _CHUNK_STEPS):
        stop = min(start + _CHUNK_STEPS, train_end)
        data = np.asarray(values[start:stop], dtype=np.float64)
        delta = data - means
        np.square(delta, out=delta)
        squared += np.sum(delta, axis=0, where=valid[start:stop])
    raw_stddev = np.sqrt(squared / counts)
    return [
        {
            "ordinal": ordinal,
            "name": name,
            "valid_count": int(counts[ordinal]),
            "mean": float(means[ordinal]),
            "stddev": float(raw_stddev[ordinal]) if raw_stddev[ordinal] > 0 else 1.0,
            "constant_channel": bool(raw_stddev[ordinal] == 0),
        }
        for ordinal, name in enumerate(channels)
    ]


def window_splits(points: dict[str, tuple[int, int]], rows: int, input_length: int, prediction_length: int, stride: int) -> dict[str, dict]:
    if input_length <= 0 or prediction_length <= 0 or stride <= 0:
        raise ValueError("window length, horizon and stride must be positive")
    result = {}
    preceding_end = 0
    for name in ("train", "validation", "test"):
        start, end = points[name]
        if start != preceding_end or not (start < end <= rows):
            raise ValueError("split intervals must be ordered, nonempty and contiguous")
        # Validation/test inputs may read earlier history; target is inside split.
        first = max(start, input_length)
        stop = end - prediction_length + 1
        if first >= stop:
            raise ValueError(f"{name} has no eligible window")
        count = (stop - first - 1) // stride + 1
        result[name] = {
            "target_interval": [start, end],
            "first_anchor": first,
            "stop_anchor_exclusive": stop,
            "stride": stride,
            "count": count,
            "input_history_policy": "preceding_split_allowed" if name != "train" else "train_only",
        }
        preceding_end = end
    return result


def iter_windows(manifest: dict, split: str) -> Iterator[dict]:
    verify_seal(manifest)
    item = manifest["splits"][split]
    prefix = manifest["sample_id_prefix"]
    input_length = manifest["input_length"]
    prediction_length = manifest["prediction_length"]
    for anchor in range(item["first_anchor"], item["stop_anchor_exclusive"], item["stride"]):
        yield {
            "sample_id": f"{prefix}/{split}/{anchor}",
            "entity_id": manifest["entity_id"],
            "anchor": anchor,
            "input_range": [anchor - input_length, anchor],
            "target_range": [anchor, anchor + prediction_length],
        }


def prepare_dataset(work_dir: Path, dataset_id: str, input_length: int = 96, prediction_length: int = 24, stride: int = 1) -> tuple[Path, Path]:
    pb00, values, valid = _verify_pb00(work_dir, dataset_id)
    points, split_rule = split_points(dataset_id, values.shape[0], work_dir / "canonical" / f"{dataset_id}.timestamps.txt")
    splits = window_splits(points, values.shape[0], input_length, prediction_length, stride)
    identity = {
        "dataset_id": dataset_id,
        "source_sha256": pb00["source_sha256"],
        "arrow_sha256": pb00["arrow"]["sha256"],
        "canonical_sha256": {name: item["sha256"] for name, item in pb00["canonical"].items()},
        "shape": pb00["shape"],
        "channels": pb00["channels"],
    }
    normalization = seal({
        "protocol": PROTOCOL,
        "kind": "per-channel-zscore",
        "fit_interval": list(points["train"]),
        "validity_policy": "fit_valid_only; invalid_values_keep_mask_and_are_not_transformed",
        "stddev_kind": "population-ddof-0; constant-channel-uses-1",
        **identity,
        "statistics": training_statistics(values, valid, points["train"][1], pb00["channels"]),
    })
    sample_prefix = f"pb01/{dataset_id}/{pb00['source_sha256']}/{normalization['artifact_id']}/L{input_length}-H{prediction_length}-S{stride}"
    windows = seal({
        "protocol": PROTOCOL,
        **identity,
        "entity_id": dataset_id,
        "normalization_artifact_id": normalization["artifact_id"],
        "split_rule": split_rule,
        "input_length": input_length,
        "prediction_length": prediction_length,
        "stride": stride,
        "anchor_semantics": "input=[anchor-L,anchor); target=[anchor,anchor+H); global zero-based step",
        "sample_id_prefix": sample_prefix,
        "splits": splits,
    })
    manifest_dir = work_dir / "manifests"
    normalization_path = manifest_dir / f"{dataset_id}-pb01-normalization.json"
    window_path = manifest_dir / f"{dataset_id}-pb01-windows-L{input_length}-H{prediction_length}-S{stride}.json"
    _write_json(normalization_path, normalization)
    _write_json(window_path, windows)
    return normalization_path, window_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    parser.add_argument("--input-length", type=int, default=96)
    parser.add_argument("--prediction-length", type=int, default=24)
    parser.add_argument("--stride", type=int, default=1)
    args = parser.parse_args()
    normalization, windows = prepare_dataset(args.work_dir, args.dataset, args.input_length, args.prediction_length, args.stride)
    print(normalization)
    print(windows)


if __name__ == "__main__":
    main()
