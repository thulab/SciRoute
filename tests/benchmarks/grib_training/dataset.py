"""Frozen D1 traces, train-only ERA5 statistics, and common F1/Q2 tensors."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Protocol

import numpy as np

from sample_contract import (CHANNELS, OFFSETS, PATCH, PROTOCOL, REGIONS,
                             SPLITS, VIEWS, Catalog, digest, parse_iso)


STATS_PROTOCOL = "iotdbfs-era5-regional-statistics/1"
TRACE_PROTOCOL = "iotdbfs-era5-regional-trace/1"


class SampleReader(Protocol):
    def sample(self, row: dict, channel_ordinals: list[int]): ...


def _artifact_id(payload: dict) -> str:
    return digest({key: value for key, value in payload.items()
                   if key not in ("artifact_id", "trace_id")})


def fit_training_statistics(catalog: Catalog, region: str, reader: object) -> dict:
    """Fit all 20 channels over train times and each spatial point exactly once.

    The 16 overlapping patches in a region cover one 176×176 rectangular union.
    This requires F1's indexed field decoder; the artifact is subsequently shared
    unchanged by every backend and by channel-subset views of the same region.
    """
    if catalog.tier != "d1":
        raise ValueError("training statistics require the D1 catalog")
    if region not in REGIONS:
        raise ValueError(f"unknown region {region}")
    if not hasattr(reader, "_decode") or getattr(reader, "catalog", None) is not catalog:
        raise ValueError("statistics must be fitted by the indexed F1 reader")
    row, col = REGIONS[region]
    side = PATCH + max(OFFSETS)
    start, end = (parse_iso(value) for value in SPLITS["train"])
    train_times = [when for when in catalog.times if start <= when < end]
    if len(train_times) != 84:
        raise ValueError("D1 train split must contain 84 six-hour time points")
    counts = np.zeros(len(CHANNELS), dtype=np.int64)
    means = np.zeros(len(CHANNELS), dtype=np.float64)
    m2 = np.zeros(len(CHANNELS), dtype=np.float64)
    for when in train_times:
        for channel in CHANNELS:
            field = reader._decode(when, channel.ordinal)
            values = np.asarray(field[row:row + side, col:col + side],
                                dtype=np.float64)
            if values.shape != (side, side) or not np.isfinite(values).all():
                raise ValueError(f"invalid train field {channel.name} at {when}")
            flat = values.reshape(-1)
            block_count = flat.size
            block_mean = float(np.mean(flat, dtype=np.float64))
            block_m2 = float(np.sum((flat - block_mean) ** 2,
                                    dtype=np.float64))
            old_count = int(counts[channel.ordinal])
            new_count = old_count + block_count
            delta = block_mean - means[channel.ordinal]
            means[channel.ordinal] += delta * block_count / new_count
            m2[channel.ordinal] += (block_m2 + delta * delta
                                    * old_count * block_count / new_count)
            counts[channel.ordinal] = new_count
    stddev = np.sqrt(m2 / counts)
    if (np.any(counts != len(train_times) * side * side)
            or not np.isfinite(means).all()
            or not np.isfinite(stddev).all()
            or np.any(stddev <= 0)):
        raise ValueError("invalid or zero-variance training statistics")
    payload = {
        "protocol": STATS_PROTOCOL,
        "snapshot_fingerprint": catalog.fingerprint,
        "region": region,
        "fit_split": "train",
        "fit_time_start": SPLITS["train"][0],
        "fit_time_end_exclusive": SPLITS["train"][1],
        "fit_time_count": len(train_times),
        "fit_domain": {"row": row, "col": col, "height": side, "width": side,
                       "unique_grid_points_per_time": side * side},
        "algorithm": "float32-raw-to-float64-block-welford-population-std/1",
        "channels": [channel.ordinal for channel in CHANNELS],
        "count": [int(value) for value in counts],
        "mean": [float(value) for value in means],
        "stddev": [float(value) for value in stddev],
    }
    payload["artifact_id"] = _artifact_id(payload)
    return payload


def save_statistics(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_statistics(path: Path, catalog: Catalog, region: str) -> dict:
    payload = json.loads(path.read_text())
    if (payload.get("protocol") != STATS_PROTOCOL
            or payload.get("artifact_id") != _artifact_id(payload)
            or payload.get("snapshot_fingerprint") != catalog.fingerprint
            or payload.get("region") != region
            or payload.get("fit_split") != "train"
            or payload.get("fit_time_start") != SPLITS["train"][0]
            or payload.get("fit_time_end_exclusive") != SPLITS["train"][1]
            or payload.get("fit_time_count") != 84
            or payload.get("algorithm")
            != "float32-raw-to-float64-block-welford-population-std/1"
            or payload.get("channels") != [c.ordinal for c in CHANNELS]):
        raise ValueError("statistics artifact does not match the D1 train domain")
    row, col = REGIONS[region]
    side = PATCH + max(OFFSETS)
    if payload.get("fit_domain") != {
        "row": row, "col": col, "height": side, "width": side,
        "unique_grid_points_per_time": side * side,
    }:
        raise ValueError("statistics fit-domain mismatch")
    if payload.get("count") != [84 * side * side] * len(CHANNELS):
        raise ValueError("statistics population count mismatch")
    means = np.asarray(payload.get("mean"), dtype=np.float64)
    stddev = np.asarray(payload.get("stddev"), dtype=np.float64)
    if (means.shape != (len(CHANNELS),) or stddev.shape != means.shape
            or not np.isfinite(means).all() or not np.isfinite(stddev).all()
            or np.any(stddev <= 0)):
        raise ValueError("statistics contain nonfinite or zero-variance values")
    return payload


def _training_rows(contract: dict, view_id: str) -> list[dict]:
    if (contract.get("protocol") != PROTOCOL
            or contract.get("tier") != "d1"
            or view_id not in VIEWS
            or view_id not in contract.get("views", {})
            or contract["views"][view_id].get("spec") != VIEWS[view_id]):
        raise ValueError("a frozen D1 view is required")
    return [row for row in contract["views"][view_id]["samples"]
            if row["split"] == "train"]


def make_trace(contract: dict, view_id: str, seed: int,
               batch_size: int, steps: int) -> dict:
    """Use PyTorch's no-replacement RandomSampler order, independent of model RNG."""
    import torch

    if batch_size <= 0 or steps <= 0:
        raise ValueError("batch size and steps must be positive")
    rows = _training_rows(contract, view_id)
    needed = batch_size * steps
    if needed > len(rows):
        raise ValueError("trace would cross a no-replacement train epoch")
    generator = torch.Generator().manual_seed(seed)
    indices = torch.randperm(len(rows), generator=generator)[:needed].tolist()
    payload = {
        "protocol": TRACE_PROTOCOL,
        "snapshot_fingerprint": contract["snapshot_fingerprint"],
        "view_id": view_id,
        "view_fingerprint": rows[0]["view_fingerprint"],
        "split": "train",
        "seed": seed,
        "batch_size": batch_size,
        "steps": steps,
        "sample_ids": [rows[index]["sample_id"] for index in indices],
    }
    payload["trace_id"] = _artifact_id(payload)
    return payload


def load_trace(path: Path, contract: dict, view_id: str,
               batch_size: int, steps: int) -> dict:
    trace = json.loads(path.read_text())
    rows = _training_rows(contract, view_id)
    legal = {row["sample_id"] for row in rows}
    if (trace.get("protocol") != TRACE_PROTOCOL
            or trace.get("trace_id") != _artifact_id(trace)
            or trace.get("snapshot_fingerprint") != contract["snapshot_fingerprint"]
            or trace.get("view_id") != view_id
            or trace.get("view_fingerprint") != rows[0]["view_fingerprint"]
            or trace.get("split") != "train"
            or trace.get("batch_size") != batch_size
            or trace.get("steps") != steps
            or len(trace.get("sample_ids", [])) != batch_size * steps
            or len(set(trace["sample_ids"])) != batch_size * steps
            or not set(trace["sample_ids"]).issubset(legal)):
        raise ValueError("trace is not a no-replacement subset of this frozen D1 view")
    return trace


def save_trace(path: Path, trace: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(trace, indent=2, sort_keys=True) + "\n")


@dataclass
class TensorSample:
    sample_id: str
    x: np.ndarray
    y: np.ndarray
    x_mask: np.ndarray
    y_mask: np.ndarray
    row: int
    col: int
    fetch_ns: int
    transform_ns: int
    reader_profile_delta: dict[str, int | float]


class FrozenERA5Dataset:
    """A trace-ordered, single-worker Dataset shared by all storage backends."""

    def __init__(self, contract: dict, view_id: str, trace: dict,
                 statistics: dict, reader: SampleReader):
        if contract.get("tier") != "d1":
            raise ValueError("training requires a D1 contract")
        view = contract["views"][view_id]
        if statistics["region"] != view["spec"]["region"]:
            raise ValueError("view and statistics region differ")
        if trace["snapshot_fingerprint"] != contract["snapshot_fingerprint"]:
            raise ValueError("trace and contract snapshot differ")
        by_id = {row["sample_id"]: row for row in _training_rows(contract, view_id)}
        self.rows = [by_id[sample_id] for sample_id in trace["sample_ids"]]
        self.channel_ordinals = list(view["spec"]["channels"])
        self.reader = reader
        self.mean = np.asarray(statistics["mean"], dtype=np.float64)[
            self.channel_ordinals][None, :, None, None]
        self.stddev = np.asarray(statistics["stddev"], dtype=np.float64)[
            self.channel_ordinals][None, :, None, None]

    def __len__(self) -> int:
        return len(self.rows)

    @staticmethod
    def _profile(reader: object) -> dict:
        if hasattr(reader, "profile"):
            return dict(reader.profile())
        if hasattr(reader, "metrics"):
            return dict(reader.metrics())
        return {name: int(getattr(reader, name)) for name in
                ("query_ns", "query_count", "ipc_bytes", "decoded_messages")
                if hasattr(reader, name)}

    def __getitem__(self, index: int) -> TensorSample:
        row = self.rows[index]
        before = self._profile(self.reader)
        begin = time.perf_counter_ns()
        raw = self.reader.sample(row, self.channel_ordinals)
        fetch_ns = time.perf_counter_ns() - begin
        after = self._profile(self.reader)
        delta = {key: after[key] - before[key] for key in before.keys() & after.keys()
                 if isinstance(before[key], (int, float))
                 and isinstance(after[key], (int, float))}
        if raw.sample_id != row["sample_id"]:
            raise ValueError("reader returned a different sample ID")
        channels = len(self.channel_ordinals)
        if (raw.x.shape != (2, channels, PATCH, PATCH)
                or raw.y.shape != (1, channels, PATCH, PATCH)
                or raw.x_mask.shape != raw.x.shape
                or raw.y_mask.shape != raw.y.shape):
            raise ValueError("reader returned an unexpected sample shape")
        started = time.perf_counter_ns()
        x_mask = np.asarray(raw.x_mask, dtype=np.bool_)
        y_mask = np.asarray(raw.y_mask, dtype=np.bool_)
        if (not x_mask.all() or not y_mask.all()
                or not np.isfinite(raw.x).all() or not np.isfinite(raw.y).all()):
            raise ValueError("D1 first-round training requires complete finite windows")
        x = ((raw.x.astype(np.float64) - self.mean) / self.stddev).astype(np.float32)
        y = ((raw.y.astype(np.float64) - self.mean) / self.stddev).astype(np.float32)
        if not np.isfinite(x).all() or not np.isfinite(y).all():
            raise ValueError("standardization produced a nonfinite value")
        transform_ns = time.perf_counter_ns() - started
        return TensorSample(row["sample_id"], x, y, x_mask, y_mask,
                            int(row["row"]), int(row["col"]),
                            fetch_ns, transform_ns, delta)
