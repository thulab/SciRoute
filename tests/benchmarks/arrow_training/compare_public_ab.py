#!/usr/bin/env python3
"""Check that a local Arrow and IoTDB-FS Trainer smoke used the same samples."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


SHARED_FIELDS = (
    "dataset", "model", "source_sha256", "normalization_artifact_id",
    "window_artifact_id", "channels", "batch_size", "max_steps", "seed",
    "visible_gpus", "samplers", "train_window_population",
    "eval_window_population",
)


def compare(local: dict, iotdbfs: dict) -> dict:
    if local.get("backend") != "local" or iotdbfs.get("backend") != "iotdbfs":
        raise ValueError("expected one local and one IoTDB-FS report")
    differences = [field for field in SHARED_FIELDS
                   if local.get(field) != iotdbfs.get(field)]
    if not isinstance(iotdbfs.get("source_version"), int):
        differences.append("exact IoTDB-FS source version")
    if local.get("visible_gpus") != 1 or iotdbfs.get("visible_gpus") != 1:
        differences.append("single GPU requirement")
    if local.get("samplers") != {"train": "RandomSampler",
                                  "validation": "SequentialSampler"}:
        differences.append("Trainer sampler requirement")
    traces = {}
    for split in ("train", "validation"):
        left = local.get("traces", {}).get(split, {})
        right = iotdbfs.get("traces", {}).get(split, {})
        equal = isinstance(left.get("sample_ids"), list) and \
            isinstance(right.get("sample_ids"), list) and \
            left.get("sample_ids") == right.get("sample_ids") and \
            left.get("sha256") == right.get("sha256") and \
            left.get("count") == right.get("count") == len(left["sample_ids"])
        traces[split] = {"same": equal, "count": left.get("count"),
                         "sha256": left.get("sha256")}
        if not equal:
            differences.append(f"{split} sample trace")
    losses = {}
    for name in ("train_loss", "eval_loss"):
        left, right = local.get(name), iotdbfs.get(name)
        equal = isinstance(left, (int, float)) and isinstance(right, (int, float)) \
            and math.isfinite(left) and math.isfinite(right) \
            and math.isclose(left, right, rel_tol=1e-6, abs_tol=1e-6)
        losses[name] = {"local": left, "iotdbfs": right, "same": equal}
        if not equal:
            differences.append(name)
    return {
        "dataset": local.get("dataset"), "model": local.get("model"),
        "source_version": iotdbfs.get("source_version"),
        "source_sha256": local.get("source_sha256"),
        "normalization_artifact_id": local.get("normalization_artifact_id"),
        "window_artifact_id": local.get("window_artifact_id"),
        "traces": traces, "losses": losses,
        "wall_seconds": {
            stage: {"local": local.get(f"{stage}_wall_seconds"),
                    "iotdbfs": iotdbfs.get(f"{stage}_wall_seconds")}
            for stage in ("train", "eval")
        },
        "passed": not differences, "differences": differences,
        "scope": (f"{local.get('max_steps')}-step Trainer smoke; timings are not "
                  "steady-state throughput."),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", type=Path, required=True)
    parser.add_argument("--iotdbfs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(json.loads(args.local.read_text()),
                     json.loads(args.iotdbfs.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
