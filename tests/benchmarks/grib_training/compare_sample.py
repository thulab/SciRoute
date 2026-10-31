#!/usr/bin/env python3
"""Compare one frozen ERA5 sample from raw GRIB and exact IoTDB-FS versions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import numpy as np

from iotdb_reader import IoTDBGribReader
from local_reader import LocalGribReader
from sample_contract import Catalog, VIEWS


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", choices=("d0", "d1"), default="d0")
    parser.add_argument("--single-manifest", type=Path, required=True)
    parser.add_argument("--pressure-manifest", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--bindings", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--view", choices=tuple(VIEWS), default="V0")
    parser.add_argument("--sample-index", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    catalog = Catalog.from_paths(
        args.single_manifest, args.pressure_manifest, args.tier)
    contract = json.loads(args.contract.read_text())
    if contract["snapshot_fingerprint"] != catalog.fingerprint:
        raise ValueError("frozen contract differs from current source identity")
    row = contract["views"][args.view]["samples"][args.sample_index]
    if row != catalog.sample_rows(args.view)[args.sample_index]:
        raise ValueError("frozen sample coordinates or ID were modified")
    ordinals = VIEWS[args.view]["channels"]
    bindings = json.loads(args.bindings.read_text())
    local_reader = LocalGribReader(catalog)
    query_reader = IoTDBGribReader(catalog, bindings["sources"], args.socket)

    start = time.perf_counter_ns()
    local = local_reader.sample(row, ordinals)
    local_ns = time.perf_counter_ns() - start
    start = time.perf_counter_ns()
    remote = query_reader.sample(row, ordinals)
    remote_ns = time.perf_counter_ns() - start
    equal_x = np.array_equal(local.x, remote.x)
    equal_y = np.array_equal(local.y, remote.y)
    equal_masks = (np.array_equal(local.x_mask, remote.x_mask)
                   and np.array_equal(local.y_mask, remote.y_mask))
    report = {
        "protocol": "iotdbfs-era5-sample-compare/1",
        "tier": args.tier,
        "sample_id": row["sample_id"],
        "snapshot_fingerprint": catalog.fingerprint,
        "view_id": args.view,
        "split": row["split"],
        "init_time": row["init_time"],
        "input_times": row["input_times"],
        "target_time": row["target_time"],
        "patch_id": row["patch_id"],
        "shape_x": list(local.x.shape),
        "shape_y": list(local.y.shape),
        "dtype": str(local.x.dtype),
        "sources": bindings["sources"],
        "local_digest": local.digest(),
        "iotdb_digest": remote.digest(),
        "equal_x": bool(equal_x), "equal_y": bool(equal_y),
        "equal_masks": bool(equal_masks),
        "max_abs_error": float(max(
            np.max(np.abs(local.x.astype(np.float64) - remote.x)),
            np.max(np.abs(local.y.astype(np.float64) - remote.y)))),
        "local_decoded_messages": local_reader.decoded_messages,
        "typed_queries": query_reader.query_count,
        "ipc_bytes": query_reader.ipc_bytes,
        "local_ns": local_ns, "iotdb_ns": remote_ns,
        "typed_query_ns": query_reader.query_ns,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True))
    if not (equal_x and equal_y and equal_masks):
        raise SystemExit("ERA5 sample values or masks differ")


if __name__ == "__main__":
    main()
