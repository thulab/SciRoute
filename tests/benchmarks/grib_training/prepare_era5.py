#!/usr/bin/env python3
"""Freeze D0/D1 regional sample coordinates and probe the local GRIB reader."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from local_reader import LocalGribReader
from sample_contract import Catalog, VIEWS


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tier", choices=("d0", "d1"), required=True)
    parser.add_argument("--single-manifest", type=Path, required=True)
    parser.add_argument("--pressure-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe-output", type=Path)
    args = parser.parse_args()

    catalog = Catalog.from_paths(
        args.single_manifest, args.pressure_manifest, args.tier)
    for source in catalog.sources:
        actual = file_sha256(catalog.paths[source["kind"]])
        if actual != source["sha256"]:
            raise ValueError(f"{source['kind']} file SHA-256 differs from manifest")
    contract = catalog.freeze()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(contract, indent=2) + "\n")
    summary = {
        "contract": str(args.output),
        "tier": args.tier,
        "snapshot_fingerprint": catalog.fingerprint,
        "utc_time_points": len(catalog.times),
        "counts": {view: spec["counts"] for view, spec in contract["views"].items()},
    }
    if args.probe_output:
        reader = LocalGribReader(catalog)
        sample_row = contract["views"]["V0"]["samples"][0]
        sample = reader.sample(sample_row, VIEWS["V0"]["channels"])
        probe = {
            "protocol": "iotdbfs-era5-local-probe/1",
            "sample": sample_row,
            "x_shape": list(sample.x.shape),
            "y_shape": list(sample.y.shape),
            "dtype": str(sample.x.dtype),
            "digest": sample.digest(),
            "x_min": float(sample.x.min()), "x_max": float(sample.x.max()),
            "y_min": float(sample.y.min()), "y_max": float(sample.y.max()),
            "decoded_messages": reader.decoded_messages,
            "cache_bytes": reader.cache_bytes,
        }
        args.probe_output.parent.mkdir(parents=True, exist_ok=True)
        args.probe_output.write_text(json.dumps(probe, indent=2) + "\n")
        summary["probe"] = str(args.probe_output)
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
