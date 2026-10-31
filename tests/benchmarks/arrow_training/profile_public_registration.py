#!/usr/bin/env python3
"""Measure public window registration without conflating it with FUSE read."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import time

from public_iotdbfs import IoTDBFSWindowReader, register, source_version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--dataset", choices=("ETTm1", "UCI_Electricity_370"), required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--anchors", type=int, nargs="+")
    parser.add_argument("--channel-counts", type=int, nargs="+")
    parser.add_argument("--valid-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10:
        parser.error("--repeats must be in [1,10]")
    manifests = args.work_dir / "manifests"
    pb00 = json.loads((manifests / f"{args.dataset}-pb00.json").read_text())
    norm = json.loads((manifests / f"{args.dataset}-pb01-normalization.json").read_text())
    windows = json.loads((manifests / f"{args.dataset}-pb01-windows-L96-H24-S1.json").read_text())
    version = source_version(args.socket, "/" + pb00["arrow"]["path"])
    width = len(pb00["channels"])
    counts = sorted(set(args.channel_counts or (1, min(7, width), width)))
    if any(count < 1 or count > width for count in counts):
        parser.error(f"channel counts must be in [1,{width}]")
    anchors = args.anchors or [windows["splits"]["train"]["first_anchor"]]
    rows = []
    for count in counts:
        reader = IoTDBFSWindowReader(
            socket_path=args.socket, mount=args.mount, pb00=pb00,
            normalization=norm, windows=windows, split="train",
            channels=tuple(range(count)), source_version_exact=version)
        for valid in ((True,) if args.valid_only else (False, True)):
            request = {"kind": "window", "snapshot": reader.snapshot,
                       "view": reader.view,
                       "request": {"coordinates": [{
                           "series_id": pb00["dataset_id"] if valid else "missing-series",
                           "anchor": anchor} for anchor in anchors],
                           "max_windows": len(anchors)}}
            encoded = json.dumps(request, separators=(",", ":"), ensure_ascii=False)
            samples = []
            paths = []
            error_message = None
            for _ in range(args.repeats):
                route = None
                start = time.perf_counter()
                try:
                    route = register(args.socket, request)
                    outcome = "ok"
                    if not valid:
                        raise AssertionError("missing series unexpectedly registered")
                    if not route.is_relative_to(args.mount):
                        raise AssertionError("route escaped the expected mount")
                except RuntimeError as error:
                    outcome = "rejected"
                    if valid:
                        raise
                    error_message = str(error)
                samples.append(time.perf_counter() - start)
                paths.append(str(route) if route is not None else None)
            rows.append({"channels": count, "valid_coordinate": valid,
                         "outcome": outcome, "request_json_bytes": len(encoded.encode()),
                         "seconds": samples, "paths": paths, "median_seconds": statistics.median(samples),
                         "error": error_message})
    report = {"dataset": args.dataset, "source_version": version,
              "anchors": anchors,
              "declared_rows": len(pb00["arrow"]["rows"]),
              "declared_channels": width,
              "declared_spans": len(pb00["arrow"]["rows"]) * width,
              "repeats": args.repeats, "cases": rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
