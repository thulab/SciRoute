#!/usr/bin/env python3
"""Freeze all 16 complete GEOS originals before publishing them to IoTDB-FS."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from geos_time_directory import native_utc_point


BASE = ("https://gmao.gsfc.nasa.gov/media/gmaoftp/ops/"
        "GEOS572_Sample_Data/inst3_3d_asm_Np/")


def sha256(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--acquisition", type=Path, required=True)
    parser.add_argument("--existing-index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    acquired = json.loads(args.acquisition.read_text())["sources"]
    existing = json.loads(args.existing_index.read_text())["entries"]
    expected = {row["timestamp"]: (row["bytes"], row["sha256"])
                for row in acquired}
    for row in existing:
        timestamp = datetime.fromtimestamp(row["utc_second"], timezone.utc)
        key = timestamp.strftime("%Y%m%d_%H%M")
        if key in expected:
            raise ValueError("duplicate time in existing and acquired sources")
        expected[key] = row["size"], row["sha256"]
    if len(expected) != 16:
        raise ValueError("requires 16 distinct frozen GEOS originals")
    sources = []
    for timestamp, (size, digest) in sorted(expected.items()):
        name = f"DAS.ops.asm.inst3_3d_asm_Np.GEOS572.{timestamp}.V01.nc4"
        path = args.raw_dir / name
        if path.stat().st_size != size or sha256(path) != digest:
            raise ValueError(f"GEOS source size or SHA-256 mismatch: {path}")
        utc_second, units, calendar = native_utc_point(path)
        if datetime.fromtimestamp(utc_second, timezone.utc).strftime(
                "%Y%m%d_%H%M") != timestamp:
            raise ValueError(f"GEOS time coordinate disagrees with frozen source: {path}")
        sources.append({"timestamp": timestamp, "raw": str(path.resolve()),
                        "url": BASE + name, "bytes": size, "sha256": digest,
                        "utc_second": utc_second, "native_units": units,
                        "calendar": calendar})
    points = [source["utc_second"] for source in sources]
    if any(b - a != 10800 for a, b in zip(points, points[1:])):
        raise ValueError("GEOS source times are not contiguous three-hour steps")
    report = {"protocol": "iotdbfs-geos-scale-sources/1", "complete": True,
              "source_count": len(sources),
              "total_bytes": sum(source["bytes"] for source in sources),
              "sources": sources}
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"source_count": len(sources),
                      "total_bytes": report["total_bytes"],
                      "output": str(args.output)}))


if __name__ == "__main__":
    main()
