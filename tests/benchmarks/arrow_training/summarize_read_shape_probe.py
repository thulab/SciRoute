"""Summarize paired, randomized read-shape observations from the live probe."""

import argparse
import csv
import json
import random
import statistics
from pathlib import Path


def percentile(values, fraction):
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    lower = int(rank)
    return ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] -
                             ordered[lower]) * (rank - lower)


def summarize(path):
    rows = list(csv.DictReader(Path(path).open()))
    expected = {(family, 1048576, ranges)
                for family in ("fixed_bytes", "adjacent_fixed_bytes")
                for ranges in (1, 4, 16, 64)}
    expected |= {("fixed_ranges", size, 16)
                 for size in (65536, 262144, 4194304)}
    expected |= {("small_bytes", 65536, ranges) for ranges in (1, 64)}
    cycles = {}
    for row in rows:
        cycle = int(row["cycle"])
        shape = (row["family"], int(row["bytes"]), int(row["ranges"]))
        if shape not in expected or shape in cycles.setdefault(cycle, {}):
            raise ValueError("unexpected or duplicate probe observation")
        cycles[cycle][shape] = int(row["nanoseconds"]) / 1e6
    if not cycles or any(set(group) != expected for group in cycles.values()):
        raise ValueError("incomplete read-shape cycle")
    groups = {shape: [cycles[c][shape] for c in sorted(cycles)] for shape in sorted(expected)}
    summary = [{"family": shape[0], "bytes": shape[1], "ranges": shape[2],
                "mean_ms": statistics.mean(values),
                "median_ms": statistics.median(values),
                "p95_ms": percentile(values, .95)}
               for shape, values in groups.items()]
    comparisons = []
    pairs = [(("fixed_bytes", 1048576, 64), ("fixed_bytes", 1048576, 1)),
             (("small_bytes", 65536, 64), ("small_bytes", 65536, 1)),
             (("fixed_bytes", 1048576, 64), ("small_bytes", 65536, 64)),
             (("fixed_bytes", 1048576, 1), ("small_bytes", 65536, 1)),
             (("adjacent_fixed_bytes", 1048576, 64),
              ("adjacent_fixed_bytes", 1048576, 1)),
             (("fixed_bytes", 1048576, 64),
              ("adjacent_fixed_bytes", 1048576, 64)),
             (("fixed_ranges", 4194304, 16), ("fixed_ranges", 65536, 16)),
             (("fixed_bytes", 1048576, 16), ("fixed_ranges", 65536, 16))]
    generator = random.Random(20260929)
    for large, small in pairs:
        differences = [a - b for a, b in zip(groups[large], groups[small])]
        means = [statistics.mean(generator.choices(differences, k=len(differences)))
                 for _ in range(5000)]
        comparisons.append({"large": {"family": large[0], "bytes": large[1], "ranges": large[2]},
                            "small": {"family": small[0], "bytes": small[1], "ranges": small[2]},
                            "paired_mean_difference_ms": statistics.mean(differences),
                            "bootstrap_95_percent_ci_ms":
                                [percentile(means, .025), percentile(means, .975)]})
    return {"cycles": len(cycles), "observations": len(rows),
            "shapes": summary, "comparisons": comparisons,
            "boundary": "One read_payload_ranges call, including IoTDB query, session acquire, response conversion and close. OS/IoTDB caches remain warm; no training decoder, IPC or FUSE.",
            "inference": "Paired differences isolate one changed dimension within tested shapes; extrapolation to other byte/range sizes is not established."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = summarize(args.csv)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded)
