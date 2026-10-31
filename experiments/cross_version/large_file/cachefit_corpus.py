#!/usr/bin/env python3
"""A second GRIB scale point that fits the unchanged 64 MiB native-field cache."""
import argparse
import json
from pathlib import Path
import time

from experiments.cross_version.large_file import large_corpus as base
from experiments.cross_version.pilot import corpus
from common import TARGET, load_manifest, publication, seal, sha_file, write_json

FORMATS = ("grib",)
SCENARIOS = base.SCENARIOS
PROTOCOL = "iotdbfs-grib-cachefit-data/1"


def resolve(root, manifest, entry):
    return base.resolve(root, manifest, entry)


def load(root):
    return base.load(root, PROTOCOL)


def verify(root):
    return base.verify(root, FORMATS, PROTOCOL)


def prepare(output):
    output = Path(output).resolve()
    started = time.perf_counter_ns()
    with publication(output) as stage:
        spec = {"protocol": "iotdbfs-multifile-spec/1", "format": "grib", "N": 8, "K": 2,
            "D": 4, "depth": 2, "seed": 20261013, "tree_width": 4,
            "payload": {"Ni": 720, "Nj": 721, "bits_per_value": 16, "packing": "grid_simple"},
            "query": TARGET, "max_output_bytes": 128 * base.BLOCK}
        root = stage / "bases/grib-data"
        base.generate_grib(spec, root)
        manifest = load_manifest(root)
        entries = []
        for scenario in SCENARIOS:
            for index, source in enumerate(manifest["sources"]):
                previous = root / source["file"]
                units = corpus.clean_units(source["units"])
                for ordinal in range(1, 9):
                    locator = {"kind": "base", "file": source["file"]}
                    if scenario == "single_unit" and ordinal > 1:
                        relative = f"objects/grib/v{ordinal:03d}/{source['file']}"
                        target = stage / relative
                        target.parent.mkdir(parents=True, exist_ok=True)
                        units = base.mutate(previous, target, "grib", units, ordinal)
                        previous, locator = target, {"kind": "local", "file": relative}
                    entries.append({"format": "grib", "scenario": scenario, "file_index": index,
                        "ordinal": ordinal, "logical_file": source["file"], "blob": locator,
                        "bytes": previous.stat().st_size, "sha256": sha_file(previous), "units": units,
                        "blocks": base.blocks(previous)})
        write_json(stage / "manifest.json", seal({"protocol": PROTOCOL,
            "bases": {"grib": {"root": str(output / "bases/grib-data"), "manifest_sha256": manifest["manifest_sha256"]}},
            "entries": entries, "controls": {k: spec[k] for k in ("N", "K", "D", "depth", "seed", "tree_width", "query")},
            "versions": 8, "block_bytes": base.BLOCK, "generation_ns": time.perf_counter_ns() - started,
            "mutation": "same cumulative one-gridpoint update as large-file case; global longitude spacing 0.5 degree, latitude spacing 0.25 degree",
            "cache_rationale": "16 native Float64 fields and axis arrays fit the unchanged 64 MiB/16-field cache; verify actual counters/times"}))
    return verify(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "verify"))
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps((prepare if args.command == "prepare" else verify)(args.output)), flush=True)
