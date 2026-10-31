#!/usr/bin/env python3
"""CLI acceptance of both formats; outputs are offline preparation evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from common import TARGET, canonical, load_manifest, sha_file, write_json


def run(args):
    home = Path(__file__).resolve().parent
    output, work = args.output.absolute(), args.work.absolute()
    if output.exists() or work.exists():
        raise FileExistsError("acceptance output/work already exists")
    output.mkdir(parents=True)
    work.mkdir(parents=True)
    env = dict(os.environ)
    env["HDF5_PLUGIN_PRELOAD"] = "::"
    steps = []

    def command(label, *arguments):
        started = time.perf_counter_ns()
        result = subprocess.run([sys.executable, str(home / "manager.py"), *map(str, arguments)],
            env=env, capture_output=True, text=True, timeout=180)
        event = {"label": label, "arguments": list(map(str, arguments)),
                 "process_wall_ns": time.perf_counter_ns() - started,
                 "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        steps.append(event)
        write_json(output / (label + ".json"), event)
        if result.returncode:
            raise RuntimeError(f"{label} failed: {result.stderr}")
        print(json.dumps({"stage": label, "status": "passed"}), flush=True)
        return json.loads(result.stdout)

    manifests = []
    for fmt in ("hdf5", "grib"):
        original, grouped, copied = (work / (fmt + suffix) for suffix in ("-original", "-regroup", "-depth8"))
        command(fmt + "-plan", "plan", "--spec", home / "specs" / (fmt + "-smoke.json"))
        command(fmt + "-generate", "generate", "--spec", home / "specs" / (fmt + "-smoke.json"), "--output", original)
        command(fmt + "-verify", "verify", "--input", original)
        command(fmt + "-regroup", "regroup", "--input", original, "--spec", home / "specs" / (fmt + "-regroup-smoke.json"), "--output", grouped)
        command(fmt + "-copy", "reorganize", "--input", grouped, "--depth", 8, "--output", copied)
        command(fmt + "-verify-copy", "verify", "--input", copied)
        parent, merged, copy = (load_manifest(x) for x in (original, grouped, copied))
        if sorted(u["unit_id"] for s in parent["sources"] for u in s["units"]) != sorted(u["unit_id"] for s in merged["sources"] for u in s["units"]):
            raise ValueError("regroup omitted or changed a unit")
        if [s["sha256"] for s in merged["sources"]] != [s["sha256"] for s in copy["sources"]]:
            raise ValueError("depth copy changed bytes")
        for name, manifest in ((fmt + "-original", parent), (fmt + "-regroup", merged), (fmt + "-depth8", copy)):
            write_json(output / (name + "-manifest.json"), manifest)
            manifests.append({"name": name, "actual": manifest["actual"],
                              "tree": manifest["tree"], "raw_bytes": manifest["raw_bytes"]})

    plan = work / "depth-plan"
    collection = work / "depth-collection"
    command("depth-gradient-plan", "gradient", "--base-spec", home / "specs/hdf5-smoke.json",
            "--axis", "depth", "--values", 1, 4, 8, "--output", plan)
    command("depth-gradient-materialize", "materialize", "--plan", plan,
            "--mode", "reorganize", "--input", work / "hdf5-original", "--output", collection)
    collection_data = json.loads((collection / "collection.json").read_text())
    for index, dataset in enumerate(collection_data["datasets"]):
        command(f"depth-gradient-verify-{index}", "verify", "--input", (collection / dataset["manifest"]).parent)
    command("depth-gradient-verify-collection", "verify", "--input", collection)
    write_json(output / "depth-collection.json", collection_data)

    if args.raw_grib:
        query = {**TARGET, "shortName": "clwmr", "validityDate": 20240318}
        write_json(work / "gfs-query.json", query)
        command("public-grib-inventory", "inventory", "--format", "grib", "--source", args.raw_grib,
                "--query-file", work / "gfs-query.json", "--output", work / "gfs-inventory")
        spec = json.loads((home / "specs/grib-regroup-smoke.json").read_text())
        spec.update(N=2, K=1, D=4, query=query)
        write_json(work / "gfs-regroup.json", spec)
        command("public-grib-regroup", "regroup", "--input", work / "gfs-inventory",
                "--spec", work / "gfs-regroup.json", "--output", work / "gfs-regroup")
        command("public-grib-verify", "verify", "--input", work / "gfs-regroup")
        m = load_manifest(work / "gfs-regroup")
        if m["actual"]["K"] != 1 or m["actual"]["K_unit"] != 1:
            raise ValueError("public GRIB selection did not match exactly one unit")
        write_json(output / "public-grib-manifest.json", m)
        manifests.append({"name": "public-grib-regroup", "actual": m["actual"], "raw_bytes": m["raw_bytes"]})

    if args.raw_hdf5:
        import h5py
        from hdf5_data import scalar_attributes
        with h5py.File(args.raw_hdf5, "r") as f:
            query = {"Xstart": scalar_attributes(f["/strain/Strain"])["Xstart"]}
        write_json(work / "gwosc-query.json", query)
        command("public-hdf5-inventory", "inventory", "--format", "hdf5", "--source", args.raw_hdf5,
                "--dataset", "/strain/Strain", "--query-file", work / "gwosc-query.json", "--output", work / "gwosc-inventory")
        command("public-hdf5-copy", "reorganize", "--input", work / "gwosc-inventory",
                "--depth", 4, "--output", work / "gwosc-copy")
        command("public-hdf5-verify", "verify", "--input", work / "gwosc-copy")
        m = load_manifest(work / "gwosc-copy")
        if m["actual"]["K"] != 1 or m["actual"]["K_unit"] != 1:
            raise ValueError("public HDF5 typed attribute selection did not match")
        write_json(output / "public-hdf5-manifest.json", m)
        manifests.append({"name": "public-hdf5-copy", "actual": m["actual"], "raw_bytes": m["raw_bytes"]})

    summary = {"protocol": "iotdbfs-multifile-manager-acceptance/1", "passed": True,
        "cli_operations": len(steps), "datasets": manifests,
        "depth_gradient": [x["value"] for x in collection_data["datasets"]],
        "scope": "functional CLI/native oracle acceptance; not FS/IoTDB-FS query performance",
        "work": str(work), "code_sha256": {name: sha_file(home / name) for name in
            ("manager.py", "common.py", "hdf5_data.py", "grib_data.py", "smoke.py")}}
    write_json(output / "summary.json", summary)
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--raw-grib", type=Path)
    parser.add_argument("--raw-hdf5", type=Path)
    run(parser.parse_args())
