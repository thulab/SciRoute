#!/usr/bin/env python3
"""Read one frozen, previously published GEOS version without re-ingestion.

This is a dependency-switch compatibility check, not a performance campaign.
The independent h5py oracle and source hashing run outside query timing.
"""
import argparse
import json
import os
from pathlib import Path

import h5py
import numpy as np

from environment import connection_environment
from runner import private_mount, query_iotdb, native_wave, sha_file, save, require
import hashlib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot", required=True, type=Path)
    parser.add_argument("--environment", required=True, type=Path)
    parser.add_argument("--source-pid", required=True, type=int)
    parser.add_argument("--database", required=True)
    parser.add_argument("--writer-lock", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    require(h5py.version.hdf5_version == "1.14.4", "oracle runtime differs")
    pilot = json.loads(args.pilot.read_text())
    require(pilot["complete"] and pilot["database"] == args.database, "frozen pilot differs")
    source = pilot["sources"][0]
    raw = Path(source["raw"])
    require(raw.stat().st_size == source["bytes"] and sha_file(raw) == source["sha256"],
            "frozen source bytes changed")
    origin, shape = [0, 10, 360, 576], [1, 1, 1, 1]
    with h5py.File(raw, "r") as handle:
        dataset = handle["/T"]
        full_shape = list(dataset.shape)
        values = np.asarray(dataset[tuple(slice(o, o + n) for o, n in zip(origin, shape))],
                            dtype="<f4", order="C")
    case = {"origin": origin, "shape": shape, "rows": 1, "dtype": "<f4",
            "value_sha256": hashlib.sha256(values.tobytes()).hexdigest(),
            "mask_sha256": hashlib.sha256(bytes([1])).hexdigest()}
    environment = json.loads(args.environment.read_text())
    for name in ("daemon", "native_probe"):
        require(sha_file(environment[name]) == environment["binaries"][name]["sha256"],
                "verified executable changed")
    env = dict(os.environ, **connection_environment(args.source_pid, args.database))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source_model = {"shape": full_shape}
    native, native_wall = native_wave(Path(environment["native_probe"]), raw, source_model,
                                      case, 1, env)
    with private_mount(Path(environment["daemon"]), output / "reader", env,
                       args.writer_lock, profile=False) as context:
        typed = query_iotdb(context, source["logical_path"], source["version"], source_model, case)
    report = {"protocol": "iotdbfs-single-file-existing-version-check/1", "complete": True,
              "performance_campaign": False, "reingestion": False,
              "hdf5_runtime": "1.14.4", "pilot_manifest_sha256": sha_file(args.pilot),
              "source": source, "full_shape": full_shape, "case": case,
              "oracle_value": float(values.reshape(-1)[0]), "native": native[0],
              "native_process_wall_ns": native_wall, "iotdb": typed}
    save(output / "existing-version.json", report)
    print(json.dumps({"complete": True, "report": str(output / "existing-version.json")}))


if __name__ == "__main__":
    main()
