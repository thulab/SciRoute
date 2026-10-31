#!/usr/bin/env python3
"""Accept the timed native GRIB reader against independent raw-file oracles.

This validates actual Q=1/10/100 observations, not a paired IoTDB-FS campaign.
The optional bound mode must use the separately agreed frozen-session policy.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import numpy as np

import runner as base
from grib_readiness import numerical_oracle


def digest(data):
    return hashlib.sha256(data).hexdigest()


def command(probe, raw, case, q, bound=False):
    args = [str(probe), "--file", str(raw), "--view", case["view"], "--repeat", str(q)]
    for key, option in (("shortName", "--short-name"), ("typeOfLevel", "--type-of-level"),
                        ("level", "--level"), ("step", "--step")):
        if key in case["filter"]:
            args += [option, str(case["filter"][key])]
    if "bounds" in case:
        args += ["--lat-range", ",".join(map(str, case["bounds"][0])),
                 "--lon-range", ",".join(map(str, case["bounds"][1]))]
    if bound:
        args.append("--bound")
    return args


def oracle(raw, inventory, case):
    selected = [r for r in inventory if all(r.get(k) == v for k, v in case["filter"].items())]
    messages, values, latitudes, longitudes, masks = [], [], [], [], []
    with raw.open("rb") as file:
        for row in selected:
            file.seek(row["offset"])
            encoded = file.read(row["totalLength"])
            base.require(len(encoded) == row["totalLength"], "oracle source read failed")
            if case["view"] == "binary":
                val, lat, lon, rows, shape = encoded, b"", b"", 1, []
            else:
                arrays = numerical_oracle(encoded, case["bounds"])
                lat, lon, val = (arrays[0].astype("<f8").tobytes(),
                                 arrays[1].astype("<f8").tobytes(),
                                 arrays[2].astype("<f4").tobytes())
                rows = len(arrays[0])
                shape = [len(np.unique(arrays[0])), len(np.unique(arrays[1]))]
            mask = bytes([1]) * rows
            messages.append({"offset": row["offset"], "length": row["totalLength"],
                             "rows": rows, "shape": shape, "value_sha256": digest(val),
                             "latitude_sha256": digest(lat), "longitude_sha256": digest(lon),
                             "mask_sha256": digest(mask)})
            values.append(val)
            latitudes.append(lat)
            longitudes.append(lon)
            masks.append(mask)
    return {"candidate_messages": len(inventory), "selected_messages": len(selected),
            "rows": sum(m["rows"] for m in messages), "messages": messages,
            "message_offsets": [m["offset"] for m in messages],
            "message_lengths": [m["length"] for m in messages],
            "value_bytes": sum(map(len, values)), "value_sha256": digest(b"".join(values)),
            "latitude_sha256": digest(b"".join(latitudes)),
            "longitude_sha256": digest(b"".join(longitudes)),
            "mask_sha256": digest(b"".join(masks))}


def validate_rows(rows, expected, q, bound, source_bytes):
    base.require(len(rows) == q, "reader did not execute exactly Q queries")
    bind = rows[0]["session_bind_ns"]
    phase_keys = ("open_file_ns", "discovery_ns", "payload_read_ns", "decode_ns",
                  "selection_setup_ns", "owned_copy_ns", "close_ns", "other_ns")
    for index, row in enumerate(rows):
        base.require(row["protocol"] == "iotdbfs-single-file-grib-native/1" and
                     row["iteration"] == index and row["mode"] == ("bound" if bound else "unbound") and
                     row["source_bytes"] == source_bytes, "mode/source/query sequence differs")
        for key, value in expected.items():
            if key == "messages":
                base.require(len(row[key]) == len(value), "message descriptors omitted")
                for actual, wanted in zip(row[key], value):
                    for field, item in wanted.items():
                        base.require(actual[field] == item, "message oracle differs: " + field)
            else:
                base.require(row[key] == value, "native oracle differs: " + key)
        base.require(all(isinstance(row[k], int) and row[k] >= 0 for k in phase_keys),
                     "invalid phase duration")
        base.require(sum(row[k] for k in phase_keys) == row["total_ns"],
                     "timing phases omit or double-count wall time")
        base.require(row["session_bind_ns"] == (bind if index == 0 else 0) and
                     (index + 1 == q or row["session_close_ns"] == 0), "bind/close accounting differs")
        selected_bytes = sum(expected["message_lengths"])
        base.require(row["actual_payload_read_bytes"] == (0 if bound and index else selected_bytes),
                     "payload accounting or bound reuse differs")
        base.require(row["decoded_messages"] ==
                     (expected["selected_messages"] if row["view"] == "values" and not (bound and index) else 0),
                     "decode count differs from actual native work")
        if bound:
            base.require(row["open_file_ns"] == 0 and row["discovery_ns"] == 0,
                         "bound query reopened or rediscovered source")
            base.require(row["cache_hits"] == (expected["selected_messages"] if index else 0) and
                         row["cache_misses"] == (0 if index else expected["selected_messages"]),
                         "bound cache hit/miss accounting differs")
            base.require(row["cache_resident_bytes"] <= row["max_working_bytes"],
                         "resident cache exceeds its budget")
            base.require(sum(row[k] for k in ("session_open_file_ns", "session_discovery_ns",
                                              "session_other_ns")) == row["session_bind_ns"],
                         "binding timing phases differ")
        else:
            base.require(bind == 0 and row["session_close_ns"] == 0,
                         "unbound query charged a session")
    return {"queries": q, "binding_ns": bind, "release_ns": rows[-1]["session_close_ns"],
            "read_sum_ns": sum(r["total_ns"] for r in rows),
            "session_total_ns": bind + sum(r["total_ns"] for r in rows) + rows[-1]["session_close_ns"]}


def main(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    base.require(0 < args.raw.stat().st_size <= 1 << 30, "validation raw input exceeds 1 GiB")
    env = dict(os.environ)
    if args.library_path:
        env["LD_LIBRARY_PATH"] = args.library_path
    versions = json.loads(subprocess.check_output([str(args.probe), "--versions"], env=env, text=True))
    inventory = json.loads(subprocess.check_output([
        "grib_ls", "-x", "-j", "-p", "offset:i,totalLength:i,shortName,typeOfLevel,level:i,step:i",
        str(args.raw)], env=env, text=True, timeout=180))["messages"]
    next_offset = 0
    for r in inventory:
        r["offset"], r["totalLength"] = int(r["offset"]), int(r["totalLength"])
        base.require(r["offset"] == next_offset and r["totalLength"] > 0, "oracle ranges not contiguous")
        next_offset += r["totalLength"]
    base.require(next_offset == args.raw.stat().st_size, "oracle did not cover source")
    picked = [r for r in inventory if r["level"] == 500 and r["step"] == 0]
    base.require(len(picked) == 1, "validation requires exactly one level500/step0 message")
    f = {k: picked[0][k] for k in ("shortName", "typeOfLevel", "level", "step")}
    cases = {"binary_one": {"view": "binary", "filter": f},
             "binary_none": {"view": "binary", "filter": {"level": -999}},
             "binary_all": {"view": "binary", "filter": {}},
             "point": {"view": "values", "filter": f, "bounds": ((0., 0.), (0., 0.))},
             "roi": {"view": "values", "filter": f, "bounds": ((0., .75), (0., .75))},
             "values_many": {"view": "values", "filter": {"shortName": f["shortName"], "step": 0},
                             "bounds": ((0., .75), (0., .75))},
             "spatial_none": {"view": "values", "filter": f, "bounds": ((100., 101.), (0., .75))}}
    waves = []
    for name, case in cases.items():
        expected = oracle(args.raw, inventory, case)
        # All-message binary is correctness-only to avoid producing unbounded
        # multi-gigabyte result/hash work in a reader acceptance run.
        for q in ([1] if name == "binary_all" else [2] if name in ("values_many", "spatial_none")
                  else [1, 10, 100]):
            for bound in ([False, True] if args.bound else [False]):
                start = time.perf_counter_ns()
                completed = subprocess.run(command(args.probe, args.raw, case, q, bound), env=env,
                                           capture_output=True, text=True, timeout=300)
                wall_ns = time.perf_counter_ns() - start
                base.require(completed.returncode == 0, "native reader failed: " + completed.stderr[-400:])
                rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
                summary = validate_rows(rows, expected, q, bound, args.raw.stat().st_size)
                waves.append({"case": name, "mode": "bound" if bound else "unbound", "q": q,
                              "summary": summary, "process_wall_ns": wall_ns, "observations": rows})

    # Failure cases must exit nonzero rather than treating corruption as EOF or
    # silently returning an empty successful batch.
    bad = output / "truncated.grib"
    with args.raw.open("rb") as stream:
        bad.write_bytes(stream.read(inventory[0]["totalLength"] - 1))
    negatives = [command(args.probe, bad, cases["binary_one"], 1),
                 command(args.probe, args.raw, {"view": "values", "filter": f,
                         "bounds": ((float("nan"), 0.), (0., 0.))}, 1),
                 command(args.probe, args.raw, {"view": "values", "filter": f,
                         "bounds": ((1., 0.), (0., 0.))}, 1)]
    negatives.extend([command(args.probe, args.raw, cases["binary_one"], 1) + ["--max-result-bytes", "1"],
                      command(args.probe, args.raw, cases["point"], 1) + ["--max-working-bytes", "1"],
                      command(args.probe, args.raw, cases["binary_one"], 1) + ["--max-candidates", "1"]])
    if args.bound:
        # A budget sufficient for one selected field must still constrain the
        # accumulated caches of a multi-message selection.
        negatives.append(command(args.probe, args.raw, cases["values_many"], 1, True) +
                         ["--max-working-bytes", "12582912"])
    failures = []
    for cmd in negatives:
        completed = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
        base.require(completed.returncode != 0 and not completed.stdout.strip(), "invalid input succeeded")
        failures.append({"arguments": cmd[1:], "exit_code": completed.returncode})
    report = {"protocol": "iotdbfs-grib-native-reader-acceptance/1", "ok": True,
              "scope": "native reader acceptance, not a paired FS/IoTDB-FS performance experiment",
              "raw": {"path": str(args.raw), "bytes": args.raw.stat().st_size,
                      "sha256": base.sha_file(args.raw), "messages": len(inventory)},
              "probe": {"path": str(args.probe), "sha256": base.sha_file(args.probe), "versions": versions},
              "waves": waves, "negative_cases": failures,
              "queries": sum(len(w["observations"]) for w in waves)}
    base.save(output / "report.json", report)
    print(json.dumps({"ok": True, "queries": report["queries"], "waves": len(waves),
                      "report": str(output / "report.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--library-path")
    parser.add_argument("--bound", action="store_true")
    main(parser.parse_args())
