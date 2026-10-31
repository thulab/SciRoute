#!/usr/bin/env python3
"""Pair old/new daemons on existing exact GRIB versions; no source publication.

Only BinaryView changes. Every observation registers/materializes a fresh IPC
result, then consumes an owned copy. Setup/release and reads remain separate.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import statistics

import grib_paired_runner as paired
from environment import connection_environment


def verify(profile, case, index, bound, cached):
    io, counters = profile["iotdb_delta"], profile["counters"]
    paired.base.require(profile.get("ok") is True and
        counters.get("typed_cursor_next_calls", 0) > 0 and
        counters.get("typed_ipc_batches", 0) > 0 and
        counters.get("typed_ipc_rows") == 1, "query/IPC was not executed")
    if bound:
        paired.base.require(counters.get("typed_prepared_reuse") == 1 and
            io["metadata_query_calls"] == io["layout_calls"] == 0 and
            counters.get("typed_prepare_calls", 0) == 0 and
            counters.get("registration_catalog_calls", 0) == 0, "bound read repeats planning")
    length = sum(case["oracle"]["message_lengths"])
    expected = length if not cached or index == 0 else 0
    paired.base.require(io["payload_requested_bytes"] == io["payload_returned_bytes"] == expected and
                        io["payload_calls"] == int(expected > 0), "payload miss/hit contract differs")
    if cached:
        runtime = profile["runtime_delta"]
        paired.base.require(runtime["payload_read_calls"] == int(expected > 0) and
                            runtime["payload_read_bytes"] == expected, "shared producer I/O accounting differs")
        paired.base.require(runtime["hits"] == int(index > 0) and runtime["misses"] == int(index == 0),
                            "encoded fragment hit/miss not observed")
        paired.base.require(runtime["decode_count"] == 0, "encoded cache incorrectly records a decode")


def wave(context, source, case, q, bound, cached):
    path, version = source["logical_path"], source["version"]
    prepared_id = None
    bind_ns = release_ns = offset = 0
    if bound:
        response, bind_ns = paired.base.control(context["socket"],
            paired.request(path, version, case, "experiment_typed_bind"))
        prepared_id = response["prepared_id"]
        _, offset = paired.consume_profile(context, offset, "experiment_typed_bind", True)
    rows, routes = [], set()
    try:
        for index in range(q):
            row = paired.query(context, path, version, case, prepared_id)
            paired.base.require(row["result_path"] not in routes, "reused result route")
            routes.add(row["result_path"])
            profile, offset = paired.consume_profile(context, offset,
                "experiment_typed_read" if bound else "typed", True)
            verify(profile, case, index, bound, cached)
            rows.append(dict(row, index=index, server_profile=profile))
    finally:
        if bound:
            response, release_ns = paired.base.control(context["socket"],
                {"kind": "experiment_typed_release", "prepared_id": prepared_id})
            paired.base.require(response.get("released") is True, "binding release failed")
            _, offset = paired.consume_profile(context, offset, "experiment_typed_release", True)
    return {"bind_ns": bind_ns, "release_ns": release_ns, "rows": rows,
            "query_sum_ns": sum(r["total_ns"] for r in rows),
            "session_total_ns": bind_ns + release_ns + sum(r["total_ns"] for r in rows)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--before-environment", type=Path, required=True)
    p.add_argument("--after-daemon", type=Path, required=True)
    p.add_argument("--baseline-results", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--writer-lock", type=Path, required=True)
    p.add_argument("--source-pid", type=int, default=1001546)
    p.add_argument("--q", type=int, default=100)
    args = p.parse_args()
    paired.base.require(args.q == 100, "this paired protocol fixes Q=100")
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    environment = json.loads(args.before_environment.read_text())
    before = Path(environment["daemon"])
    paired.base.require(paired.base.sha_file(before) == environment["binaries"]["daemon"]["sha256"],
                        "before daemon changed")
    baseline = json.loads(gzip.decompress((args.baseline_results / "campaign.json.gz").read_bytes()))
    inputs = json.loads((args.baseline_results / "data-manifest.json").read_text())["sources"]
    env = dict(os.environ, **connection_environment(args.source_pid, baseline["database"]))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    report = {"protocol": "iotdbfs-grib-binary-cache-ablation/1", "complete": False,
              "source_publication": "none; reused immutable exact versions from accepted campaign",
              "q": args.q, "campaigns": 3,
              "before_sha256": paired.base.sha_file(before),
              "after_sha256": paired.base.sha_file(args.after_daemon),
              "baseline_raw_sha256": hashlib.sha256(gzip.decompress(
                  (args.baseline_results / "campaign.json.gz").read_bytes())).hexdigest(),
              "paired": [], "summary": []}
    for campaign in baseline["campaigns"]:
        index = campaign["index"]
        order = ["before", "after"] if index % 2 == 0 else ["after", "before"]
        for tier in ("small", "medium"):
            source = campaign["sources"][tier]
            case = inputs[tier]["cases"]["binary_one"]
            for mode in ("unbound", "bound"):
                group = {"campaign": index, "tier": tier, "mode": mode, "order": order,
                         "source": source, "arms": {}}
                for arm in order:
                    daemon = before if arm == "before" else args.after_daemon
                    root = output / f"campaign-{index}-{tier}-{mode}-{arm}"
                    with paired.base.private_mount(daemon, root, env, args.writer_lock,
                                                  frozen=mode == "bound") as context:
                        group["arms"][arm] = wave(context, source, case, args.q, mode == "bound", arm == "after")
                report["paired"].append(group)
                paired.base.save(output / "ablation.json", report)
                print(json.dumps({"campaign": index, "tier": tier, "mode": mode, "complete": True}), flush=True)
    for tier in ("small", "medium"):
        for mode in ("unbound", "bound"):
            groups = [g for g in report["paired"] if g["tier"] == tier and g["mode"] == mode]
            result = {"tier": tier, "mode": mode, "arms": {}}
            for arm in ("before", "after"):
                sessions = [g["arms"][arm] for g in groups]
                warm = [r for s in sessions for r in s["rows"][1:]]
                result["arms"][arm] = {"warm_p50_ns": statistics.median(r["total_ns"] for r in warm),
                    "session_total_ns": statistics.median(s["session_total_ns"] for s in sessions),
                    "warm_payload_calls": statistics.median(r["server_profile"]["iotdb_delta"]["payload_calls"] for r in warm),
                    "warm_sql_calls": statistics.median(r["server_profile"]["iotdb_delta"]["query_calls"] for r in warm)}
            result["paired_session_after_before_ratio"] = statistics.median(
                g["arms"]["after"]["session_total_ns"] / g["arms"]["before"]["session_total_ns"] for g in groups)
            report["summary"].append(result)
    report["complete"] = True
    paired.base.save(output / "ablation.json", report)
    print(json.dumps({"complete": True, "actual_queries": 2400, "summary": report["summary"]}), flush=True)


if __name__ == "__main__":
    main()
