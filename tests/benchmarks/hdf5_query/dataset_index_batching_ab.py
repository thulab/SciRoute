#!/usr/bin/env python3
"""Paired GEOS discovery replay with per-source/batched index validation and filtering binaries.

No ingestion or chmod occurs during timing. The FS index is validated once;
each independent mount gets a warmup for both hit and miss at every cohort.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics

import geos_pilot as pilot
import geos_time_directory as directory
import geos_time_scale as scale
from grib_hdf5_boundary_probe import (
    connection_environment, grib_request, hdf5_request, run_campaign,
)


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--typed-pilot", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if not 1 <= args.pairs <= 5 or not 1 <= args.repeats <= 20:
        raise ValueError("unbounded campaign")
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((args.pilot_dir / "pilot.json").read_text())
    pilot.check(manifest["complete"] and len(manifest["sources"]) == 16,
                "requires all 16 frozen GEOS files")
    index = directory.build(args.pilot_dir / "pilot.json", args.output / "fs-index.json")
    entries = index["entries"]
    target = next(e for e in entries if e["utc_second"] == 1309597200)
    priority = sorted(entries, key=lambda e: (abs(e["utc_second"] - target["utc_second"]), e["utc_second"]))
    report = {"protocol": "iotdbfs-discovery-index-ab/1", "complete": False,
              "database": manifest["database"], "pairs": args.pairs, "repeats": args.repeats,
              "binary_sha256": {name: hashlib.sha256(getattr(args, name).read_bytes()).hexdigest()
                                for name in ("before", "after")},
              "index_build_ns": index["build_ns"], "observations": [], "typed_controls": [],
              "semantics": "Interleaved independent mounts; discovery TTL=0; typed controls TTL=1. Warmup excluded from medians. OS/IoTDB caches not cleared. Query timers exclude index build, mount, unmount and FS oracle."}
    save(args.output / "report.json", report)
    os.environ["GEOS_FUSE_OPTIONS"] = "attr_timeout=0,entry_timeout=0"
    for pair in range(args.pairs):
        order = ("before", "after") if pair % 2 == 0 else ("after", "before")
        for variant in order:
            sizes = (1, 4, 8, 16) if pair % 2 == 0 else (16, 8, 4, 1)
            for count in sizes:
                cohort = sorted(priority[:count], key=lambda e: e["utc_second"])
                run = args.output / f"{variant}-{pair}-n{count}"
                run.mkdir()
                env = pilot.connection_environment(args.source_pid, manifest["database"])
                env["IOTDB_CONFIG_FILE"] = str(args.pilot_dir / "profile.properties")
                env["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock.resolve())
                socket = Path(f"/tmp/cdisc-{os.getpid()}-{variant}-{pair}-{count}.sock")
                env["IOTDBFS_QUERY_SOCKET"] = str(socket)
                env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(run / "profile.jsonl")
                process, log = pilot.mount(getattr(args, variant), run / "mount", env, socket)
                emitted = []
                try:
                    for repeat in range(-1, args.repeats):
                        for label, lower in (("hit", target["utc_second"]), ("miss", target["utc_second"] + 3600)):
                            row = scale.run_case(socket, cohort, lower, lower + 1, pair, repeat, label)
                            row.update(variant=variant, pair=pair)
                            emitted.append(row)
                        response, elapsed = pilot.control(socket, scale.request(cohort))
                        pilot.check(scale.identities(response["datasets"]) == scale.identities(cohort),
                                    "attribute discovery identity changed")
                        emitted.append({"variant": variant, "pair": pair, "n": count,
                            "repeat": repeat, "case": "attribute-only", "iotdb_client_ns": elapsed,
                            "identities": scale.identities(response["datasets"]),
                            "iotdb_sql_queries": response["sql_queries"],
                            "iotdb_catalog_read_bytes": response["catalog_read_bytes"]})
                finally:
                    pilot.unmount(run / "mount", process, log)
                profiles = [json.loads(line) for line in (run / "profile.jsonl").read_text().splitlines()]
                pilot.check(len(profiles) == len(emitted), "missing discovery profiles")
                for row, profile in zip(emitted, profiles):
                    pilot.check(profile["ok"], "discovery failed")
                    row.update(server_total_ns=profile["server_total_ns"],
                               counters=profile["counters"], iotdb_delta=profile["iotdb_delta"],
                               stages_ns=profile["stages_ns"])
                    expected_sources = 0
                    expected_parents = 0
                    pilot.check(row["counters"].get("authorization_source_metadata_calls", 0) == expected_sources,
                                "unexpected source permission reread")
                    pilot.check(row["counters"].get("authorization_parent_metadata_calls", 0) == expected_parents,
                                "unexpected parent permission reread")
                    pilot.check(row["counters"].get("authorization_source_catalog_reuse", 0) == count,
                                "missing per-source permission reuse")
                    candidates = count if row["case"] == "attribute-only" else (1 if row["case"] == "hit" else 0)
                    expected_sql = 2 + (5 * candidates if variant == "before" else 2 * candidates + (3 if candidates else 0))
                    pilot.check(row["iotdb_delta"]["metadata_query_calls"] == expected_sql,
                                "index batch query count differs from prediction")
                    pilot.check(row["counters"].get("dataset_catalog_manifest_revalidation.calls", 0) == 1,
                                "missing bounded manifest revalidation call")
                    pilot.check(row["counters"].get("dataset_catalog_manifest_revalidation.keys", 0) == count
                                and row["counters"].get("dataset_catalog_manifest_revalidation.rows", 0) == count,
                                "revalidation did not cover every selected source")
                    if variant == "after":
                        pilot.check(row["counters"].get("dataset_catalog_index_completeness.calls", 0) == (2 if candidates else 0),
                                    "index count queries were not batched")
                        pilot.check(row["counters"].get("dataset_catalog_filter_sql_calls", 0) == (1 if candidates else 0),
                                    "attribute filter queries were not batched")
                report["observations"].extend(emitted)
                save(args.output / "report.json", report)
                print(json.dumps({"pair": pair, "variant": variant, "n": count, "ok": True}), flush=True)
    # Single-source typed controls exercise both adapters, unchanged by the
    # discovery-only callback and catalog snapshot repair.
    typed_manifest = json.loads(args.typed_pilot.read_text())
    requests = {"hdf5": hdf5_request(typed_manifest["sources"][0]),
                "grib": grib_request("/geos-pilot-9856a092a812/grib-control.grib2")}
    env = connection_environment(args.source_pid, "h09probe1790939889dbfc1f")
    env["IOTDB_CONFIG_FILE"] = str(args.pilot_dir / "profile.properties")
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "typed-namespace.lock")
    for variant in ("before", "after"):
        (args.output / f"typed-{variant}").mkdir()
    for pair in range(args.pairs):
        for variant in (("before", "after") if pair % 2 == 0 else ("after", "before")):
            rows = run_campaign(args.output / f"typed-{variant}", pair, getattr(args, variant),
                                env, requests, args.repeats)
            for row in rows:
                row.update(variant=variant, pair=pair)
            report["typed_controls"].extend(rows)
            save(args.output / "report.json", report)
    summary = []
    for count in (1, 4, 8, 16):
        for case in ("hit", "miss", "attribute-only"):
            item = {"n": count, "case": case}
            for variant in ("before", "after"):
                rows = [r for r in report["observations"] if r["n"] == count and r["case"] == case
                        and r["variant"] == variant and r["repeat"] >= 0]
                item[variant] = {"samples": len(rows),
                    "client_p50_ms": statistics.median(r["iotdb_client_ns"] for r in rows) / 1e6,
                    "server_p50_ms": statistics.median(r["server_total_ns"] for r in rows) / 1e6,
                    "metadata_sql_calls": sorted({r["iotdb_delta"]["metadata_query_calls"] for r in rows}),
                    "catalog_bytes": sorted({r["iotdb_catalog_read_bytes"] for r in rows}),
                    "pair_client_p50_ms": [statistics.median(r["iotdb_client_ns"] for r in rows if r["pair"] == p)/1e6 for p in range(args.pairs)]}
            pilot.check(item["before"]["catalog_bytes"] == item["after"]["catalog_bytes"], "catalog bytes changed")
            item["client_reduction_percent"] = 100 * (1 - item["after"]["client_p50_ms"] / item["before"]["client_p50_ms"])
            summary.append(item)
    report["summary"] = summary
    typed_summary = {}
    for fmt in ("grib", "hdf5"):
        typed_summary[fmt] = {}
        for variant in ("before", "after"):
            rows = [r for r in report["typed_controls"] if r["format"] == fmt and r["variant"] == variant and r["repetition"] >= 0]
            typed_summary[fmt][variant] = {
                "samples": len(rows), "server_p50_ms": statistics.median(r["server_total_ns"] for r in rows)/1e6,
                "complete_client_p50_ms": statistics.median(r["control_ns"]+r["stat_ns"]+r["read_ns"] for r in rows)/1e6,
                "ipc_sha256": sorted({r["ipc_sha256"] for r in rows}), "ipc_bytes": sorted({r["ipc_bytes"] for r in rows}),
                "metadata_sql_calls": sorted({r["iotdb_delta"]["metadata_query_calls"] for r in rows})}
        pilot.check(typed_summary[fmt]["before"]["ipc_sha256"] == typed_summary[fmt]["after"]["ipc_sha256"], "typed control bytes changed")
    report.update(typed_summary=typed_summary, complete=True)
    save(args.output / "report.json", report)
    print(json.dumps({"complete": True, "report": str(args.output / "report.json"), "summary": summary, "typed_summary": typed_summary}))


if __name__ == "__main__":
    main()
