"""Read-only HDF5 N32/N128 cache and predicate-stage diagnostic replay."""
import argparse
import json
import os
from pathlib import Path
import statistics

import small_runner as q


def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    args.work.mkdir(parents=True, exist_ok=False)
    environment = json.loads(args.environment.read_text())
    daemon = Path(environment["daemon"])
    q.sf.require(q.sha_file(daemon) == environment["binaries"]["daemon"]["sha256"], "baseline daemon changed")
    env = dict(os.environ, **q.connection_environment(1001546, "h09probe1790939889dbfc1f"))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    report = {"protocol": "iotdbfs-hdf5-cache-diagnosis/1", "complete": False,
        "daemon_sha256": q.sha_file(daemon), "code_sha256": q.sha_file(__file__), "records": [],
        "semantics": "same original binary/corpora; full compound query vs no-predicate diagnostic; different result sizes; profile collection excluded"}
    for campaign in range(3):
        sizes = (32, 128) if campaign % 2 == 0 else (128, 32)
        for n in sizes:
            prior = json.loads((args.scale_root/f"n{n}-d4/report.json").read_text())
            manifest = json.loads((args.scale_root/f"n{n}-d4/hdf5-data-manifest.json").read_text())
            versions = prior["preparation"]["hdf5"]["versions"]
            prefix = prior["logical_prefix"] + "/hdf5/raw"
            all_units = {(s["file"].removeprefix("raw/"), u["dataset"]) for s in manifest["sources"] for u in s["units"]}
            with q.sf.private_mount(daemon, args.work/f"c{campaign}-n{n}", env, args.writer_lock) as context:
                offset = 0
                for repeat in range(5):
                    cases = ("compound", "no-predicate") if repeat % 2 == 0 else ("no-predicate", "compound")
                    for case in cases:
                        response, batches, row, offset = q.profile_call(context, q.discovery_request(prefix, manifest, case == "compound"), offset)
                        rows = q.discovered(response, batches, "hdf5", prefix)
                        wanted = set(q.expected(manifest)) if case == "compound" else all_units
                        q.sf.require({q.key(r, "hdf5") for r in rows} == wanted and len(rows) == len(wanted) and
                            all(versions[r["path"]] == r["version"] for r in rows), "discovery identities changed")
                        row.update(N=n, D=4, campaign=campaign, repeat=repeat, case=case, returned_datasets=len(rows))
                        report["records"].append(row)
                print(json.dumps({"campaign": campaign, "N": n, "passed": True}), flush=True)
            q.sf.save(args.output/"report.json", report)
    report["summary"] = []
    for n in (32, 128):
        for case in ("compound", "no-predicate"):
            rows = [r for r in report["records"] if r["N"] == n and r["case"] == case and r["repeat"] > 0]
            def median(section, key):
                return statistics.median(r["server_profile"].get(section, {}).get(key, 0) for r in rows)
            cache_keys = ("dataset_catalog_parsed_cache.hits", "dataset_catalog_parsed_cache.misses",
                "dataset_catalog_parsed_cache.evictions", "dataset_catalog_object_read.calls")
            item = {"N": n, "case": case, "warm_observations": len(rows),
                "client_ms": statistics.median(r["total_ns"] for r in rows)/1e6,
                "server_ms": statistics.median(r["server_profile"]["server_total_ns"] for r in rows)/1e6,
                "stages_ms": {k: median("stages_ns", k)/1e6 for k in (
                    "dataset_catalog_index_completeness", "dataset_catalog_attribute_filter",
                    "dataset_catalog_manifest_revalidation", "dataset_catalog_marker_read", "dataset_catalog_prepare",
                    "published_catalog_scan", "authorization_parent_metadata")},
                "cache_counters": {k: median("counters", k) for k in cache_keys},
                "sql_calls": median("iotdb_delta", "query_calls"),
                "layout_calls_max": max(r["server_profile"]["iotdb_delta"]["layout_calls"] for r in rows)}
            q.sf.require(all(r["server_profile"]["counters"].get("dataset_catalog_parsed_cache.hits", 0) == n and
                r["server_profile"]["counters"].get("dataset_catalog_parsed_cache.misses", 0) == 0 and
                r["server_profile"]["counters"].get("dataset_catalog_parsed_cache.evictions", 0) == 0 and
                r["server_profile"]["counters"].get("dataset_catalog_object_read.calls", 0) == 0 for r in rows),
                "unexpected warm catalog cache churn")
            report["summary"].append(item)
    report["complete"] = True
    q.sf.save(args.output/"report.json", report)
    print(json.dumps({"complete": True, "records": len(report["records"]), "summary": report["summary"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("environment", "scale-root", "writer-lock", "output", "work"):
        parser.add_argument("--"+name, type=Path, required=True)
    main(parser.parse_args())
