"""Read-only old/fixed/default-capacity control over published N8/32/128 corpora."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess

import small_runner as q


def main(args):
    args.output.mkdir(parents=True, exist_ok=False)
    args.work.mkdir(parents=True, exist_ok=False)
    environment = json.loads(args.environment.read_text())
    old = Path(environment["daemon"])
    q.sf.require(q.sha_file(old) == environment["binaries"]["daemon"]["sha256"], "old daemon changed")
    env = dict(os.environ, **q.connection_environment(1001546, "h09probe1790939889dbfc1f"))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    daemons = {"old-default": old, "fixed-default": args.fixed_daemon, "fixed-cap16": args.fixed_daemon}
    report = {"protocol": "iotdbfs-grib-index-cache-ab/1", "complete": False,
        "campaigns": 3, "repeats": 5, "records": [], "datasets": {},
        "binary_sha256": {name: q.sha_file(path) for name, path in daemons.items()},
        "native_sha256": q.sha_file(args.native), "code_sha256": q.sha_file(__file__),
        "byte_budget": 67108864, "count_caps": {"old-default": 16, "fixed-default": 65536, "fixed-cap16": 16},
        "measurement": "same pilot native/client clocks, hash proofs excluded, warm OS caches, no data publication"}
    for n in (8, 32, 128):
        point = args.scale_root / f"n{n}-d4"
        base = json.loads((point / "report.json").read_text())
        manifest = json.loads((point / "grib-data-manifest.json").read_text())
        q.sf.require(base["complete"] and base["native_sha256"] == report["native_sha256"], "prepared corpus/native differs")
        report["datasets"][str(n)] = {"manifest": manifest, "prefix": base["logical_prefix"] + "/grib/raw",
            "versions": base["preparation"]["grib"]["versions"], "data_work": base["data_work"],
            "prior_report_sha256": q.sha_file(point / "report.json")}
    if args.resume_report:
        prior = json.loads(args.resume_report.read_text())
        q.sf.require(not prior["complete"] and prior["binary_sha256"] == report["binary_sha256"] and
            prior["native_sha256"] == report["native_sha256"] and prior["datasets"] == report["datasets"],
            "cannot resume a different campaign")
        report["records"] = prior["records"]
        report["resume"] = {"prior_report": str(args.resume_report), "prior_sha256": q.sha_file(args.resume_report),
            "retained_records": len(prior["records"]), "reason": "profile sink flush race; only uncommitted variant restarted"}
    q.sf.save(args.output / "report.json", report)
    for campaign in range(3):
        for n in (8, 32, 128):
            dataset = report["datasets"][str(n)]
            variants = ["old-default", "fixed-default"]
            if campaign % 2: variants.reverse()
            if n > 8: variants.append("fixed-cap16")
            for variant in variants:
                retained = [r for r in report["records"] if r["campaign"] == campaign and r["N"] == n and r["variant"] == variant]
                if retained:
                    q.sf.require(len(retained) == (30 if variant == "fixed-default" else 10), "partial variant cannot be resumed")
                    continue
                native = q.NativeWorker(args.native, env) if variant == "fixed-default" else None
                try:
                    with q.sf.private_mount(daemons[variant], args.work/f"c{campaign}-n{n}-{variant}", env,
                        args.writer_lock, grib_index_cache_max_versions=16 if variant == "fixed-cap16" else None) as context:
                        offset = 0
                        for repeat in range(5):
                            for phase in ("discover", "discover-values"):
                                values = phase == "discover-values"
                                arms = ["iotdb"] if native is None else ["fs-scan", "fs-index", "iotdb"]
                                if native is not None:
                                    arms = arms[campaign % 3:] + arms[:campaign % 3]
                                for arm in arms:
                                    if arm == "iotdb":
                                        row, offset = q.iotdb_query(context, dataset["manifest"], dataset["prefix"], dataset["versions"], offset, values)
                                    else:
                                        data = Path(dataset["data_work"])
                                        row = native.call({"op": "query", "root": str(data/"grib-fs/raw"), "format": "grib",
                                            "mode": "scan" if arm == "fs-scan" else "index", "values": values,
                                            "predicate": dataset["manifest"]["query"], "index": str(data/"grib-index.sqlite")})
                                        q.check_native(row, dataset["manifest"], q.expected(dataset["manifest"]), values)
                                    row.update(N=n, D=4, K=2, depth=2, campaign=campaign, repeat=repeat, phase=phase, variant=variant, arm=arm)
                                    report["records"].append(row)
                        q.sf.save(args.output/"report.json", report)
                        print(json.dumps({"campaign": campaign, "N": n, "variant": variant, "passed": True}), flush=True)
                finally:
                    if native is not None: native.close()
    summary = []
    for n in (8, 32, 128):
        for phase in ("discover", "discover-values"):
            for variant in daemons:
                rows = [r for r in report["records"] if r["N"] == n and r["phase"] == phase and
                        r["variant"] == variant and r["arm"] == "iotdb" and r["repeat"] > 0]
                if not rows: continue
                calls = [[r["discovery"], *r["value_calls"]] for r in rows]
                def counter(section, key):
                    return statistics.median(sum(c["server_profile"].get(section, {}).get(key, 0) for c in group) for group in calls)
                summary.append({"N": n, "phase": phase, "variant": variant,
                    "warm_observations": len(rows), "client_ms": statistics.median(r["total_ns"] for r in rows)/1e6,
                    "sql_calls": counter("iotdb_delta", "query_calls"), "layout_calls": counter("iotdb_delta", "layout_calls"),
                    "payload_calls": counter("iotdb_delta", "payload_calls"),
                    "hits": counter("counters", "grib.index.cache.hits"), "misses": counter("counters", "grib.index.cache.misses"),
                    "version_evictions": counter("counters", "grib.index.cache.version_limit_evictions"),
                    "byte_evictions": counter("counters", "grib.index.cache.byte_limit_evictions")})
    report["summary"] = summary
    # Direct acceptance: every warm fixed-default observation must avoid layout IO.
    fixed = [r for r in report["records"] if r["variant"] == "fixed-default" and r["arm"] == "iotdb" and r["repeat"] > 0]
    q.sf.require(all(c["server_profile"]["iotdb_delta"]["layout_calls"] == 0 for r in fixed
        for c in [r["discovery"], *r["value_calls"]]), "fixed cache still rereads warm layouts")
    # Source audit is outside query clocks and runs in a fresh private daemon.
    with q.sf.private_mount(args.fixed_daemon, args.work/"source-audit", env, args.writer_lock) as context:
        offset = 0
        report["post_audit"] = {}
        for n in (8, 32, 128):
            data = report["datasets"][str(n)]
            root = Path(data["data_work"])
            q.manager.verify(root/"grib-data")
            for source in data["manifest"]["sources"]:
                q.sf.require(q.sha_file(root/"grib-fs"/source["file"]) == source["sha256"], "FS bytes changed")
                relative = source["file"].removeprefix("raw/")
                q.sf.require(q.sha_file(context["mount"]/data["prefix"].lstrip("/")/relative) == source["sha256"], "IoTDB bytes changed")
            response, batches, _, offset = q.profile_call(context, q.discovery_request(data["prefix"], data["manifest"], False), offset)
            rows = q.discovered(response, batches, "grib", data["prefix"])
            q.sf.require(len(rows) == n*4 and {r["path"]: r["version"] for r in rows} == data["versions"], "source versions changed")
            report["post_audit"][str(n)] = {"files": n, "units": n*4, "hashes_and_versions_unchanged": True}
    targets = subprocess.check_output(["findmnt", "-rn", "-o", "TARGET", "-t", "fuse.iotdb_fs"], text=True)
    q.sf.require("/experiments/multi_file/work/" not in targets, "private experiment mount remains")
    for pid in (41286, 1001546, 3624304):
        q.sf.require(Path(f"/proc/{pid}/comm").read_text().strip() == "iotdb_fs", "protected daemon changed")
    q.sf.require(len(report["records"]) == 420, "campaign record count differs")
    report.update(complete=True, private_mounts_remaining=0, protected_daemons_alive=[41286, 1001546, 3624304])
    q.sf.save(args.output/"report.json", report)
    print(json.dumps({"complete": True, "observations": len(report["records"]), "summary": summary}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("environment", "fixed-daemon", "native", "scale-root", "writer-lock", "output", "work"):
        parser.add_argument("--"+name, type=Path, required=True)
    parser.add_argument("--resume-report", type=Path)
    main(parser.parse_args())
