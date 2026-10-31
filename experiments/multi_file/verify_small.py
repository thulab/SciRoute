"""Post-campaign source/version audit, outside all online query clocks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

import analyze_small
import small_runner as runner


def verify(args):
    report = json.loads(args.report.read_text())
    analyze_small.analyze(report)
    runner.sf.require(runner.sha_file(runner.__file__) == report["code_sha256"], "runner changed since measurement")
    environment = json.loads(args.environment.read_text())
    daemon = Path(environment["daemon"])
    runner.sf.require(runner.sha_file(daemon) == report["daemon_sha256"], "daemon changed")
    env = dict(os.environ, **runner.connection_environment(1001546, report["database"]))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    data = Path(report["data_work"])
    result = {"protocol": "iotdbfs-multifile-post-audit/1", "report_sha256": runner.sha_file(args.report),
              "source_identity": {}, "outside_query_timing": True}
    with runner.sf.private_mount(daemon, args.work, env, args.writer_lock) as context:
        offset = 0
        for fmt in ("hdf5", "grib"):
            started = time.perf_counter_ns()
            runner.manager.verify(data / (fmt + "-data"))
            manifest = runner.load_manifest(data / (fmt + "-data"))
            prefix = report["logical_prefix"] + "/" + fmt
            for source in manifest["sources"]:
                for base in (data / (fmt + "-fs"), context["mount"] / prefix.lstrip("/")):
                    runner.sf.require(runner.sha_file(base / source["file"]) == source["sha256"], "published source changed")
            response, batches, _, offset = runner.profile_call(context, runner.discovery_request(prefix + "/raw", manifest, False), offset)
            rows = runner.discovered(response, batches, fmt, prefix + "/raw")
            mapping = {r["path"]: r["version"] for r in rows}
            runner.sf.require(len(rows) == manifest["spec"]["N"] * manifest["spec"]["D"] and mapping == report["preparation"][fmt]["versions"], "published identity changed")
            result["source_identity"][fmt] = {"files": len(mapping), "units": len(rows),
                "raw_and_published_sha256_verified": True, "versions_unchanged": True,
                "audit_ns": time.perf_counter_ns() - started}
    # Query daemons are private and must already have unmounted here.
    targets = subprocess.check_output(["findmnt", "-rn", "-o", "TARGET", "-t", "fuse.iotdb_fs"], text=True).splitlines()
    runner.sf.require(not any("/experiments/multi_file/work/" in p for p in targets), "private experiment mount remains")
    pids = [41286, 1001546, 3624304]
    for pid in pids:
        runner.sf.require(Path(f"/proc/{pid}/comm").read_text().strip() == "iotdb_fs", "protected daemon changed")
    result.update(ok=True, protected_daemons_alive=pids, private_mounts_remaining=0)
    runner.sf.save(args.output, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    verify(parser.parse_args())
