#!/usr/bin/env python3
"""Broad-dataset attribute discovery control for an existing SC performance run."""
import argparse
import json
import os
from pathlib import Path
import sys
import time

from sc_perf_campaign import control, require, save, start_mount, stop_mount


def normalize(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def scan(files):
    import h5py
    begin = time.perf_counter_ns()
    result = []
    for source in files:
        with h5py.File(source["raw"], "r") as file:
            def visit(name, obj):
                if isinstance(obj, h5py.Dataset):
                    value = obj.attrs.get("long_name")
                    result.append({"path": source["logical_path"],
                                   "version": source["version"],
                                   "dataset_path": "/" + name,
                                   "long_name": normalize(value) if value is not None else None})
            file.visititems(visit)
    return result, time.perf_counter_ns() - begin


def request(prefix, value):
    return {"kind": "dataset_catalog", "request": {
        "sources": {"paths": [{"kind": "subtree", "path": prefix}]},
        "predicate": {"kind": "compare", "field": "hdf5.attr:long_name", "operator": "eq",
                      "value": {"type": "utf8", "value": value}}}}


def identities(items):
    return sorted((item["path"], item["version"], item["dataset_path"]) for item in items)


def main(args):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
    from a12_migration_smoke import connection_environment
    base = args.campaign.resolve()
    manifest = json.loads((base / "campaign.json").read_text())
    require(manifest["complete"] and manifest["protocol"] == "iotdbfs-hdf5-sc-perf-campaign/1",
            "requires completed SC campaign")
    files = manifest["sources"]
    prefix = files[0]["logical_path"].rsplit("/", 1)[0]
    require(all(source["logical_path"].startswith(prefix + "/") for source in files),
            "sources have different catalog prefixes")
    result_root = args.output.resolve()
    result_root.mkdir(parents=True, exist_ok=False)
    prebuilt, build_ns = scan(files)
    save(result_root / "fs-prebuilt.json", {"build_ns": build_ns, "entries": prebuilt})
    env = connection_environment(manifest["source_pid"], manifest["database"])
    env["IOTDBFS_NAMESPACE_LOCK"] = manifest["writer_lock"]
    env["IOTDB_CONFIG_FILE"] = str(base / "profile.properties")
    report = {"protocol": "iotdbfs-hdf5-sc-catalog-compare/1", "complete": False,
              "source_campaign": str(base), "prebuilt_build_ns": build_ns,
              "prebuilt_entries": len(prebuilt), "runs": []}
    save(result_root / "report.json", report)
    for campaign in range(3):
        work = result_root / f"campaign-{campaign}"
        work.mkdir()
        socket = Path("/tmp") / f"hsc-cat-{os.getpid()}-{campaign}.sock"
        current = env.copy()
        current["IOTDBFS_QUERY_SOCKET"] = str(socket)
        current["IOTDBFS_QUERY_PROFILE_JSONL"] = str(work / "server-profile.jsonl")
        mount = work / "mount"
        process = start_mount(args.build.resolve() / "bin/iotdb_fs", mount, current,
                              work / "mount.log", socket)
        try:
            recorded = []
            for label in ("Daily sea surface temperature", "__absent_sc_benchmark__"):
                native, native_ns = scan(files)
                start = time.perf_counter_ns()
                fs = [item for item in prebuilt if item["long_name"] == label]
                fs_filter_ns = time.perf_counter_ns() - start
                native = [item for item in native if item["long_name"] == label]
                response, iotdb_ns, request_bytes, response_bytes = control(socket, request(prefix, label))
                require(identities(native) == identities(fs) == identities(response["datasets"]),
                        f"attribute candidate sets differ for {label}")
                item = {"label": label, "matched": len(fs), "native_scan_ns": native_ns,
                        "fs_prebuilt_filter_ns": fs_filter_ns, "iotdb_client_ns": iotdb_ns,
                        "request_bytes": request_bytes, "response_bytes": response_bytes,
                        "candidate_datasets": response["candidate_datasets"],
                        "selected_sources": response["selected_sources"],
                        "sql_queries": response["sql_queries"],
                        "catalog_read_bytes": response["catalog_read_bytes"],
                        "identities": identities(fs)}
                recorded.append(item)
            save(work / "observations.json", recorded)
            profiles = [json.loads(line) for line in (work / "server-profile.jsonl").read_text().splitlines()]
            require(len(profiles) == 2 and all(p["operation"] == "dataset_catalog" and p["ok"] for p in profiles),
                    "catalog server profile count mismatch")
            report["runs"].append({"campaign": campaign, "observations": recorded,
                                   "server_profile_ns": [p["server_total_ns"] for p in profiles],
                                   "server_profile_counters": [p["counters"] for p in profiles],
                                   "iotdb_delta": [p["iotdb_delta"] for p in profiles]})
            save(result_root / "report.json", report)
        finally:
            stop_mount(mount, process)
    report["complete"] = True
    save(result_root / "report.json", report)
    print(json.dumps({"complete": True, "runs": len(report["runs"]),
                      "prebuilt_entries": len(prebuilt), "report": str(result_root / "report.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args())
