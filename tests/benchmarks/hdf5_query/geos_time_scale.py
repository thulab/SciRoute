#!/usr/bin/env python3
"""Measure GEOS cross-file time pruning against two ordinary-FS controls."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import time

import geos_pilot as pilot
import geos_time_directory as directory


PREDICATE = {"kind": "compare", "field": "hdf5.attr:standard_name",
             "operator": "eq", "value": {"type": "utf8", "value": "air_temperature"}}


def identities(rows):
    return sorted((row["path"], row["version"]) for row in rows)


def scan_originals(entries, lower, upper):
    begin = time.perf_counter_ns()
    found = []
    for entry in entries:
        point, _, _ = directory.native_utc_point(Path(entry["raw"]))
        if lower <= point < upper:
            found.append(entry)
    return found, time.perf_counter_ns() - begin


def request(entries, lower=None, upper=None):
    selection = {"sources": {"paths": [
        {"kind": "exact", "path": entry["path"]} for entry in entries]},
        "dataset_path": "/T", "predicate": PREDICATE}
    if lower is not None:
        selection["time_range"] = {"lower_unix_s": lower, "upper_unix_s": upper}
    return {"kind": "dataset_catalog", "request": selection}


def run_case(socket, entries, lower, upper, campaign, repeat, label):
    subset = {"protocol": directory.PROTOCOL, "coverage_protocol": "utc-point/1",
              "entries": entries}
    fs, fs_ns = directory.search(subset, lower, upper)
    scanned, scan_ns = scan_originals(entries, lower, upper)
    expected = identities(fs)
    pilot.check(expected == identities(scanned), "FS index and native scan disagree")
    response, iotdb_ns = pilot.control(socket, request(entries, lower, upper))
    pilot.check(identities(response["datasets"]) == expected,
                "IoTDB and ordinary-FS time candidates disagree")
    pilot.check(response["selected_sources"] == len(entries) and
                response["time_candidate_sources"] == len(expected),
                "IoTDB candidate accounting disagrees")
    return {"campaign": campaign, "n": len(entries), "repeat": repeat,
            "case": label, "lower_unix_s": lower, "upper_unix_s": upper,
            "k": len(expected), "identities": expected,
            "fs_index_ns": fs_ns, "fs_native_scan_ns": scan_ns,
            "iotdb_client_ns": iotdb_ns,
            "iotdb_sql_queries": response["sql_queries"],
            "iotdb_catalog_read_bytes": response["catalog_read_bytes"],
            "selected_sources": response["selected_sources"],
            "time_candidate_sources": response["time_candidate_sources"]}


def save(path, document):
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def main(args):
    manifest = json.loads((args.pilot_dir / "pilot.json").read_text())
    pilot.check(manifest["complete"] and len(manifest["sources"]) == 16,
                "requires 16 published, distinct GEOS time files")
    args.output.mkdir(parents=True, exist_ok=False)
    index_path = args.output / "fs-time-index.json"
    index = directory.build(args.pilot_dir / "pilot.json", index_path)
    entries = index["entries"]
    pilot.check(len({entry["utc_second"] for entry in entries}) == 16,
                "GEOS source times are not distinct")
    source_by_key = {(item["logical_path"], item["version"]): item
                     for item in manifest["sources"]}
    target = next((entry for entry in entries
                   if entry["utc_second"] == 1309597200), None)
    pilot.check(target is not None, "missing fixed 2011-07-02 09:00 UTC target")
    priority = sorted(entries, key=lambda entry:
                      (abs(entry["utc_second"] - target["utc_second"]),
                       entry["utc_second"]))
    report = {"protocol": "iotdbfs-geos-time-scale/1", "complete": False,
              "pilot": str(args.pilot_dir), "database": manifest["database"],
              "index": str(index_path), "index_build_ns": index["build_ns"],
              "index_bytes": index_path.stat().st_size,
              "campaigns": args.campaigns, "repeats": args.repeats,
              "records": [], "typed_checks": []}
    save(args.output / "report.json", report)
    for campaign in range(args.campaigns):
        sizes = (1, 4, 8, 16) if campaign % 2 == 0 else (16, 8, 4, 1)
        for count in sizes:
            cohort = sorted(priority[:count], key=lambda entry: entry["utc_second"])
            point = target["utc_second"]
            environment = pilot.connection_environment(args.source_pid,
                                                        manifest["database"])
            environment["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock.resolve())
            environment["IOTDB_CONFIG_FILE"] = str(args.pilot_dir / "profile.properties")
            environment["IOTDBFS_QUERY_SOCKET"] = (
                f"/tmp/geos-time-scale-{os.getpid()}-{campaign}-{count}.sock")
            run_dir = args.output / f"campaign-{campaign}-n{count}"
            run_dir.mkdir()
            environment["IOTDBFS_QUERY_PROFILE_JSONL"] = str(run_dir / "server.jsonl")
            socket = Path(environment["IOTDBFS_QUERY_SOCKET"])
            process, log = pilot.mount(args.daemon, run_dir / "mount",
                                       environment, socket)
            emitted = []
            try:
                for repeat in range(args.repeats):
                    for label, lower in (("hit", point), ("miss", point + 3600)):
                        record = run_case(socket, cohort, lower, lower + 1,
                                          campaign, repeat, label)
                        record["profile_index"] = len(emitted)
                        emitted.append(record)
                        report["records"].append(record)
                    if repeat == 0:
                        unfiltered, elapsed = pilot.control(socket,
                                                            request(cohort))
                        pilot.check(identities(unfiltered["datasets"]) ==
                                    identities(cohort),
                                    "attribute-only dataset catalog mismatch")
                        report["records"].append({"campaign": campaign,
                            "n": count, "repeat": repeat, "case": "attribute-only",
                            "k": count, "iotdb_client_ns": elapsed,
                            "iotdb_sql_queries": unfiltered["sql_queries"],
                            "iotdb_catalog_read_bytes":
                                unfiltered["catalog_read_bytes"],
                            "profile_index": len(emitted)})
                        emitted.append(report["records"][-1])
                        source = source_by_key[(target["path"], target["version"])]
                        bounds = pilot.CASES["point"]
                        native = pilot.query_native(source["raw"], bounds)
                        typed = pilot.query_iotdb(socket, source, bounds)
                        pilot.check((native["rows"], native["digest"]) ==
                                    (typed["rows"], typed["digest"]),
                                    "selected GEOS /T value does not match H5Dread")
                        check = {"campaign": campaign, "n": count,
                                 "source": target["path"], "version": target["version"],
                                 "rows": native["rows"], "digest": native["digest"],
                                 "fs_h5dread_ns": native["ns"],
                                 "iotdb_typed_ns": typed["ns"],
                                 "iotdb_ipc_bytes": typed["ipc_bytes"],
                                 "profile_index": len(emitted)}
                        report["typed_checks"].append(check)
                        emitted.append(check)
                save(args.output / "report.json", report)
            finally:
                pilot.unmount(run_dir / "mount", process, log)
            profiles = [json.loads(line) for line in
                        (run_dir / "server.jsonl").read_text().splitlines()]
            pilot.check(len(profiles) == len(emitted),
                        "server profile count does not match registrations")
            for row, profile in zip(emitted, profiles):
                row["server_total_ns"] = profile["server_total_ns"]
                row["server_iotdb_delta"] = profile.get("iotdb_delta", {})
                row["server_counters"] = profile.get("counters", {})
            save(args.output / "report.json", report)
    summary = []
    for count in (1, 4, 8, 16):
        for label in ("hit", "miss"):
            records = [row for row in report["records"]
                       if row["n"] == count and row["case"] == label]
            summary.append({"n": count, "case": label,
                "k": records[0]["k"],
                "fs_index_median_ns": statistics.median(
                    row["fs_index_ns"] for row in records),
                "fs_native_scan_median_ns": statistics.median(
                    row["fs_native_scan_ns"] for row in records),
                "iotdb_client_median_ns": statistics.median(
                    row["iotdb_client_ns"] for row in records),
                "iotdb_server_median_ns": statistics.median(
                    row["server_total_ns"] for row in records),
                "iotdb_sql_median": statistics.median(
                    row["iotdb_sql_queries"] for row in records),
                "iotdb_catalog_bytes_median": statistics.median(
                    row["iotdb_catalog_read_bytes"] for row in records)})
    report["summary"] = summary
    report["complete"] = True
    save(args.output / "report.json", report)
    print(json.dumps({"complete": True, "report": str(args.output / "report.json"),
                      "summary": summary}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--campaigns", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=5)
    main(parser.parse_args())
