#!/usr/bin/env python3
"""Measure publication prefill at registration and subsequent FUSE consumption.

Two matched binaries, paired campaigns, attr_timeout=1 plus a zero-TTL control.
The separate authorization profile verifies reads saved after registration.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

from grib_hdf5_boundary_probe import connection_environment, consume, grib_request, hdf5_request, submit


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def campaign(root, index, daemon, env, requests, repeats, ttl):
    directory = root / f"campaign-{index:02d}"
    directory.mkdir()
    mount = directory / "mount"
    mount.mkdir()
    socket_path = directory / "query.sock"
    profile_path = directory / "profile.jsonl"
    authorization_path = directory / "authorization.jsonl"
    local_env = env.copy()
    local_env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    local_env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(profile_path)
    local_env["IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL"] = str(authorization_path)
    with (directory / "mount.log").open("wb") as log:
        process = subprocess.Popen([str(daemon), "-f", "-o",
                                    f"attr_timeout={ttl},entry_timeout=1", str(mount)],
                                   env=local_env, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 90
            while not (os.path.ismount(mount) and socket_path.exists()):
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("mount did not start; see mount.log")
                time.sleep(.1)
            observations = []
            order = list(requests)
            for repetition in range(-1, repeats):
                if (repetition + index) % 2:
                    order.reverse()
                for name in order:
                    response, control_ns = submit(socket_path, requests[name])
                    result = consume(response["path"])
                    observations.append({"format": name, "repetition": repetition,
                                         "control_ns": control_ns,
                                         "route": response["path"][len(str(mount)):], **result})
            profiles = records(profile_path)
            checks = records(authorization_path)
            if len(profiles) != len(observations):
                raise RuntimeError("registration profile count mismatch")
            for observed, profile in zip(observations, profiles):
                if not profile.get("ok") or profile.get("operation") != "typed":
                    raise RuntimeError("typed profile failure")
                observed["server_total_ns"] = profile["server_total_ns"]
                observed["stages_ns"] = profile["stages_ns"]
                observed["iotdb_delta"] = profile["iotdb_delta"]
                observed["counters"] = profile["counters"]
                selected = [c for c in checks if c["route"] == observed["route"]]
                if not selected or any(not c["ok"] for c in selected):
                    raise RuntimeError("missing or failed FUSE authorization profile")
                observed["authorization"] = selected
                observed["fuse_metadata_sql_calls"] = sum(c["iotdb_delta"]["metadata_query_calls"] for c in selected)
                observed["fuse_cache_hits"] = sum(c["cache_hit"] for c in selected)
                observed["fuse_cache_misses"] = sum(not c["cache_hit"] for c in selected)
            if sum(len(o["authorization"]) for o in observations) != len(checks):
                raise RuntimeError("unattributed FUSE authorization profiles")
            return observations
        finally:
            if os.path.ismount(mount):
                subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30)
            elif process.poll() is None:
                process.terminate()
            process.wait(timeout=30)


def summarize(observations):
    output = {}
    for ttl in sorted({r["attr_timeout"] for r in observations}):
        output[str(ttl)] = {}
        for variant in ("before", "after"):
            output[str(ttl)][variant] = {}
            for fmt in ("grib", "hdf5"):
                rows = [r for r in observations if r["attr_timeout"] == ttl and
                        r["variant"] == variant and r["format"] == fmt and r["repetition"] >= 0]
                metrics = {
                    "server_total_ms": [r["server_total_ns"] / 1e6 for r in rows],
                    "client_control_ms": [r["control_ns"] / 1e6 for r in rows],
                    "client_stat_ms": [r["stat_ns"] / 1e6 for r in rows],
                    "client_read_ms": [r["read_ns"] / 1e6 for r in rows],
                    "client_consume_ms": [(r["stat_ns"] + r["read_ns"]) / 1e6 for r in rows],
                    "complete_client_ms": [(r["control_ns"] + r["stat_ns"] + r["read_ns"]) / 1e6 for r in rows],
                    "authorize_ms": [r["stages_ns"]["registration_authorize"] / 1e6 for r in rows],
                }
                item = {"samples": len(rows),
                        "metrics": {key: {"p50": statistics.median(values), "p95": percentile(values, .95)}
                                    for key, values in metrics.items()},
                        "ipc_sha256": sorted({r["ipc_sha256"] for r in rows}),
                        "ipc_bytes": sorted({r["ipc_bytes"] for r in rows}),
                        "registration_metadata_sql_calls": sorted({r["iotdb_delta"]["metadata_query_calls"] for r in rows}),
                        "publication_prefills": sorted({r["counters"].get("authorization_publication_prefill", 0) for r in rows}),
                        "fuse_metadata_sql_calls": sorted({r["fuse_metadata_sql_calls"] for r in rows}),
                        "fuse_cache_hits": sorted({r["fuse_cache_hits"] for r in rows}),
                        "fuse_cache_misses": sorted({r["fuse_cache_misses"] for r in rows}),
                        "campaign_consume_p50_ms": [statistics.median((r["stat_ns"] + r["read_ns"]) / 1e6
                                                                      for r in rows if r["pair"] == pair)
                                                    for pair in sorted({r["pair"] for r in rows})]}
                if item["registration_metadata_sql_calls"] != [1]:
                    raise RuntimeError("unexpected registration SQL count")
                expected_prefill = [1] if ttl == 1 and variant == "after" else [0]
                if item["publication_prefills"] != expected_prefill:
                    raise RuntimeError("unexpected publication-prefill count")
                if ttl == 1:
                    if variant == "after" and (item["fuse_metadata_sql_calls"] != [0] or item["fuse_cache_misses"] != [0]):
                        raise RuntimeError("publication prefill did not cover initial FUSE access")
                    if variant == "before" and item["fuse_metadata_sql_calls"] != [2]:
                        raise RuntimeError("expected one source and one bulk-parent SQL after registration")
                else:
                    if item["fuse_cache_hits"] != [0] or min(item["fuse_metadata_sql_calls"]) <= 0:
                        raise RuntimeError("zero TTL unexpectedly cached permission checks")
                output[str(ttl)][variant][fmt] = item
        for fmt in ("grib", "hdf5"):
            hashes = [output[str(ttl)][v][fmt]["ipc_sha256"] for v in ("before", "after")]
            if len(hashes[0]) != 1 or hashes[0] != hashes[1]:
                raise RuntimeError("IPC changed across experiment arms")
            if ttl == 0 and output["0"]["before"][fmt]["fuse_metadata_sql_calls"] != output["0"]["after"][fmt]["fuse_metadata_sql_calls"]:
                raise RuntimeError("zero-TTL metadata SQL counts differ across arms")
    for fmt in ("grib", "hdf5"):
        hashes = [output[str(ttl)][variant][fmt]["ipc_sha256"]
                  for ttl in (0, 1) for variant in ("before", "after")]
        if any(digest != hashes[0] for digest in hashes):
            raise RuntimeError("IPC changed across TTL conditions")
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--catalog-probe", type=Path, required=True)
    parser.add_argument("--pilot-manifest", type=Path, required=True)
    parser.add_argument("--profile-config", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.pairs <= 5 or not 1 <= args.repeats <= 20:
        raise ValueError("campaign or repeat bound exceeded")
    if len(str(args.output / "ttl-1/after/campaign-00/query.sock").encode()) >= 108:
        raise ValueError("use a shorter output directory for Unix socket paths")
    args.output.mkdir(parents=True, exist_ok=False)
    database = "h09probe1790939889dbfc1f"
    env = connection_environment(args.source_pid, database)
    env["IOTDB_CONFIG_FILE"] = str(args.profile_config)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "namespace.lock")
    source = json.loads(args.pilot_manifest.read_text())["sources"][0]
    requests = {"hdf5": hdf5_request(source), "grib": grib_request("/geos-pilot-9856a092a812/grib-control.grib2")}

    def inventory(label):
        result = subprocess.run([str(args.catalog_probe)], env=env, capture_output=True, text=True, timeout=120)
        (args.output / f"catalog-probe-{label}.log").write_text(result.stderr)
        if result.returncode:
            raise RuntimeError("catalog equivalence probe failed")
        lines = [line for line in result.stdout.splitlines() if line.startswith('{"namespace_published_versions"')]
        if len(lines) != 1:
            raise RuntimeError("missing catalog equivalence report")
        return json.loads(lines[0])

    first = inventory("before")
    observations = []
    for ttl in (1, 0):
        root = args.output / f"ttl-{ttl}"
        root.mkdir()
        for variant in ("before", "after"):
            (root / variant).mkdir()
        for pair in range(args.pairs):
            order = ("before", "after") if pair % 2 == 0 else ("after", "before")
            for variant in order:
                rows = campaign(root / variant, pair, getattr(args, variant), env, requests, args.repeats, ttl)
                for row in rows:
                    row.update(attr_timeout=ttl, variant=variant, pair=pair)
                observations.extend(rows)
    last = inventory("after")
    def identity(census):
        return census["namespace_published_versions"], [(c["name"], c["sql_rows"], c["selected"]) for c in census["cases"]]
    if identity(first) != identity(last):
        raise RuntimeError("catalog changed during timing")
    report = {"protocol": "iotdbfs-authorization-prefill-ab/1", "database": database,
              "before_sha256": hashlib.sha256(args.before.read_bytes()).hexdigest(),
              "after_sha256": hashlib.sha256(args.after.read_bytes()).hexdigest(),
              "catalog_equivalence": first, "requests": requests,
              "summary": summarize(observations), "observations": observations,
              "semantics": "Matched binaries differ only in publication prefill. Both normalize groups identically. Independent paired mounts, one warmup per format, OS/IoTDB caches unchanged. Client consumption includes exists/getsize/open/full IPC read. Separate optional FUSE-authorization profiling enabled identically; registration timing excludes later FUSE consumption."}
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(args.output / "report.json"), "summary": report["summary"]}, indent=2))


if __name__ == "__main__":
    main()
