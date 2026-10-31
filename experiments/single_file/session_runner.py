#!/usr/bin/env python3
"""Paired reopen/re-register versus bind-once continuous single-file reads.

Every bound Q is an actual fresh session, including its own bind and release.
The fixed typed interface is a private root-only experimental contract: its
source, query and permissions are frozen for the bounded read-only session.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import uuid

import runner as base

PROTOCOL = "iotdbfs-single-file-session-paired/1"


def session_summary(write_ns, source_locator_ns, binding_ns, release_ns, rows, q):
    base.require(len(rows) == q and q > 0, "actual session must contain exactly Q reads")
    times = [r["total_ns"] for r in rows]
    base.require(all(t >= 0 for t in times) and binding_ns >= 0 and release_ns >= 0,
                 "negative session timing")
    reads_ns = sum(times)
    total_ns = binding_ns + reads_ns + release_ns
    warm = times[1:]
    return {"queries": q, "bind_ns": binding_ns, "release_ns": release_ns,
            "query_sum_ns": reads_ns, "session_total_ns": total_ns,
            "write_ns": write_ns, "source_locator_ns": source_locator_ns,
            "lifecycle_total_ns": write_ns + source_locator_ns + total_ns,
            "first_query_ns": times[0], "all_p50_ns": statistics.median(times),
            "all_p95_ns": base.percentile(times, .95),
            "warm_p50_ns": statistics.median(warm) if warm else None,
            "warm_p95_ns": base.percentile(warm, .95) if warm else None}


def consume_profile(context, offset, operation, enabled):
    if not enabled:
        return None, offset
    # The daemon writes the response frame before flushing its profile sink.
    # Keep the original offset until a complete newline-terminated record is
    # visible. This wait is after control/query wall timing has finished.
    path = context["root"] / "queries.jsonl"
    deadline = time.monotonic() + 5
    while True:
        if path.exists():
            with path.open("rb") as stream:
                stream.seek(offset)
                raw = stream.read()
                next_offset = stream.tell()
            lines = [line for line in raw.splitlines(keepends=True) if line.strip()]
            base.require(len(lines) <= 1, "profile/operation count differs")
            if lines and all(line.endswith(b"\n") for line in lines):
                try:
                    profile = json.loads(lines[0])
                except json.JSONDecodeError:
                    profile = None
                if profile is not None:
                    base.require(profile.get("ok") is True and profile.get("operation") == operation,
                                 "profile/operation count differs")
                    return profile, next_offset
        base.require(time.monotonic() < deadline, "profile sink timed out waiting for complete record")
        time.sleep(.001)


def validate_bound_profile(profile, index, expected_rows=None):
    counts = profile.get("counters", {})
    base.require(counts.get("registration_catalog_calls", 0) == 0 and
                 counts.get("catalog_scan_calls", 0) == 0 and
                 counts.get("typed_prepare_calls", 0) == 0 and
                 counts.get("typed_prepared_reuse", 0) > 0 and
                 counts.get("hdf5.selection.validation_calls", 0) == 0 and
                 counts.get("hdf5.selection.decoder_compiles", 0) == 0,
                 "bound read repeated catalog or plan compilation")
    base.require(counts.get("typed_cursor_next_calls", 0) > 0 and
                 counts.get("typed_ipc_batches", 0) > 0 and
                 (expected_rows is None or counts.get("typed_ipc_rows", 0) == expected_rows),
                 "bound read did not execute a fresh cursor and encode all rows")
    io = profile.get("iotdb_delta", {})
    if index:
        base.require(io.get("query_calls", 0) == 0 and io.get("payload_calls", 0) == 0 and
                     profile.get("runtime_delta", {}).get("decode_count", 0) == 0,
                     "warm bound read re-read or re-decoded payload")


def native_bound_wave(probe, file, source, case, count, env):
    command = [str(probe), str(file), "/T", ",".join(map(str, case["origin"])),
               ",".join(map(str, case["shape"])), str(count), "--bound"]
    start = time.perf_counter_ns()
    completed = subprocess.run(command, capture_output=True, text=True, timeout=300, env=env)
    wall_ns = time.perf_counter_ns() - start
    base.require(completed.returncode == 0, "native bound probe failed: " + completed.stderr[-300:])
    rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    base.require(len(rows) == count, "native bound probe omitted observations")
    bind_ns = rows[0]["session_bind_ns"]
    for index, row in enumerate(rows):
        base.require(row.get("mode") == "bound" and row["iteration"] == index and
                     row["runtime_hdf5"] == "1.14.4" and row["library_threadsafe"],
                     "native bound mode or runtime differs")
        for key in ("rows", "dtype", "value_sha256", "mask_sha256"):
            base.require(row[key] == case[key], "native bound result differs: " + key)
        base.require(row["origin"] == case["origin"] and row["shape"] == case["shape"] and
                     row["source_shape"] == source["shape"], "native bound shape differs")
        base.require(row["session_bind_ns"] == bind_ns, "native bind cost changed within session")
        base.require(index + 1 == count or row["session_close_ns"] == 0,
                     "native session closed before last read")
        base.require(row["open_file_ns"] == 0 and row["open_dataset_ns"] == 0,
                     "native bound query re-opened handles")
    return {"observations": rows, "binding": {"total_ns": bind_ns},
            "release": {"total_ns": rows[-1]["session_close_ns"]},
            "native_probe_process_wall_ns": wall_ns}


def bound_iotdb_session(context, bound, source, case, count, profile):
    declaration = base.query_request(bound["logical_path"], bound["version"], case)
    declaration["kind"] = "experiment_typed_bind"
    response, bind_ns = base.control(context["socket"], declaration)
    prepared_id = response.get("prepared_id")
    base.require(isinstance(prepared_id, str) and prepared_id and response.get("ttl_ms", 0) > 0,
                 "fixed typed bind omitted identity or TTL")
    deadline = time.monotonic() + response["ttl_ms"] / 1000
    rows, paths = [], set()
    offset = 0
    bind_profile = None
    release = None
    try:
        bind_profile, offset = consume_profile(context, offset, "experiment_typed_bind", profile)
        for index in range(count):
            base.require(time.monotonic() < deadline, "fixed typed session TTL elapsed")
            row = base.query_iotdb(context, bound["logical_path"], bound["version"], source,
                                   case, prepared_id=prepared_id)
            base.require(row["result_path"] not in paths,
                         "bound read reused a previous result rather than materializing")
            paths.add(row["result_path"])
            read_profile, offset = consume_profile(context, offset, "experiment_typed_read", profile)
            if profile:
                validate_bound_profile(read_profile, index, case["rows"])
                row["server_profile"] = read_profile
            row["index"] = index
            rows.append(row)
    finally:
        released, release_ns = base.control(context["socket"], {
            "kind": "experiment_typed_release", "prepared_id": prepared_id})
        base.require(released.get("released") is True, "fixed typed release did not retire binding")
        release_profile, offset = consume_profile(context, offset, "experiment_typed_release", profile)
        release = {"total_ns": release_ns, "released": True}
        if profile:
            release["server_profile"] = release_profile
    binding = {"total_ns": bind_ns, "prepared_id": prepared_id, "ttl_ms": response["ttl_ms"]}
    if profile:
        binding["server_profile"] = bind_profile
    return {"observations": rows, "binding": binding, "release": release,
            "unique_result_paths": len(paths)}


def run(args):
    from environment import connection_environment
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    data = json.loads(args.manifest.read_text())
    base.require(data["protocol"] == "iotdbfs-single-file-hdf5-data/1" and
                 data["all_real_size_gates_passed"] and not data["test_only_scaled"],
                 "unaccepted size-tier manifest")
    environment = json.loads(args.environment.read_text())
    base.require(environment["native_versions"]["runtime_hdf5"] == "1.14.4" and
                 environment["python_versions"]["hdf5_version"] == "1.14.4", "HDF5 environment differs")
    daemon, probe = Path(environment["daemon"]), Path(environment["native_probe"])
    for name, path in (("daemon", daemon), ("native_probe", probe)):
        base.require(base.sha_file(path) == environment["binaries"][name]["sha256"],
                     "environment binary changed: " + name)
    env = dict(os.environ, **connection_environment(args.source_pid, args.database))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    sources = {tier: data["sources"][tier] for tier in args.tiers}
    for source in sources.values():
        raw = args.manifest.parent / source["file"]
        base.require(raw.stat().st_size == source["bytes"] and base.sha_file(raw) == source["sha256"],
                     "source identity changed")
    result = {"protocol": PROTOCOL, "complete": False, "smoke_only": args.smoke,
              "data_manifest_sha256": base.sha_file(args.manifest),
              "environment_sha256": base.sha_file(args.environment), "q_values": args.q_values,
              "database": args.database, "hdf5_runtime": "1.14.4", "profiling": args.profile,
              "synthetic_controlled": True, "campaigns": [],
              "cache_semantics": {"os_page_cache": "not dropped; full source SHA warms pages",
                  "fs_external_index": False, "fs_bound_handles": "one H5F/H5D open per independent Q session",
                  "iotdb_bound": "one exact source/version and fixed-query plan; new cursor/IPC/result every read",
                  "iotdb_runtime": "fresh daemon per tier/case/mode; bound also fresh for every Q",
                  "iotdb_server_cache": "not reset", "fuse_attr_timeout_seconds": 1,
                  "native_cli_startup_in_query_ns": False},
              "fixed_session_scope": "Private root-only 0600 socket; explicit experimental flag only on bound daemons; immutable query and source/permission assumptions; maximum five-minute TTL; not a production authorization cache.",
              "durability_semantics": "Both arms fsync the file and close. Parent setup and daemon startup are excluded. Backend WAL policy is unchanged; equivalent power-loss durability is not claimed.",
              "validation_outside_query_timing": True}
    base.save(output / "data-manifest.json", data)
    base.save(output / "environment.json", environment)
    base.save(output / "campaign.json", result)
    for campaign in range(args.campaigns):
        work = output / f"campaign-{campaign}"
        work.mkdir()
        fs = work / "fs"
        fs.mkdir()
        prefix = "/single-file-" + uuid.uuid4().hex[:16]
        order = ["fs", "iotdb"] if campaign % 2 == 0 else ["iotdb", "fs"]
        item = {"campaign": campaign, "prefix": prefix, "sources": {},
                "unbound_cases": [], "bound_cases": [], "query_order": order,
                "mode_order": ["unbound", "bound"] if campaign % 2 == 0 else ["bound", "unbound"]}
        result["campaigns"].append(item)
        with base.private_mount(daemon, work / "publisher", env, args.writer_lock,
                                profile=args.profile) as context:
            (context["mount"] / prefix.lstrip("/")).mkdir()
            for tier, source in sources.items():
                logical = prefix + "/" + tier + ".h5"
                destinations = {"fs": fs / (tier + ".h5"),
                                "iotdb": context["mount"] / logical.lstrip("/")}
                writes = {arm: base.checked_copy(args.manifest.parent / source["file"], destinations[arm])
                          for arm in order}
                version, locator_ns = base.bind_version(context["socket"], logical)
                base.require(all(path.stat().st_size == source["bytes"] for path in destinations.values()),
                             "published size differs")
                item["sources"][tier] = {"logical_path": logical, "version": version,
                    "raw_sha256": source["sha256"], "bytes": source["bytes"], "writes": writes,
                    "source_locator_ns": locator_ns}
                base.save(output / "campaign.json", result)
        for tier, source in sources.items():
            bound = item["sources"][tier]
            for name, case in data["cases"].items():
                for mode in item["mode_order"]:
                    record = {"tier": tier, "case": name, "source_shape": source["shape"],
                              "selection": case, "query_order": order}
                    if mode == "unbound":
                        observations = {"fs": None, "iotdb": []}
                        with base.private_mount(daemon, work / f"{tier}-{name}-unbound", env,
                                                args.writer_lock, profile=args.profile) as context:
                            for arm in order:
                                if arm == "fs":
                                    observations[arm], native_wall = base.native_wave(
                                        probe, fs / (tier + ".h5"), source, case, max(args.q_values), env)
                                else:
                                    offset = 0
                                    for index in range(max(args.q_values)):
                                        row = base.query_iotdb(context, bound["logical_path"],
                                                               bound["version"], source, case)
                                        profile, offset = consume_profile(context, offset, "typed", args.profile)
                                        if args.profile:
                                            row["server_profile"] = profile
                                        row["index"] = index
                                        observations[arm].append(row)
                            summaries = {arm: base.summary(bound["writes"][arm]["total_ns"],
                                bound["source_locator_ns"] if arm == "iotdb" else 0,
                                observations[arm], args.q_values) for arm in order}
                            record.update(observations=observations, summary=summaries,
                                daemon_startup_ns=context["startup_ns"], native_probe_process_wall_ns=native_wall)
                        item["unbound_cases"].append(record)
                    else:
                        record["sessions"] = []
                        item["bound_cases"].append(record)
                        for q in args.q_values:
                            sessions = {}
                            with base.private_mount(daemon, work / f"{tier}-{name}-bound-q{q}", env,
                                args.writer_lock, profile=args.profile, frozen=True) as context:
                                for arm in order:
                                    if arm == "fs":
                                        sessions[arm] = native_bound_wave(probe, fs / (tier + ".h5"),
                                                                          source, case, q, env)
                                    else:
                                        sessions[arm] = bound_iotdb_session(context, bound, source,
                                                                             case, q, args.profile)
                                summaries = {arm: session_summary(bound["writes"][arm]["total_ns"],
                                    bound["source_locator_ns"] if arm == "iotdb" else 0,
                                    sessions[arm]["binding"]["total_ns"], sessions[arm]["release"]["total_ns"],
                                    sessions[arm]["observations"], q) for arm in order}
                                record["sessions"].append({"q": q, "arms": sessions, "summary": summaries,
                                                          "daemon_startup_ns": context["startup_ns"]})
                            base.save(output / "campaign.json", result)
                    base.save(output / "campaign.json", result)
        with base.private_mount(daemon, work / "verify", env, args.writer_lock, profile=False) as context:
            for tier, source in sources.items():
                bound = item["sources"][tier]
                logical = Path(bound["logical_path"])
                mounted = context["mount"] / str(logical.parent).lstrip("/") / (
                    "." + logical.stem + "_" + str(bound["version"]) + logical.suffix)
                base.require(base.sha_file(mounted) == source["sha256"] and
                             base.sha_file(fs / (tier + ".h5")) == source["sha256"], "full copied SHA differs")
                bound["full_copy_sha_verified"] = True
        base.save(output / "campaign.json", result)
    result["complete"] = True
    result["acceptance_semantics"] = "Correctness and performance acceptance are separate; all latency outcomes are retained. Smoke is not formal Q=1/10/100 evidence."
    base.save(output / "campaign.json", result)
    print(json.dumps({"complete": True, "smoke_only": args.smoke, "report": str(output / "campaign.json")}))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "environment", "writer-lock", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--campaigns", type=int, default=3)
    parser.add_argument("--tiers", default="small,medium,large")
    parser.add_argument("--q-values", default="1,10,100")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--no-profile", dest="profile", action="store_false", default=True)
    args = parser.parse_args(argv)
    args.tiers = args.tiers.split(",")
    args.q_values = [int(x) for x in args.q_values.split(",")]
    base.require(1 <= args.campaigns <= 10, "campaign count must be 1..10")
    base.require(args.tiers and len(set(args.tiers)) == len(args.tiers) and
                 set(args.tiers) <= {"small", "medium", "large"}, "invalid tiers")
    base.require(args.q_values and args.q_values == sorted(set(args.q_values)) and
                 args.q_values[0] == 1 and args.q_values[-1] <= 100, "invalid Q values")
    base.require(args.database.replace("_", "").isalnum() and
                 (args.database == "geostime20261007" or args.database.startswith("singlefile")),
                 "isolated experiment database required")
    if args.smoke:
        args.campaigns, args.tiers, args.q_values = 1, ["small"], [1, 2]
    else:
        base.require(args.q_values == [1, 10, 100] and set(args.tiers) == {"small", "medium", "large"}
                     and args.campaigns == 3, "formal campaign requires three paired campaigns/all tiers/Q=1/10/100")
    return args


if __name__ == "__main__":
    run(parse_args())
