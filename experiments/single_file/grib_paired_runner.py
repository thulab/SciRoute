#!/usr/bin/env python3
"""Paired native FS / typed IoTDB-FS GRIB single-file campaigns.

Every Q is an independent session. FS has no sidecar; bound FS buffers are
explicit experiment caches. Sources and permissions stay frozen during reads.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import numpy as np
import pyarrow as pa

import runner as base
import grib_native_validation as native
from environment import connection_environment
from grib_native_environment import library
from grib_readiness import ATTRS, declaration, equal, read_ipc
from session_runner import consume_profile, session_summary

PROTOCOL = "iotdbfs-grib-single-file-paired/1"
FILES = {"small": "KWBCGFS_CMRA_100_2024031800_GLB_0_0_0P25.grib2",
         "medium": "KWBCGFS_CMRA_100_2024031800_GLB_1_2_0P25.grib2"}
CASES = {"binary_one": {"view": "binary"},
         "point": {"view": "values", "bounds": ((0., 0.), (0., 0.))},
         "roi": {"view": "values", "bounds": ((0., .75), (0., .75))}}


def progress(output, event, **fields):
    with (output / "progress.jsonl").open("a") as file:
        file.write(json.dumps({"event": event, "wall_unix_ns": time.time_ns(), **fields}) + "\n")


def prepare_sources(raw_dir, tiers):
    sources = {}
    for tier in tiers:
        raw = raw_dir / FILES[tier]
        rows = json.loads(subprocess.check_output([
            "grib_ls", "-x", "-j", "-p",
            "offset:i,totalLength:i,shortName,typeOfLevel,level:i,step:i,Ni:i,Nj:i,packingType,bitsPerValue:i",
            str(raw)], text=True, timeout=300))["messages"]
        position = 0
        for row in rows:
            row["offset"], row["totalLength"] = int(row["offset"]), int(row["totalLength"])
            base.require(row["offset"] == position and row["totalLength"] > 0,
                         "source has noncontiguous native message ranges")
            position += row["totalLength"]
        base.require(position == raw.stat().st_size, "native directory omits source bytes")
        chosen = [r for r in rows if r["shortName"] == "clwmr" and
                  r["typeOfLevel"] == "isobaricInhPa" and r["level"] == 500 and r["step"] == 0]
        base.require(len(chosen) == 1 and chosen[0]["Ni"] == 1440 and chosen[0]["Nj"] == 721 and
                     chosen[0]["packingType"] == "grid_complex_spatial_differencing" and
                     chosen[0]["bitsPerValue"] == 16, "selected GFS geometry/packing differs")
        source = {"raw": str(raw), "bytes": raw.stat().st_size, "sha256": base.sha_file(raw),
                  "inventory": rows, "selected": chosen[0], "cases": {}}
        for name, description in CASES.items():
            case = dict(description, filter={k: chosen[0][k] for k in
                                            ("shortName", "typeOfLevel", "level", "step")})
            case["oracle"] = native.oracle(raw, rows, case)
            base.require(case["oracle"]["selected_messages"] == 1, "case is not single-message")
            source["cases"][name] = case
        sources[tier] = source
    return sources


def request(path, version, case, kind="typed"):
    names = {"level": "level", "step": "step", "typeOfLevel": "type_of_level", "shortName": "short_name"}
    predicate = {"kind": "and", "operands": [equal(names[k], case["filter"][k]) for k in names]}
    projection = (["grib.message.bytes"] if case["view"] == "binary" else
                  ["grib.grid.latitude", "grib.grid.longitude", "grib.data.value"])
    slices = None
    if "bounds" in case:
        slices = [{"dimension": field,
                   "lower": {"kind": "inclusive", "value": {"type": "float64", "value": low}},
                   "upper": {"kind": "inclusive", "value": {"type": "float64", "value": high}}}
                  for field, (low, high) in zip(projection[:2], case["bounds"])]
    return declaration(path, version, projection, predicate, slices, kind)


def locator(context, path, source):
    # Obtain one exact version, rather than transferring every message's
    # attributes merely to learn its source identity. The case's labels/offset
    # are validated here; full bytes are independently checked after queries.
    predicate = request(path, None, source["cases"]["binary_one"])["request"]["subset"]["predicate"]
    start = time.perf_counter_ns()
    response, _ = base.control(context["socket"], declaration(path, None,
                                ["grib.message." + a for a in ATTRS], predicate))
    table, references = read_ipc(response["path"])
    end = time.perf_counter_ns()
    expected = [(r["shortName"], r["typeOfLevel"], r["level"], r["step"], r["offset"])
                for r in [source["selected"]]]
    base.require(list(zip(*(c.to_pylist() for c in table.columns))) == expected,
                 "published attributes differ from raw native directory")
    versions = {s["version"] for s in references}
    base.require(len(versions) == 1 and all(s["path"] == path and s["format_id"] == "grib"
                 for s in references), "source locator omitted exact GRIB identity")
    return versions.pop(), end - start


def extract(reader, view):
    """Mapped buffers stay in this function; returned arrays/bytes are owned."""
    batches, metadata = [], []
    for item in reader.iter_batches_with_custom_metadata():
        batches.append(item.batch)
        metadata.append(dict(item.custom_metadata or {}))
    table = pa.Table.from_batches(batches, schema=reader.schema)
    decoded = time.perf_counter_ns()
    if view == "binary":
        owned = {"values": [value.as_py() for value in table.column(0)],
                 "mask": np.ones(table.num_rows, dtype=np.uint8)}
    else:
        base.require(table.num_columns == 3 and
                     table.column(0).type == pa.float64() and table.column(1).type == pa.float64() and
                     table.column(2).type == pa.float32(), "typed output schema differs")
        owned = {"latitude": np.array(table.column(0).to_numpy(), copy=True),
                 "longitude": np.array(table.column(1).to_numpy(), copy=True),
                 "values": np.array(table.column(2).to_numpy(), copy=True),
                 "mask": np.asarray(table.column(2).is_valid().to_numpy(), dtype=np.uint8).copy()}
    return owned, metadata, table.num_rows, decoded


def query(context, path, version, case, prepared_id=None):
    start = time.perf_counter_ns()
    spec = ({"kind": "experiment_typed_read", "prepared_id": prepared_id} if prepared_id else
            request(path, version, case))
    response, registration_ns = base.control(context["socket"], spec)
    before_open = time.perf_counter_ns()
    stream = pa.memory_map(response["path"], "r")
    opened = time.perf_counter_ns()
    try:
        reader = pa.ipc.open_stream(stream)
        try:
            owned, metadata, rows, decoded = extract(reader, case["view"])
        finally:
            reader.close()
            del reader
    finally:
        stream.close()
    closed = time.perf_counter_ns()
    # Identity, dimensions and digests are correctness checks outside wall timing.
    shapes = []
    for meta in metadata:
        base.require(meta.get(b"iotdbfs.sources") == b"1" and
                     meta[b"iotdbfs.source.0.path"].decode() == path and
                     int(meta[b"iotdbfs.source.0.version"]) == version and
                     meta[b"iotdbfs.source.0.format_id"] == b"grib", "exact source provenance differs")
        if case["view"] == "values":
            base.require(meta[b"iotdbfs.domain.present"] == b"1", "missing numerical domain")
            shapes.append([int(meta[("iotdbfs.domain.dimension." + str(i) + ".shape").encode()])
                           for i in range(2)])
    base.require(rows == case["oracle"]["rows"], "typed row count differs")
    if case["view"] == "values":
        base.require(shapes == [case["oracle"]["messages"][0]["shape"]], "typed selected shape differs")
        value_bytes = owned["values"].astype("<f4", copy=False).tobytes()
        latitude = owned["latitude"].astype("<f8", copy=False).tobytes()
        longitude = owned["longitude"].astype("<f8", copy=False).tobytes()
    else:
        value_bytes, latitude, longitude = b"".join(owned["values"]), b"", b""
    digest = lambda value: hashlib.sha256(value).hexdigest()
    hashes = {"value_sha256": digest(value_bytes), "latitude_sha256": digest(latitude),
              "longitude_sha256": digest(longitude), "mask_sha256": digest(owned["mask"].tobytes())}
    base.require(all(hashes[k] == case["oracle"][k] for k in hashes), "typed hashes differ from raw oracle")
    return {"rows": rows, "total_ns": closed - start, "registration_ns": registration_ns,
            "mmap_open_ns": opened - before_open, "ipc_decode_ns": decoded - opened,
            "owned_copy_close_ns": closed - decoded, "result_path": response["path"], **hashes}


def check_profile(profile, case, index, bound):
    counters, io = profile["counters"], profile["iotdb_delta"]
    base.require(counters.get("typed_cursor_next_calls", 0) > 0 and
                 counters.get("typed_ipc_batches", 0) > 0 and
                 counters.get("typed_ipc_rows") == case["oracle"]["rows"], "cursor/encoding not executed")
    if bound:
        base.require(counters.get("typed_prepared_reuse", 0) == 1 and
                     counters.get("registration_catalog_calls", 0) == 0 and
                     counters.get("typed_prepare_calls", 0) == 0 and
                     io["metadata_query_calls"] == 0 and io["layout_calls"] == 0,
                     "bound read repeated discovery/planning SQL")
    size = sum(case["oracle"]["message_lengths"]) if index == 0 else 0
    base.require(io["payload_requested_bytes"] == size and io["payload_returned_bytes"] == size and
                 io["payload_calls"] == (1 if size else 0), "message pruning or reuse differs")


def iotdb_wave(context, path, version, case, q, bound):
    binding_ns = release_ns = offset = 0
    prepared_id, binding_profile, release_profile = None, None, None
    if bound:
        response, binding_ns = base.control(context["socket"], request(path, version, case,
                                           "experiment_typed_bind"))
        prepared_id = response["prepared_id"]
        binding_profile, offset = consume_profile(context, offset, "experiment_typed_bind", True)
    rows, paths, released = [], set(), False
    try:
        for index in range(q):
            row = query(context, path, version, case, prepared_id)
            base.require(row["result_path"] not in paths, "old registered IPC reused")
            paths.add(row["result_path"])
            profile, offset = consume_profile(context, offset,
                         "experiment_typed_read" if bound else "typed", True)
            check_profile(profile, case, index, bound)
            row.update(index=index, server_profile=profile)
            rows.append(row)
    finally:
        if bound:
            response, release_ns = base.control(context["socket"], {
                "kind": "experiment_typed_release", "prepared_id": prepared_id})
            released = response.get("released") is True
            base.require(released, "fixed binding not released")
            release_profile, offset = consume_profile(context, offset, "experiment_typed_release", True)
    return {"binding_ns": binding_ns, "release_ns": release_ns, "observations": rows,
            "released": released, "binding_profile": binding_profile, "release_profile": release_profile,
            "prepared_id": prepared_id}


def native_wave(probe, path, source, case, q, bound, env):
    command = native.command(probe, path, case, q, bound) + ["--max-working-bytes", "67108864"]
    start = time.perf_counter_ns()
    completed = subprocess.run(command, env=env, capture_output=True, text=True, timeout=900)
    wall_ns = time.perf_counter_ns() - start
    base.require(completed.returncode == 0, "native probe failed: " + completed.stderr[-500:])
    rows = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
    measured = native.validate_rows(rows, case["oracle"], q, bound, source["bytes"])
    return {"binding_ns": measured["binding_ns"], "release_ns": measured["release_ns"],
            "observations": rows, "native_process_wall_ns": wall_ns}


def main(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = json.loads(args.environment.read_text())
    native_env = json.loads(args.native_environment.read_text())
    daemon, probe = Path(environment["daemon"]), Path(native_env["probe"]["path"])
    base.require(base.sha_file(daemon) == environment["binaries"]["daemon"]["sha256"] ==
                 native_env["daemon_sha256"] and base.sha_file(probe) == native_env["probe"]["sha256"],
                 "native/daemon binary changed")
    base.require(library(probe) == library(daemon, environment["runtime_library_path"]) ==
                 native_env["native_eccodes"], "ecCodes runtime identities differ")
    sources = prepare_sources(args.raw_dir, args.tiers)
    base.save(output / "data-manifest.json", {"protocol": "iotdbfs-grib-paired-inputs/1", "sources": sources})
    base.save(output / "environment.json", environment)
    base.save(output / "native-environment.json", native_env)
    env = dict(os.environ, **connection_environment(args.source_pid, args.database))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    result = {"protocol": PROTOCOL, "complete": False, "smoke_only": args.smoke,
              "q_values": args.q_values, "tiers": args.tiers, "campaigns": [], "database": args.database,
              "data_manifest_sha256": base.sha_file(output / "data-manifest.json"),
              "daemon_sha256": base.sha_file(daemon), "native_sha256": base.sha_file(probe),
              "cache_semantics": {"fs_sidecar": False, "fs_working_budget_bytes": 67108864,
                  "iotdb_grib_decoded_cache_bytes": 67108864, "iotdb_grib_decoded_max_fields": 16,
                  "iotdb_grib_index_cache_bytes": 67108864, "iotdb_grib_index_max_versions": 16,
                  "iotdb_message_cache_entries": 32, "binary_cache_asymmetry": False,
                  "iotdb_encoded_cache_shared_bytes": 268435456,
                  "iotdb_encoded_cache_shared_max_entries": 65536,
                  "binary_cache_policy": "shared bounded QueryRuntime encoded fragments; first miss reads, subsequent hits reuse",
                  "fresh_daemon_each_case_mode_q": True, "os_pages": "warmed; not dropped",
                  "database_cache": "not reset", "fuse_attr_timeout_seconds": 1},
              "timing_scope": "Native query phases versus complete typed registration+IPC open+owned consumption; hashes/JSON and native process startup excluded and process wall recorded separately.",
              "frozen_session_scope": "Explicit private root-only fixed-query lease; no source deletion or permission mutation during session.",
              "publication_scope": "Same 1 MiB copy + file fsync + close; directory setup excluded; database WAL policy unchanged; power-loss durability not equated."}
    base.save(output / "campaign.json", result)
    progress(output, "prepared", tiers=args.tiers)
    for index in range(args.campaigns):
        work = output / f"campaign-{index}"
        work.mkdir()
        fs = work / "fs"
        fs.mkdir()
        prefix = "/single-grib-" + uuid.uuid4().hex[:16]
        order = ["fs", "iotdb"] if index % 2 == 0 else ["iotdb", "fs"]
        item = {"index": index, "prefix": prefix, "query_order": order,
                "mode_order": ["unbound", "bound"] if index % 2 == 0 else ["bound", "unbound"],
                "sources": {}, "cases": []}
        result["campaigns"].append(item)
        with base.private_mount(daemon, work / "publisher", env, args.writer_lock) as context:
            (context["mount"] / prefix.lstrip("/")).mkdir()
            for tier, source in sources.items():
                logical = prefix + "/" + tier + ".grib2"
                destinations = {"fs": fs / (tier + ".grib2"),
                                "iotdb": context["mount"] / logical.lstrip("/")}
                progress(output, "publication_started", campaign=index, tier=tier)
                writes = {arm: base.checked_copy(source["raw"], destinations[arm]) for arm in order}
                version, source_locator_ns = locator(context, logical, source)
                item["sources"][tier] = {"raw": source["raw"], "logical_path": logical, "version": version,
                    "bytes": source["bytes"], "sha256": source["sha256"], "writes": writes,
                    "source_locator_ns": source_locator_ns}
                base.save(output / "campaign.json", result)
                progress(output, "published", campaign=index, tier=tier, version=version)
        for tier, source in sources.items():
            bound_source = item["sources"][tier]
            for name, case in source["cases"].items():
                for mode in item["mode_order"]:
                    record = {"tier": tier, "case": name, "mode": mode, "selection": case, "sessions": []}
                    item["cases"].append(record)
                    for q in args.q_values:
                        progress(output, "session_started", campaign=index, tier=tier, case=name, mode=mode, q=q)
                        arms = {}
                        with base.private_mount(daemon, work / f"{tier}-{name}-{mode}-q{q}", env,
                                                args.writer_lock, frozen=mode == "bound") as context:
                            for arm in order:
                                if arm == "fs":
                                    arms[arm] = native_wave(probe, fs / (tier + ".grib2"), source, case,
                                                            q, mode == "bound", env)
                                else:
                                    arms[arm] = iotdb_wave(context, bound_source["logical_path"],
                                        bound_source["version"], case, q, mode == "bound")
                            summaries = {arm: session_summary(bound_source["writes"][arm]["total_ns"],
                                bound_source["source_locator_ns"] if arm == "iotdb" else 0,
                                arms[arm]["binding_ns"], arms[arm]["release_ns"], arms[arm]["observations"], q)
                                for arm in order}
                            record["sessions"].append({"q": q, "arms": arms, "summary": summaries,
                                                       "daemon_startup_ns": context["startup_ns"]})
                        base.save(output / "campaign.json", result)
                        progress(output, "session_complete", campaign=index, tier=tier, case=name, mode=mode, q=q)
        with base.private_mount(daemon, work / "verify", env, args.writer_lock, profile=False) as context:
            for tier, source in sources.items():
                entry = item["sources"][tier]
                path = Path(entry["logical_path"])
                exact = context["mount"] / str(path.parent).lstrip("/") / (
                    "." + path.stem + "_" + str(entry["version"]) + path.suffix)
                base.require(base.sha_file(exact) == source["sha256"] and
                             base.sha_file(fs / (tier + ".grib2")) == source["sha256"] and
                             base.sha_file(source["raw"]) == source["sha256"], "full source SHA changed")
                entry["full_copy_sha_verified"] = True
        base.save(output / "campaign.json", result)
        progress(output, "campaign_complete", campaign=index)
    result["complete"] = True
    base.save(output / "campaign.json", result)
    progress(output, "complete")
    print(json.dumps({"complete": True, "report": str(output / "campaign.json")}))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", type=Path, required=True)
    parser.add_argument("--native-environment", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--database", default="h09probe1790939889dbfc1f")
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    base.require(args.database == "h09probe1790939889dbfc1f", "use the isolated GRIB pilot namespace")
    args.campaigns, args.tiers, args.q_values = (1, ["small"], [1, 2]) if args.smoke else (
        3, ["small", "medium"], [1, 10, 100])
    return args


if __name__ == "__main__":
    main(parse_args())
