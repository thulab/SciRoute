#!/usr/bin/env python3
"""Single-file raw FS versus complete typed IoTDB-FS query campaign.

No FS sidecar, no native coordinate pre-read, no repeated IPC-only read counted
as a new query. Generation, runtime deployment and hashes stay outside query
timing; source-version binding is included in lifecycle totals separately.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import statistics
import struct
import subprocess
import time
import uuid

PROTOCOL = "iotdbfs-single-file-campaign/1"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha_file(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def percentile(values, fraction):
    ordered = sorted(values)
    require(bool(ordered), "empty latency sample")
    index = (len(ordered) - 1) * fraction
    lo, hi = math.floor(index), math.ceil(index)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (index - lo)


def query_request(path, version, case):
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": path}],
                    "versions": {"kind": "exact", "exact": version}},
        "subset": {"projection": ["hdf5.value:%2FT"], "slices": [
            {"dimension": f"hdf5.axis.{axis}",
             "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": origin}},
             "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": origin + extent}}}
            for axis, (origin, extent) in enumerate(zip(case["origin"], case["shape"]))]}}}


def receive(stream, count):
    result = bytearray()
    while len(result) < count:
        part = stream.recv(count - len(result))
        require(bool(part), "short control response")
        result.extend(part)
    return bytes(result)


def control(socket_path, request):
    start = time.perf_counter_ns()
    payload = json.dumps(request, separators=(",", ":"), allow_nan=False).encode()
    require(len(payload) <= 1 << 20, "control request exceeds frame budget")
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(300)
        stream.connect(str(socket_path))
        stream.sendall(struct.pack(">I", len(payload)) + payload)
        length, = struct.unpack(">I", receive(stream, 4))
        require(0 < length <= 1 << 20, "control response exceeds frame budget")
        response = json.loads(receive(stream, length))
    elapsed = time.perf_counter_ns() - start
    require(response.get("ok") is True, "query registration failed: " + str(response.get("error", "unknown")))
    return response, elapsed


def profiles_since(path, offset):
    path = Path(path)
    if not path.exists():
        return [], offset
    with path.open("rb") as stream:
        stream.seek(offset)
        lines = stream.readlines()
        return [json.loads(line) for line in lines if line.strip()], stream.tell()


def checked_copy(source, destination):
    """Same user-space 1 MiB transfer and explicit file fsync on both arms."""
    destination = Path(destination)
    require(not destination.exists(), "destination already exists")
    start = time.perf_counter_ns()
    source_stream = Path(source).open("rb", buffering=0)
    target = destination.open("xb", buffering=0)
    opened = time.perf_counter_ns()
    try:
        for block in iter(lambda: source_stream.read(1 << 20), b""):
            pending = memoryview(block)
            while pending:
                written = target.write(pending)
                require(written is not None and written > 0, "short destination write")
                pending = pending[written:]
        copied = time.perf_counter_ns()
        target.flush()
        os.fsync(target.fileno())
        synced = time.perf_counter_ns()
    finally:
        target.close()
        source_stream.close()
    closed = time.perf_counter_ns()
    return {"open_ns": opened - start, "copy_ns": copied - opened,
            "file_fsync_ns": synced - copied, "close_ns": closed - synced,
            "total_ns": closed - start,
            "completion": "explicit file fsync acknowledgement and close; excludes parent setup",
            "parent_directory_fsync_included": False}


@contextmanager
def private_mount(daemon, root, env, writer_lock, *, profile=True, frozen=False,
                  grib_index_cache_max_versions=None, startup_timeout_s=90):
    root = Path(root)
    root.mkdir(exist_ok=False)
    mount = root / "mount"
    mount.mkdir()
    socket_path = Path("/tmp") / ("sf-" + uuid.uuid4().hex[:16] + ".sock")
    config = root / "profile.properties"
    config.write_text("query.hdf5.read_parallelism=1\nquery.cache.memory_budget_bytes=268435456\n"
                      "query.cache.max_entries=65536\ncache.read.full_prefetch_max_bytes=1\n"
                      "cache.raw_file.max_bytes=1\nwriter.transfer_chunk_bytes=1048576\n"
                      "writer.manifest_batch_max_bytes=8388608\n")
    if grib_index_cache_max_versions is not None:
        require(type(grib_index_cache_max_versions) is int and
                1 <= grib_index_cache_max_versions <= 65536,
                "invalid experimental GRIB parsed-index capacity")
        with config.open("a") as properties:
            properties.write(f"query.grib.index_cache.max_versions={grib_index_cache_max_versions}\n")
    config.chmod(0o600)
    current = env.copy()
    if frozen:
        current["IOTDBFS_EXPERIMENT_FROZEN_TYPED"] = "1"
    else:
        current.pop("IOTDBFS_EXPERIMENT_FROZEN_TYPED", None)
    current.update(IOTDB_CONFIG_FILE=str(config), IOTDBFS_QUERY_SOCKET=str(socket_path),
                   IOTDBFS_NAMESPACE_LOCK=str(Path(writer_lock).resolve()))
    for key in ("HDF5_DRIVER", "HDF5_VOL_CONNECTOR", "HDF5_PLUGIN_PATH"):
        require(key not in current, f"{key} override forbidden")
    if profile:
        current.update(IOTDBFS_QUERY_PROFILE_JSONL=str(root / "queries.jsonl"),
                       IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL=str(root / "authorization.jsonl"),
                       IOTDBFS_PUBLICATION_PROFILE_JSONL=str(root / "publication.jsonl"))
    else:
        for name in ("IOTDBFS_QUERY_PROFILE_JSONL", "IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL",
                     "IOTDBFS_PUBLICATION_PROFILE_JSONL"):
            current.pop(name, None)
    log_path = root / "mount.log"
    log = log_path.open("xb")
    log_path.chmod(0o600)
    process = subprocess.Popen([str(daemon), "-f", "-o", "attr_timeout=1,entry_timeout=1", str(mount)],
                               env=current, stdout=log, stderr=subprocess.STDOUT)
    started = time.perf_counter_ns()
    try:
        require(0 < startup_timeout_s <= 900, "invalid daemon startup timeout")
        deadline = time.monotonic() + startup_timeout_s
        while not (os.path.ismount(mount) and socket_path.exists()):
            require(process.poll() is None, "private daemon exited; private mount.log is not exported")
            require(time.monotonic() < deadline, "private daemon startup timed out")
            time.sleep(.05)
        yield {"mount": mount, "socket": socket_path, "root": root,
               "startup_ns": time.perf_counter_ns() - started,
               "config_sha256": sha_file(config), "pid": process.pid}
    finally:
        try:
            if os.path.ismount(mount):
                subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        finally:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=30)
            finally:
                log.close()


def bind_version(socket_path, logical_path):
    response, elapsed = control(socket_path, {"kind": "dataset_catalog", "request": {
        "sources": {"paths": [{"kind": "exact", "path": logical_path}],
                    "versions": {"kind": "latest"}}, "dataset_path": "/T"}})
    rows = response.get("datasets", [])
    require(len(rows) == 1 and rows[0]["path"] == logical_path, "exact source binding failed")
    return rows[0]["version"], elapsed


def validate_descriptor(metadata, path, version, full_shape, case):
    prefix = b"iotdbfs.result_descriptor.0."
    expected = {"protocol": "iotdbfs-hdf5-subarray/1", "source.path": path,
                "source.version": str(version), "dataset.path": "/T", "dataset.rank": "4",
                "output.value_type": "float32", "order": "C", "selection.row_count": str(case["rows"])}
    for axis in range(4):
        expected.update({f"axis.{axis}.full_extent": str(full_shape[axis]),
                         f"axis.{axis}.selection_origin": str(case["origin"][axis]),
                         f"axis.{axis}.selection_extent": str(case["shape"][axis])})
    require(metadata.get(b"iotdbfs.result_descriptors") == b"1", "missing result descriptor")
    for key, value in expected.items():
        require(metadata.get(prefix + key.encode()) == value.encode(), f"descriptor differs: {key}")


def query_iotdb(context, logical_path, version, source, case, *, prepared_id=None):
    import numpy as np
    import pyarrow as pa
    start = time.perf_counter_ns()
    request = (query_request(logical_path, version, case) if prepared_id is None else
               {"kind": "experiment_typed_read", "prepared_id": prepared_id})
    response, registration_ns = control(context["socket"], request)
    ready = time.perf_counter_ns()
    result_path = Path(response["path"])
    require(os.path.commonpath([result_path, context["mount"]]) == str(context["mount"]),
            "result path escapes owned mount")
    with pa.memory_map(str(result_path), "r") as stream:
        mapped = time.perf_counter_ns()
        table = col = None
        try:
            table = pa.ipc.open_stream(stream).read_all()
            decoded = time.perf_counter_ns()
            require(table.num_columns == 1 and table.num_rows == case["rows"], "IPC rows/columns differ")
            col = table.column(0)
            require(col.type == pa.float32(), "IPC dtype differs")
            values = np.array(col.to_numpy(zero_copy_only=False), dtype="<f4", copy=True, order="C")
            valid = np.asarray(col.is_valid().to_numpy(zero_copy_only=False), dtype=np.uint8).copy()
            metadata = dict(table.schema.metadata or {})
            ipc_bytes = stream.size()
        finally:
            # Remove mapped-buffer owners even when an exception traceback
            # retains this frame, so owned FUSE mounts can be unmounted.
            table = col = None
    end = time.perf_counter_ns()
    validate_descriptor(metadata, logical_path, version, source["shape"], case)
    require(hashlib.sha256(values.tobytes()).hexdigest() == case["value_sha256"], "IPC value hash differs")
    require(hashlib.sha256(valid.tobytes()).hexdigest() == case["mask_sha256"], "IPC mask hash differs")
    return {"total_ns": end - start, "registration_ns": registration_ns,
            "mmap_open_ns": mapped - ready, "ipc_decode_ns": decoded - mapped,
            "owned_copy_and_close_ns": end - decoded, "rows": len(values), "dtype": "<f4",
            "ipc_bytes": ipc_bytes, "value_bytes": values.nbytes, "mask_bytes": valid.nbytes,
            "value_sha256": case["value_sha256"], "mask_sha256": case["mask_sha256"],
            "result_path": str(result_path)}


def native_wave(probe, file, source, case, count, env):
    args = [str(probe), str(file), "/T", ",".join(map(str, case["origin"])),
            ",".join(map(str, case["shape"])), str(count)]
    begin = time.perf_counter_ns()
    result = subprocess.run(args, capture_output=True, text=True, timeout=300, env=env)
    require(result.returncode == 0, "native probe failed: " + result.stderr[-300:])
    rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    require(len(rows) == count, "native probe omitted observations")
    for row in rows:
        require(row["runtime_hdf5"] == "1.14.4" and row["library_threadsafe"], "native library mismatch")
        for key in ("rows", "dtype", "value_sha256", "mask_sha256"):
            require(row[key] == case[key], "native result differs: " + key)
        require(row["origin"] == case["origin"] and row["shape"] == case["shape"], "native selection differs")
        require(row["source_shape"] == source["shape"], "native full source shape differs")
    return rows, time.perf_counter_ns() - begin


def summary(write_ns, binding_ns, observations, qs):
    times = [row["total_ns"] for row in observations]
    require(len(times) >= max(qs), "not enough actual observations for Q totals")
    warm = times[1:]
    return {"queries": len(times), "first_query_ns": times[0],
            "all_p50_ns": statistics.median(times), "all_p95_ns": percentile(times, .95),
            "warm_p50_ns": statistics.median(warm) if warm else None,
            "warm_p95_ns": percentile(warm, .95) if warm else None,
            "query_sum_ns_by_q": {str(q): sum(times[:q]) for q in qs},
            "lifecycle_ns_by_q": {str(q): write_ns + binding_ns + sum(times[:q]) for q in qs},
            "write_ns": write_ns, "exact_source_binding_ns": binding_ns}


def run(args):
    from environment import connection_environment
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    data = json.loads(args.manifest.read_text())
    require(data["protocol"] == "iotdbfs-single-file-hdf5-data/1" and
            data["all_real_size_gates_passed"] and not data["test_only_scaled"], "unaccepted size-tier manifest")
    environment = json.loads(args.environment.read_text())
    require(environment["native_versions"]["runtime_hdf5"] == "1.14.4" and
            environment["python_versions"]["hdf5_version"] == "1.14.4", "environment not unified to HDF5 1.14.4")
    daemon, probe = Path(environment["daemon"]), Path(environment["native_probe"])
    require(sha_file(daemon) == environment["binaries"]["daemon"]["sha256"] and
            sha_file(probe) == environment["binaries"]["native_probe"]["sha256"], "environment binaries changed")
    env = dict(os.environ, **connection_environment(args.source_pid, args.database))
    env["LD_LIBRARY_PATH"] = environment["runtime_library_path"]
    sources = {tier: data["sources"][tier] for tier in args.tiers}
    for source in sources.values():
        path = args.manifest.parent / source["file"]
        require(path.stat().st_size == source["bytes"] and sha_file(path) == source["sha256"], "source identity changed")
    result = {"protocol": PROTOCOL, "complete": False, "smoke_only": args.smoke,
              "data_manifest_sha256": sha_file(args.manifest), "environment_sha256": sha_file(args.environment),
              "hdf5_runtime": "1.14.4", "database": args.database, "q_values": args.q_values,
              "campaigns": [], "synthetic_controlled": True,
              "cache_semantics": {"os_page_cache": "not dropped; whole-source SHA warms source pages",
                  "fs_external_index": False, "native_file_handles": "open/dataset/read/copy/close per query",
                  "iotdb_runtime": "fresh daemon per tier/case; retained for repetitions within that case",
                  "iotdb_server_cache": "not reset", "fuse_attr_timeout_seconds": 1,
                  "native_cli_startup_in_query_ns": False, "native_cli_wall_ns_reported_separately": True},
              "durability_semantics": "Both arms explicitly fsync the file then close. FS fsync returns kernel acknowledgement; IoTDB-FS fsync commits and awaits backend publication acknowledgement. Parent-directory creation is setup; database WAL/fsync policy is not overridden or claimed equivalent power-loss durability.",
              "profiling": args.profile, "validation_outside_query_timing": True}
    save(output / "campaign.json", result)
    for campaign in range(args.campaigns):
        work = output / f"campaign-{campaign}"
        work.mkdir()
        fs = work / "fs"
        fs.mkdir()
        prefix = "/single-file-" + uuid.uuid4().hex[:16]
        item = {"campaign": campaign, "prefix": prefix, "sources": {}, "cases": []}
        result["campaigns"].append(item)
        with private_mount(daemon, work / "publisher", env, args.writer_lock, profile=args.profile) as context:
            (context["mount"] / prefix.lstrip("/")).mkdir()
            for tier, source in sources.items():
                raw = args.manifest.parent / source["file"]
                logical = prefix + "/" + tier + ".h5"
                destinations = {"fs": fs / (tier + ".h5"), "iotdb": context["mount"] / logical.lstrip("/")}
                writes = {}
                for arm in (["fs", "iotdb"] if campaign % 2 == 0 else ["iotdb", "fs"]):
                    writes[arm] = checked_copy(raw, destinations[arm])
                version, binding_ns = bind_version(context["socket"], logical)
                require(destinations["fs"].stat().st_size == source["bytes"] and
                        destinations["iotdb"].stat().st_size == source["bytes"], "published size differs")
                item["sources"][tier] = {"logical_path": logical, "version": version,
                    "raw_sha256": source["sha256"], "bytes": source["bytes"], "writes": writes,
                    "exact_source_binding_ns": binding_ns}
                save(output / "campaign.json", result)
        for tier, source in sources.items():
            bound = item["sources"][tier]
            for name, case in data["cases"].items():
                observations = {"fs": None, "iotdb": []}
                with private_mount(daemon, work / (tier + "-" + name), env, args.writer_lock,
                                   profile=args.profile) as context:
                    for arm in (["fs", "iotdb"] if campaign % 2 == 0 else ["iotdb", "fs"]):
                        if arm == "fs":
                            observations["fs"], native_wall = native_wave(probe, fs / (tier + ".h5"), source, case, max(args.q_values), env)
                        else:
                            query_offset = 0
                            for index in range(max(args.q_values)):
                                row = query_iotdb(context, bound["logical_path"], bound["version"], source, case)
                                profiles, query_offset = profiles_since(context["root"] / "queries.jsonl", query_offset)
                                if args.profile:
                                    require(len(profiles) == 1 and profiles[0].get("ok"), "profile/request count differs")
                                    row["server_profile"] = profiles[0]
                                row["index"] = index
                                observations["iotdb"].append(row)
                    summaries = {arm: summary(bound["writes"][arm]["total_ns"],
                                              bound["exact_source_binding_ns"] if arm == "iotdb" else 0,
                                              observations[arm], args.q_values) for arm in observations}
                    ratio = summaries["iotdb"]["all_p50_ns"] / summaries["fs"]["all_p50_ns"]
                    item["cases"].append({"tier": tier, "case": name, "source_shape": source["shape"],
                        "selection": case, "observations": observations, "summary": summaries,
                        "native_probe_process_wall_ns": native_wall,
                        "daemon_startup_ns": context["startup_ns"], "query_order": ["fs", "iotdb"] if campaign % 2 == 0 else ["iotdb", "fs"],
                        "iotdb_fs_p50_ratio": ratio, "under_10x_target": ratio < 10})
                save(output / "campaign.json", result)
        # Full original-byte verification after all timing. This includes large
        # files and deliberately warms cache only after this campaign's queries.
        with private_mount(daemon, work / "verify", env, args.writer_lock, profile=False) as context:
            for tier, source in sources.items():
                bound = item["sources"][tier]
                logical = Path(bound["logical_path"])
                mounted = context["mount"] / str(logical.parent).lstrip("/") / (
                    "." + logical.stem + "_" + str(bound["version"]) + logical.suffix)
                require(sha_file(mounted) == source["sha256"], "full published payload SHA differs")
                require(sha_file(fs / (tier + ".h5")) == source["sha256"], "full FS copy SHA differs")
                bound["full_copy_sha_verified"] = True
        save(output / "campaign.json", result)
    result["complete"] = True
    result["acceptance_semantics"] = "Correctness completion is independent of the under-10x latency target; failures of the performance target are reported, never discarded. Smoke is preparation evidence, not the formal Q=1/10/100 campaign."
    save(output / "campaign.json", result)
    print(json.dumps({"complete": True, "smoke_only": args.smoke, "report": str(output / "campaign.json")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--environment", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--campaigns", type=int, default=3)
    parser.add_argument("--tiers", default="small,medium,large")
    parser.add_argument("--q-values", default="1,10,100")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--no-profile", dest="profile", action="store_false", default=True)
    args = parser.parse_args()
    args.tiers = args.tiers.split(",")
    args.q_values = [int(x) for x in args.q_values.split(",")]
    require(1 <= args.campaigns <= 10, "campaign count must be 1..10")
    require(args.tiers and len(set(args.tiers)) == len(args.tiers) and
            set(args.tiers) <= {"small", "medium", "large"}, "invalid tiers")
    require(args.q_values and args.q_values == sorted(set(args.q_values)) and
            args.q_values[0] == 1 and args.q_values[-1] <= 100, "invalid Q values")
    require(args.database.replace("_", "").isalnum() and
            (args.database == "geostime20261007" or args.database.startswith("singlefile")), "isolated experiment database required")
    if args.smoke:
        args.campaigns, args.tiers, args.q_values = 1, ["small"], [1, 2]
    else:
        require(args.q_values == [1, 10, 100] and set(args.tiers) == {"small", "medium", "large"},
                "formal campaign must include all tiers and Q=1/10/100; use --smoke for preparation")
    run(args)


if __name__ == "__main__":
    main()
