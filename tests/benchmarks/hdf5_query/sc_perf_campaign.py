#!/usr/bin/env python3
"""Matched A--E OISST attribute-discovery and scientific-query timing.

This experiment uses only new paths in an existing isolated H09 database. It
keeps source publication and native catalog construction outside online query
timings, preserves raw observations, and rejects any cross-path value mismatch.
"""
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid


PROTOCOL = "iotdbfs-hdf5-sc-perf-campaign/1"
PREDICATE = "Daily sea surface temperature"
SCENARIOS = {
    "point": ([0.0, 0.25], [180.0, 180.25]),
    "roi": ([-1.0, 1.0], [180.0, 182.0]),
    "full": (None, None),
    "negative": ([91.0, 92.0], None),
}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def control(path, request):
    data = json.dumps(request, allow_nan=False, separators=(",", ":")).encode()
    require(len(data) <= 1 << 20, "control request exceeds 1 MiB")
    start = time.perf_counter_ns()
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(300)
        connection.connect(str(path))
        connection.sendall(struct.pack(">I", len(data)) + data)
        def receive(size):
            parts = bytearray()
            while len(parts) < size:
                block = connection.recv(size - len(parts))
                require(block, "short control response")
                parts.extend(block)
            return bytes(parts)
        size, = struct.unpack(">I", receive(4))
        require(0 < size <= 1 << 20, "control response exceeds 1 MiB")
        response = json.loads(receive(size))
    elapsed = time.perf_counter_ns() - start
    require(response.get("ok"), f"registration failed: {response}")
    return response, elapsed, len(data), size


def native_catalog(raw):
    import h5py
    start = time.perf_counter_ns()
    selected = []
    for source in raw:
        with h5py.File(source, "r") as file:
            dataset = file["/sst"]
            label = dataset.attrs.get("long_name")
            if isinstance(label, bytes):
                label = label.decode("utf-8")
            if label == PREDICATE:
                selected.append(source.name)
    return sorted(selected), time.perf_counter_ns() - start


def prebuilt_catalog(raw):
    import h5py
    start = time.perf_counter_ns()
    entries = []
    for source in raw:
        with h5py.File(source, "r") as file:
            label = file["/sst"].attrs.get("long_name")
            if isinstance(label, bytes):
                label = label.decode("utf-8")
            entries.append({"source": source.name, "dataset": "/sst", "long_name": label})
    return entries, time.perf_counter_ns() - start


def fnv64(values, validity):
    import numpy as np
    require(values.dtype == np.float64, "expected owned Float64")
    require(len(values) == len(validity), "validity length mismatch")
    if len(values) == 0:
        return "cbf29ce484222325"
    canonical = np.where(validity, values, 0.0).astype("<f8", copy=False)
    interleaved = np.empty((len(values), 9), dtype=np.uint8)
    interleaved[:, 0] = validity.astype(np.uint8, copy=False)
    interleaved[:, 1:] = canonical.view(np.uint8).reshape(-1, 8)
    state = 14695981039346656037
    for byte in memoryview(interleaved).cast("B"):
        state = ((state ^ byte) * 1099511628211) & 0xffffffffffffffff
    return f"{state:016x}"


def query_request(source, scenario):
    latitude, longitude = SCENARIOS[scenario]
    slices = []
    for field, bounds in (("hdf5.coord.latitude", latitude),
                          ("hdf5.coord.longitude", longitude)):
        if bounds:
            slices.append({"dimension": field,
                           "lower": {"kind": "inclusive", "value": {"type": "float64", "value": bounds[0]}},
                           "upper": {"kind": "exclusive", "value": {"type": "float64", "value": bounds[1]}}})
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": source["logical_path"]}],
                    "versions": {"kind": "exact", "exact": source["version"]}},
        "subset": {"projection": ["hdf5.science.value:%2Fsst"], "slices": slices}}}


def timed_ipc(path):
    import numpy as np
    import pyarrow as pa
    start = time.perf_counter_ns()
    with pa.memory_map(path, "r") as stream:
        table = pa.ipc.open_stream(stream).read_all()
        require(table.num_columns == 1, "IPC must contain one scientific value")
        column = table.column(0)
        validity = column.is_valid().to_numpy().copy()
        values = column.to_numpy(zero_copy_only=False).astype(np.float64, copy=True)
        values[~validity] = 0.0
        require(values.flags.owndata and validity.flags.owndata, "IPC output is not owned")
    elapsed = time.perf_counter_ns() - start
    validation_start = time.perf_counter_ns()
    digest = fnv64(values, validity)
    result = {"ipc_owned_ns": elapsed, "rows": len(values),
              "masked": int((~validity).sum()), "owned_value_bytes": int(values.nbytes + validity.nbytes),
              "ipc_file_bytes": Path(path).stat().st_size,
              "digest_fnv64": digest}
    del table, column, values, validity
    gc.collect()
    result["validation_cleanup_ns"] = time.perf_counter_ns() - validation_start
    return result


def run_native(binary, source, scenario, passes, path, campaign):
    command = [str(binary), str(source), "/sst", scenario, str(passes)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=600)
    require(result.returncode == 0, f"native {path} failed: {result.stderr[-2000:]}")
    entries = [json.loads(line) for line in result.stdout.splitlines()]
    require(len(entries) == passes * (len(SCENARIOS) if scenario == "all" else 1),
            "native output pass count mismatch")
    for entry in entries:
        require(entry["protocol"] == "iotdbfs-hdf5-sc-native/1", "native protocol mismatch")
        entry.update(path=path, campaign=campaign, source_id=Path(source).stem,
                     case=entry["scenario"])
    return entries


def run_direct(binary, root, path, sources, cases, passes, cache_bytes, config, campaign, env):
    direct_sources = [{"id": source["id"], "local_path": source["raw"],
                       "logical_path": source["logical_path"], "version": source["version"]}
                      for source in sources]
    specification = {"protocol": "iotdbfs-hdf5-sc-perf-run/1", "path": path,
                     "config": str(config), "sources": direct_sources, "cases": cases,
                     "passes": passes, "cache_bytes": cache_bytes,
                     "parallelism": 1, "target_batch_rows": 65536}
    input_path = root / f"{path}-request.json"
    output_path = root / f"{path}-raw.jsonl"
    save(input_path, specification)
    with (root / f"{path}-log.txt").open("w") as log:
        result = subprocess.run([str(binary), str(input_path), str(output_path)],
                                stdout=log, stderr=subprocess.STDOUT, env=env, timeout=1800)
    require(result.returncode == 0, f"{path} direct probe failed; see {path}-log.txt")
    entries = [json.loads(line) for line in output_path.read_text().splitlines()]
    require(entries and entries[0]["kind"] == "setup", "direct setup row missing")
    for entry in entries:
        entry["campaign"] = campaign
    return entries


def start_mount(binary, mount, env, log_path, socket_path):
    mount.mkdir()
    with log_path.open("wb") as log:
        process = subprocess.Popen([str(binary), "-f", "-o", "attr_timeout=0,entry_timeout=0", str(mount)],
                                   env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 90
    try:
        while not (os.path.ismount(mount) and socket_path.exists()):
            require(process.poll() is None, f"mount exited; see {log_path}")
            require(time.monotonic() < deadline, f"mount timed out; see {log_path}")
            time.sleep(.1)
    except Exception:
        if os.path.ismount(mount):
            subprocess.run(["fusermount", "-u", str(mount)], check=False, timeout=30)
        process.wait(timeout=30)
        raise
    return process


def stop_mount(mount, process):
    if os.path.ismount(mount):
        subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30)
    process.wait(timeout=30)


def catalog_request(prefix):
    return {"kind": "dataset_catalog", "request": {
        "sources": {"paths": [{"kind": "subtree", "path": prefix}]},
        "dataset_path": "/sst",
        "predicate": {"kind": "compare", "field": "hdf5.attr:long_name", "operator": "eq",
                      "value": {"type": "utf8", "value": PREDICATE}}}}


def case_records(sources):
    return [{"id": f"{source['id']}:{scenario}", "source": source["id"], "dataset": "/sst",
             **({"latitude": latitude} if latitude else {}),
             **({"longitude": longitude} if longitude else {})}
            for source in sources for scenario, (latitude, longitude) in SCENARIOS.items()]


def validate_observations(observations, cases, passes, paths):
    expected = {(path, p, case["id"]) for path in paths for p in range(passes) for case in cases}
    actual = {}
    for observation in observations:
        key = (observation["path"], observation["pass"], observation["case_id"])
        require(key not in actual, f"duplicate query: {key}")
        actual[key] = observation
    require(set(actual) == expected, f"query matrix incomplete: missing {len(expected-set(actual))}")
    for case in cases:
        base = actual[("A", 0, case["id"])]
        for path in paths:
            for p in range(passes):
                item = actual[(path, p, case["id"])]
                require((item["rows"], item["masked"], item["digest_fnv64"]) ==
                        (base["rows"], base["masked"], base["digest_fnv64"]),
                        f"value/mask mismatch for {path}, pass {p}, {case['id']}")


def main(args):
    import h5py
    import numpy as np
    import pyarrow as pa
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
    from a12_migration_smoke import connection_environment
    require(args.database.startswith("h09probe"), "requires existing isolated h09probe database")
    require(1 <= args.campaigns <= 3 and 1 <= args.passes <= 8, "campaign/pass limit")
    raw = [file.resolve() for file in args.raw]
    require(2 <= len(raw) <= 31 and len(set(raw)) == len(raw), "need 2..31 unique days")
    for file in raw:
        require(file.is_file() and file.stat().st_size <= 64 << 20, "source missing or too large")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    build = args.build.resolve()
    native = build / "bin/sc_perf_native"
    direct = build / "bin/sc_perf_direct"
    daemon = build / "bin/iotdb_fs"
    for binary in (native, direct, daemon):
        require(binary.is_file(), f"binary missing: {binary}")
    manifest = {"protocol": PROTOCOL, "complete": False, "database": args.database,
                "source_pid": args.source_pid, "writer_lock": str(args.writer_lock.resolve()),
                "cache_bytes": args.cache_bytes, "campaigns": args.campaigns, "passes": args.passes,
                "paths": "ABCDE", "cases": SCENARIOS,
                "versions": {"h5py": h5py.__version__, "numpy": np.__version__,
                             "pyarrow": pa.__version__, "python": sys.version, "platform": platform.platform()},
                "sources": [{"id": f"day{i+1:02}", "raw": str(file),
                             "sha256": sha(file), "bytes": file.stat().st_size,
                             "url": "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/202401/" + file.name}
                            for i, file in enumerate(raw)], "runs": []}
    save(root / "campaign.json", manifest)
    entries, build_ns = prebuilt_catalog(raw)
    save(root / "fs-catalog.json", {"entries": entries, "construction_ns": build_ns})
    require(len(entries) == len(raw), "prebuilt catalog cardinality")
    env = connection_environment(args.source_pid, args.database)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock.resolve())
    config = root / "profile.properties"
    config.write_text(f"query.hdf5.read_parallelism=1\nquery.cache.memory_budget_bytes={args.cache_bytes}\n"
                      "query.cache.max_entries=65536\ncache.read.full_prefetch_max_bytes=1\n"
                      "cache.raw_file.max_bytes=1\n")
    env["IOTDB_CONFIG_FILE"] = str(config)
    prefix = "/h09-scperf-" + uuid.uuid4().hex[:12]
    seed = root / "seed"
    seed.mkdir()
    socket_path = Path("/tmp") / f"hsc-{os.getpid()}-seed.sock"
    env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(seed / "profile.jsonl")
    process = start_mount(daemon, seed / "mount", env, seed / "mount.log", socket_path)
    try:
        (seed / "mount" / prefix[1:]).mkdir()
        for source in manifest["sources"]:
            source["logical_path"] = prefix + "/" + source["id"] + ".nc"
            destination = seed / "mount" / source["logical_path"].lstrip("/")
            start = time.perf_counter_ns()
            shutil.copyfile(source["raw"], destination)
            source["publish_ns"] = time.perf_counter_ns() - start
            require(sha(destination) == source["sha256"], "published original bytes mismatch")
        found, elapsed, request_bytes, response_bytes = control(socket_path, catalog_request(prefix))
        save(seed / "catalog.json", {"response": found, "client_ns": elapsed,
                                    "request_bytes": request_bytes, "response_bytes": response_bytes})
        mapping = {row["path"]: row for row in found["datasets"]}
        require(set(mapping) == {source["logical_path"] for source in manifest["sources"]},
                "exact catalog source set differs from native")
        for source in manifest["sources"]:
            source["version"] = mapping[source["logical_path"]]["version"]
            source["layout_digest"] = mapping[source["logical_path"]]["layout_digest"]
        save(root / "campaign.json", manifest)
    finally:
        stop_mount(seed / "mount", process)
    all_cases = case_records(manifest["sources"])
    for campaign in range(args.campaigns):
        work = root / f"campaign-{campaign}"
        work.mkdir()
        mount = work / "mount"
        socket_path = Path("/tmp") / f"hsc-{os.getpid()}-{campaign}.sock"
        current = env.copy()
        current["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
        current["IOTDBFS_QUERY_PROFILE_JSONL"] = str(work / "server-profile.jsonl")
        process = start_mount(daemon, mount, current, work / "mount.log", socket_path)
        observations = []
        catalog = {}
        try:
            selected, scan_ns = native_catalog(raw)
            require(selected == sorted(file.name for file in raw), "native attr discovery differs")
            catalog["native_scan_ns"] = scan_ns
            start = time.perf_counter_ns()
            fs_selected = sorted(row["source"] for row in entries if row["long_name"] == PREDICATE)
            catalog["fs_prebuilt_filter_ns"] = time.perf_counter_ns() - start
            require(fs_selected == selected, "prebuilt FS catalog differs")
            response, elapsed, request_bytes, response_bytes = control(socket_path, catalog_request(prefix))
            catalog.update(iotdb_client_ns=elapsed, iotdb_request_bytes=request_bytes,
                           iotdb_response_bytes=response_bytes,
                           iotdb_selected_sources=response["selected_sources"],
                           iotdb_candidate_datasets=response["candidate_datasets"],
                           iotdb_sql_queries=response["sql_queries"],
                           iotdb_catalog_read_bytes=response["catalog_read_bytes"])
            require({(row["path"], row["version"]) for row in response["datasets"]} ==
                    {(row["logical_path"], row["version"]) for row in manifest["sources"]},
                    "IoTDB exact catalog differs from frozen source set")
            save(work / "catalog-timing.json", catalog)
            order = "ABCDE" if campaign % 2 == 0 else "EDCBA"
            for path in order:
                if path in "AC":
                    for source in manifest["sources"]:
                        file = Path(source["raw"]) if path == "A" else mount / source["logical_path"].lstrip("/")
                        rows = run_native(native, file, "all", args.passes, path, campaign)
                        save(work / f"{path}-{source['id']}.json", rows)
                        observations.extend({"path": path, "pass": row["pass"],
                            "case_id": f"{source['id']}:{row['scenario']}", "rows": row["rows"],
                            "masked": row["masked"], "digest_fnv64": row["digest_fnv64"],
                            "latency_ns": row["total_ns"], "setup_ns": row["setup_ns"]} for row in rows)
                elif path in "BD":
                    rows = run_direct(direct, work, path, manifest["sources"], all_cases,
                                      args.passes, args.cache_bytes, config, campaign, current)
                    save(work / f"{path}-parsed.json", rows)
                    observations.extend({"path": path, "pass": row["pass"],
                        "case_id": row["case_id"], "rows": row["rows"],
                        "masked": row["masked"], "digest_fnv64": row["fnv1a64"],
                        "latency_ns": row["latency_ns"]} for row in rows if row["kind"] == "query")
                else:
                    raw_e = []
                    for p in range(args.passes):
                        for case in all_cases:
                            source = next(row for row in manifest["sources"] if row["id"] == case["source"])
                            response, registration_ns, request_bytes, response_bytes = control(
                                socket_path, query_request(source, case["id"].split(":", 1)[1]))
                            owned = timed_ipc(response["path"])
                            item = {"path": "E", "pass": p, "case_id": case["id"],
                                    "logical_path": source["logical_path"], "version": source["version"],
                                    "registration_ns": registration_ns, "request_bytes": request_bytes,
                                    "response_bytes": response_bytes, "result_path": response["path"],
                                    "latency_ns": registration_ns + owned["ipc_owned_ns"], **owned}
                            raw_e.append(item)
                            observations.append(item)
                    save(work / "E-raw.json", raw_e)
            validate_observations(observations, all_cases, args.passes, "ABCDE")
            save(work / "observations.json", observations)
            profile_path = work / "server-profile.jsonl"
            profiles = [json.loads(line) for line in profile_path.read_text().splitlines()]
            require(len([p for p in profiles if p["operation"] == "typed" and p["ok"]]) ==
                    len(all_cases) * args.passes, "E server profile cardinality mismatch")
            save(work / "profile-index.json", {"profiles": len(profiles),
                "typed": len(all_cases) * args.passes,
                "catalog": len([p for p in profiles if p["operation"] == "dataset_catalog"])})
            manifest["runs"].append({"campaign": campaign, "directory": str(work),
                                    "queries": len(observations), "validated": True,
                                    "path_order": order, "catalog": catalog})
            save(root / "campaign.json", manifest)
        finally:
            stop_mount(mount, process)
    manifest["complete"] = True
    save(root / "campaign.json", manifest)
    print(json.dumps({"complete": True, "campaigns": args.campaigns,
                      "query_observations": sum(row["queries"] for row in manifest["runs"]),
                      "report": str(root / "campaign.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--raw", type=Path, nargs="+", required=True)
    parser.add_argument("--campaigns", type=int, default=3)
    parser.add_argument("--passes", type=int, default=4)
    parser.add_argument("--cache-bytes", type=int, default=64 << 20)
    main(parser.parse_args())
