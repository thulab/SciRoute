#!/usr/bin/env python3
"""H11 fixed A–E comparisons. Server lifecycle/deployment are external."""
from __future__ import annotations
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
from urllib.parse import quote

PREP = "iotdbfs-hdf5-h11-prepared/1"
RUN = "iotdbfs-hdf5-h11-run/1"
RESULT = "iotdbfs-hdf5-h11-probe/1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    def invalid(value):
        raise ValueError("nonfinite JSON number: " + value)
    return json.loads(Path(path).read_text(), object_pairs_hook=unique, parse_constant=invalid)


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha_file(path):
    result = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def case_identity(source, case):
    value = {key: case[key] for key in ("dataset", "origin", "shape", "label")}
    value["source_sha256"] = source["sha256"]
    return sha_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def prepare(args):
    import h5py
    import numpy as np
    require(not args.output.exists(), "prepare needs a fresh output directory")
    locked = read_json(Path(__file__).with_name("public-data.lock.json"))
    public = next(item for item in locked["files"] if item["id"] == "H1")
    require(sha_file(args.public_file) == public["sha256"], "GWOSC source differs from locked H1 original")
    args.output.mkdir(parents=True)
    rng = np.random.default_rng(20261006)
    data = (rng.normal(size=(512, 256)) + np.arange(512)[:, None] / 64).astype("<f8")
    sources = []
    for direction, chunk in (("row", (64, 8)), ("column", (8, 64))):
        for codec in ("raw", "deflate"):
            sid = f"{direction}_{codec}"
            path = args.output / (sid + ".hdf5")
            with h5py.File(path, "w") as native:
                options = dict(chunks=chunk)
                if codec == "deflate":
                    options.update(compression="gzip", compression_opts=1, shuffle=True, fletcher32=True)
                dataset = native.create_dataset("signal", data=data, **options)
                dataset.attrs["units"] = np.bytes_("synthetic amplitude")
                native.attrs["fixture_seed"] = np.int64(20261006)
            sources.append(dict(id=sid, local_path=str(path.resolve()), dataset="/signal", codec=codec))
    sources.append(dict(id="gwosc", local_path=str(args.public_file.resolve()), dataset="/strain/Strain", codec="original"))
    cases = []
    for source in sources:
        source["sha256"] = sha_file(source["local_path"])
        source["file_bytes"] = Path(source["local_path"]).stat().st_size
        with h5py.File(source["local_path"], "r") as native:
            datasets = []
            native.visititems(lambda name, item: datasets.append(name) if isinstance(item, h5py.Dataset) else None)
            source["dataset_count"] = len(datasets)
            dataset = native[source["dataset"]]
            source.update(shape=list(dataset.shape), chunk_shape=list(dataset.chunks), dtype=str(dataset.dtype),
                          decoded_dataset_bytes=dataset.size * dataset.dtype.itemsize)
            if source["id"] == "gwosc":
                definitions = [("small", [3], [64]), ("aligned", [2048], [2048]),
                    ("misaligned", [2049], [2048]), ("crosschunk", [2047], [8194]),
                    ("overlap", [4096], [4096]), ("nonoverlap", [65536], [32768])]
            else:
                chunk = list(dataset.chunks)
                definitions = [("small", [3, 5], [8, 4]), ("aligned", [0, 0], chunk),
                    ("misaligned", [1, 1], chunk), ("crosschunk", [0, 127], [512, 2]),
                    ("overlap", [128, 127], [256, 2]), ("nonoverlap", [256, 192], [256, 64])]
            definitions.append(("pressure_scan", [0] * dataset.ndim, list(dataset.shape)))
            for label, origin, shape in definitions:
                selection = tuple(slice(o, o+s) for o, s in zip(origin, shape))
                array = np.asarray(dataset[selection])
                canonical = array.astype(array.dtype.newbyteorder("<"), copy=False).tobytes(order="C")
                case = dict(source=source["id"], dataset=source["dataset"], label=label,
                    origin=origin, shape=shape, dtype=str(array.dtype), value_bytes=len(canonical),
                    values_sha256=sha_bytes(canonical), selection_fraction=math.prod(shape) / dataset.size)
                allocated = []
                for index in range(dataset.id.get_num_chunks()):
                    chunk_info = dataset.id.get_chunk_info(index)
                    if all(c < o+n and o < c+w for c, o, n, w in zip(chunk_info.chunk_offset, origin, shape, dataset.chunks)):
                        allocated.append(dict(origin=list(chunk_info.chunk_offset), offset=chunk_info.byte_offset,
                                              encoded_bytes=chunk_info.size))
                case["selected_chunks"] = allocated
                case["native_chunk_decoded_bytes"] = len(allocated) * math.prod(dataset.chunks) * dataset.dtype.itemsize
                case["id"] = case_identity(source, case)
                cases.append(case)
    manifest = dict(protocol=PREP, sources=sources, cases=cases, oracle=dict(h5py=h5py.__version__,
        hdf5=h5py.version.hdf5_version, numpy=np.__version__), seed=20261006,
        metadata_predicate=dict(field="sigma.hdf5.datasets", operator="gt", value=1),
        scope="original public bytes plus labelled synthetic chunk-direction fixtures; no derived-layout claim")
    dump(args.output / "manifest.json", manifest)
    print(json.dumps({"manifest": str(args.output / "manifest.json"), "sources": len(sources), "cases": len(cases)}))


def load_prepared(path):
    prepared = read_json(path)
    require(prepared.get("protocol") == PREP, "wrong prepared protocol")
    sources = {item["id"]: item for item in prepared["sources"]}
    require(len(sources) == len(prepared["sources"]) and sources, "duplicate or empty sources")
    seen = set()
    for case in prepared["cases"]:
        require(case["source"] in sources, "unknown case source")
        require(case["id"] == case_identity(sources[case["source"]], case), "edited case identity")
        require(case["id"] not in seen, "duplicate case")
        seen.add(case["id"])
    return prepared


# Remaining execution/validation helpers are defined below.

def selected_cases(prepared, scenario):
    if scenario == "pressure":
        return [case for case in prepared["cases"] if case["label"] == "pressure_scan"]
    eligible = {source["id"] for source in prepared["sources"]
                if scenario != "catalog" or source["dataset_count"] > 1}
    return [case for case in prepared["cases"] if case["source"] in eligible and case["label"] != "pressure_scan"]


def inputs(args):
    prepared = load_prepared(args.prepared)
    deployment = read_json(args.deployment)
    require(deployment.get("protocol") == "iotdbfs-hdf5-h11-deployment/1", "wrong deployment protocol")
    deployed = {source["id"]: source for source in deployment["sources"]}
    require(len(deployed) == len(deployment["sources"]), "duplicate deployed source")
    combined = []
    for source in prepared["sources"]:
        require(source["id"] in deployed, "source missing from deployment")
        require(sha_file(source["local_path"]) == source["sha256"], "original source changed")
        item = {**source, **deployed[source["id"]]}
        require(type(item["version"]) is int and item["version"] > 0, "invalid exact version")
        require(item["logical_path"].startswith("/"), "source path must be absolute")
        item.setdefault("fuse_path", str(Path(deployment["mount"]) / item["logical_path"].lstrip("/")))
        combined.append(item)
    return prepared, deployment, combined


def catalogue_selection(prepared, deployment, sources, directory):
    import pyarrow as pa
    from h08_mount_smoke import register, profile_record
    start = time.perf_counter_ns()
    selected = sorted(source["id"] for source in prepared["sources"] if source["dataset_count"] > 1)
    fs_ns = time.perf_counter_ns() - start
    request = {"kind": "typed", "request": {"sources": {
        "paths": [{"kind": "exact", "path": source["logical_path"]} for source in sources],
        "versions": {"kind": "latest"}, "file_predicate": {"kind": "compare",
            "field": "sigma.hdf5.datasets", "operator": "gt", "value": {"type": "int64", "value": 1}}}}}
    start = time.perf_counter_ns()
    route = register(deployment["socket"], request)
    with pa.memory_map(route, "r") as mapped:
        table = pa.ipc.open_stream(mapped).read_all()
        paths = table.column("source.path").to_pylist()
        versions = table.column("source.version").to_pylist()
    service_ns = time.perf_counter_ns() - start
    by_path = {source["logical_path"]: source for source in sources}
    require(len(set(paths)) == len(paths), "catalog selected duplicate paths")
    actual = []
    for path, version in zip(paths, versions):
        require(path in by_path and by_path[path]["version"] == version, "catalog changed source/version")
        actual.append(by_path[path]["id"])
    require(sorted(actual) == selected, "FS/native catalog predicate disagrees with IoTDB predicate")
    profile = profile_record(deployment["profile"], route)
    result = dict(predicate=prepared["metadata_predicate"], candidate_count=len(sources),
                  selected_source_ids=selected, fs_prebuilt_catalog_filter_ns=fs_ns,
                  iotdb_catalog_service_ns=service_ns, server_profile=profile,
                  fs_catalog_build="h5py inventory in prepare; outside query", 
                  pruning_claim="No index-pruning claim; production catalog selection and predicate evaluation")
    dump(directory / "catalog-selection.json", result)
    return result


def native_manifest(path, args, cases, sources, deployment, sidecars):
    return dict(protocol=RUN, path=path, config=deployment.get("config", ""), clients=args.clients,
        passes=args.passes, cache_bytes=args.cache_bytes, cache_policy=args.cache_policy, scenario=args.scenario,
        parallelism=args.parallelism, max_call_ranges=args.max_call_ranges,
        target_batch_rows=args.target_batch_rows,
        sources=[{**source, "source_sha256": source["sha256"], "layout_path": str((sidecars / (source["id"] + ".layout")).resolve())}
                 for source in sources], cases=cases)


def native_run(path, args, cases, sources, deployment, output):
    output.mkdir()
    sidecars = output / "sidecars"
    sidecars.mkdir()
    manifest = native_manifest(path, args, cases, sources, deployment, sidecars)
    workers = args.clients if path in "AC" else 1
    barrier = output / "barrier"
    if workers > 1:
        barrier.mkdir()
    children, logs = [], []
    try:
        for worker in range(workers):
            spec = dict(manifest)
            if path in "AC":
                spec.update(clients=1, cache_bytes=args.cache_bytes // workers)
            if workers > 1:
                spec.update(barrier_dir=str(barrier.resolve()), participant=worker, participants=workers)
            spec_path = output / f"worker-{worker}.manifest.json"
            dump(spec_path, spec)
            log = (output / f"worker-{worker}.log").open("wb")
            logs.append(log)
            children.append(subprocess.Popen([str(args.probe.resolve()), str(spec_path.resolve()),
                                               str((output / f"worker-{worker}.json").resolve())],
                                              stdout=log, stderr=subprocess.STDOUT))
        if workers > 1:
            for repeat in range(args.passes):
                for case_index in range(len(cases)):
                    deadline = time.monotonic() + 300
                    markers = [barrier / f"pass-{repeat}-case-{case_index}-ready-{worker}" for worker in range(workers)]
                    while not all(marker.exists() for marker in markers):
                        require(all(child.poll() is None for child in children), "native child exited before barrier; inspect worker logs")
                        require(time.monotonic() < deadline, "native barrier timed out")
                        time.sleep(.001)
                    marker = barrier / f"pass-{repeat}-case-{case_index}-go"
                    pending_marker = marker.with_suffix(".tmp")
                    pending_marker.write_text(str(time.monotonic_ns() + 10_000_000))
                    os.replace(pending_marker, marker)
        for child in children:
            require(child.wait(timeout=600) == 0, "native probe failed; inspect worker logs")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill(); child.wait(timeout=10)
        for log in logs:
            log.close()
    reports = [read_json(output / f"worker-{worker}.json") for worker in range(workers)]
    require(all(report.get("protocol") == RESULT for report in reports), "wrong native result protocol")
    requests, passes = [], []
    for worker, report in enumerate(reports):
        for request in report["requests"]:
            request = dict(request)
            if path in "AC":
                request["client"] = worker
            requests.append(request)
    for repeat in range(args.passes):
        if workers == 1:
            raw_pass = next(item for item in reports[0]["passes"] if item["pass"] == repeat)
            passes.append(raw_pass)
        else:
            duration = 0
            for case in cases:
                wave = [item for item in requests if item["pass"] == repeat and item["case_id"] == case["id"]]
                require(len(wave) == workers, "multiprocess case wave incomplete")
                duration += max(item["steady_end_ns"] for item in wave) - min(item["steady_start_ns"] for item in wave)
            passes.append(dict(pass_index=repeat, **{"pass": repeat}, wall_ns=duration,
                runtime_delta=None, timing_scope="sum synchronized owned-result case wave envelopes; excludes barriers and hash validation"))
    return dict(protocol=RESULT, path=path, config=manifest, requests=requests, passes=passes,
                setup=[report.get("setup", {}) for report in reports],
                worker_reports=[f"worker-{worker}.json" for worker in range(workers)],
                native_processes=workers, total_cache_bytes=args.cache_bytes,
                actual_native_cache_bytes_per_process=args.cache_bytes // workers if path in "AC" else None)


def e_request(case, source, deployment, rows):
    import pyarrow as pa
    from h08_mount_smoke import declaration, register
    request = declaration(source["logical_path"], source["version"], case["dataset"], case["origin"],
                          [o+s for o, s in zip(case["origin"], case["shape"])], axes=False, rows=rows)
    start = time.monotonic_ns()
    route = register(deployment["socket"], request)
    registered = time.monotonic_ns()
    with pa.memory_map(route, "r") as mapped:
        table = pa.ipc.open_stream(mapped).read_all()
        column = table.column("hdf5.value:" + quote(case["dataset"], safe=""))
        require(column.null_count == 0, "E physical value contains NULL")
        array = column.combine_chunks().to_numpy(zero_copy_only=False).copy()
        metadata = table.schema.metadata
    end = time.monotonic_ns()
    return dict(case_id=case["id"], source_id=case["source"], dataset=case["dataset"],
        origin=case["origin"], shape=case["shape"], dtype=str(array.dtype), value_bytes=array.nbytes,
        steady_start_ns=start, steady_end_ns=end, route=route,
        timings_ns=dict(total=end-start, registration=registered-start, ipc_owned_array=end-registered)), array, metadata


def service_run(args, cases, sources, deployment, output):
    import numpy as np
    import pyarrow as pa
    from h08_mount_smoke import profile_record
    require(args.fresh_runtime, "E requires a fresh isolated runtime declaration")
    require(args.cache_policy == "retain", "E has no remote cache-policy control; use retain")
    output.mkdir()
    # Remove lazy conversion/library initialization from every timed worker.
    pa.array([1.0], type=pa.float64()).to_numpy().copy()
    by_id = {source["id"]: source for source in sources}
    requests, passes, profiles = [], [], []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        for repeat in range(args.passes):
            duration, dispatch_ns, waves = 0, 0, []
            for case in cases:
                started = time.monotonic_ns()
                pending = [pool.submit(e_request, case, by_id[case["source"]], deployment, args.target_batch_rows)
                           for _ in range(args.clients)]
                completed = [future.result() for future in pending]
                dispatched_end = time.monotonic_ns()
                wave_start = min(value[0]["steady_start_ns"] for value in completed)
                wave_end = max(value[0]["steady_end_ns"] for value in completed)
                duration += wave_end - wave_start
                dispatch_ns += dispatched_end - started
                waves.append(dict(case_id=case["id"], steady_start_ns=wave_start,
                                  steady_end_ns=wave_end, dispatch_start_ns=started, dispatch_end_ns=dispatched_end))
                # Hashes and metadata assertions are deliberately outside each case wave.
                for client, (record, values, metadata) in enumerate(completed):
                    record.update({"pass": repeat, "client": client,
                        "values_sha256": sha_bytes(values.astype(values.dtype.newbyteorder("<"), copy=False).tobytes(order="C"))})
                    expected = by_id[case["source"]]
                    prefix = b"iotdbfs.result_descriptor.0."
                    require(metadata[prefix+b"source.path"].decode() == expected["logical_path"] and
                            int(metadata[prefix+b"source.version"]) == expected["version"], "E exact source mismatch")
                    require(metadata[prefix+b"dataset.path"].decode() == case["dataset"], "E dataset identity mismatch")
                    for axis, (origin, shape) in enumerate(zip(case["origin"], case["shape"])):
                        require(int(metadata[prefix+f"axis.{axis}.selection_origin".encode()]) == origin and
                                int(metadata[prefix+f"axis.{axis}.selection_extent".encode()]) == shape, "E rectangle mismatch")
                    profile = profile_record(deployment["profile"], record["route"])
                    require(profile["runtime_config"]["memory_budget_bytes"] == args.cache_bytes and
                            profile["server_hdf5_execution"]["read_parallelism"] == args.parallelism,
                            "E daemon runtime differs from declared configuration")
                    profiles.append(profile)
                    requests.append(record)
            relevant = profiles[-len(cases)*args.clients:]
            passes.append({"pass": repeat, "wall_ns": duration, "dispatch_join_ns": dispatch_ns, "waves": waves,
                "runtime_delta": None, "runtime_after": max(relevant, key=lambda value: value["steady_end_ns"])["runtime_after"],
                "timing_scope": "sum owned-result case wave envelopes; validation/profile lookup excluded",
                "profile_scope": "per-request daemon deltas overlap; never sum across clients"})
            dump(output / "server-profiles.json", profiles)
    config = native_manifest("E", args, cases, sources, deployment, output)
    return dict(protocol=RESULT, path="E", config=config, requests=requests, passes=passes,
                setup={"source_ingestion": "external", "daemon": "fresh external runtime", "python_conversion_warmed": True},
                total_cache_bytes=args.cache_bytes)

def validate_result(result, specification):
    require(result.get("protocol") == RESULT, "wrong result protocol")
    config = result["config"]
    for key in ("path", "clients", "passes", "cache_bytes", "cache_policy", "parallelism", "max_call_ranges", "target_batch_rows"):
        require(config[key] == specification[key], "result configuration mismatch: " + key)
    cases = {case["id"]: case for case in specification["cases"]}
    expected = {(repeat, client, cid) for repeat in range(specification["passes"])
                for client in range(specification["clients"]) for cid in cases}
    actual = set()
    for row in result["requests"]:
        key = (row["pass"], row["client"], row["case_id"])
        require(key not in actual, "duplicate request sample")
        require(key in expected, "unexpected request sample")
        actual.add(key)
        case = cases[row["case_id"]]
        for field, original in (("source_id", "source"), ("dataset", "dataset"), ("origin", "origin"),
                                ("shape", "shape"), ("dtype", "dtype"), ("value_bytes", "value_bytes"),
                                ("values_sha256", "values_sha256")):
            require(row[field] == case[original], "incorrect request " + field)
        require(type(row["timings_ns"]["total"]) is int and row["timings_ns"]["total"] >= 0, "invalid request timing")
        require(type(row["steady_start_ns"]) is int and type(row["steady_end_ns"]) is int and
                row["steady_end_ns"] >= row["steady_start_ns"], "invalid request interval")
    require(actual == expected, "missing request samples")
    for client in range(specification["clients"]):
        order = [(row["pass"], row["case_id"]) for row in result["requests"] if row["client"] == client]
        require(order == [(repeat, case["id"]) for repeat in range(specification["passes"])
                          for case in specification["cases"]], "client trace order changed")
    passes = result["passes"]
    require(len(passes) == specification["passes"] and
            [item["pass"] for item in passes] == list(range(specification["passes"])), "missing/duplicate/reordered pass")
    for item in passes:
        require(type(item["wall_ns"]) is int and item["wall_ns"] >= 0, "invalid pass wall")
    return dict(cold_app_wall_ns=passes[0]["wall_ns"],
                warm_app_mean_wall_ns=statistics.mean(item["wall_ns"] for item in passes[1:]),
                warm_passes=specification["passes"]-1,
                request_count=len(expected), logical_useful_bytes_per_pass=sum(case["value_bytes"] for case in cases.values()) * specification["clients"],
                setup=result["setup"], passes=passes,
                timing_scope="owned-result query case waves; source setup/index build/hash validation outside")


def validate_workers(result, output, specification):
    if result["path"] == "E":
        profiles = read_json(output / "server-profiles.json")
        require(len(profiles) == len(result["requests"]), "missing E server profiles")
        routes = [profile.get("path") for profile in profiles]
        require(len(set(routes)) == len(routes), "duplicate E server profiles")
        by_route = {profile["path"]: profile for profile in profiles}
        require(set(routes) == {row["route"] for row in result["requests"]}, "wrong E profile route")
        for row in result["requests"]:
            profile = by_route[row["route"]]
            require(profile.get("protocol") == "iotdbfs-query-profile/1" and profile.get("ok") is True,
                    "E profile did not complete typed materialization")
            require(profile["runtime_config"]["memory_budget_bytes"] == specification["cache_bytes"] and
                    profile["server_hdf5_execution"]["read_parallelism"] == specification["parallelism"],
                    "E observed configuration mismatch")
            require(row["steady_start_ns"] <= profile["steady_start_ns"] <= profile["steady_end_ns"] <= row["steady_end_ns"],
                    "E server interval outside client query")
        for repeat in result["passes"]:
            waves = repeat["waves"]
            require([wave["case_id"] for wave in waves] == [case["id"] for case in specification["cases"]],
                    "missing E case wave")
            total = dispatch = 0
            for wave in waves:
                rows = [row for row in result["requests"] if row["pass"] == repeat["pass"] and row["case_id"] == wave["case_id"]]
                begin, end = min(row["steady_start_ns"] for row in rows), max(row["steady_end_ns"] for row in rows)
                require(begin == wave["steady_start_ns"] and end == wave["steady_end_ns"], "E wave interval mismatch")
                require(wave["dispatch_start_ns"] <= begin <= end <= wave["dispatch_end_ns"], "E dispatch interval mismatch")
                total += end-begin
                dispatch += wave["dispatch_end_ns"]-wave["dispatch_start_ns"]
            require(total == repeat["wall_ns"] and dispatch == repeat["dispatch_join_ns"], "edited E pass wall time")
        return
    files = result["worker_reports"]
    expected_workers = specification["clients"] if result["path"] in "AC" else 1
    require(len(files) == expected_workers and len(set(files)) == len(files), "wrong native worker count")
    for worker, name in enumerate(files):
        native = read_json(output / name)
        require(native.get("protocol") == RESULT and native.get("ok") is True, "native probe did not finish successfully")
        require(native["config"]["path"] == specification["path"], "native path mismatch")
        for field in ("cache_policy", "parallelism", "max_call_ranges", "target_batch_rows"):
            require(native["config"][field] == specification[field], "native configuration mismatch: " + field)
        count = 1 if result["path"] in "AC" else specification["clients"]
        budget = specification["cache_bytes"] // expected_workers
        require(native["config"]["clients"] == count and native["config"]["cache_bytes"] == budget,
                "native total cache/client allocation mismatch")
        grouped = [row for row in result["requests"] if result["path"] not in "AC" or row["client"] == worker]
        changed = [dict(row, client=0) if result["path"] in "AC" else row for row in grouped]
        require(changed == native["requests"], "normalized request differs from native evidence")
        if expected_workers == 1:
            require(result["passes"] == native["passes"], "normalized pass differs from native evidence")
        if specification["path"] in "BD":
            counters = [entry["runtime_delta"] for entry in native["passes"]]
            require(all(item["unique_decode_trace_complete"] == 1 for item in counters), "unique decode trace is incomplete")
            policy = specification["cache_policy"]
            if specification.get("scenario") == "pressure":
                require(counters[-1]["unique_decoded_bytes_cumulative"] > native["config"]["decoded_cache_budget_bytes"],
                        "pressure working set did not exceed measured decoded budget")
                require(sum(item["evictions"] for item in counters) > 0 and sum(item["redecodes"] for item in counters[1:]) > 0,
                        "pressure run did not observe eviction and redecode")
            if policy == "encoded":
                require(counters[-1]["encoded_cache_hits_cumulative"] > 0, "encoded cache was never hit")
                require(sum(item["decode_count"] for item in counters[1:]) > 0, "encoded-cache run did not redecode")
                require(sum(item["backend_payload_bytes"] for item in counters[1:]) < sum(item["payload_read_bytes"] for item in counters[1:]),
                        "encoded cache did not avoid backend bytes")
            if policy == "evict_before_request":
                require(sum(item["decode_count"] for item in counters[1:]) > 0 and sum(item["redecodes"] for item in counters[1:]) > 0,
                        "evict-before-request did not observe redecode")
    for repeat in result["passes"]:
        total = 0
        for case in specification["cases"]:
            rows = [row for row in result["requests"] if row["pass"] == repeat["pass"] and row["case_id"] == case["id"]]
            total += max(row["steady_end_ns"] for row in rows)-min(row["steady_start_ns"] for row in rows)
        require(total == repeat["wall_ns"], "native pass wall disagrees with owned-read intervals")


def summarize(run, output):
    require(output.resolve() != (run / "experiment.json").resolve(), "summary cannot overwrite manifest")
    dump(output, dict(protocol="iotdbfs-hdf5-h11-summary/1", passed=False, error="validation incomplete"))
    manifest = read_json(run / "experiment.json")
    require(manifest.get("protocol") == "iotdbfs-hdf5-h11-experiment/1", "wrong experiment protocol")
    expected_files = [entry["directory"] + "/result.json" for entry in manifest["runs"]]
    require(len(set(expected_files)) == len(expected_files), "duplicate planned run")
    actual_files = sorted(str(path.relative_to(run)) for path in run.glob("campaign-*/*/result.json"))
    require(sorted(expected_files) == actual_files, "missing or unexpected path/campaign result")
    results, grouped = [], {}
    for entry in manifest["runs"]:
        path = run / entry["directory"]
        result = read_json(path / "result.json")
        summary = validate_result(result, entry["specification"])
        validate_workers(result, path, entry["specification"])
        summary.update(campaign=entry["campaign"], path=entry["path"], directory=entry["directory"])
        results.append(summary)
        grouped.setdefault(entry["path"], []).append(summary)
    aggregate = {}
    for path, independent in grouped.items():
        require(len({row["campaign"] for row in independent}) == len(independent), "duplicate independent campaign")
        values = [row["warm_app_mean_wall_ns"] for row in independent]
        aggregate[path] = dict(independent_campaigns=len(values), warm_campaign_means_ns=values,
            warm_mean_ns=statistics.mean(values), warm_median_ns=statistics.median(values),
            warm_stdev_ns=statistics.stdev(values) if len(values) > 1 else None,
            cold_campaign_values_ns=[row["cold_app_wall_ns"] for row in independent])
    summary = dict(protocol="iotdbfs-hdf5-h11-summary/1", passed=True, matrix=manifest["matrix"],
        runs=results, aggregate=aggregate, evidence_sha256=sha_file(run / "experiment.json"),
        statistical_unit="one campaign mean across warm passes; warm requests are not independent replicates",
        unavailable=["physical disk cold state", "true decoded-cache bypass", "total-process-RSS equalization"],
        constraints=manifest["constraints"])
    dump(output, summary)
    return summary


def run(args):
    require(args.campaigns > 0 and args.passes >= 2, "need independent campaigns and at least one warm pass")
    require(args.paths and len(set(args.paths)) == len(args.paths) and set(args.paths) <= set("ABCDE"), "bad path set")
    require(args.cache_bytes >= 65536 * args.clients, "per-client cache budget too small")
    require(args.cache_policy == "retain" or set(args.paths) <= set("BD"), "diagnostic cache policies apply only to B/D")
    require("E" not in args.paths or (args.campaigns == 1 and args.fresh_runtime), "E needs a new daemon for each independent campaign")
    prepared, deployment, sources = inputs(args)
    cases = selected_cases(prepared, args.scenario)
    require(cases, "empty scenario")
    args.output.mkdir(parents=True, exist_ok=False)
    fs_start = time.perf_counter_ns()
    selected_ids = {case["source"] for case in cases}
    sources = [source for source in sources if source["id"] in selected_ids]
    selection_ns = time.perf_counter_ns() - fs_start
    if args.scenario == "catalog":
        _, _, all_sources = inputs(args)
        catalogue_selection(prepared, deployment, all_sources, args.output)
    matrix = {key: getattr(args, key) for key in ("clients", "cache_bytes", "cache_policy", "parallelism",
        "max_call_ranges", "passes", "scenario", "target_batch_rows")}
    experiment = dict(protocol="iotdbfs-hdf5-h11-experiment/1", matrix=matrix, runs=[],
        source_manifest_sha256=sha_file(args.prepared), probe_sha256=sha_file(args.probe),
        sources=sources, source_selection_ns=selection_ns,
        constraints=dict(application_cache="first trace pass, not every case; independent native process/shared daemon runtime",
            os_database_cache="uncontrolled; previous paths may warm OS/DB caches; no cold disk claim",
            C_mount_cache="daemon raw-file cache configuration external; retain with campaign evidence",
            native_cache="A/C split total raw chunk budget across processes and datasets; native metadata caches extra",
            shared_fs_control="B shared runtime threads with same production kernel as D",
            source_setup="source hashing and deployed ordinary-byte verification outside query timing"))
    for campaign in range(args.campaign_offset, args.campaign_offset + args.campaigns):
        parent = args.output / f"campaign-{campaign}"
        parent.mkdir()
        ordering = args.paths if campaign % 2 == 0 else args.paths[::-1]
        for path in ordering:
            output = parent / path
            specification = native_manifest(path, args, cases, sources, deployment, output / "sidecars")
            experiment["runs"].append(dict(campaign=campaign, path=path,
                directory=str(output.relative_to(args.output)), specification=specification))
    dump(args.output / "experiment.json", experiment)
    for entry in experiment["runs"]:
        output = args.output / entry["directory"]
        result = service_run(args, cases, sources, deployment, output) if entry["path"] == "E" else \
            native_run(entry["path"], args, cases, sources, deployment, output)
        validate_result(result, entry["specification"])
        dump(output / "result.json", result)
    return summarize(args.output, args.output / "summary.json")


def aggregate(campaign_file, output):
    require(output.resolve() != campaign_file.resolve(), "aggregate cannot overwrite its input")
    dump(output, dict(protocol="iotdbfs-hdf5-h11-aggregate/1", passed=False, error="validation incomplete"))
    campaign = read_json(campaign_file)
    require(campaign.get("protocol") == "iotdbfs-hdf5-h11-campaign/1" and campaign.get("complete") is True,
            "campaign did not complete")
    expected = campaign["expected_runs"]
    identities = [(entry["variant"], entry["campaign"]) for entry in expected]
    actual = [(entry["variant"], entry["campaign"]) for entry in campaign["runs"]]
    require(len(set(identities)) == len(identities) and len(set(actual)) == len(actual), "duplicate matrix cell")
    require(set(identities) == set(actual) and identities, "missing or unexpected matrix cell")
    variant_rows = {}
    for entry in expected:
        if "directory" in entry:
            directory = Path(entry["directory"])
            require(not directory.is_absolute() and ".." not in directory.parts, "unsafe matrix directory")
            root = campaign_file.parent / directory
        else:
            root = Path(entry["output"])
            if not root.is_dir():
                root = campaign_file.parent / root.parent.name / root.name
        summary = summarize(root, root / "independent-summary.json")
        require(all(row["campaign"] == entry["campaign"] for row in summary["runs"]), "campaign identity mismatch")
        variant_rows.setdefault(entry["variant"], []).append(summary)
    variants = {}
    for variant, campaigns in variant_rows.items():
        reference = campaigns[0]["matrix"]
        require(all(item["matrix"] == reference for item in campaigns), "configuration changed across independent campaigns")
        path_set = set(campaigns[0]["aggregate"])
        require(all(set(item["aggregate"]) == path_set for item in campaigns), "path coverage changed across campaigns")
        path_results = {}
        for path in sorted(path_set):
            rows = [next(row for row in item["runs"] if row["path"] == path) for item in campaigns]
            require(len({row["campaign"] for row in rows}) == len(rows), "duplicate campaign statistics")
            warm = [row["warm_app_mean_wall_ns"] for row in rows]
            cold = [row["cold_app_wall_ns"] for row in rows]
            path_results[path] = dict(independent_campaigns=len(rows), warm_campaign_means_ns=warm,
                warm_mean_ns=statistics.mean(warm), warm_median_ns=statistics.median(warm),
                warm_stdev_ns=statistics.stdev(warm) if len(warm)>1 else None,
                cold_campaign_values_ns=cold, cold_mean_ns=statistics.mean(cold))
        paired = {}
        for numerator, denominator in (("B", "A"), ("C", "A"), ("D", "B"), ("E", "D"), ("E", "A")):
            if {numerator, denominator} <= path_set:
                ratios = []
                for item in campaigns:
                    a = item["aggregate"][numerator]["warm_mean_ns"]
                    b = item["aggregate"][denominator]["warm_mean_ns"]
                    require(a > 0 and b > 0, "cannot form latency ratio from zero duration")
                    ratios.append(a/b)
                paired[numerator + "_over_" + denominator] = dict(values=ratios, mean=statistics.mean(ratios))
        variants[variant] = dict(matrix=reference, paths=path_results, paired_warm_latency_ratios=paired)
    value = dict(protocol="iotdbfs-hdf5-h11-aggregate/1", passed=True,
                 matrix_cells=len(expected), variants=variants,
                 statistical_unit="independent campaign mean; 3 warm passes are nested observations, not n=3 replicates",
                 cache_scope="application caches only; OS/IoTDB/storage cache uncontrolled",
                 significance="No significance test or population confidence claim")
    dump(output, value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--public-file", type=Path, required=True)
    prep.add_argument("--output", type=Path, required=True)
    execution = sub.add_parser("run")
    execution.add_argument("--prepared", type=Path, required=True)
    execution.add_argument("--deployment", type=Path, required=True)
    execution.add_argument("--probe", type=Path, required=True)
    execution.add_argument("--output", type=Path, required=True)
    execution.add_argument("--campaign-offset", type=int, default=0)
    execution.add_argument("--campaigns", type=int, default=1)
    execution.add_argument("--passes", type=int, default=4)
    execution.add_argument("--clients", type=int, choices=(1, 2, 4), default=1)
    execution.add_argument("--parallelism", type=int, choices=(1, 2, 4), default=1)
    execution.add_argument("--cache-bytes", type=int, default=64*1024*1024)
    execution.add_argument("--cache-policy", choices=("retain", "evict_before_request", "encoded"), default="retain")
    execution.add_argument("--max-call-ranges", type=int, choices=(1, 32), default=32)
    execution.add_argument("--target-batch-rows", type=int, default=4096)
    execution.add_argument("--scenario", choices=("full", "pressure", "catalog"), default="full")
    execution.add_argument("--paths", default="ABCDE")
    execution.add_argument("--fresh-runtime", action="store_true")
    summary = sub.add_parser("summarize")
    summary.add_argument("--run", type=Path, required=True)
    summary.add_argument("--output", type=Path, required=True)
    aggregation = sub.add_parser("aggregate")
    aggregation.add_argument("--campaign", type=Path, required=True)
    aggregation.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args)
    else:
        if args.action == "aggregate":
            result = aggregate(args.campaign, args.output)
            print(json.dumps({"passed": result["passed"], "matrix_cells": result["matrix_cells"]}))
        else:
            result = run(args) if args.action == "run" else summarize(args.run, args.output)
            print(json.dumps({"passed": result["passed"], "runs": len(result["runs"])}))


if __name__ == "__main__":
    main()
