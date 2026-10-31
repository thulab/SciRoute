#!/usr/bin/env python3
"""Fixed H10 traces and strict profile validation; no H11 A–E comparison."""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
from urllib.parse import quote

TRACE_PROTOCOL = "iotdbfs-hdf5-h10-trace/1"
RAW_PROTOCOL = "iotdbfs-hdf5-h10-probe/1"
CLIENT_PROTOCOL = "iotdbfs-hdf5-h10-client/1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    result = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    def reject(value):
        raise ValueError(f"nonfinite JSON number: {value}")
    return json.loads(Path(path).read_text(), object_pairs_hook=unique, parse_constant=reject)


def sample_identity(source_hash, item):
    identity = {key: item[key] for key in ("dataset", "origin", "shape", "phase")}
    identity["source_sha256"] = source_hash
    return digest(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())


def load_trace(path):
    value = read_json(path)
    require(value.get("protocol") == TRACE_PROTOCOL, "wrong trace protocol")
    require(len(value["source_sha256"]) == 64, "invalid source hash")
    require(value["samples"], "empty trace")
    ids = set()
    for item in value["samples"]:
        sid = item["sample_id"]
        require(sid == sample_identity(value["source_sha256"], item), "unstable or edited sample identity")
        require(sid not in ids, "duplicate expected sample")
        ids.add(sid)
        require(item["cache_action"] == "retain", "canonical trace must retain cache")
        require(len(item["origin"]) == len(item["shape"]) in (1, 2, 3), "bad trace rank")
        require(all(type(x) is int and x >= 0 for x in item["origin"] + item["shape"]), "bad coordinates")
        require(item["useful_bytes"] == math.prod(item["shape"]) * item["element_bytes"], "bad useful bytes")
    return value


def write_tsv(path, samples, evict=False):
    lines = []
    for sample in samples:
        require(not any(c in sample["dataset"] for c in "\t\r\n"), "unsafe TSV dataset")
        lines.append("\t".join([sample["sample_id"], sample["dataset"],
            ",".join(map(str, sample["origin"])), ",".join(map(str, sample["shape"])),
            "evict_before_request" if evict else "retain"]))
    Path(path).write_text("\n".join(lines) + "\n")


def prepare(args):
    import h5py
    import numpy as np
    prepared = read_json(args.prepared / "manifest.json")
    require(prepared["protocol"] == "iotdbfs-hdf5-h08-fixture/1", "unknown H08 fixture protocol")
    require(not args.output.exists(), "trace output already exists; choose a fresh directory")
    args.output.mkdir(parents=True)
    cases = [("small", "fixture", "/array", [1, 2], [5, 8], [2, 3], [3, 5]),
             ("gwosc", "public", "/strain/Strain", [2047], [8194], [4096], [4096])]
    for name, key, dataset, origin, shape, overlap_origin, overlap_shape in cases:
        source = Path(prepared[key]).resolve()
        source_hash = file_sha(source)
        require(source_hash == prepared[key + "_sha256"], "prepared source hash changed")
        directory = args.output / name
        directory.mkdir()
        manifest = {"protocol": TRACE_PROTOCOL, "name": name, "source": str(source),
                    "source_sha256": source_hash, "source_bytes": source.stat().st_size,
                    "oracle": {"library": "h5py", "version": h5py.__version__,
                               "hdf5": h5py.version.hdf5_version}, "samples": []}
        with h5py.File(source, "r") as native:
            data = native[dataset]
            for phase, start, count in [("first", origin, shape), ("warm", origin, shape),
                                        ("overlap", overlap_origin, overlap_shape), ("warm_repeat", origin, shape)]:
                selected = np.asarray(data[tuple(slice(o, o+s) for o, s in zip(start, count))])
                canonical = selected.astype(selected.dtype.newbyteorder("<"), copy=False).tobytes(order="C")
                item = {"dataset": dataset, "origin": start, "shape": count, "phase": phase,
                        "cache_action": "retain", "dtype": selected.dtype.newbyteorder("<").str,
                        "element_bytes": selected.dtype.itemsize, "useful_bytes": len(canonical),
                        "value_sha256": digest(canonical), "full_shape": list(data.shape)}
                item["sample_id"] = sample_identity(source_hash, item)
                (directory / (item["sample_id"] + ".oracle.bin")).write_bytes(canonical)
                manifest["samples"].append(item)
        write_tsv(directory / "trace.tsv", manifest["samples"])
        dump(directory / "manifest.json", manifest)
    print(json.dumps({"prepared": str(args.output), "sources": ["small", "gwosc"]}))


def integer_map(value, name):
    require(isinstance(value, dict) and value, f"missing {name}")
    require(all(type(x) is int and x >= 0 for x in value.values()), f"invalid {name} counter")


def match_records(trace, records, name):
    require(isinstance(records, list), f"missing {name} records")
    actual = [item.get("sample_id") for item in records]
    expected = [item["sample_id"] for item in trace["samples"]]
    require(len(set(actual)) == len(actual), f"duplicate {name} sample")
    require(actual == expected, f"missing, unexpected or reordered {name} sample")


def client_read(path, item, values_output=None):
    """Page touches precede IPC reconstruction; ordinary read is measured last."""
    import numpy as np
    import pyarrow as pa
    times = {}
    start = time.perf_counter_ns()
    mapped = pa.memory_map(str(path), "r")
    times["mmap_setup"] = time.perf_counter_ns() - start
    try:
        size = mapped.size()
        before = resource.getrusage(resource.RUSAGE_SELF)
        start = time.perf_counter_ns()
        checksum = 0
        for offset in range(0, size, os.sysconf("SC_PAGE_SIZE")):
            checksum ^= mapped.read_at(1, offset)[0]
        times["page_touch"] = time.perf_counter_ns() - start
        after = resource.getrusage(resource.RUSAGE_SELF)
        mapped.seek(0)
        start = time.perf_counter_ns()
        reader = pa.ipc.open_stream(mapped)
        table = reader.read_all()
        times["arrow_reconstruct"] = time.perf_counter_ns() - start
        value_field = "hdf5.value:" + quote(item["dataset"], safe="")
        start = time.perf_counter_ns()
        column = table.column(value_field)
        require(column.null_count == 0, "unexpected NULL in physical values")
        values = column.combine_chunks().to_numpy(zero_copy_only=False).copy()
        times["numpy_materialize_copy"] = time.perf_counter_ns() - start
        start = time.perf_counter_ns()
        require(values.size == math.prod(item["shape"]), "IPC value cardinality mismatch")
        require(values.dtype == np.dtype(item["dtype"]), "IPC value dtype mismatch")
        canonical = values.astype(np.dtype(item["dtype"]), copy=False).tobytes()
        actual = digest(canonical)
        require(actual == item["value_sha256"], "IPC oracle hash mismatch")
        metadata = {k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()}
        prefix = "iotdbfs.result_descriptor.0."
        require(metadata.get(prefix + "dataset.path") == item["dataset"], "IPC dataset identity missing")
        require(int(metadata[prefix + "dataset.rank"]) == len(item["shape"]), "IPC rank mismatch")
        for axis, (origin, shape) in enumerate(zip(item["origin"], item["shape"])):
            require(int(metadata[prefix + f"axis.{axis}.selection_origin"]) == origin and
                    int(metadata[prefix + f"axis.{axis}.selection_extent"]) == shape, "IPC selection identity mismatch")
        times["validation"] = time.perf_counter_ns() - start
        del column, values, table, reader
    finally:
        mapped.close()
    if values_output is not None:
        Path(values_output).write_bytes(canonical)
    start = time.perf_counter_ns()
    with open(path, "rb") as handle:
        encoded = handle.read()
    times["ordinary_read_after_touch"] = time.perf_counter_ns() - start
    return {"sample_id": item["sample_id"], "path": str(path), "value_sha256": actual,
            "ipc_sha256": digest(encoded), "ipc_bytes": len(encoded), "timings_ns": times,
            "metrics": {"minor_faults_during_touch": after.ru_minflt - before.ru_minflt,
                        "major_faults_during_touch": after.ru_majflt - before.ru_majflt,
                        "page_touch_checksum": checksum,
                        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)},
            "source_path": metadata[prefix + "source.path"],
            "source_version": int(metadata[prefix + "source.version"])}


def validate_raw(trace, raw):
    require(raw.get("protocol") == RAW_PROTOCOL, "wrong raw protocol")
    require(raw["source_sha256"] == trace["source_sha256"], "raw source hash mismatch")
    require(raw["config"]["backend"] in ("recording-memory", "iotdb_live"), "unknown backend")
    match_records(trace, raw["requests"], "native")
    for expected, actual in zip(trace["samples"], raw["requests"]):
        for key in ("dataset", "origin", "shape"):
            require(actual[key] == expected[key], f"native {key} mismatch")
        action = "evict_before_request" if raw["config"].get("evict_before_request") else "retain"
        require(actual["cache_action"] == action, "native cache action mismatch")
        integer_map(actual["timings_ns"], "native timings")
        integer_map(actual["metrics"], "native metrics")
        for key in ("decode_count", "read_bytes", "read_ranges", "payload_read_calls",
                    "payload_read_bytes", "payload_read_ranges"):
            require(key in actual["metrics"], f"missing required metric {key}")
        if raw["config"]["backend"] == "recording-memory":
            for measured, recorded in (("payload_read_bytes", "recorded_payload_bytes"),
                                       ("payload_read_ranges", "recorded_payload_ranges"),
                                       ("payload_read_calls", "recorded_payload_calls")):
                require(recorded in actual["metrics"] and
                        actual["metrics"][measured] == actual["metrics"][recorded], "runtime/recording source mismatch")
        if raw["config"]["backend"] == "iotdb_live":
            integer_map(actual["iotdb_delta"], "IoTDB metrics")
            for measured, recorded in (("payload_read_bytes", "payload_returned_bytes"),
                                       ("payload_read_ranges", "payload_ranges"),
                                       ("payload_read_calls", "payload_calls")):
                require(actual["metrics"][measured] == actual["iotdb_delta"][recorded],
                        "runtime/IoTDB source mismatch")
            require(actual["iotdb_delta"]["payload_failures"] == 0, "IoTDB payload failure during accepted sample")
        if "values_sha256" in actual:
            require(actual["values_sha256"] == expected["value_sha256"], "native values hash mismatch")
        for key in ("stages_ns", "counters"):
            require(key in actual and isinstance(actual[key], dict), f"missing native {key}")
            require(all(type(v) is int and v >= 0 for v in actual[key].values()), f"invalid {key}")
        if "useful_bytes" in actual["metrics"]:
            require(actual["metrics"]["useful_bytes"] == expected["useful_bytes"], "useful-byte counter mismatch")
        if raw["config"].get("expect_resident") and expected["phase"] != "first" and action == "retain":
            require(all(actual["metrics"][key] == 0 for key in ("decode_count", "read_bytes", "payload_read_calls")),
                    "explicit resident warm request performed payload I/O or decode")


def summarize(trace_path, run, output):
    require(output.resolve() not in {trace_path.resolve(), (run / "raw.json").resolve(),
                                    (run / "client.json").resolve()}, "summary cannot overwrite input evidence")
    # Revalidation failure must invalidate any prior successful summary.
    dump(output, {"protocol": "iotdbfs-hdf5-h10-summary/1", "passed": False,
                  "error": "validation incomplete; inspect the command failure"})
    trace = load_trace(trace_path)
    raw, client = read_json(run / "raw.json"), read_json(run / "client.json")
    validate_raw(trace, raw)
    require(client.get("protocol") == CLIENT_PROTOCOL, "wrong client protocol")
    require(client["source_sha256"] == trace["source_sha256"], "client source hash mismatch")
    match_records(trace, client["requests"], "client")
    rows = []
    for expected, native, local in zip(trace["samples"], raw["requests"], client["requests"]):
        require(local["value_sha256"] == expected["value_sha256"], "client value hash mismatch")
        require(local["source_path"] == raw["config"]["source_path"] and
                local["source_version"] == raw["config"]["source_version"], "client exact source mismatch")
        integer_map(local["timings_ns"], "client timings")
        integer_map(local["metrics"], "client metrics")
        artifact = run / (expected["sample_id"] + ".ipc")
        require(artifact.is_file() and file_sha(artifact) == local["ipc_sha256"], "missing or changed IPC artifact")
        require(artifact.stat().st_size == local["ipc_bytes"], "encoded byte count mismatch")
        values = run / (expected["sample_id"] + ".values.bin")
        require(values.is_file() and values.stat().st_size == expected["useful_bytes"] and
                file_sha(values) == expected["value_sha256"], "missing or changed value artifact")
        oracle = trace_path.parent / (expected["sample_id"] + ".oracle.bin")
        require(oracle.is_file() and file_sha(oracle) == expected["value_sha256"], "oracle artifact changed")
        metrics = native["metrics"]
        rows.append({"sample_id": expected["sample_id"], "phase": expected["phase"],
                     "useful_bytes": expected["useful_bytes"], "ipc_bytes": local["ipc_bytes"],
                     "native_timings_ns": native["timings_ns"], "native_stages_ns": native["stages_ns"],
                     "native_counters": native["counters"], "native_metrics": metrics,
                     "iotdb_delta": native.get("iotdb_delta"), "iotdb_after": native.get("iotdb_after"),
                     "runtime_after": native.get("runtime_after"),
                     "client_timings_ns": local["timings_ns"], "client_metrics": local["metrics"],
                     "payload_bytes_per_call": metrics["payload_read_bytes"] / metrics["payload_read_calls"] if metrics["payload_read_calls"] else None,
                     "ranges_per_call": metrics["payload_read_ranges"] / metrics["payload_read_calls"] if metrics["payload_read_calls"] else None})
    summary = {"protocol": "iotdbfs-hdf5-h10-summary/1", "passed": True,
               "source_sha256": trace["source_sha256"], "config": raw["config"], "setup": raw["setup"],
               "expected_samples": len(trace["samples"]), "samples": rows,
               "time_rule": "Nested or concurrent durations are not additive wall-time components.",
               "cache_rule": "No OS cache drop; mapping/touch faults do not measure physical disk bytes.",
               "unconfigured": ["cache-off", "encoded-only cache", "H11 A–E comparison"]}
    dump(output, summary)
    return summary


def recording(args):
    trace = load_trace(args.trace)
    require(file_sha(trace["source"]) == trace["source_sha256"], "source changed")
    args.output.mkdir(parents=True, exist_ok=False)
    tsv = args.output / "trace.tsv"
    write_tsv(tsv, trace["samples"], args.evict_before_request)
    command = [str(args.probe.resolve()), trace["source"], str(args.output.resolve()), str(tsv.resolve()),
               str(args.parallelism), str(args.cache_bytes), str(args.max_call_ranges)]
    start = time.perf_counter_ns()
    process = subprocess.run(command, capture_output=True, text=True, timeout=300)
    dump(args.output / "invocation.json", {"command": command, "returncode": process.returncode,
        "stdout": process.stdout, "stderr": process.stderr, "process_wall_ns": time.perf_counter_ns() - start})
    require(process.returncode == 0, "probe failed; see invocation.json")
    raw = read_json(args.output / "raw.json")
    dump(args.output / "probe-raw.json", raw)
    require(raw["config"]["backend"] == "recording-memory", "probe backend mismatch")
    require(raw["config"]["parallelism"] == args.parallelism and
            raw["config"]["cache_bytes"] == args.cache_bytes and
            raw["config"]["max_call_ranges"] == args.max_call_ranges, "probe configuration mismatch")
    raw["config"].update(expect_resident=args.expect_resident,
                          evict_before_request=args.evict_before_request)
    dump(args.output / "raw.json", raw)
    validate_raw(trace, raw)
    clients = [client_read(args.output / (item["sample_id"] + ".ipc"), item) for item in trace["samples"]]
    dump(args.output / "client.json", {"protocol": CLIENT_PROTOCOL, "source_sha256": trace["source_sha256"],
        "mapping_surface": "local IPC artifact written by probe", "requests": clients})
    return summarize(args.trace, args.output, args.output / "summary.json")


def live(args):
    from h08_mount_smoke import declaration, register, profile_record
    trace = load_trace(args.trace)
    require(args.fresh_runtime, "live requires --fresh-runtime attestation and an isolated daemon")
    require(os.path.ismount(args.mount), "live path is not a mountpoint")
    source = args.mount / args.source_path.lstrip("/")
    require(file_sha(source) == trace["source_sha256"], "mounted source byte hash mismatch")
    args.output.mkdir(parents=True, exist_ok=False)
    raw = {"protocol": RAW_PROTOCOL, "source_sha256": trace["source_sha256"],
           "setup": {"status": "ingestion and daemon setup measured externally; outside trace"},
           "config": {"backend": "iotdb_live", "expect_resident": args.expect_resident,
                      "evict_before_request": False, "fresh_runtime_attested": True,
                      "source_path": args.source_path, "source_version": args.version}, "requests": []}
    clients = []
    profile_sequence = None
    last_profile_end = None
    for item in trace["samples"]:
        request = declaration(args.source_path, args.version, item["dataset"], item["origin"],
                              [o+s for o, s in zip(item["origin"], item["shape"])], axes=False, rows=4096)
        start = time.perf_counter_ns()
        route = register(str(args.socket), request)
        registration_ns = time.perf_counter_ns() - start
        client = client_read(route, item, args.output / (item["sample_id"] + ".values.bin"))
        require(client["source_path"] == args.source_path and client["source_version"] == args.version,
                "live IPC exact source mismatch")
        profile = profile_record(args.profile, route)
        dump(args.output / (item["sample_id"] + ".server.json"), profile)
        require(profile["ok"] and profile["protocol"] == "iotdbfs-query-profile/1", "failed server profile")
        if profile_sequence is not None:
            require(profile["sequence"] == profile_sequence + 1 and
                    profile["steady_start_ns"] >= last_profile_end,
                    "live profiling requires an isolated sequential runtime")
        profile_sequence, last_profile_end = profile["sequence"], profile["steady_end_ns"]
        raw["config"].update(runtime=profile["runtime_config"], hdf5=profile["server_hdf5_execution"])
        native = {key: item[key] for key in ("sample_id", "dataset", "origin", "shape", "cache_action")}
        native.update(timings_ns={"server_total": profile["server_total_ns"],
                                 "client_registration_roundtrip": registration_ns},
                      stages_ns=profile["stages_ns"], counters=profile.get("counters", {}),
                      values_sha256=client["value_sha256"], metrics=profile["runtime_delta"],
                      iotdb_delta=profile["iotdb_delta"], iotdb_after=profile["iotdb_after"],
                      runtime_after=profile["runtime_after"])
        raw["requests"].append(native)
        # Saving artifacts and oracle proof happens outside all client timings.
        (args.output / (item["sample_id"] + ".ipc")).write_bytes(Path(route).read_bytes())
        clients.append(client)
        dump(args.output / "raw.json", raw)
        dump(args.output / "client.json", {"protocol": CLIENT_PROTOCOL, "source_sha256": trace["source_sha256"],
            "mapping_surface": "immutable FUSE IPC result path", "requests": clients})
    require(raw["requests"][0]["metrics"]["decode_count"] > 0, "fresh runtime first request did not decode")
    return summarize(args.trace, args.output, args.output / "summary.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--prepared", type=Path, required=True)
    prep.add_argument("--output", type=Path, required=True)
    for action in ("recording", "live", "summarize"):
        sub = commands.add_parser(action)
        sub.add_argument("--trace", type=Path, required=True)
        sub.add_argument("--output", type=Path, required=True)
        if action == "summarize":
            sub.add_argument("--run", type=Path, required=True)
            continue
        sub.add_argument("--expect-resident", action="store_true")
        if action == "recording":
            sub.add_argument("--probe", type=Path, required=True)
            sub.add_argument("--parallelism", type=int, choices=(1, 2, 4), default=1)
            sub.add_argument("--cache-bytes", type=int, default=64*1024*1024)
            sub.add_argument("--max-call-ranges", type=int, choices=(1, 32), default=32)
            sub.add_argument("--evict-before-request", action="store_true")
        else:
            sub.add_argument("--mount", type=Path, required=True)
            sub.add_argument("--socket", type=Path, required=True)
            sub.add_argument("--profile", type=Path, required=True)
            sub.add_argument("--source-path", required=True)
            sub.add_argument("--version", type=int, required=True)
            sub.add_argument("--fresh-runtime", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args)
    else:
        result = summarize(args.trace, args.run, args.output) if args.action == "summarize" else globals()[args.action](args)
        print(json.dumps({"passed": result["passed"], "samples": result["expected_samples"]}))


if __name__ == "__main__":
    main()
