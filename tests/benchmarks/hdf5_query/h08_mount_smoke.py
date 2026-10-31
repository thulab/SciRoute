#!/usr/bin/env python3
"""H08 isolated live FUSE oracle; prepare uses h5py, run/client use PyArrow.

Do not point this at a production mount. The run creates only one uniquely
named directory and removes that directory's sources unless --keep-sources.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import time
from urllib.parse import quote
import uuid

import numpy as np

PREFIX = "iotdbfs.result_descriptor.0."
GPS = "hdf5.coord.gps_seconds"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare(args):
    import h5py  # Deliberately absent from run/client imports.

    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    values = np.arange(77, dtype=np.float64).reshape(7, 11) * .125 - 7.25
    fixture = directory / "compressed.hdf5"
    with h5py.File(fixture, "w") as file:
        data = file.create_dataset("array", data=values, chunks=(3, 4),
                                   compression="gzip", compression_opts=1,
                                   shuffle=True, fletcher32=True)
        file["alias"] = data
        data.attrs["units"] = np.bytes_("arbitrary")
        file.attrs["title"] = np.bytes_("H08 independent native oracle")
    np.save(directory / "compressed.npy", values, allow_pickle=False)
    raw = Path(args.public_raw).resolve()
    if raw.is_dir():
        candidates = sorted(raw.glob("H-H1_LOSC_4_V2-*-32.hdf5"))
        require(len(candidates) == 1, "--public-raw must contain one locked H1 file")
        raw = candidates[0]
    require(raw.is_file(), "public H1 file is absent")
    begin, end = 3, 67
    with h5py.File(raw, "r") as file:
        dataset = file["/strain/Strain"]
        expected = dataset[begin:end]
        start, spacing = float(dataset.attrs["Xstart"]), float(dataset.attrs["Xspacing"])
        count = int(dataset.shape[0])
        coordinates = start + np.arange(begin, end, dtype=np.float64) * spacing
        np.save(directory / "public-values.npy", expected, allow_pickle=False)
        np.save(directory / "public-gps.npy", coordinates, allow_pickle=False)
    manifest = {"protocol": "iotdbfs-hdf5-h08-fixture/1",
                "fixture": str(fixture), "fixture_sha256": sha256(fixture),
                "public": str(raw), "public_sha256": sha256(raw),
                "public_begin": begin, "public_end": end,
                "public_start": start, "public_spacing": spacing,
                "public_count": count, "public_dtype": str(expected.dtype),
                "numpy": np.__version__, "h5py": h5py.__version__,
                "hdf5": h5py.version.hdf5_version}
    write_json(directory / "manifest.json", manifest)
    print(json.dumps({"prepared": str(directory), "manifest": manifest}))


def exact_receive(connection, count):
    data = bytearray()
    while len(data) < count:
        part = connection.recv(count - len(data))
        require(bool(part), "incomplete registration frame")
        data.extend(part)
    return bytes(data)


def register(socket_path, declaration, expect_failure=False):
    encoded = json.dumps(declaration, separators=(",", ":"), allow_nan=False).encode()
    require(len(encoded) <= 1024 * 1024, "registration exceeds frame limit")
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(300)
        connection.connect(socket_path)
        connection.sendall(struct.pack(">I", len(encoded)) + encoded)
        size, = struct.unpack(">I", exact_receive(connection, 4))
        require(0 < size <= 1024 * 1024, "invalid registration response size")
        response = json.loads(exact_receive(connection, size))
    ok = response.get("ok") in (True, "true")
    require(ok != expect_failure, f"unexpected registration response: {response}")
    if expect_failure:
        return response
    path = response["path"]
    require("&result=" in path, "route is not an immutable result path")
    return path


def field(path):
    return "hdf5.value:" + quote(path, safe="")


def bound(value, kind, type_name="uint64"):
    return {"kind": kind, "value": {"type": type_name, "value": value}}


def declaration(path, version, dataset, origin, end, axes=True, rows=5):
    projection = [field(dataset)]
    if axes:
        projection += [f"hdf5.axis.{axis}" for axis in range(len(origin))]
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": path}],
                    "versions": {"kind": "exact", "exact": int(version)}},
        "subset": {"projection": projection, "slices": [
            {"dimension": f"hdf5.axis.{axis}",
             "lower": bound(begin, "inclusive"), "upper": bound(stop, "exclusive")}
            for axis, (begin, stop) in enumerate(zip(origin, end))]},
        "limits": {"target_batch_rows": rows}}}


def metadata(value):
    if value is None:
        return {}
    return {(key.decode() if isinstance(key, bytes) else key):
            (item.decode() if isinstance(item, bytes) else item)
            for key, item in value.items()}


def read_hdf5(route, expected, dataset, source, version, expected_origin,
              expect_axes=True, gps_expected=None):
    import pyarrow as pa

    batches, descriptors = [], []
    with pa.memory_map(route, "r") as mapped:
        reader = pa.ipc.open_stream(mapped)
        schema = reader.schema
        raw = metadata(schema.metadata)
        info = {key[len(PREFIX):]: value for key, value in raw.items() if key.startswith(PREFIX)}
        require(info.get("protocol") == "iotdbfs-hdf5-subarray/1", "missing HDF5 result descriptor")
        require(info["source.path"] == source and int(info["source.version"]) == version, "descriptor source mismatch")
        require(info["dataset.path"] == dataset and bool(info["dataset.object_id"]), "descriptor dataset mismatch")
        require(info["order"] == "C", "unexpected result storage order")
        rank = int(info["dataset.rank"])
        shape = tuple(int(info[f"axis.{i}.selection_extent"]) for i in range(rank))
        origin = tuple(int(info[f"axis.{i}.selection_origin"]) for i in range(rank))
        require(shape == expected.shape and origin == tuple(expected_origin), "selection descriptor mismatch")
        require(int(info["selection.row_count"]) == expected.size, "descriptor row count mismatch")
        require(info["dataset.value_type"] == str(expected.dtype), "descriptor dtype mismatch")
        require(field(dataset) in schema.names, "value field identity missing")
        if not expect_axes:
            require(schema.names == [field(dataset)], "value-only projection added fields")
        reconstructed = np.empty(shape, dtype=expected.dtype)
        coverage = np.zeros(shape, dtype=np.uint8)
        gps_values = np.empty(shape, dtype=np.float64) if gps_expected is not None else None
        while True:
            try:
                batch, custom = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                break
            details = metadata(custom)
            require(details.get("iotdbfs.domain.present") == "1", "batch has no domain descriptor")
            require(details.get("iotdbfs.domain.kind") == "dense_grid", "batch is not a dense grid")
            batch_origin = tuple(int(details[f"iotdbfs.domain.dimension.{i}.origin"]) for i in range(rank))
            batch_shape = tuple(int(details[f"iotdbfs.domain.dimension.{i}.shape"]) for i in range(rank))
            require(math.prod(batch_shape) == batch.num_rows, "slab cardinality mismatch")
            require(details["iotdbfs.source.0.path"] == source and
                    int(details["iotdbfs.source.0.version"]) == version, "batch source mismatch")
            for i in range(rank):
                require(origin[i] <= batch_origin[i] <= origin[i] + shape[i] and
                        batch_origin[i] + batch_shape[i] <= origin[i] + shape[i], "slab outside selection")
            local = tuple(slice(batch_origin[i] - origin[i], batch_origin[i] - origin[i] + batch_shape[i])
                          for i in range(rank))
            values = batch.column(schema.get_field_index(field(dataset)))
            require(values.null_count == 0, "physical HDF5 values unexpectedly null")
            reconstructed[local] = values.to_numpy(zero_copy_only=False).reshape(batch_shape)
            coverage[local] += 1
            indices = np.indices(batch_shape, dtype=np.uint64)
            for i in range(rank):
                axis_name = f"hdf5.axis.{i}"
                if axis_name in schema.names:
                    actual = batch.column(schema.get_field_index(axis_name)).to_numpy(zero_copy_only=False)
                    np.testing.assert_array_equal(actual, (indices[i] + batch_origin[i]).ravel(order="C"))
            if gps_values is not None:
                require(GPS in schema.names, "GPS projection missing")
                gps_values[local] = batch.column(schema.get_field_index(GPS)).to_numpy(zero_copy_only=False).reshape(batch_shape)
            batches.append(batch.num_rows)
            descriptors.append({"origin": batch_origin, "shape": batch_shape})
        np.testing.assert_array_equal(coverage, np.ones(shape, dtype=np.uint8))
        np.testing.assert_array_equal(reconstructed.view(np.uint8).ravel(), np.ascontiguousarray(expected).view(np.uint8).ravel())
        if gps_values is not None:
            np.testing.assert_array_equal(gps_values.view(np.uint64), gps_expected.view(np.uint64))
            require(info["coordinate.time_system"] == "GPS" and info["coordinate.unit"] == "second", "GPS unit metadata mismatch")
            require(info["coordinate.transform"] == "none" and info["coordinate.mapping"] == "fma(ordinal,spacing,start)", "GPS mapping metadata missing")
            require(info["coordinate.units_provenance"] in ("verified_attribute", "application_declared"), "GPS provenance missing")
    return {"path": route, "rows": int(expected.size), "batches": batches,
            "batch_domains": descriptors, "schema_names": schema.names, "descriptor": info}


def filesystem_checks(route):
    import pyarrow as pa

    length = os.stat(route).st_size
    require(length > 0, "IPC must have a schema even for empty results")
    with open(route, "rb") as first, open(route, "rb") as second:
        data = first.read()
        require(len(data) == length, "stat/read length mismatch")
        require(second.read(23) == data[:23], "independent handle read mismatch")
        second.seek(max(0, length - 17)); require(second.read(64) == data[-17:], "seek/tail mismatch")
        second.seek(length); require(second.read(1) == b"", "EOF mismatch")
        second.seek(length + 13); require(second.read(1) == b"", "past-EOF mismatch")
    with pa.memory_map(route, "r") as mapped:
        require(mapped.read() == data, "mmap bytes differ from ordinary reads")
    def independent_open(_):
        with open(route, "rb") as handle:
            return hashlib.sha256(handle.read()).hexdigest()
    digest = hashlib.sha256(data).hexdigest()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        require(list(pool.map(independent_open, range(2))) == [digest, digest], "concurrent handles disagree")
    try:
        fd = os.open(route, os.O_WRONLY)
    except OSError:
        pass
    else:
        os.close(fd); raise AssertionError("typed route accepted write open")
    return {"size": length, "sha256": digest, "stat_read_seek_eof_mmap": True, "concurrent_open": True}


def client(args):
    if args.ready:
        Path(args.ready).touch()
    if args.gate:
        deadline = time.monotonic() + 30
        while not Path(args.gate).exists():
            require(time.monotonic() < deadline, "client start gate timed out")
            time.sleep(.01)
    request = json.loads(Path(args.request).read_text())
    start = time.monotonic_ns()
    route = register(args.socket, request)
    result = {"pid": os.getpid(), "path": route, "elapsed_ns": time.monotonic_ns() - start,
              "filesystem": filesystem_checks(route)}
    write_json(args.output, result)


def profile_record(path, route):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if Path(path).exists():
            for line in Path(path).read_text().splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("path") == route:
                    return record
        time.sleep(.02)
    raise AssertionError(f"profile record was not flushed for {route}")


def run(args):
    import pyarrow as pa

    prepared = Path(args.prepared).resolve(); output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((prepared / "manifest.json").read_text())
    require(manifest["protocol"] == "iotdbfs-hdf5-h08-fixture/1", "unknown prepared manifest")
    require(sha256(manifest["fixture"]) == manifest["fixture_sha256"], "fixture changed since prepare")
    require(sha256(manifest["public"]) == manifest["public_sha256"], "public input changed since prepare")
    expected = np.load(prepared / "compressed.npy", allow_pickle=False)
    public_values = np.load(prepared / "public-values.npy", allow_pickle=False)
    public_gps = np.load(prepared / "public-gps.npy", allow_pickle=False)
    logical_dir = "/h08-smoke-" + uuid.uuid4().hex
    mount_dir = Path(args.mount).resolve() / logical_dir[1:]
    mount_dir.mkdir()
    created, children = [], []
    report = {"protocol": "iotdbfs-hdf5-h08-live/1", "ok": False,
              "mount": str(Path(args.mount).resolve()), "socket": args.socket,
              "logical_directory": logical_dir, "pyarrow": pa.__version__, "numpy": np.__version__,
              "sources": {}, "queries": {}, "clients": {}, "profile": {}}
    def child(label, request_file, gate=None):
        target = output / f"{label}.json"
        command = [sys.executable, str(Path(__file__).resolve()), "client", "--socket", args.socket,
                   "--request", str(request_file), "--output", str(target)]
        if gate:
            command += ["--gate", str(gate), "--ready", str(gate) + "." + label + ".ready"]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children.append(process)
        return process, target
    def joined(process, target):
        try:
            stdout, stderr = process.communicate(timeout=330)
        except subprocess.TimeoutExpired:
            process.kill(); process.communicate()
            raise
        require(process.returncode == 0, f"client failed: {stdout}\n{stderr}")
        return json.loads(target.read_text())
    try:
        for name, raw in (("compressed.hdf5", manifest["fixture"]), ("public.hdf5", manifest["public"]),
                          ("concurrent.hdf5", manifest["fixture"])):
            destination = mount_dir / name; created.append(destination)
            with open(raw, "rb") as src, open(destination, "wb") as dst:
                shutil.copyfileobj(src, dst, 1024 * 1024)
            digest = sha256(destination)
            require(digest == sha256(raw), "ordinary source file bytes changed")
            require(os.getxattr(destination, "user.hdf5.layout_version") == b"3", "ingestion did not publish layout /3")
            logical = logical_dir + "/" + name
            catalog_route = register(args.socket, {"kind": "typed", "request": {
                "sources": {"paths": [{"kind": "exact", "path": logical}], "versions": {"kind": "latest"}}}})
            with pa.memory_map(catalog_route, "r") as mapped:
                catalog = pa.ipc.open_stream(mapped).read_all()
            require(catalog.num_rows == 1 and catalog.column("source.format").to_pylist() == ["hdf5"], "catalog format mismatch")
            version = int(catalog.column("source.version").to_pylist()[0])
            report["sources"][name] = {"path": logical, "version": version, "sha256": digest}
        source = report["sources"]["compressed.hdf5"]
        query = declaration(source["path"], source["version"], "/array", (1, 2), (6, 9))
        request_file = output / "physical-request.json"; write_json(request_file, query)
        for label in ("first", "second"):
            process, target = child(label, request_file)
            report["clients"][label] = joined(process, target)
            route = report["clients"][label]["path"]
            report["queries"][label] = read_hdf5(route, expected[1:6, 2:9], "/array", source["path"], source["version"], (1, 2))
            require(len(report["queries"][label]["batches"]) > 1, "target_batch_rows did not produce multiple batches")
            if args.profile:
                report["profile"][label] = profile_record(args.profile, route)
        require(report["clients"]["first"]["pid"] != report["clients"]["second"]["pid"], "clients were not independent processes")
        require(report["clients"]["first"]["filesystem"]["sha256"] == report["clients"]["second"]["filesystem"]["sha256"], "identical queries produced different IPC")
        if args.profile:
            warm = report["profile"]["second"]["runtime_delta"]
            require(warm["hits"] > 0 and warm["decode_count"] == 0 and warm["read_bytes"] == 0 and warm["payload_read_calls"] == 0, "second process did not reuse resident chunks")
            require(report["profile"]["first"]["runtime_delta"]["decode_count"] > 0, "first process did not execute cold decoding")
        only = declaration(source["path"], source["version"], "/alias", (2, 3), (5, 8), axes=False)
        route = register(args.socket, only)
        report["queries"]["values_only"] = read_hdf5(route, expected[2:5, 3:8], "/alias", source["path"], source["version"], (2, 3), False)
        report["queries"]["values_only"]["filesystem"] = filesystem_checks(route)
        require(report["queries"]["first"]["descriptor"]["dataset.object_id"] == report["queries"]["values_only"]["descriptor"]["dataset.object_id"], "hard link alias identity differs")
        empty = declaration(source["path"], source["version"], "/array", (7, 0), (7, 11))
        route = register(args.socket, empty)
        report["queries"]["empty"] = read_hdf5(route, expected[7:7, :], "/array", source["path"], source["version"], (7, 0))
        report["queries"]["empty"]["filesystem"] = filesystem_checks(route)
        if args.profile:
            report["profile"]["before_concurrent"] = profile_record(args.profile, route)
        cold = report["sources"]["concurrent.hdf5"]
        concurrent_request = declaration(cold["path"], cold["version"], "/array", (0, 0), (7, 11), rows=16)
        concurrent_file = output / "concurrent-request.json"; write_json(concurrent_file, concurrent_request)
        gate = output / ("gate-" + uuid.uuid4().hex)
        jobs = [child(f"concurrent-{i}", concurrent_file, gate) for i in range(2)]
        ready_files = [Path(str(gate) + f".concurrent-{i}.ready") for i in range(2)]
        deadline = time.monotonic() + 30
        while not all(path.exists() for path in ready_files):
            require(time.monotonic() < deadline, "concurrent clients did not reach their start barrier")
            require(all(process.poll() is None for process, _ in jobs), "concurrent client exited before its start barrier")
            time.sleep(.01)
        gate.touch()
        for i, (process, target) in enumerate(jobs):
            result = joined(process, target); report["clients"][f"concurrent-{i}"] = result
            report["queries"][f"concurrent-{i}"] = read_hdf5(result["path"], expected, "/array", cold["path"], cold["version"], (0, 0))
            if args.profile:
                report["profile"][f"concurrent-{i}"] = profile_record(args.profile, result["path"])
        gate.unlink()
        for path in ready_files:
            path.unlink()
        require(report["clients"]["concurrent-0"]["pid"] != report["clients"]["concurrent-1"]["pid"], "concurrent clients share a process")
        require(report["clients"]["concurrent-0"]["filesystem"]["sha256"] == report["clients"]["concurrent-1"]["filesystem"]["sha256"], "concurrent clients disagree")
        if args.profile:
            profiles = [report["profile"][f"concurrent-{i}"] for i in range(2)]
            before = report["profile"]["before_concurrent"]["runtime_after"]
            # Per-request deltas overlap; compare cumulative counters at the
            # quiescent boundaries instead of counting a shared producer twice.
            decodes = max(item["runtime_after"]["decode_count"] for item in profiles) - before["decode_count"]
            require(decodes == math.ceil(7 / 3) * math.ceil(11 / 4), "concurrent readers decoded a native chunk more than once")
            overlap = max(item["steady_start_ns"] for item in profiles) < min(item["steady_end_ns"] for item in profiles)
            require(overlap, "independent registration clients did not overlap on the server")
            report["concurrent_cache_proof"] = {"unique_native_chunks": decodes, "server_intervals_overlap": overlap}
        public = report["sources"]["public.hdf5"]
        begin, end = manifest["public_begin"], manifest["public_end"]
        gps = declaration(public["path"], public["version"], "/strain/Strain", (begin,), (end,), rows=13)
        gps["hdf5_binding"] = {"profile": "gwosc-gps-seconds/1", "dataset_path": "/strain/Strain"}
        gps["request"]["subset"]["projection"].append(GPS)
        gps["request"]["subset"]["slices"] = [{"dimension": GPS,
            "lower": bound(float(public_gps[0]), "inclusive", "float64"),
            "upper": bound(manifest["public_start"] + end * manifest["public_spacing"], "exclusive", "float64")}]
        route = register(args.socket, gps)
        report["queries"]["gps"] = read_hdf5(route, public_values, "/strain/Strain", public["path"], public["version"], (begin,), gps_expected=public_gps)
        report["queries"]["gps"]["filesystem"] = filesystem_checks(route)
        no_binding = json.loads(json.dumps(gps)); no_binding.pop("hdf5_binding")
        report["gps_without_binding"] = register(args.socket, no_binding, expect_failure=True)
        malformed = json.loads(json.dumps(query)); malformed["request"]["subset"]["unknown_hdf5_option"] = True
        report["malformed"] = register(args.socket, malformed, expect_failure=True)
        report["ok"] = True
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        for process in children:
            if process.poll() is None:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill(); process.communicate()
        cleanup_errors = []
        if not args.keep_sources:
            for path in reversed(created):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                except OSError as error:
                    cleanup_errors.append(str(error))
            try:
                mount_dir.rmdir()
            except OSError as error:
                cleanup_errors.append(str(error))
        report["sources_retained"] = bool(args.keep_sources)
        report["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            report["ok"] = False
        write_json(output / "summary.json", report)
    require(not report["cleanup_errors"], "owned source cleanup failed; see summary.json")
    print(json.dumps({"ok": True, "summary": str(output / "summary.json"),
                      "queries": len(report["queries"]), "profile_verified": bool(args.profile)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    preparation = sub.add_parser("prepare")
    preparation.add_argument("--directory", required=True)
    preparation.add_argument("--public-raw", required=True)
    execution = sub.add_parser("run")
    execution.add_argument("--prepared", required=True)
    execution.add_argument("--mount", required=True)
    execution.add_argument("--socket", required=True)
    execution.add_argument("--output", required=True)
    execution.add_argument("--profile")
    execution.add_argument("--keep-sources", action="store_true")
    worker = sub.add_parser("client")
    worker.add_argument("--socket", required=True)
    worker.add_argument("--request", required=True)
    worker.add_argument("--output", required=True)
    worker.add_argument("--gate")
    worker.add_argument("--ready")
    args = parser.parse_args()
    {"prepare": prepare, "run": run, "client": client}[args.command](args)


if __name__ == "__main__":
    main()
