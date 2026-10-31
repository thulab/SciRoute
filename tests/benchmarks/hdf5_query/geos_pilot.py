#!/usr/bin/env python3
"""Bounded GEOS-5.7.2 HDF5 pilot against native HDF5 and a live IoTDB-FS mount.

Keeps all observations and source identities in an isolated output directory.
The physical value view is intentional: the public GEOS sample has an invalid
valid_range=[1e15,1e15] on every 4-D variable.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid

import h5py
import numpy as np
if os.environ.get("GEOS_PYARROW_SITE"):
    sys.path.append(os.environ["GEOS_PYARROW_SITE"])
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
from a12_migration_smoke import connection_environment

CASES = {
    "point": ((0, 1), (10, 11), (360, 361), (576, 577)),
    "roi": ((0, 1), (10, 11), (352, 368), (568, 584)),
    "multichunk": ((0, 1), (10, 11), (280, 440), (500, 660)),
    "levels": ((0, 1), (8, 13), (352, 368), (568, 584)),
}


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""):
            digest.update(data)
    return digest.hexdigest()


def write(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def control(path, request):
    data = json.dumps(request, separators=(",", ":")).encode()
    start = time.perf_counter_ns()
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(300)
        sock.connect(str(path))
        sock.sendall(struct.pack(">I", len(data)) + data)
        def receive(count):
            chunks = bytearray()
            while len(chunks) < count:
                part = sock.recv(count - len(chunks))
                check(part, "short registration response")
                chunks.extend(part)
            return bytes(chunks)
        count, = struct.unpack(">I", receive(4))
        check(0 < count < (1 << 20), "unbounded registration response")
        response = json.loads(receive(count))
    elapsed = time.perf_counter_ns() - start
    check(response.get("ok"), f"registration failed: {response}")
    return response, elapsed


def mount(daemon, path, env, socket_path):
    path.mkdir()
    log = open(path.parent / "mount.log", "wb")
    fuse_options = os.environ.get("GEOS_FUSE_OPTIONS", "attr_timeout=0,entry_timeout=0")
    process = subprocess.Popen([str(daemon), "-f", "-o", fuse_options, str(path)],
                               env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 90
    while not (os.path.ismount(path) and socket_path.exists()):
        check(process.poll() is None, "mount exited; see mount.log")
        check(time.monotonic() < deadline, "mount startup timed out")
        time.sleep(.1)
    return process, log


def unmount(path, process, log):
    if os.path.ismount(path):
        subprocess.run(["fusermount", "-u", str(path)], check=True, timeout=30)
    process.wait(timeout=30)
    log.close()


def request(source, bounds):
    slices = []
    for axis, (lo, hi) in enumerate(bounds):
        slices.append({"dimension": f"hdf5.axis.{axis}",
                       "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": lo}},
                       "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": hi}}})
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": source["logical_path"]}],
                    "versions": {"kind": "exact", "exact": source["version"]}},
        "subset": {"projection": ["hdf5.value:%2FT"], "slices": slices}}}


def query_native(path, bounds):
    start = time.perf_counter_ns()
    with h5py.File(path, "r") as f:
        data = f["/T"][tuple(slice(*part) for part in bounds)].ravel(order="C").copy()
    elapsed = time.perf_counter_ns() - start
    check(data.dtype == np.dtype("float32"), "unexpected native dtype")
    return {"ns": elapsed, "rows": int(data.size), "digest": hashlib.sha256(data.astype("<f4", copy=False).tobytes()).hexdigest()}


def query_iotdb(socket_path, source, bounds):
    response, registration_ns = control(socket_path, request(source, bounds))
    start = time.perf_counter_ns()
    with pa.memory_map(response["path"], "r") as stream:
        mmap_ns = time.perf_counter_ns() - start
        table = pa.ipc.open_stream(stream).read_all()
        ipc_decode_ns = time.perf_counter_ns() - start - mmap_ns
        check(table.num_columns == 1, "expected one value column")
        col = table.column(0)
        check(col.null_count == 0, "physical value unexpectedly null")
        data = col.to_numpy(zero_copy_only=False).astype("<f4", copy=True)
    read_ns = time.perf_counter_ns() - start
    return {"registration_ns": registration_ns, "ipc_read_ns": read_ns,
            "mmap_open_ns": mmap_ns, "ipc_decode_ns": ipc_decode_ns,
            "value_copy_ns": read_ns - mmap_ns - ipc_decode_ns,
            "ns": registration_ns + read_ns, "rows": int(data.size),
            "ipc_bytes": Path(response["path"]).stat().st_size,
            "digest": hashlib.sha256(data.tobytes()).hexdigest()}


def main(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = connection_environment(args.source_pid, args.database)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock.resolve())
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(output / "server-profile.jsonl")
    socket_path = Path("/tmp") / f"geos-pilot-{os.getpid()}.sock"
    env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    config = output / "profile.properties"
    config.write_text("query.hdf5.read_parallelism=1\nquery.cache.memory_budget_bytes=268435456\n"
                      "query.cache.max_entries=65536\ncache.read.full_prefetch_max_bytes=1\n"
                      "cache.raw_file.max_bytes=1\n")
    env["IOTDB_CONFIG_FILE"] = str(config)
    prefix = "/geos-pilot-" + uuid.uuid4().hex[:12]
    sources = [{"id": f"file{i+1}", "raw": str(p.resolve()),
                "bytes": p.stat().st_size, "sha256": sha(p),
                "logical_path": prefix + f"/file{i+1}.nc4"} for i, p in enumerate(args.raw)]
    check(1 <= len(sources) <= 16, "pilot supports 1..16 files")
    manifest = {"protocol": "iotdbfs-geos-pilot/1", "complete": False,
                "database": args.database, "source_pid": args.source_pid,
                "source_prefix": prefix, "sources": sources, "observations": []}
    write(output / "pilot.json", manifest)
    process, log = mount(args.daemon, output / "mount", env, socket_path)
    try:
        (output / "mount" / prefix[1:]).mkdir()
        for source in sources:
            dest = output / "mount" / source["logical_path"].lstrip("/")
            start = time.perf_counter_ns()
            shutil.copyfile(source["raw"], dest)
            source["publish_ns"] = time.perf_counter_ns() - start
            source["published_bytes"] = dest.stat().st_size
            check(source["published_bytes"] == source["bytes"], "published source size mismatch")
            write(output / "pilot.json", manifest)
        response, catalog_ns = control(socket_path, {"kind": "dataset_catalog", "request": {
            "sources": {"paths": [{"kind": "subtree", "path": prefix}]},
            "dataset_path": "/T",
            "predicate": {"kind": "compare", "field": "hdf5.attr:standard_name", "operator": "eq",
                          "value": {"type": "utf8", "value": "air_temperature"}}}})
        manifest["catalog"] = {"ns": catalog_ns, "response": response}
        mapping = {row["path"]: row for row in response["datasets"]}
        check(set(mapping) == {row["logical_path"] for row in sources}, "catalog mismatch")
        for source in sources:
            source["version"] = mapping[source["logical_path"]]["version"]
        write(output / "pilot.json", manifest)
        for pass_num in range(args.passes):
            for source in sources:
                for case, bounds in CASES.items():
                    native = query_native(source["raw"], bounds)
                    iotdb = query_iotdb(socket_path, source, bounds)
                    check((native["rows"], native["digest"]) == (iotdb["rows"], iotdb["digest"]),
                          f"result mismatch: {source['id']} {case} pass {pass_num}")
                    manifest["observations"].append({"source": source["id"], "case": case,
                        "pass": pass_num, "native": native, "iotdb": iotdb})
                    write(output / "pilot.json", manifest)
    finally:
        unmount(output / "mount", process, log)
    manifest["complete"] = True
    write(output / "pilot.json", manifest)
    print(json.dumps({"complete": True, "report": str(output / "pilot.json"),
                      "observations": len(manifest["observations"])}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw", type=Path, nargs="+", required=True)
    parser.add_argument("--passes", type=int, default=2)
    main(parser.parse_args())
