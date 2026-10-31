#!/usr/bin/env python3
"""Read-only, same-daemon GRIB/HDF5 query-boundary timing probe.

The first boundary includes typed registration and materialization. The second
deliberately times only a read of the already published IPC result, mirroring
the historical read_perf timer's position after its exists/file_size checks.
It must never be described as a full query latency.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import time


def connection_environment(pid, database):
    env = os.environ.copy()
    peer = {}
    for item in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
        if b"=" in item:
            key, value = item.split(b"=", 1)
            peer[key.decode()] = value.decode()
    for name in ("IOTDB_HOST", "IOTDB_PORT", "IOTDB_USER", "IOTDB_PASS"):
        if name in peer:
            env[name] = peer[name]
    env["IOTDB_DB"] = database
    return env


def submit(path, request):
    encoded = json.dumps(request, separators=(",", ":")).encode()
    started = time.perf_counter_ns()
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(300)
        client.connect(str(path))
        client.sendall(struct.pack(">I", len(encoded)) + encoded)

        def receive(count):
            received = bytearray()
            while len(received) < count:
                part = client.recv(count - len(received))
                if not part:
                    raise RuntimeError("short socket response")
                received.extend(part)
            return bytes(received)

        length, = struct.unpack(">I", receive(4))
        if not 0 < length < (1 << 20):
            raise RuntimeError("invalid socket response length")
        result = json.loads(receive(length))
    elapsed = time.perf_counter_ns() - started
    if not result.get("ok"):
        raise RuntimeError(f"registration failed: {result}")
    return result, elapsed


def hdf5_request(source):
    slices = []
    for axis, lo, hi in ((0, 0, 1), (1, 10, 11), (2, 360, 361),
                         (3, 576, 577)):
        slices.append({
            "dimension": f"hdf5.axis.{axis}",
            "lower": {"kind": "inclusive", "value": {"type": "uint64", "value": lo}},
            "upper": {"kind": "exclusive", "value": {"type": "uint64", "value": hi}},
        })
    return {"kind": "typed", "request": {
        "sources": {
            "paths": [{"kind": "exact", "path": source["logical_path"]}],
            "versions": {"kind": "exact", "exact": source["version"]},
        },
        "subset": {"projection": ["hdf5.value:%2FT"], "slices": slices},
    }}


def grib_request(path):
    return {"kind": "typed", "request": {
        "sources": {
            "paths": [{"kind": "exact", "path": path}],
            "versions": {"kind": "latest"},
        },
        "subset": {
            "projection": ["grib.message.bytes"],
            "predicate": {"kind": "compare", "field": "grib.message.level",
                          "operator": "eq", "value": {"type": "int64", "value": 500}},
            "output": {"mode": "binary_view"},
        },
    }}


def consume(path):
    started = time.perf_counter_ns()
    if not os.path.exists(path):
        raise RuntimeError("published IPC path disappeared")
    size = os.path.getsize(path)
    stat_ns = time.perf_counter_ns() - started
    started = time.perf_counter_ns()
    parts = []
    total = 0
    with open(path, "rb", buffering=0) as stream:
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            parts.append(chunk)
            total += len(chunk)
    read_ns = time.perf_counter_ns() - started
    if total != size or total == 0:
        raise RuntimeError("incomplete IPC result read")
    digest = hashlib.sha256(b"".join(parts))
    return {"stat_ns": stat_ns, "read_ns": read_ns,
            "ipc_bytes": total, "ipc_sha256": digest.hexdigest()}


def run_campaign(root, index, daemon, env, requests, repeats):
    directory = root / f"campaign-{index:02d}"
    directory.mkdir()
    mount = directory / "mount"
    mount.mkdir()
    socket_path = directory / "query.sock"
    profile_path = directory / "profile.jsonl"
    local_env = env.copy()
    local_env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    local_env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(profile_path)
    with (directory / "mount.log").open("wb") as log:
        process = subprocess.Popen(
            [str(daemon), "-f", "-o", "attr_timeout=1,entry_timeout=1", str(mount)],
            env=local_env, stdin=subprocess.DEVNULL, stdout=log,
            stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 90
            while not (os.path.ismount(mount) and socket_path.exists()):
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("mount did not start; see mount.log")
                time.sleep(.1)
            observations = []
            # The requests are alternated and reverse order on odd campaigns.
            order = list(requests)
            for repetition in range(-1, repeats):
                if (repetition + index) % 2:
                    order.reverse()
                for name in order:
                    response, control_ns = submit(socket_path, requests[name])
                    result = consume(response["path"])
                    observations.append({"format": name, "repetition": repetition,
                                         "control_ns": control_ns, **result})
            profiles = [json.loads(line) for line in profile_path.read_text().splitlines()]
            if len(profiles) != len(observations):
                raise RuntimeError("profile count mismatch")
            for observed, profile in zip(observations, profiles):
                if not profile.get("ok") or profile.get("operation") != "typed":
                    raise RuntimeError("typed profile failure")
                observed["server_total_ns"] = profile["server_total_ns"]
                observed["stages_ns"] = profile["stages_ns"]
                observed["iotdb_delta"] = profile["iotdb_delta"]
                observed["counters"] = profile["counters"]
            return observations
        finally:
            if os.path.ismount(mount):
                subprocess.run(["fusermount", "-u", str(mount)], check=True,
                               timeout=30)
            process.wait(timeout=30)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-manifest", type=Path, required=True)
    parser.add_argument("--grib-path", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--profile-config", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--campaigns", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.campaigns <= 5 or not 1 <= args.repeats <= 20:
        raise ValueError("campaign or repeat bound exceeded")
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.pilot_manifest.read_text())
    source = manifest["sources"][0]
    if not source.get("version"):
        raise ValueError("pilot source lacks an exact version")
    requests = {"hdf5": hdf5_request(source), "grib": grib_request(args.grib_path)}
    env = connection_environment(args.source_pid, args.database)
    env["IOTDB_CONFIG_FILE"] = str(args.profile_config)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "namespace.lock")
    all_observations = []
    for index in range(args.campaigns):
        all_observations.extend(run_campaign(args.output, index, args.daemon,
                                             env, requests, args.repeats))
    report = {"protocol": "iotdbfs-grib-hdf5-boundaries/1",
              "database": args.database,
              "hdf5_source": {"path": source["logical_path"],
                              "version": source["version"]},
              "grib_source": {"path": args.grib_path, "version": "latest"},
              "observations": all_observations}
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"report": str(args.output / "report.json"),
                      "observations": len(all_observations)}))


if __name__ == "__main__":
    main()
