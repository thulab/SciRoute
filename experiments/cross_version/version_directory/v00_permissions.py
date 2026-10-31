#!/usr/bin/env python3
"""V00 live permission/version oracle in an owned, temporary FUSE mount.

Creates only a random /v00-semantic-* subtree and a tiny local HDF5 fixture.
Requires Linux/root, the existing cross-version runtime, and its writer lock.
Results distinguish live checks from unit-only protocol/error-order coverage.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
import uuid

from experiments.cross_version.pilot import publish
from experiments.cross_version.pilot.corpus import require
from environment import atomic_json

BENCH = Path(__file__).resolve().parents[3] / "tests/benchmarks/hdf5_query"
sys.path.insert(0, str(BENCH))
import h08_mount_smoke as smoke
from h08_permission_smoke import CHILD


def unprivileged():
    os.setgroups([40077])
    os.setgid(65534)
    os.setuid(65534)


@contextmanager
def mount_runtime(daemon, env, work, writer_lock):
    mount = work / "mount"
    mount.mkdir(mode=0o755)
    socket_path = Path("/tmp") / ("v00-" + uuid.uuid4().hex[:16] + ".sock")
    config = work / "profile.properties"
    config.write_text("query.hdf5.read_parallelism=1\nquery.cache.memory_budget_bytes=268435456\n"
                      "cache.read.full_prefetch_max_bytes=1\ncache.raw_file.max_bytes=1\n")
    config.chmod(0o600)
    current = dict(env, IOTDB_CONFIG_FILE=str(config),
                   IOTDBFS_QUERY_SOCKET=str(socket_path), IOTDBFS_QUERY_SOCKET_MODE="0666",
                   IOTDBFS_NAMESPACE_LOCK=str(writer_lock))
    current.pop("IOTDBFS_EXPERIMENT_FROZEN_TYPED", None)
    with (work / "mount.log").open("xb") as log:
        os.chmod(work / "mount.log", 0o600)
        process = subprocess.Popen([str(daemon), "-f", "-o",
            "allow_other,attr_timeout=0,entry_timeout=0", str(mount)],
            env=current, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 90
            while not (os.path.ismount(mount) and socket_path.exists()):
                require(process.poll() is None, "private daemon exited; inspect private mount.log")
                require(time.monotonic() < deadline, "private mount startup timed out")
                time.sleep(.05)
            yield mount, str(socket_path)
        finally:
            try:
                if os.path.ismount(mount):
                    subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30)
            finally:
                if process.poll() is None and not os.path.ismount(mount):
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        process.wait(timeout=30)


def catalog_rows(socket_path, logical, versions="latest", hdf5_only=False):
    import pyarrow as pa
    selector = {"paths": [{"kind": "exact", "path": logical}], "versions": {"kind": versions}}
    if hdf5_only:
        selector["formats"] = {"adapters": "listed", "adapter_ids": ["hdf5"],
                               "include_byte_passthrough": False}
    path = smoke.register(socket_path, {"kind": "typed", "request": {"sources": selector}})
    with pa.memory_map(path, "r") as mapped:
        table = pa.ipc.open_stream(mapped).read_all()
    return table.to_pylist()


def bind(socket_path, logical, hdf5_only=False):
    rows = catalog_rows(socket_path, logical, hdf5_only=hdf5_only)
    require(len(rows) == 1, "expected exactly one latest file")
    return int(rows[0]["source.version"])


def inventory(socket_path, mount, logical):
    rows = catalog_rows(socket_path, logical, versions="all")
    for row in rows:
        path = publish.history_path(mount, logical, row["source.version"])
        info = path.stat()
        row["inode"] = {"mode": info.st_mode & 0o7777, "uid": info.st_uid,
                        "gid": info.st_gid, "size": info.st_size,
                        "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}
        row["sha256"] = smoke.sha256(path)
    return rows


def expect_errno(action, expected):
    try:
        action()
    except OSError as error:
        require(error.errno == expected, f"expected errno {expected}, got {error.errno}")
        return {"errno": error.errno}
    raise AssertionError(f"operation unexpectedly succeeded; expected errno {expected}")


def unlink_after_release(path):
    # FUSE release can finish after userspace close. Only retry EBUSY after
    # every test-owned ordinary file handle has already been closed.
    deadline = time.monotonic() + 5
    while True:
        try:
            path.unlink(missing_ok=True)
            return
        except OSError as error:
            if error.errno != errno.EBUSY or time.monotonic() >= deadline:
                raise
            time.sleep(.02)


def run(args):
    require(sys.platform == "linux" and os.geteuid() == 0, "Linux/root required")
    args.work = args.work.resolve()
    args.work.mkdir(parents=True, exist_ok=False, mode=0o755)
    args.report = args.report.resolve()
    require(not args.report.exists(), "refusing to overwrite an existing report")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    environment, daemon, env = publish.runtime(args.environment, args.source_pid, args.database)
    report = {"protocol": "iotdbfs-v00-live-permissions/1", "ok": False,
        "database": args.database, "daemon_sha256": publish.sha_file(daemon),
        "attr_timeout": 0, "entry_timeout": 0, "checks": {},
        "scope": "isolated live FUSE/socket semantics; not performance or head atomicity evidence"}
    import h5py
    import numpy as np
    fixture = args.work / "fixture.h5"
    with h5py.File(fixture, "w") as file:
        file.create_dataset("array", data=np.arange(16, dtype=np.float64).reshape(4, 4))
    report["fixture_sha256"] = smoke.sha256(fixture)
    try:
        with mount_runtime(daemon, env, args.work, args.writer_lock.resolve()) as (mount, socket_path):
            logical_dir = "/v00-semantic-" + uuid.uuid4().hex
            logical = logical_dir + "/versions.h5"
            directory = mount / logical_dir[1:]
            source = mount / logical[1:]
            directory.mkdir(mode=0o755)
            os.chmod(directory, 0o755)
            report["logical_directory"] = logical_dir
            try:
                publish.write_version(fixture, source, True)
                os.chown(source, 0, 40077)
                os.chmod(source, 0o640)
                v1 = bind(socket_path, logical)
                report["after_first_write"] = inventory(socket_path, mount, logical)
                publish.write_version(fixture, source, False)
                v2 = bind(socket_path, logical)
                require(v2 > v1, "same-path write did not produce a new version")
                os.chown(source, 0, 0)
                os.chmod(source, 0o600)
                old_path = publish.history_path(mount, logical, v1)
                latest_path = publish.history_path(mount, logical, v2)
                require(smoke.sha256(old_path) == report["fixture_sha256"], "historical bytes differ")
                require(os.stat(old_path).st_gid == 40077 and os.stat(source).st_gid == 0,
                        "exact historical inode permissions did not remain independent")
                report["versions"] = [v1, v2]
                report["after_second_write"] = inventory(socket_path, mount, logical)
                queries = {v: smoke.declaration(logical, v, "/array", (0, 0), (4, 4)) for v in (v1, v2)}
                routes = {v: smoke.register(socket_path, queries[v]) for v in (v1, v2)}

                def child(label, version, denied):
                    route = routes[version]
                    request = {"source": str(publish.history_path(mount, logical, version)),
                        "source_sha256": report["fixture_sha256"], "warm_route": route,
                        "result_sha256": smoke.sha256(route), "declaration": queries[version],
                        "socket": socket_path, "denied": denied}
                    completed = subprocess.run([sys.executable, "-c", CHILD], input=json.dumps(request),
                        text=True, capture_output=True, timeout=330, cwd="/", preexec_fn=unprivileged)
                    evidence = json.loads(completed.stdout)
                    report["checks"][label] = evidence
                    require(completed.returncode == 0 and evidence.get("ok"), label + ": " + str(evidence))

                child("historical_exact_inode_group_allowed", v1, False)
                child("latest_different_inode_group_denied", v2, True)
                os.chmod(directory, 0o700)
                child("historical_uses_current_ancestor_search_permissions", v1, True)
                os.chmod(directory, 0o755)
                for name, operation in {
                    "version_chmod_eperm": lambda: os.chmod(old_path, 0o777),
                    "version_chown_eperm": lambda: os.chown(old_path, 65534, 65534),
                }.items():
                    report["checks"][name] = expect_errno(operation, errno.EPERM)
                with source.open("rb"):
                    report["checks"]["ordinary_open_unlink_ebusy"] = expect_errno(source.unlink, errno.EBUSY)

                # A non-root child opens an authorized IPC before the parent
                # revokes search permission; the same child then tries a new open.
                code = r'''
import errno, hashlib, json, os, sys
route = sys.argv[1]
with open(route, "rb") as held:
    expected = held.read()
    before = os.fstat(held.fileno()).st_size
    print("ready", flush=True)
    assert sys.stdin.readline().strip() == "revoked"
    held.seek(0)
    assert held.read() == expected
    assert os.fstat(held.fileno()).st_size == before
    try:
        with open(route, "rb"):
            pass
    except OSError as error:
        assert error.errno in (errno.EACCES, errno.EPERM), repr(error)
        print(json.dumps({"held_read": True, "held_fstat": True,
            "new_open_denied": True, "errno": error.errno,
            "sha256": hashlib.sha256(expected).hexdigest()}), flush=True)
    else:
        raise AssertionError("new open bypassed ancestor revocation")
'''
                process = subprocess.Popen([sys.executable, "-c", code, routes[v1]],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, cwd="/", preexec_fn=unprivileged)
                try:
                    ready, _, _ = select.select([process.stdout], [], [], 60)
                    require(bool(ready) and process.stdout.readline().strip() == "ready",
                            "held IPC child did not open its result")
                    os.chmod(directory, 0o700)
                    out, error = process.communicate("revoked\n", timeout=60)
                    require(process.returncode == 0, "held IPC revoke check failed: " + error)
                    report["checks"]["opened_ipc_survives_revoke_new_open_denied"] = json.loads(out)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=10)
                    os.chmod(directory, 0o755)

                # Generic latest selects the greatest surviving version,
                # including a separately published empty O_TRUNC snapshot.
                # HDF5-filtered latest selects the greatest surviving HDF5 version.
                report["before_latest_delete"] = inventory(socket_path, mount, logical)
                unlink_after_release(latest_path)
                survivors = inventory(socket_path, mount, logical)
                report["after_latest_delete"] = survivors
                latest_all = bind(socket_path, logical)
                latest_hdf5 = bind(socket_path, logical, hdf5_only=True)
                report["after_delete_selection"] = {"latest_all": latest_all, "latest_hdf5": latest_hdf5,
                    "bare_path_sha256": smoke.sha256(source), "bare_path_size": source.stat().st_size}
                require(survivors and latest_all == max(r["source.version"] for r in survivors),
                        "generic latest differs from greatest surviving version")
                require(latest_hdf5 == v1, "HDF5 latest did not expose surviving HDF5 v1")
                require(v2 not in {r["source.version"] for r in survivors}, "deleted exact v2 remained visible")
                chosen = next(r for r in survivors if r["source.version"] == latest_all)
                if latest_all != v1:
                    require(v1 < latest_all < v2 and chosen["source.format"] is None and
                            chosen["source.payload_size"] == 0,
                            "unexpected surviving intermediate version; not an empty truncation snapshot")
                require(smoke.sha256(source) == chosen["sha256"], "bare path differs from generic latest bytes")
                expect_errno(lambda: latest_path.open("rb"), errno.ENOENT)
                report["checks"]["delete_latest_respects_format_scope_exact_v2_absent"] = {
                    "latest_all": latest_all, "latest_hdf5": latest_hdf5, "deleted": v2,
                    "empty_intermediate_snapshot": latest_all != v1}
                child("historical_permission_still_valid_after_latest_delete", v1, False)
                report["ok"] = True
            finally:
                try:
                    os.chmod(directory, 0o755)
                    # Remove every exact version in this exclusively owned
                    # subtree; older baseline binaries may retain history on
                    # bare-path unlink. Do not assume newer source semantics.
                    report["cleanup_before"] = [p.name for p in directory.iterdir()]
                    for row in catalog_rows(socket_path, logical, versions="all"):
                        unlink_after_release(publish.history_path(mount, logical, row["source.version"]))
                    for child_path in list(directory.iterdir()):
                        require(child_path.is_file(), "unexpected non-file in owned cleanup subtree")
                        unlink_after_release(child_path)
                    directory.rmdir()
                except OSError as error:
                    report["cleanup_error"] = str(error)
                    report["ok"] = False
    except Exception as error:
        report["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        atomic_json(args.report, report)
    require(report["ok"], "live checks passed but cleanup failed")
    print(json.dumps({"ok": True, "checks": list(report["checks"]), "report": str(args.report)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, type=Path)
    parser.add_argument("--source-pid", required=True, type=int)
    parser.add_argument("--database", default=publish.DATABASE)
    parser.add_argument("--writer-lock", required=True, type=Path)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    run(parser.parse_args())
