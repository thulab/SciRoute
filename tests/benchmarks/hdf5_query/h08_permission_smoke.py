#!/usr/bin/env python3
"""H08 permission oracle for an isolated root-owned Linux FUSE mount.

The daemon must allow other users and expose a 0666 registration socket through
searchable parent directories. Mutations are made through this same mount so
both zero and nonzero authorization-cache TTLs must revoke permissions.
Only a unique directory created by this run is modified. No host accounts are
created. Child processes use anonymous numeric IDs and supplementary groups.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time
import uuid


# Children intentionally use only the standard library: the permission test
# must not depend on another user's access to the fixture or project checkout.
CHILD = r'''
import errno, hashlib, json, mmap, os, socket, struct, sys
request = json.load(sys.stdin)

def require(condition, message):
    if not condition:
        raise AssertionError(message)

def checked_read(path, expected, denied):
    advisory_access = os.access(path, os.R_OK)
    try:
        with open(path, "rb") as stream:
            data = stream.read()
            if path == request["source"]:
                digest = hashlib.sha256(data).hexdigest()
            else:
                with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
                    require(mapped[:] == data, "typed mmap differs from ordinary read")
                digest = hashlib.sha256(data).hexdigest()
    except OSError as error:
        require(denied and error.errno in (errno.EACCES, errno.EPERM),
                "unexpected read denial: " + repr(error))
        return {"denied": True, "errno": error.errno, "advisory_access_read": advisory_access}
    require(not denied, "unauthorized source or cached result was readable")
    require(digest == expected, "read bytes differ from root oracle")
    return {"denied": False, "sha256": digest, "bytes": len(data), "advisory_access_read": advisory_access}

def receive(connection, length):
    data = bytearray()
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        require(bool(chunk), "registration response ended early")
        data.extend(chunk)
    return bytes(data)

try:
    result = {"identity": {"uid": os.getuid(), "gid": os.getgid(), "groups": os.getgroups()},
              "ordinary": checked_read(request["source"], request["source_sha256"], request["denied"]),
              "warm_result": checked_read(request["warm_route"], request["result_sha256"], request["denied"])}
    encoded = json.dumps(request["declaration"], separators=(",", ":")).encode()
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(300)
        connection.connect(request["socket"])
        connection.sendall(struct.pack(">I", len(encoded)) + encoded)
        length, = struct.unpack(">I", receive(connection, 4))
        require(0 < length <= 1024 * 1024, "registration response size invalid")
        response = json.loads(receive(connection, length))
    if request["denied"]:
        require(response.get("ok") is False and response.get("code") == 2,
                "registration did not return PermissionDenied: " + repr(response))
        result["registration"] = {"denied": True, "code": response["code"]}
    else:
        require(response.get("ok") is True, "authorized registration failed: " + repr(response))
        result["registration"] = {"denied": False, "path": response["path"]}
        result["new_result"] = checked_read(response["path"], request["result_sha256"], False)
    print(json.dumps({"ok": True, "checks": result}))
except Exception as error:
    print(json.dumps({"ok": False, "error": type(error).__name__ + ": " + str(error)}))
    sys.exit(1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", required=True)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--prepared", required=True)
    parser.add_argument("--output", help="Optional JSON evidence file, including failures")
    parser.add_argument("--ancestor-depth", type=int, default=1,
                        help="Number of non-root source parent directories (1..8)")
    args = parser.parse_args()
    if not 1 <= args.ancestor_depth <= 8:
        parser.error("--ancestor-depth must be within 1..8")
    if sys.platform != "linux" or os.geteuid() != 0:
        parser.error("this isolated permission test requires Linux and a root caller")

    # Imported only for the explicitly requested run. No native HDF5 is needed.
    import h08_mount_smoke as smoke
    import numpy as np
    import pyarrow as pa

    mount = Path(args.mount).resolve()
    prepared = Path(args.prepared).resolve()
    smoke.require(mount.is_dir() and os.path.ismount(mount), "--mount is not a mounted filesystem")
    smoke.require(stat.S_ISSOCK(os.stat(args.socket).st_mode), "registration socket is absent")
    smoke.require(stat.S_IMODE(os.stat(args.socket).st_mode) == 0o666,
                  "permission oracle requires the isolated socket mode 0666")
    manifest = json.loads((prepared / "manifest.json").read_text())
    smoke.require(manifest["protocol"] == "iotdbfs-hdf5-h08-fixture/1", "unexpected fixture manifest")
    fixture = Path(manifest["fixture"])
    smoke.require(smoke.sha256(fixture) == manifest["fixture_sha256"], "prepared fixture hash changed")
    expected = np.load(prepared / "compressed.npy", allow_pickle=False)[1:4, 2:6]
    logical_dir = "/h08-permission-" + uuid.uuid4().hex
    logical_dir += "".join("/depth-" + str(i) for i in range(1, args.ancestor_depth))
    directory = mount / logical_dir[1:]
    source = directory / "permissions.hdf5"
    logical_source = logical_dir + "/permissions.hdf5"
    report = {"protocol": "iotdbfs-hdf5-h08-permissions/1", "ok": False,
              "mount": str(mount), "socket": args.socket,
              "logical_directory": logical_dir, "checks": {}}
    created_directories = []
    current = mount
    for component in logical_dir[1:].split("/"):
        current = current / component
        current.mkdir(mode=0o755)
        os.chmod(current, 0o755)
        created_directories.append(current)
    report["ancestor_depth"] = args.ancestor_depth
    try:
        with fixture.open("rb") as incoming, source.open("wb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
        layout_version = os.getxattr(source, "user.hdf5.layout_version")
        smoke.require(layout_version == b"5", "source was not indexed with current HDF5 layout /5")
        report["layout_version"] = layout_version.decode()
        source_hash = smoke.sha256(source)
        smoke.require(source_hash == manifest["fixture_sha256"], "ingestion changed original bytes")
        catalog_path = smoke.register(args.socket, {"kind": "typed", "request": {"sources": {
            "paths": [{"kind": "exact", "path": logical_source}], "versions": {"kind": "latest"}}}})
        with pa.memory_map(catalog_path, "r") as mapped:
            catalog = pa.ipc.open_stream(mapped).read_all()
        smoke.require(catalog.num_rows == 1, "catalog did not find the unique source")
        version = int(catalog.column("source.version").to_pylist()[0])
        query = smoke.declaration(logical_source, version, "/array", (1, 2), (4, 6))
        warm_route = smoke.register(args.socket, query)
        report["root_oracle"] = smoke.read_hdf5(warm_route, expected, "/array", logical_source, version, (1, 2))
        with open(warm_route, "rb") as handle:
            result_bytes = handle.read()
        result_hash = hashlib.sha256(result_bytes).hexdigest()
        report["source"] = {"path": logical_source, "version": version, "sha256": source_hash}

        def child(label, supplementary_group, denied, include_primary=False):
            def drop_identity():
                groups = [40077] if supplementary_group else []
                if include_primary:
                    groups += [65534, 40077]
                os.setgroups(groups)
                os.setgid(65534)
                os.setuid(65534)
            request = {"source": str(source), "source_sha256": source_hash,
                       "warm_route": warm_route, "result_sha256": result_hash,
                       "declaration": query, "socket": args.socket, "denied": denied}
            result = subprocess.run([sys.executable, "-c", CHILD], input=json.dumps(request),
                                    text=True, capture_output=True, timeout=330, cwd="/",
                                    preexec_fn=drop_identity)
            try:
                evidence = json.loads(result.stdout)
            except json.JSONDecodeError:
                raise AssertionError("permission child could not start: " + result.stderr[-2000:])
            report["checks"][label] = evidence
            smoke.require(result.returncode == 0 and evidence.get("ok"),
                          label + " failed: " + json.dumps(evidence))

        os.chown(source, 0, 40077)
        os.chmod(source, 0o640)
        source_stat = os.stat(source)
        smoke.require(source_stat.st_gid == 40077 and stat.S_IMODE(source_stat.st_mode) == 0o640,
                      "source group permissions did not persist")
        child("supplementary_group_allowed", True, False)
        child("supplementary_group_with_primary_and_duplicate_allowed", True, False,
              include_primary=True)
        child("no_group_denied", False, True)
        os.chmod(source, 0o600)
        child("chmod_revokes_warm_and_new_queries", True, True)
        os.chmod(source, 0o640)
        child("restored_permissions_allow_queries", True, False)

        os.chmod(directory, 0o700)
        child("parent_search_permission_denied", True, True)
        os.chmod(directory, 0o755)
        if len(created_directories) > 1:
            os.chmod(created_directories[0], 0o700)
            child("upper_ancestor_search_permission_denied", True, True)
            os.chmod(created_directories[0], 0o755)
        os.chown(source, 65534, 0)
        os.chmod(source, 0o600)
        child("source_owner_allowed", False, False)
        os.chown(source, 0, 40077)
        os.chmod(source, 0o640)

        # Existing handles retain their immutable result bytes, just as an open
        # file survives unlink. A new path lookup must reauthorize the source.
        with open(warm_route, "rb") as held:
            child("no_group_denied_while_result_held", False, True)
            source.unlink()
            smoke.require(os.fstat(held.fileno()).st_size == len(result_bytes),
                          "held result fstat changed after source unlink")
            smoke.require(os.stat(warm_route).st_size == len(result_bytes),
                          "pinned immutable result attributes disappeared")
            held.seek(0)
            smoke.require(held.read() == result_bytes, "held result handle changed after source unlink")
            try:
                with open(warm_route, "rb"):
                    pass
            except OSError as error:
                smoke.require(error.errno in (errno.ENOENT, errno.ESTALE, errno.EACCES),
                              "unexpected new-open error after source unlink: " + repr(error))
                report["checks"]["unlink"] = {"held_handle_readable": True,
                                                "held_handle_fstat": True,
                                                "pinned_path_stat_visible": True,
                                                "new_open_denied": True, "errno": error.errno}
            else:
                raise AssertionError("new result open bypassed source removal")
            # default_permissions evaluates access(2) locally from mode 0444.
            # Record its advisory answer separately from the enforced open.
            def drop_unprivileged_identity():
                os.setgroups([])
                os.setgid(65534)
                os.setuid(65534)
            probe = subprocess.run([sys.executable, "-c", r'''
import json, os, sys
route = sys.argv[1]
evidence = {"advisory_access_read": os.access(route, os.R_OK),
            "stat_size": os.stat(route).st_size}
try:
    with open(route, "rb"):
        pass
except OSError as error:
    evidence.update(new_open_denied=True, errno=error.errno)
else:
    evidence["new_open_denied"] = False
print(json.dumps(evidence))
''', warm_route], capture_output=True, text=True, timeout=30,
                cwd="/", preexec_fn=drop_unprivileged_identity)
            smoke.require(probe.returncode == 0, "unlinked advisory probe failed: " + probe.stderr)
            evidence = json.loads(probe.stdout)
            smoke.require(evidence["stat_size"] == len(result_bytes) and evidence["new_open_denied"],
                          "unprivileged pinned-result attributes/open boundary failed")
            report["checks"]["unlink_unprivileged_advisory"] = evidence
        # FUSE release may run after close returns. Once released, the table
        # must stop exposing the inactive route's attributes.
        deadline = time.monotonic() + 5
        while True:
            try:
                os.stat(warm_route)
            except OSError as error:
                smoke.require(error.errno in (errno.ENOENT, errno.ESTALE, errno.EACCES),
                              "unexpected stat error after last close: " + repr(error))
                report["checks"]["unlink"]["last_close_removes_attributes"] = True
                break
            smoke.require(time.monotonic() < deadline,
                          "inactive route attributes survived the last handle close")
            time.sleep(.01)
        report["ok"] = True
    except Exception as error:
        report["error"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        try:
            source.unlink(missing_ok=True)
            for ancestor in reversed(created_directories):
                ancestor.rmdir()
        except OSError as error:
            report["cleanup_error"] = str(error)
            report["ok"] = False
        if args.output:
            Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, sort_keys=True), flush=True)
    if not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
