#!/usr/bin/env python3
"""Run the permission oracle in a temporary shared socket/mount, then unmount."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from grib_hdf5_boundary_probe import connection_environment


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--profile-config", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ancestor-depth", type=int, default=1)
    parser.add_argument("--attr-timeout", type=float, choices=(0.0, 1.0), default=0.0)
    args = parser.parse_args()
    if os.geteuid() != 0 or sys.platform != "linux":
        raise ValueError("permission oracle requires root on Linux")
    args.output.mkdir(parents=True, exist_ok=False)
    mount = args.output / "mount"
    mount.mkdir()
    socket_path = args.output / "query.sock"
    if len(str(socket_path).encode()) >= 108:
        raise ValueError("Unix socket output path is too long")
    env = connection_environment(args.source_pid, "h09probe1790939889dbfc1f")
    env["IOTDB_CONFIG_FILE"] = str(args.profile_config)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.output / "namespace.lock")
    env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    env["IOTDBFS_QUERY_SOCKET_MODE"] = "0666"
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(args.output / "profile.jsonl")
    env["IOTDBFS_TYPED_AUTHORIZATION_PROFILE_JSONL"] = str(args.output / "authorization.jsonl")
    with (args.output / "mount.log").open("wb") as log:
        daemon = subprocess.Popen([str(args.daemon), "-f", "-o",
                                   f"allow_other,attr_timeout={args.attr_timeout:g},entry_timeout=0", str(mount)],
                                  env=env, stdout=log, stderr=subprocess.STDOUT,
                                  stdin=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 90
            while not (os.path.ismount(mount) and socket_path.exists()):
                if daemon.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("permission mount unavailable; see mount.log")
                time.sleep(.1)
            with (args.output / "permission.log").open("wb") as output:
                subprocess.run([
                    sys.executable, str(Path(__file__).with_name("h08_permission_smoke.py")),
                    "--mount", str(mount), "--socket", str(socket_path),
                    "--prepared", str(args.prepared), "--output", str(args.output / "permissions.json"),
                    "--ancestor-depth", str(args.ancestor_depth),
                ], env=env, stdout=output, stderr=subprocess.STDOUT, check=True,
                    timeout=600)
            report = json.loads((args.output / "permissions.json").read_text())
            print(json.dumps({"ok": report["ok"], "checks": list(report["checks"]),
                              "report": str(args.output / "permissions.json")}))
        finally:
            if os.path.ismount(mount):
                subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30)
            elif daemon.poll() is None:
                daemon.terminate()
            daemon.wait(timeout=30)


if __name__ == "__main__":
    main()
