#!/usr/bin/env python3
"""Lock the Linux native GRIB reader and daemon to the same ecCodes library."""
import argparse
import json
import os
from pathlib import Path
import subprocess

import runner as base


def library(executable, library_path=None):
    env = dict(os.environ)
    if library_path:
        env["LD_LIBRARY_PATH"] = library_path
    output = subprocess.check_output(["ldd", str(executable)], env=env, text=True)
    base.require("not found" not in output, "unresolved native shared dependency")
    lines = [line for line in output.splitlines() if "libeccodes.so" in line]
    base.require(len(lines) == 1 and "=>" in lines[0], "ambiguous ecCodes shared library")
    path = Path(lines[0].split("=>", 1)[1].split("(", 1)[0].strip()).resolve(strict=True)
    return {"path": str(path), "sha256": base.sha_file(path)}


def main(args):
    root = Path(__file__).resolve().parent
    previous = json.loads(args.daemon_environment.read_text())
    daemon = Path(previous["daemon"])
    base.require(base.sha_file(daemon) == previous["binaries"]["daemon"]["sha256"],
                 "daemon changed after environment verification")
    native_lib = library(args.probe)
    daemon_lib = library(daemon, previous["runtime_library_path"])
    base.require(native_lib == daemon_lib, "reader and daemon must use the same ecCodes library bytes")
    versions = json.loads(subprocess.check_output([str(args.probe), "--versions"], text=True))
    cache = (args.probe.parent / "CMakeCache.txt").read_text().splitlines()
    settings = {line.split(":", 1)[0]: line.split("=", 1)[1] for line in cache
                if "=" in line and not line.startswith(("//", "#"))}
    compiler = settings["CMAKE_CXX_COMPILER"]
    files = ("native/grib_native_probe.cpp", "native/grib/CMakeLists.txt", "native/CMakeLists.txt",
             "grib_native_validation.py", "grib_native_environment.py", "grib_readiness.py")
    record = {"protocol": "iotdbfs-grib-native-environment/1", "native_eccodes": native_lib,
              "daemon_eccodes": daemon_lib, "probe": {"path": str(args.probe),
              "sha256": base.sha_file(args.probe), "versions": versions},
              "daemon_sha256": base.sha_file(daemon), "compiler": subprocess.check_output(
                  [compiler, "--version"], text=True).splitlines()[0],
              "build_type": settings["CMAKE_BUILD_TYPE"], "compiler_flags": settings["CMAKE_CXX_FLAGS"],
              "release_flags": settings.get("CMAKE_CXX_FLAGS_RELEASE", ""),
              "source_hashes": {name: base.sha_file(root / name) for name in files},
              "acceptance_sha256": base.sha_file(args.acceptance),
              "services_started": False, "published_data_modified": False}
    acceptance = json.loads(args.acceptance.read_text())
    base.require(acceptance["ok"] and acceptance["probe"]["sha256"] == record["probe"]["sha256"] and
                 acceptance["probe"]["versions"] == versions, "acceptance belongs to another probe")
    base.save(args.output, record)
    print(json.dumps({"same_eccodes_library": True, "runtime_eccodes": versions["runtime_eccodes"],
                      "output": str(args.output)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--daemon-environment", type=Path, required=True)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    main(parser.parse_args())
