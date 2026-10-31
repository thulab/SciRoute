#!/usr/bin/env python3
"""Diagnostic ablation: keep batched SQL but disable parsed-artifact reuse.

Only a copied backend translation unit is changed. All production headers,
source files, normal object files and archives remain untouched.
Requires an already-built Makefile-based CMake release build on Linux.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source, build, run = args.source.resolve(), args.build.resolve(), args.output.resolve()
    run.mkdir(parents=True, exist_ok=False)
    for name in ("bin", "diag-src", "diag-obj"):
        (run / name).mkdir()
    original = (source / "src/IoTDBStructuredBlobBackend.cpp").read_text()
    lookup = "auto cached = dataset_catalog_cache_.get(identity);"
    admission = ("dataset_catalog_cache_.put_validated(\n"
                 "            make_dataset_catalog_cache_identity(storage_namespace(), manifest, marker), artifact);")
    if original.count(lookup) != 1 or original.count(admission) != 1:
        raise ValueError("expected exactly one parsed-cache lookup and admission site")
    modified = original.replace(lookup, "auto cached = std::shared_ptr<const DatasetCatalogArtifact>{};")
    modified = modified.replace(admission, "(void)artifact; // Diagnostic: disable parsed-cache admission.")
    cpp = run / "diag-src/IoTDBStructuredBlobBackend.cpp"
    cpp.write_text(modified)
    flags = {}
    for line in (build / "CMakeFiles/iotdb_fs_core.dir/flags.make").read_text().splitlines():
        if line.startswith(("CXX_DEFINES =", "CXX_INCLUDES =", "CXX_FLAGS =")):
            key, value = line.split("=", 1)
            flags[key.strip()] = shlex.split(value)
    # Preserve the archive member name, and keep every other translation unit
    # (including callers that construct the backend) identical to production.
    obj = run / "diag-obj/IoTDBStructuredBlobBackend.cpp.o"
    subprocess.run(["/usr/bin/c++", *flags["CXX_DEFINES"], *flags["CXX_INCLUDES"],
                    *flags["CXX_FLAGS"], "-c", str(cpp), "-o", str(obj)], check=True)
    lib = run / "bin/libiotdb_fs_core-batch-only.a"
    shutil.copy2(build / "libiotdb_fs_core.a", lib)
    subprocess.run(["ar", "r", str(lib), str(obj)], check=True)
    members = subprocess.check_output(["ar", "t", str(lib)], text=True).splitlines()
    if members.count(obj.name) != 1:
        raise RuntimeError("backend archive member was not replaced exactly once")
    binary = run / "bin/iotdb_fs-batch-only"
    cmd = shlex.split((build / "CMakeFiles/iotdb_fs.dir/link.txt").read_text())
    cmd[cmd.index("-o") + 1] = str(binary)
    cmd[cmd.index("libiotdb_fs_core.a")] = str(lib)
    subprocess.run(cmd, cwd=build, check=True)
    manifest = {
        "diagnostic_only": True, "headers_unchanged": True,
        "source_sha256": hashlib.sha256(cpp.read_bytes()).hexdigest(),
        "production_backend_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "daemon_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "changes": ["disable parsed-cache lookup", "disable parsed-cache insertion"],
        "production_build_unchanged": True,
    }
    (run / "diagnostic.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest))


if __name__ == "__main__":
    main()
