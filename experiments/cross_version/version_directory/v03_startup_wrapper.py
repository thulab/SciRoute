#!/usr/bin/env python3
"""Execute the frozen runner with an explicitly receipted mount-startup deadline.

No frozen file or query clock is edited. Each campaign still starts its own daemon.
Only private_mount's readiness wait changes; default 90 seconds is unchanged.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import difflib
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import runpy
import sys
import time

RUNNER_SHA = "be728432fc2b01f810af167a5d07308211a50cf980a478a0dfcf4e229ce65155"
NATIVE_SHA = "49f7f7127e6a7c73bab304f9ec230f46e540ebf26b3d3e40fed05b3a6e398864"
HELPER_SHA = "08d5830f85ebef35894dc411c087140f3fd71b1eb36aae36d5ec83f0360083bf"
DEPENDENCIES = {
    "multi_file/common.py": "f5fc72fc9c8da8317dadb027dcd4d22d43498fde5f7c32a08a522eea6e01e8ae",
    "multi_file/manager.py": "207709c893e75fc02028d7f44ea82025e78dd121206676245f79495b6ec629f1",
    "multi_file/grib_data.py": "c92e9aa08d75abfd3a8a2d8eb6c398abdce14f9583ddc3852badd99cac27e2b8",
    "multi_file/hdf5_data.py": "b3050b4648dfbb665ee9336b3ff09f8200a44636a15859a0868d5fb44c154e35",
    "multi_file/specs/runner-hdf5-n128-d4.json": "c559c9818e5b1c4f7919bf010ca518165d99e1460750c23e1a7a8ea4703317cf",
    "multi_file/specs/runner-grib-n128-d4.json": "f69915f26a7a0c6e7a387b83997e79654d75897b4445e7111b1eb964a0be2fe6",
    "single_file/runner.py": HELPER_SHA,
    "single_file/environment.py": "5c3c74571c9b16b5ae9542207a9d715c4daae315191b29876cf5f7da04dd9b90",
}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def require(value, message):
    if not value:
        raise RuntimeError(message)


def append_receipt(path, value):
    with Path(path).open("a") as stream:
        os.chmod(path, 0o600)
        stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def load_runner(path):
    path = Path(path).resolve()
    require(sha(path) == RUNNER_SHA, "frozen runner SHA differs")
    for relative, expected in DEPENDENCIES.items():
        require(sha(path.parents[1] / relative) == expected,
                "frozen imported dependency differs: " + relative)
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("v03_frozen_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def memory(pid):
    result = {}
    for line in (Path("/proc") / str(pid) / "status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM"):
            result[key + "_kib"] = int(value.split()[0])
    return result


def adjust_mount(single, seconds, receipt):
    require(type(seconds) is int and 90 <= seconds <= 1800, "invalid startup deadline")
    require(sha(single.__file__) == HELPER_SHA, "imported private_mount helper SHA differs")
    original = inspect.getsource(single.private_mount)
    old = "deadline = time.monotonic() + 90"
    require(original.count(old) == 1, "unexpected private_mount readiness implementation")
    adjusted = original.replace(old, "deadline = time.monotonic() + " + str(seconds))
    namespace = dict(single.__dict__)
    exec(compile(adjusted, str(Path(single.__file__).resolve()) + ":v03-startup-only", "exec"), namespace)
    private_mount = namespace["private_mount"]
    append_receipt(receipt, {"kind": "startup_wrapper", "runner_sha256": RUNNER_SHA,
        "helper_path": str(Path(single.__file__).resolve()), "helper_file_sha256": sha(single.__file__),
        "original_function_sha256": hashlib.sha256(original.encode()).hexdigest(),
        "executed_function_sha256": hashlib.sha256(adjusted.encode()).hexdigest(),
        "only_replacement": {"from_seconds": 90, "to_seconds": seconds},
        "function_patch": "".join(difflib.unified_diff(original.splitlines(True),
            adjusted.splitlines(True), fromfile="frozen/private_mount", tofile="startup/private_mount")),
        "frozen_dependency_sha256": DEPENDENCIES,
        "wrapper_sha256": sha(__file__), "query_clock_code_changed": False})

    @contextmanager
    def observed(*args, **kwargs):
        started = time.perf_counter_ns()
        with private_mount(*args, **kwargs) as context:
            pid = context["pid"]
            record = {"kind": "mount", "root": str(context["root"]),
                "startup_ns": context["startup_ns"], "mount_context_entry_ns": time.perf_counter_ns() - started,
                "daemon_pid": pid, "daemon_exe": str((Path("/proc") / str(pid) / "exe").resolve()),
                "daemon_sha256": sha(Path("/proc") / str(pid) / "exe"),
                "memory_at_startup": memory(pid)}
            append_receipt(receipt, record)
            try:
                yield context
            finally:
                if (Path("/proc") / str(pid) / "status").exists():
                    append_receipt(receipt, {"kind": "mount_before_stop", "root": str(context["root"]),
                        "daemon_pid": pid, "memory": memory(pid)})
    single.private_mount = observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frozen-runner", type=Path, required=True)
    parser.add_argument("--startup-timeout", type=int, default=90)
    parser.add_argument("--receipt", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    if remaining and remaining[0] == "--":
        remaining = remaining[1:]
    require(not args.receipt.exists(), "startup receipt already exists")
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    module = load_runner(args.frozen_runner)
    adjust_mount(module.sf, args.startup_timeout, args.receipt)
    sys.argv = [str(args.frozen_runner.resolve()), *remaining]
    runpy.run_path(str(args.frozen_runner.resolve()), run_name="__main__")


if __name__ == "__main__":
    main()
