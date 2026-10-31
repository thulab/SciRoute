#!/usr/bin/env python3
"""Build/verify the isolated HDF5 single-file benchmark environment.

Timed native queries and the daemon share one thread-safe core-only HDF5 1.14.4
library. The offline generator/oracle uses the h5py 3.12.1 manylinux wheel's
separate HDF5 1.14.4 runtime; it is never part of native query timing. This avoids
HDF5's unsupported high-level plus thread-safe build configuration.

No daemon is started and no database, mount, system dependency or original
source checkout is modified. Credentials are never written or logged. The caller
starts the isolated daemon with IOTDBFS_NAMESPACE_LOCK set to the namespace's
existing writer lock; ordinary mounts must remain untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
HDF5_VERSION = "1.14.4"
HDF5_SHA256 = "019ac451d9e1cf89c0482ba2a06f07a46166caf23f60fea5ef3c37724a318e03"
HDF5_URL = "https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.tar.gz"


def connection_environment(pid: int, database: str) -> dict[str, str]:
    """Read an owned daemon's connection settings in memory only.

    Callers must never log this return value or serialize it into configuration,
    reports, command arguments or env.json. Precedence matches main.cpp: config
    properties before environment, then the daemon's built-in defaults. Only
    the requested database is overridden. This does not acquire the writer lock.
    """
    if not isinstance(pid, int) or pid < 1 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise RuntimeError("invalid trusted daemon identity or database")
    process = Path("/proc") / str(pid)
    try:
        executable = os.readlink(process / "exe").removesuffix(" (deleted)")
        if process.stat().st_uid != os.geteuid() or Path(executable).name != "iotdb_fs":
            raise RuntimeError("connection source is not a daemon owned by this user")
        with (process / "environ").open("rb") as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise RuntimeError("trusted daemon environment exceeds read limit")
        settings = {}
        for item in raw.split(b"\0"):
            if b"=" in item:
                key, value = item.split(b"=", 1)
                settings[key.decode("utf-8", "surrogateescape")] = value.decode("utf-8", "surrogateescape")
        config = Path(settings.get("IOTDB_CONFIG_FILE") or
                      str(Path(settings.get("IOTDB_CONF_DIR") or "resources") / "config.properties"))
        if not config.is_absolute():
            config = (process / "cwd").resolve() / config
        props = {}
        if config.is_file():
            with config.open("r", encoding="utf-8") as stream:
                for line in stream:
                    line = line.strip()
                    if line and not line.startswith(("#", "!")) and "=" in line:
                        key, value = line.split("=", 1)
                        props[key.strip()] = value.strip()
        result = {
            "IOTDB_HOST": props.get("iotdb.host") or settings.get("IOTDB_HOST") or "127.0.0.1",
            "IOTDB_PORT": props.get("iotdb.port") or settings.get("IOTDB_PORT") or "6667",
            "IOTDB_USER": props.get("iotdb.user") or settings.get("IOTDB_USER") or "root",
            "IOTDB_PASS": props.get("iotdb.pass") or props.get("iotdb.password") or
                          settings.get("IOTDB_PASS") or "root",
            "IOTDB_DB": database,
        }
        if not result["IOTDB_PORT"].isdigit() or not 1 <= int(result["IOTDB_PORT"]) <= 65535:
            raise RuntimeError("trusted daemon has an invalid database port")
        return result
    except (OSError, UnicodeError) as error:
        # Do not propagate source contents, environment entries or credentials.
        raise RuntimeError("cannot read trusted daemon connection settings") from None


def sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(data)
    return value.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def runtime_environment(prefix: Path, arrow: Path, sdk: Path) -> dict[str, str]:
    env = dict(os.environ)
    libraries = [prefix / "lib", prefix / "lib64", arrow / "lib", sdk / "lib"]
    env["LD_LIBRARY_PATH"] = ":".join(str(path) for path in libraries)
    if os.environ.get("LD_LIBRARY_PATH"):
        env["LD_LIBRARY_PATH"] += ":" + os.environ["LD_LIBRARY_PATH"]
    # Reject overrides rather than silently changing a caller's runtime policy.
    for key in ("HDF5_DRIVER", "HDF5_VOL_CONNECTOR"):
        if env.get(key):
            raise RuntimeError(f"{key} must be unset for the matched baseline")
    return env


def python_environment(work: Path) -> dict[str, str]:
    """Keep offline Python/IPC wheels separate from the C++ library profile.

    The custom Arrow C++ prefix is intentionally IPC-only and must not shadow
    wheel libraries needed by PyArrow's parquet/dataset modules. A Python CLI
    must itself be launched with clean LD_LIBRARY_PATH before importing wheels;
    changing this dictionary cannot unload a library already in its process.
    """
    env = dict(os.environ)
    env.pop("LD_LIBRARY_PATH", None)
    env.pop("PYTHONPATH", None)
    env["TMPDIR"] = str(work / "tmp")
    env["PIP_CACHE_DIR"] = str(work / "cache/pip")
    return env


def run(command: list[str], log: Path, env: dict[str, str] | None = None) -> None:
    # Commands passed here contain paths/build flags only, never credentials.
    with log.open("w") as stream:
        stream.write(json.dumps(command) + "\n")
        stream.flush()
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                       env=env, check=True)


def source_hashes(source: Path) -> dict[str, str]:
    result = {}
    for top in ("src", "include", "cmake", "ci", "experiments/single_file"):
        for path in sorted((source / top).rglob("*")):
            if path.is_file() and not any(part in ("__pycache__", "work", "results") for part in path.relative_to(source).parts):
                result[str(path.relative_to(source))] = sha256(path)
    result["CMakeLists.txt"] = sha256(source / "CMakeLists.txt")
    return result


def freeze_source(original: Path, destination: Path) -> None:
    if original == destination:
        if not (destination / "CMakeLists.txt").is_file():
            raise RuntimeError("the supplied source snapshot is incomplete")
        return
    if destination.exists():
        raise RuntimeError("source snapshot already exists; choose a fresh work directory")
    shutil.copytree(original, destination, ignore=shutil.ignore_patterns(
        ".git", "work", "build", "build-*", "third_party", "__pycache__",
        ".venv", "venv", "logs", "results", ".ccls-cache"))


def verify(work: Path, arrow: Path, sdk: Path) -> dict:
    prefix, source = work / "hdf5-prefix", work / "source"
    python = work / "venv/bin/python"
    daemon = work / "daemon-build/bin/iotdb_fs"
    helper = work / "daemon-build/bin/iotdbfs_hdf5_scale_worker"
    native = work / "native-build/native_probe"
    env = runtime_environment(prefix, arrow, sdk)
    pyenv = python_environment(work)
    subprocess.check_output([sys.executable, str(source / "ci/install_dependencies.py"),
                             "--only", "hdf5", "--prefix", str(prefix),
                             "--verify-only"], env=env, text=True)
    install_marker = prefix / ".iotdbfs-dependencies.json"
    installation = json.loads(install_marker.read_text())
    check = subprocess.check_output([str(native), "--versions"], env=env, text=True)
    native_versions = json.loads(check)
    pycheck = """import h5py, json, numpy, pyarrow
print(json.dumps({'hdf5_version':h5py.version.hdf5_version,
 'h5py_version':h5py.__version__,'pyarrow_version':pyarrow.__version__,
 'numpy_version':numpy.__version__,'h5py_extension':h5py.h5.__file__,
 'pyarrow_extension':pyarrow.lib.__file__}))
"""
    python_versions = json.loads(subprocess.check_output(
        [str(python), "-c", pycheck], env=pyenv, text=True))
    if python_versions["hdf5_version"] != HDF5_VERSION or \
            python_versions["h5py_version"] != "3.12.1" or \
            python_versions["pyarrow_version"] != "21.0.0" or \
            python_versions["numpy_version"] != "2.1.3":
        raise RuntimeError("Python dependencies do not match the benchmark lock")
    links = {}
    timed_core_hashes = []
    for name, binary in (("daemon", daemon), ("scale_worker", helper),
                         ("native_probe", native),
                         ("h5py", Path(python_versions["h5py_extension"]))):
        timed = name != "h5py"
        resolved = subprocess.check_output(["ldd", str(binary)], env=env if timed else pyenv, text=True)
        if "not found" in resolved:
            raise RuntimeError(f"unresolved shared dependency in {name}")
        hdf5_lines = [line.strip() for line in resolved.splitlines() if "libhdf5" in line]
        if not hdf5_lines:
            raise RuntimeError(f"{name} has no resolved HDF5 dependency")
        if timed and any("=> " + str(prefix) + "/" not in line for line in hdf5_lines):
            raise RuntimeError(f"{name} is not using only the isolated HDF5 prefix")
        dependency_hashes = {}
        for line in hdf5_lines:
            library = Path(line.split("=> ", 1)[1].split(" (", 1)[0]).resolve()
            if timed and not library.is_relative_to(prefix):
                raise RuntimeError(f"{name} resolves an HDF5 library outside its prefix")
            if not timed and not library.is_relative_to(work / "venv"):
                raise RuntimeError("offline h5py is not using its isolated wheel's HDF5 libraries")
            dependency_hashes[str(library)] = sha256(library)
        if timed:
            timed_core_hashes.append(dependency_hashes)
        links[name] = {"binary": str(binary), "sha256": sha256(binary),
                       "ldd": resolved, "hdf5_dependencies": hdf5_lines,
                       "hdf5_dependency_hashes": dependency_hashes,
                       "role": "timed-query-core" if timed else "offline-preparation-and-oracle"}
    if any(core != timed_core_hashes[0] for core in timed_core_hashes[1:]):
        raise RuntimeError("daemon, scale worker and native probe do not share the exact HDF5 core")
    pyarrow_binary = Path(python_versions["pyarrow_extension"])
    arrow_links = subprocess.check_output(["ldd", str(pyarrow_binary)], env=pyenv, text=True)
    if "not found" in arrow_links:
        raise RuntimeError("unresolved shared dependency in PyArrow wheel")
    arrow_hashes = {}
    for line in arrow_links.splitlines():
        if "libarrow" in line or "libparquet" in line:
            library = Path(line.split("=> ", 1)[1].split(" (", 1)[0]).resolve()
            if not library.is_relative_to(work / "venv"):
                raise RuntimeError("PyArrow is not using its isolated wheel's shared libraries")
            arrow_hashes[str(library)] = sha256(library)
    if not arrow_hashes:
        raise RuntimeError("PyArrow wheel has no resolved Arrow dependency")
    links["pyarrow"] = {"binary": str(pyarrow_binary), "sha256": sha256(pyarrow_binary),
                        "ldd": arrow_links, "arrow_dependency_hashes": arrow_hashes,
                        "role": "IPC-client-wheel; separate from daemon C++ library profile"}
    hashes = source_hashes(source)
    frozen = json.loads((work / "source-hashes.json").read_text())
    if hashes != frozen:
        raise RuntimeError("isolated source changed after build preparation")
    record = {
        "protocol": "iotdbfs-single-file-environment/1",
        "work": str(work), "source": str(source), "prefix": str(prefix),
        "python": str(python), "daemon": str(daemon), "native_probe": str(native),
        "scale_worker": str(helper), "arrow_prefix": str(arrow), "sdk_root": str(sdk),
        "hdf5_source": {"version": HDF5_VERSION, "url": HDF5_URL,
                         "sha256": HDF5_SHA256},
        "hdf5_installation": {"marker": str(install_marker), "sha256": sha256(install_marker),
                               "component": installation["component"],
                               "compiler": installation["compiler"],
                               "threadsafe": True, "parallel": False, "high_level": False,
                               "dynamic_plugins": False},
        "python_role": "offline generation, independent oracle and IPC client; never native query reader",
        "python_hdf5_role": "manylinux wheel core; version matched, not claimed byte-identical to timed core",
        "python_versions": python_versions, "native_versions": native_versions,
        "binaries": links, "source_hashes_sha256": sha256(work / "source-hashes.json"),
        "runtime_library_path": env["LD_LIBRARY_PATH"],
        "python_library_policy": "LD_LIBRARY_PATH and PYTHONPATH unset; wheels own HDF5/Arrow libraries",
        "namespace_lock_environment_variable": "IOTDBFS_NAMESPACE_LOCK",
        "services_started": False, "credentials_recorded": False,
    }
    atomic_json(work / "env.json", record)
    return record


def build(args: argparse.Namespace, work: Path, arrow: Path, sdk: Path) -> dict:
    if not args.source_dir:
        raise RuntimeError("build requires --source-dir with a current source snapshot")
    source, prefix = work / "source", work / "hdf5-prefix"
    freeze_source(args.source_dir.resolve(), source)
    lock = json.loads((source / "ci/dependencies.lock.json").read_text())
    hdf5_lock = lock["sources"]["hdf5"]
    if hdf5_lock["version"] != HDF5_VERSION or hdf5_lock["sha256"] != HDF5_SHA256:
        raise RuntimeError("the source snapshot does not contain the HDF5 1.14.4 source lock")
    atomic_json(work / "source-hashes.json", source_hashes(source))
    logs = work / "logs"
    logs.mkdir(exist_ok=True)
    temporary = work / "tmp"
    temporary.mkdir(exist_ok=True)
    install_env = dict(os.environ, TMPDIR=str(temporary))
    install = [sys.executable, str(source / "ci/install_dependencies.py"), "--only", "hdf5",
               "--prefix", str(prefix), "--jobs", str(args.jobs),
               "--cc", args.cc, "--cxx", args.cxx]
    if args.source_archive:
        install += ["--source-archive", str(args.source_archive.resolve())]
    run(install, logs / "hdf5-install.log", install_env)
    env = runtime_environment(prefix, arrow, sdk)
    env["TMPDIR"] = str(temporary)
    env["PIP_CACHE_DIR"] = str(work / "cache/pip")
    pyenv = python_environment(work)
    run([args.python, "-m", "venv", str(work / "venv")], logs / "venv.log", pyenv)
    python = work / "venv/bin/python"
    run([str(python), "-m", "pip", "install", "--only-binary=:all:",
         "numpy==2.1.3", "pyarrow==21.0.0", "h5py==3.12.1"],
        logs / "python-dependencies.log", pyenv)
    common = ["-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_C_COMPILER=" + args.cc,
              "-DCMAKE_CXX_COMPILER=" + args.cxx]
    run(["cmake", "-S", str(source), "-B", str(work / "daemon-build"), *common,
         "-DBUILD_TESTS=ON", "-DIOTDB_ROOT=" + str(sdk),
         "-DCMAKE_PREFIX_PATH=" + str(prefix) + ";" + str(arrow)], logs / "daemon-configure.log", env)
    run(["cmake", "--build", str(work / "daemon-build"), "--target", "iotdb_fs", "unit_tests",
         "publication_payload_verify", "-j", str(args.jobs)], logs / "daemon-build.log", env)
    run(["cmake", "-S", str(source / "experiments/single_file/native"),
         "-B", str(work / "native-build"), *common, "-DCMAKE_PREFIX_PATH=" + str(prefix)],
        logs / "native-configure.log", env)
    run(["cmake", "--build", str(work / "native-build"), "-j", str(args.jobs)],
        logs / "native-build.log", env)
    return verify(work, arrow, sdk)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "verify"))
    parser.add_argument("--work", type=Path, default=SCRIPT_DIR / "work")
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--source-archive", type=Path)
    parser.add_argument("--arrow-prefix", type=Path, required=True)
    parser.add_argument("--sdk-root", type=Path, required=True)
    parser.add_argument("--python", default="python3")
    parser.add_argument("--cc", default="gcc")
    parser.add_argument("--cxx", default="g++")
    parser.add_argument("--jobs", type=int, default=6)
    args = parser.parse_args()
    try:
        work = args.work.resolve()
        allowed = SCRIPT_DIR / "work"
        if work != allowed and not work.is_relative_to(allowed):
            raise RuntimeError("--work must be experiments/single_file/work or a child directory")
        if args.jobs < 1 or args.jobs > 64:
            raise RuntimeError("--jobs must be in [1,64]")
        work.mkdir(parents=True, exist_ok=True)
        result = (build(args, work, args.arrow_prefix.resolve(), args.sdk_root.resolve())
                  if args.action == "build" else
                  verify(work, args.arrow_prefix.resolve(), args.sdk_root.resolve()))
        print(json.dumps({"ok": True, "env_json": str(work / "env.json"),
                          "hdf5_version": result["native_versions"]["runtime_hdf5"]}))
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError, KeyError) as error:
        print(json.dumps({"ok": False, "error": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
