"""Out-of-clock DataNode quiescence and redacted live-environment receipts.

Quiescence issues the caller's database-scoped FLUSH, then only observes the
identified DataNode. It does not flush other databases, drop caches, change
settings, signal processes or alter service placement. Environment collection
never copies complete command lines, environment variables or configurations.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


SAMPLE_SECONDS = 2.0
REQUIRED_WINDOWS = 3
MAX_CPU_CORES = 0.10
MAX_WRITE_BYTES_PER_WINDOW = 1024 * 1024
MAX_WAIT_SECONDS = 180.0


def _require(condition, message):
    if not condition:
        raise RuntimeError(message)


def _save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def _process_sample(pid):
    process = Path("/proc") / str(pid)
    # stat's comm may contain spaces and closing parentheses.
    fields = (process / "stat").read_text().rsplit(") ", 1)[1].split()
    _require(fields[0] != "Z", "DataNode is a zombie")
    io = dict(line.split(":", 1) for line in (process / "io").read_text().splitlines())
    return dict(pid=pid, start_ticks=int(fields[19]),
                cpu_ticks=int(fields[11]) + int(fields[12]),
                read_bytes=int(io["read_bytes"]), write_bytes=int(io["write_bytes"]),
                sampled_monotonic_ns=time.monotonic_ns())


def quiesce(admin, pid, output_path):
    """Wait for three quiet DataNode windows; save evidence even on failure.

    ``admin.call('flush')`` must be the already qualified helper issuing
    ``FLUSH <this experiment database>``. Its scope is a caller precondition;
    this function never substitutes a global SQL flush. The whole operation is
    outside performance clocks. Thresholds are fixed before formal collection.
    """
    _require(type(pid) is int and pid > 0, "positive DataNode PID required")
    output_path = Path(output_path)
    _require(not output_path.exists(), "quiescence receipt already exists")
    report = dict(protocol="iotdbfs-table5-quiescence/1", complete=False, pid=pid,
                  sample_seconds=SAMPLE_SECONDS, required_consecutive_windows=REQUIRED_WINDOWS,
                  cpu_core_threshold=MAX_CPU_CORES,
                  write_bytes_per_window_threshold=MAX_WRITE_BYTES_PER_WINDOW,
                  deadline_seconds=MAX_WAIT_SECONDS,
                  flush_scope="caller-qualified experiment database only", windows=[])
    begin = time.monotonic_ns()
    deadline = begin + int(MAX_WAIT_SECONDS * 1e9)
    try:
        before_flush = _process_sample(pid)
        report["process_start_ticks"] = before_flush["start_ticks"]
        _save(output_path, report)
        _require(admin.call("flush") == "OK", "database-scoped FLUSH was not acknowledged")
        report["flush_acknowledged"] = True
        report["flush_wall_ns"] = time.monotonic_ns() - begin
        previous = _process_sample(pid)
        _require(previous["start_ticks"] == before_flush["start_ticks"], "DataNode changed during FLUSH")
        report["post_flush_baseline"] = previous
        hz = os.sysconf("SC_CLK_TCK")
        report["clock_ticks_per_second"] = hz
        consecutive = 0
        while consecutive < REQUIRED_WINDOWS:
            _require(time.monotonic_ns() + int(SAMPLE_SECONDS * 1e9) <= deadline,
                     "DataNode did not quiesce before the fixed deadline")
            time.sleep(SAMPLE_SECONDS)
            current = _process_sample(pid)
            _require(current["start_ticks"] == before_flush["start_ticks"], "DataNode changed while awaiting quiescence")
            elapsed_ns = current["sampled_monotonic_ns"] - previous["sampled_monotonic_ns"]
            cpu_ticks = current["cpu_ticks"] - previous["cpu_ticks"]
            write_bytes = current["write_bytes"] - previous["write_bytes"]
            read_bytes = current["read_bytes"] - previous["read_bytes"]
            _require(elapsed_ns > 0 and min(cpu_ticks, write_bytes, read_bytes) >= 0,
                     "DataNode resource counters regressed")
            cpu_cores = cpu_ticks / hz / (elapsed_ns / 1e9)
            quiet = cpu_cores <= MAX_CPU_CORES and write_bytes <= MAX_WRITE_BYTES_PER_WINDOW
            consecutive = consecutive + 1 if quiet else 0
            report["windows"].append(dict(start_ns=previous["sampled_monotonic_ns"],
                end_ns=current["sampled_monotonic_ns"], elapsed_ns=elapsed_ns,
                cpu_ticks_delta=cpu_ticks, cpu_cores=cpu_cores,
                write_bytes_delta=write_bytes, read_bytes_delta=read_bytes,
                quiet=quiet, consecutive_quiet_windows=consecutive))
            report["elapsed_ns"] = time.monotonic_ns() - begin
            _save(output_path, report)
            previous = current
        _require(time.monotonic_ns() <= deadline, "quiescence observation exceeded the fixed deadline")
        report.update(complete=True, elapsed_ns=time.monotonic_ns()-begin,
                      final_sample=previous)
        _save(output_path, report)
        return report
    except BaseException as error:
        report.update(complete=False, elapsed_ns=time.monotonic_ns()-begin,
                      failure_type=type(error).__name__,
                      failure="quiescence qualification failed; see retained windows and private runner error")
        _save(output_path, report)
        raise


def _digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _mount(path):
    path = Path(path).resolve(strict=True)
    result = subprocess.run(["findmnt", "--json", "--target", str(path),
                             "--output", "TARGET,SOURCE,FSTYPE,MAJ:MIN"],
                            check=True, text=True, capture_output=True, timeout=30)
    filesystems = json.loads(result.stdout).get("filesystems", [])
    _require(len(filesystems) == 1, "mount identity is ambiguous")
    mount = filesystems[0]
    # Do not expose userinfo on unusual network mount source strings.
    source = str(mount.get("source", ""))
    _require("@" not in source and "password=" not in source.lower(), "mount source needs private inspection")
    return dict(path=str(path), mount=mount, device_number=path.stat().st_dev)


def live_environment(pid, port, work):
    """Return allowlisted live DataNode/JAR/storage/resource evidence only."""
    _require(type(pid) is int and pid > 0 and type(port) is int and 0 < port < 65536,
             "valid DataNode PID and RPC port required")
    proc = Path("/proc") / str(pid)
    initial = _process_sample(pid)
    arguments = (proc / "cmdline").read_bytes().split(b"\0")
    allowed_properties = ("IOTDB_HOME", "IOTDB_CONF", "IOTDB_DATA_HOME", "CONFIGNODE_HOME", "CONFIGNODE_CONF")
    properties = {}
    flags = []
    for argument in arguments:
        text = argument.decode(errors="strict")
        if re.fullmatch(r"-Xm[sx][0-9]+[kKmMgG]?", text) or re.fullmatch(r"-XX:MaxDirectMemorySize=[0-9]+[kKmMgG]?", text):
            flags.append(text)
        for key in allowed_properties:
            prefix = "-D" + key + "="
            if text.startswith(prefix):
                properties[key] = text[len(prefix):]
    _require("IOTDB_HOME" in properties, "live DataNode home missing")
    _require(any(arg in (b"org.apache.iotdb.db.service.DataNode", b"com.timecho.iotdb.DataNode") for arg in arguments),
             "identified process is not a DataNode")
    cwd = (proc / 'cwd').resolve(strict=True)
    def absolute(value):
        path=Path(value)
        return (path if path.is_absolute() else cwd/path).resolve(strict=True)
    home = absolute(properties["IOTDB_HOME"])
    conf = absolute(properties.get("IOTDB_CONF", str(home / "conf")))
    config = conf / "iotdb-system.properties"
    # Only storage-path settings are retained; complete configuration is never returned.
    wanted = {"dn_data_dirs", "dn_wal_dirs", "dn_system_dir", "dn_consensus_dir",
              "cn_system_dir", "cn_consensus_dir"}
    settings = {}
    for line in config.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "!")) or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() in wanted:
            settings[key.strip()] = value.strip()
    _require("dn_data_dirs" in settings and "dn_wal_dirs" in settings,
             "explicit DataNode storage paths missing")
    socket_result = subprocess.run(["ss", "-H", "-ltnp", "sport = :"+str(port)],
                                   check=True, text=True, capture_output=True, timeout=30)
    listeners = []
    for line in socket_result.stdout.splitlines():
        pids = {int(value) for value in re.findall(r"\bpid=(\d+)", line)}
        _require(pids == {pid}, "RPC listener PID differs from qualified DataNode")
        fields = line.split()
        _require(len(fields) >= 5 and fields[0] == "LISTEN", "RPC listener fields differ")
        listeners.append(dict(local_address=fields[3], pid=pid))
    _require(bool(listeners), "qualified DataNode does not own RPC listener")
    lib = (home / "lib").resolve(strict=True)
    jars = {path.name: _digest(path) for path in sorted(lib.glob("*.jar"))}
    _require(bool(jars), "live release contains no JARs")
    mounts = {"work": _mount(work)}
    for key, value in settings.items():
        entries = [entry.strip() for entry in re.split(r"[,;]", value) if entry.strip()]
        _require(bool(entries), "empty configured storage path")
        for index, entry in enumerate(entries):
            _require("://" not in entry, "nonlocal storage needs a separate evidence profile")
            location = Path(entry)
            if not location.is_absolute():
                location = home / location
            # ConfigNode consensus/state directories may not exist in standalone modes.
            if location.exists():
                mounts[key+":"+str(index)] = _mount(location)
            else:
                _require(key.startswith("cn_"), "configured DataNode storage path is absent")
                mounts[key+":"+str(index)] = dict(path=str(location), exists=False)
    membership = [line[3:] for line in (proc / "cgroup").read_text().splitlines() if line.startswith("0::")]
    _require(len(membership) == 1 and membership[0].startswith("/"), "unified DataNode cgroup missing")
    group = Path("/sys/fs/cgroup") / membership[0].lstrip("/")
    resources = {name: (group / name).read_text().strip() if (group/name).exists() else None for name in
                 ("memory.max", "memory.swap.max", "memory.swap.current", "memory.events", "cpu.max", "cpuset.cpus.effective")}
    _require("-Xmx28672M" in flags and "-XX:MaxDirectMemorySize=4096M" in flags,
             "live database memory differs from the predeclared shared-service profile")
    _require(_process_sample(pid)["start_ticks"] == initial["start_ticks"], "DataNode changed during environment capture")
    return dict(protocol="iotdbfs-table5-live-environment/1", pid=pid,
                process_start_ticks=initial["start_ticks"],
                java_executable=str((proc / "exe").resolve(strict=True)),
                jvm_memory_flags=flags, java_properties=properties,
                home=str(home), conf=str(conf), config_sha256=_digest(config),
                storage_settings=settings, rpc_port=port, listeners=listeners,
                resolved_release_lib=str(lib), jar_sha256=jars,
                mounts=mounts, cgroup=membership[0], resources=resources)
