#!/usr/bin/env python3
"""V01 acceptance against new table prefixes in the authorized original instance.

Uses existing credentials in memory. Preserves owner journals and all audit logs.
Temporarily changes only original DataNode WAL/partial-insert properties and
restores them in finally; never starts an isolated node or changes activation.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
import uuid

from experiments.cross_version.pilot import corpus
from experiments.cross_version.pilot import publish
from experiments.cross_version.version_directory import v00_permissions as permissions
from experiments.cross_version.version_directory.v00_head_probe import ExistingInstance, save_json, sha
from common import load_manifest


def require(value, message):
    if not value:
        raise RuntimeError(message)


def process_memory(pid):
    fields = {}
    for line in (Path("/proc") / str(pid) / "status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM"):
            fields[key + "_kib"] = int(value.split()[0])
    return fields


@contextmanager
def mount_owner(daemon, env, work, prefix, journal, initialize=False):
    work.mkdir(parents=True, exist_ok=False, mode=0o755)
    mount = work / "mount"
    mount.mkdir(mode=0o755)
    socket_path = Path("/tmp") / ("v01-" + uuid.uuid4().hex[:16] + ".sock")
    profile = work / "profile.properties"
    profile.write_text("query.hdf5.read_parallelism=1\n"
        "query.cache.memory_budget_bytes=268435456\n"
        "cache.read.full_prefetch_max_bytes=1\ncache.raw_file.max_bytes=1\n"
        "storage.version_directory.mode=owner\n"
        f"storage.version_directory.table_prefix={prefix}\n"
        f"storage.version_directory.owner_directory={journal}\n"
        f"storage.version_directory.initialize={str(initialize).lower()}\n")
    profile.chmod(0o600)
    current = dict(env, IOTDB_CONFIG_FILE=str(profile),
        IOTDBFS_QUERY_SOCKET=str(socket_path), IOTDBFS_QUERY_SOCKET_MODE="0666")
    # An alternate legacy lock must not bypass mandatory owner prefix locking.
    current["IOTDBFS_NAMESPACE_LOCK"] = str(work / "irrelevant-legacy.lock")
    current.pop("IOTDBFS_EXPERIMENT_FROZEN_TYPED", None)
    with (work / "mount.log").open("xb") as log:
        os.chmod(log.name, 0o600)
        process = subprocess.Popen([str(daemon), "-f", "-o",
            "allow_other,attr_timeout=0,entry_timeout=0", str(mount)],
            env=current, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            startup = time.perf_counter_ns()
            deadline = time.monotonic() + 90
            while not (os.path.ismount(mount) and socket_path.exists()):
                require(process.poll() is None, "owner daemon exited; inspect private mount.log")
                require(time.monotonic() < deadline, "owner mount startup timed out")
                time.sleep(.05)
            yield {"mount": mount, "socket": str(socket_path), "root": work,
                   "process": process, "environment": current, "profile": profile,
                   "startup_ns": time.perf_counter_ns() - startup}
        finally:
            if os.path.ismount(mount):
                subprocess.run(["fusermount", "-u", str(mount)], check=True, timeout=30)
            if process.poll() is None:
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=30)


def rejected_mount(args, env, work, prefix, journal, initialize, reason):
    try:
        with mount_owner(args.daemon, env, work, prefix, journal, initialize):
            raise AssertionError("unsafe owner mount was admitted")
    except RuntimeError as error:
        require(str(error).startswith("owner daemon exited"), "unexpected rejection reason")
    log = (work / "mount.log").read_text(errors="replace")
    require(reason in log, "mount rejection lacks expected diagnostic: " + reason)
    return {"rejected": True, "log_sha256": sha(work / "mount.log")}


def run(args):
    require(sys.platform == "linux" and os.geteuid() == 0, "Linux/root required")
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    require(not args.report.exists() and not args.work.exists(), "use fresh work/report paths")
    require(args.release == (args.runtime / "current").resolve(), "original release mismatch")
    args.work.mkdir(parents=True, mode=0o755)
    control = args.work / "control"
    control.mkdir(mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    runtime, legacy_daemon, env = publish.runtime(args.environment, args.source_pid, args.database)
    token = uuid.uuid4().hex[:16]
    prefix = "vd_v01_" + token
    journal = args.work / "owner"
    report = {"protocol": "iotdbfs-v01-acceptance/1", "ok": False,
        "database": args.database, "table_prefix": prefix, "journal": str(journal),
        "account_policy": "existing shared account; controlled single-host owner, no account isolation",
        "daemon_sha256": sha(args.daemon), "probe_sha256": sha(args.probe),
        "legacy_daemon_sha256": sha(legacy_daemon), "script_sha256": sha(Path(__file__)),
        "credentials_recorded": False, "checks": {}, "bindings": [],
        "scope": "V01 directory correctness/recovery, not V02 proof caching or payload dedup/performance"}
    def checkpoint():
        save_json(args.report, report)
    def event(kind, **data):
        record = {"time_ns": time.time_ns(), "event": kind, **data}
        with (control / "operations.jsonl").open("a") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        print(json.dumps({"event": kind}), flush=True)
    def probe(command, selected=prefix, journal_path=None):
        number = time.time_ns()
        command_line = [str(args.probe), command, "--prefix", selected]
        if journal_path:
            command_line.extend(["--journal", str(journal_path)])
        with (control / f"probe-{number}.stderr").open("xb") as log:
            completed = subprocess.run(command_line, env=env, stdout=subprocess.PIPE,
                stderr=log, text=True, timeout=300)
        # SDK diagnostics can precede JSON. Preserve complete output privately.
        output = control / f"probe-{number}.stdout"
        output.write_text(completed.stdout)
        output.chmod(0o600)
        require(completed.returncode == 0, "native owner probe failed; inspect private log")
        objects = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
        require(len(objects) == 1 and (objects[0].get("completed") or
            (command == "head" and "seq" in objects[0])), "native probe JSON missing")
        return objects[0]
    def head():
        result = probe("head")
        return result
    def one_commit(label, operation):
        before = head()
        started = time.perf_counter_ns()
        operation()
        elapsed = time.perf_counter_ns() - started
        after = head()
        require(after["seq"] == before["seq"] + 1, label + " did not publish exactly one head")
        report["checks"][label] = {"before": before, "after": after, "operation_ns": elapsed}
        checkpoint()
    lock = (args.runtime / "run/v00-head-experiment.lock").open("a+b")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    instance = ExistingInstance(SimpleNamespace(runtime=args.runtime, release=args.release,
        work=control, datanode_pid=args.datanode_pid), event)
    def terminate(signum, frame):
        raise RuntimeError("acceptance interrupted by signal " + str(signum))
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGHUP, terminate)
    checkpoint()
    try:
        instance.enable_sync()
        event("v01_sync_window_ready")
        report["fault_probe"] = probe("faults", "vd_v01_fault_" + token, args.work / "fault-owner")
        checkpoint()
        manifest = corpus.load(args.input)
        selected = [e for e in manifest["entries"] if e["scenario"] == "identical" and e["file_index"] == 1]
        require(len(selected) == 16, "expected 8 versions for each scientific format")
        templates = {fmt: load_manifest(base["root"]) for fmt, base in manifest["bases"].items()}
        with mount_owner(args.daemon, env, args.work / "initial", prefix, journal, True) as context:
            mount = context["mount"]
            require(not (mount / "cross-version-1b723c3e5e464290").exists(), "legacy namespace leaked")
            report["initial_head"] = head()
            report["initial_mount_startup_ns"] = context["startup_ns"]
            report["initial_directory_objects"] = probe("stats")
            report["initial_daemon_memory"] = process_memory(context["process"].pid)
            report["checks"]["second_owner_same_prefix_rejected"] = rejected_mount(args, env,
                args.work / "competitor", prefix, args.work / "other-owner", False,
                "namespace writer lock is held by another process")
            directory = mount / "science"
            one_commit("mkdir_single_head", lambda: directory.mkdir(mode=0o755))
            one_commit("chmod_single_head", lambda: os.chmod(directory, 0o750))
            one_commit("chown_single_head", lambda: os.chown(directory, 0, 40077))
            one_commit("utimens_single_head", lambda: os.utime(directory, ns=(1700000000000000000, 1700000001000000000)))
            os.chmod(directory, 0o755)
            for entry in selected:
                logical = "/science/versions." + ("h5" if entry["format"] == "hdf5" else "grib")
                fixture = corpus.resolve(args.input, manifest, entry)
                write_ns = publish.write_version(fixture, mount / logical[1:], entry["ordinal"] == 1)
                rows = publish.discovery(context, logical, entry["format"], templates[entry["format"]])
                report["last_publication"] = {"format": entry["format"], "ordinal": entry["ordinal"],
                    "logical": logical, "discovery_rows": rows, "publication_ns": write_ns}
                checkpoint()
                require(len(rows) == 4, "scientific latest catalog differs")
                version = rows[0]["version"]
                require(all(r["version"] == version for r in rows), "mixed exact scientific versions")
                require(sha(publish.history_path(mount, logical, version)) == entry["sha256"], "exact bytes differ")
                units = publish.validate_values(context, entry, logical, version)
                report["bindings"].append({"format": entry["format"], "ordinal": entry["ordinal"],
                    "logical": logical, "version": version, "sha256": entry["sha256"],
                    "typed_units": units, "publication_ns": write_ns})
                checkpoint()
            listed = {path.name for path in directory.iterdir()}
            for fmt in ("hdf5", "grib"):
                logical = "/science/versions." + ("h5" if fmt == "hdf5" else "grib")
                versions = permissions.catalog_rows(context["socket"], logical, versions="all")
                latest = max(row["source.version"] for row in versions)
                for row in versions:
                    hidden = publish.history_path(mount, logical, row["source.version"]).name
                    require((hidden in listed) == (row["source.version"] != latest),
                            "readdir did not hide exactly the older versions")
            report["checks"]["history_readdir_order"] = True
            # A normal reader must observe the last committed content while a
            # writer retains an unpublished staged buffer on its own handle.
            plain = mount / "pending.bin"
            with plain.open("wb", buffering=0) as file:
                file.write(b"old"); os.fsync(file.fileno())
            with plain.open("r+b", buffering=0) as file:
                file.write(b"new")
                require(plain.read_bytes() == b"old", "ordinary reader saw uncommitted buffer")
                file.seek(0)
                require(file.read() == b"new", "writer lost own staged buffer")
                os.fsync(file.fileno())
                require(plain.read_bytes() == b"new", "fsync did not publish writer buffer")
            report["checks"]["staged_writer_isolation"] = True
            one_commit("rename_subtree_single_head", lambda: directory.rename(mount / "renamed"))
            require(not directory.exists(), "rename old current path remains")
            for binding in report["bindings"]:
                renamed = binding["logical"].replace("/science/", "/renamed/", 1)
                require(sha(publish.history_path(mount, renamed, binding["version"])) == binding["sha256"], "renamed history bytes differ")
                entry = next(e for e in selected if e["format"] == binding["format"] and e["ordinal"] == binding["ordinal"])
                publish.validate_values(context, entry, renamed, binding["version"])
                binding["renamed"] = renamed
            victim = report["bindings"][-1]
            target = publish.history_path(mount, victim["renamed"], victim["version"])
            one_commit("exact_delete_single_head", lambda: permissions.unlink_after_release(target))
            require(not target.exists(), "deleted exact version remains visible")
            victim["deleted"] = True
            # Equal bytes at the other seven versions remain live.
            report["before_restart_head"] = head()
            report["populated_directory_objects"] = probe("stats")
            report["populated_daemon_memory"] = process_memory(context["process"].pid)
            checkpoint()
        event("owner_graceful_close")
        # Reuse the V00 permission oracle unchanged apart from its mount factory.
        original_factory = permissions.mount_runtime
        @contextmanager
        def permission_factory(daemon, current_env, work, ignored_lock):
            with mount_owner(args.daemon, current_env, work / "owner-mount", prefix, journal) as context:
                yield context["mount"], context["socket"]
        permissions.mount_runtime = permission_factory
        try:
            permissions.run(SimpleNamespace(work=args.work / "permission-oracle", report=args.work / "permissions.json",
                environment=args.environment, source_pid=args.source_pid, database=args.database,
                writer_lock=args.work / "unused.lock"))
        finally:
            permissions.mount_runtime = original_factory
        report["permissions"] = json.loads((args.work / "permissions.json").read_text())
        report["permissions"]["runtime_reference_daemon_sha256"] = report["permissions"]["daemon_sha256"]
        report["permissions"]["daemon_sha256"] = sha(args.daemon)
        report["permissions"]["mount_factory"] = "v01_validation.mount_owner"
        report["permissions"]["table_prefix"] = prefix
        save_json(args.work / "permissions.json", report["permissions"])
        report["before_crash_head"] = head()
        checkpoint()
        instance.stop(crash=True)
        instance.start()
        event("original_datanode_crash_recovered")
        with mount_owner(args.daemon, env, args.work / "recovered", prefix, journal) as context:
            report["recovered_mount_startup_ns"] = context["startup_ns"]
            require(head() == report["before_crash_head"], "durable head changed across crash")
            for binding in report["bindings"]:
                path = publish.history_path(context["mount"], binding["renamed"], binding["version"])
                if binding.get("deleted"):
                    require(not path.exists(), "tombstone resurrected after restart")
                else:
                    require(sha(path) == binding["sha256"], "recovered version bytes differ")
                    entry = next(e for e in selected if e["format"] == binding["format"] and e["ordinal"] == binding["ordinal"])
                    publish.validate_values(context, entry, binding["renamed"], binding["version"])
            report["checks"]["datanode_crash_and_owner_recovery"] = True
            report["recovered_daemon_memory"] = process_memory(context["process"].pid)
        report["checks"]["reinitialize_existing_namespace_rejected"] = rejected_mount(args, env,
            args.work / "reinitialize", prefix, journal, True,
            "initialization requires an empty namespace")
        report["final_head"] = head()
        report["final_directory_objects"] = probe("stats")
        checkpoint()
    except BaseException as error:
        # Private mount/probe logs retain diagnostics; no SDK exception or secret
        # is serialized into the public report.
        report["failure_type"] = type(error).__name__
        if isinstance(error, (RuntimeError, AssertionError)):
            report["failure"] = str(error)
        raise
    finally:
        try:
            instance.restore()
            report["original_service_restored"] = True
            report["original_config_sha256"] = sha(instance.config)
            report["final_datanode_pid"] = instance.pid
        finally:
            checkpoint()
            lock.close()
    report["checks"]["async_mode_refuses_owner_activation"] = rejected_mount(args, env,
        args.work / "async-rejection", "vd_v01_async_" + token, args.work / "async-owner", True,
        "version store requires effective WAL=SYNC")
    report["ok"] = True
    checkpoint()
    print(json.dumps({"ok": True, "report": str(args.report), "scientific_versions": len(report["bindings"])}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("environment", "input", "daemon", "probe", "runtime", "release", "work", "report"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--source-pid", required=True, type=int)
    parser.add_argument("--datanode-pid", required=True, type=int)
    parser.add_argument("--database", default=publish.DATABASE)
    run(parser.parse_args())
