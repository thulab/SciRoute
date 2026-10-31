#!/usr/bin/env python3
"""Isolated live H09 migration proof; preserves its database and evidence.

Uses real SIGKILL at a durable pre-ingestion seam. The after-publication case
is explicitly a simulated lost final journal event, not a claimed crash test.
Connection secrets are inherited from a trusted daemon and never logged.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import uuid


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def exact_version_path(mount, logical, version):
    stem, extension = os.path.splitext(logical[1:])
    return mount / f".{stem}_{version}{extension}"


class Experiment:
    def __init__(self, args):
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "arrow_training"))
        from a12_migration_smoke import connection_environment
        self.root = Path(tempfile.mkdtemp(prefix="h09-migration-", dir=args.reports_parent)).resolve()
        self.database = f"h09probe{int(time.time())}{uuid.uuid4().hex[:6]}"
        self.env = connection_environment(args.source_pid, self.database)
        self.env["IOTDB_CONFIG_FILE"] = "/nonexistent/h09-isolated-config.properties"
        self.env["IOTDBFS_NAMESPACE_LOCK"] = str(self.root / "namespace.lock")
        self.fixture = args.build.resolve() / "bin/h09_migration_fixture"
        self.cli = args.build.resolve() / "bin/iotdb_fs"
        self.prepared = args.prepared.resolve()
        self.commands = 0
        self.summary = {"protocol": "iotdbfs-hdf5-h09-live/1", "database": self.database,
                        "report": str(self.root), "checks": {}}
        for binary in (self.fixture, self.cli):
            require(binary.is_file(), f"missing executable: {binary}")

    def run(self, command, expected=0):
        self.commands += 1
        result = subprocess.run([str(item) for item in command], env=self.env,
                                capture_output=True, text=True, timeout=180)
        log = self.root / f"command-{self.commands:03}.json"
        write_json(log, {"arguments": [str(item) for item in command], "returncode": result.returncode,
                         "stdout": result.stdout, "stderr": result.stderr})
        require(result.returncode == expected,
                f"unexpected command exit {result.returncode}, expected {expected}; see {log}")
        return [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]

    def seed(self, path, raw, mode="pass"):
        result = self.run([self.fixture, "seed", path, raw, mode])
        require(len(result) == 1, "seed must publish exactly one version")
        return result[0]

    def migration(self, action, report, path=None, expected=0):
        args = [self.cli, "hdf5-migrate", action, "--report", report]
        if path is not None:
            args += ["--path", path]
        return self.run(args, expected)

    def verify(self, path, version, raw, format_id, protocol=None):
        actual = self.run([self.fixture, "verify", path, version, raw])
        require(len(actual) == 1 and actual[0]["format_id"] == format_id,
                "exact stored format identity mismatch")
        if protocol:
            require(actual[0]["layout_protocol"] == protocol, "exact layout protocol mismatch")
        return actual[0]

    def check_result(self, result, source):
        require(result["state"] == "verified", "migration was not verified")
        require(result["source_version"] == source["version"] and
                result["destination_version"] > source["version"], "migration changed wrong version")
        require(result["source_sha256"] == result["destination_sha256"] == source["sha256"],
                "migration did not preserve original file bytes")
        require(len(result["layout_sha256"]) == 64, "migration has no layout hash")

    def before_write_interruption(self, raw):
        path = "/h09-interrupted.hdf5"
        old = self.seed(path, raw)
        report = self.root / "interrupted"
        self.migration("inventory", report, path)
        marker = self.root / "before-ingest.ready"
        with (self.root / "pause-fixture.log").open("wb") as log:
            child = subprocess.Popen([str(self.fixture), "pause-migrate", path, str(marker), str(report)],
                                     env=self.env, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 120
                while not marker.exists():
                    require(child.poll() is None, "pause fixture exited before durable marker")
                    require(time.monotonic() < deadline, "pause fixture timed out")
                    time.sleep(0.1)
                started = json.loads(marker.read_text())
                require(started["state"] == "started", "fault seam is not pre-ingestion")
                child.kill()
                require(child.wait(timeout=10) == -signal.SIGKILL, "fixture did not exit through SIGKILL")
            finally:
                if child.poll() is None:
                    child.kill(); child.wait(timeout=10)
        before_resume = self.migration("inventory", self.root / "interrupted-after-kill", path)
        require(len(before_resume) == 1 and before_resume[0]["source_version"] == old["version"] and
                before_resume[0]["kind"] == "candidate", "SIGKILL seam published a destination")
        resumed = self.migration("resume", report)
        require(len(resumed) == 1, "resume returned unexpected results")
        self.check_result(resumed[0], old)
        repeated = self.migration("resume", report)
        require(repeated == resumed, "repeat resume was not idempotent")
        self.verify(path, old["version"], raw, None)
        self.verify(path, resumed[0]["destination_version"], raw, "hdf5", "IOTDBFS-HDF5-LAYOUT/3")
        return {"actual_sigkill_before_ingestion": True, "no_destination_published_before_resume": True,
                "started": started,
                "resumed": resumed[0], "repeat_resume_identical": True}

    def conflict(self, raw):
        path, report = "/h09-conflict.hdf5", self.root / "conflict"
        old = self.seed(path, raw)
        self.migration("inventory", report, path)
        newer = self.seed(path, raw)
        result = self.migration("run", report, expected=1)
        require(len(result) == 1 and result[0]["state"] != "verified", "stale inventory was migrated")
        self.verify(path, newer["version"], raw, None)
        post = self.migration("inventory", self.root / "conflict-after", path)
        candidates = [item for item in post if item["kind"] == "candidate"]
        require(len(post) == 2 and len(candidates) == 1 and
                candidates[0]["source_version"] == newer["version"], "conflict unexpectedly published a target")
        return {"frozen_source": old, "newer_source": newer, "rejection": result[0],
                "no_destination_published": True}

    def live_mount(self, manifest, sources, migrated):
        from h08_mount_smoke import declaration, register, read_hdf5, filesystem_checks, sha256
        import numpy as np
        mount = self.root / "mount"
        mount.mkdir()
        socket_path = Path(f"/tmp/h09-query-{os.getpid()}-{uuid.uuid4().hex[:6]}.sock")
        env = self.env.copy()
        env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
        env["IOTDBFS_QUERY_SOCKET_MODE"] = "0600"
        process = None
        checks = {}
        try:
            with (self.root / "mount.log").open("wb") as log:
                process = subprocess.Popen([str(self.cli), "-f", "-o", "attr_timeout=0,entry_timeout=0", str(mount)],
                                           env=env, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 60
            while not (socket_path.exists() and os.path.ismount(mount)):
                require(process.poll() is None, "isolated mount exited; see mount.log")
                require(time.monotonic() < deadline, "isolated mount did not become ready")
                time.sleep(0.1)
            self.migration("inventory", self.root / "contended", expected=3)
            checks["shared_writer_lock_rejected_migration"] = True
            for path, old in sources.items():
                new = migrated[path]
                require(sha256(mount / path[1:]) == old["sha256"], "ordinary latest file hash mismatch")
                for version in (old["version"], new["destination_version"]):
                    require(sha256(exact_version_path(mount, path, version)) == old["sha256"],
                            "ordinary exact-version file hash mismatch")
            checks["ordinary_latest_and_exact_versions_sha256"] = True
            small = "/h09-small.hdf5"
            request = declaration(small, migrated[small]["destination_version"], "/array", [1, 2], [6, 10])
            route = register(str(socket_path), request)
            checks["compressed_typed"] = read_hdf5(route, np.load(self.prepared / "compressed.npy")[1:6, 2:10],
                "/array", small, migrated[small]["destination_version"], [1, 2])
            checks["typed_filesystem"] = filesystem_checks(route)
            public = "/h09-public.hdf5"
            begin, end = manifest["public_begin"], manifest["public_end"]
            request = declaration(public, migrated[public]["destination_version"], "/strain/Strain", [begin], [end])
            route = register(str(socket_path), request)
            checks["public_typed"] = read_hdf5(route, np.load(self.prepared / "public-values.npy"),
                "/strain/Strain", public, migrated[public]["destination_version"], [begin])
            request = declaration(small, sources[small]["version"], "/array", [0, 0], [1, 1])
            checks["old_pass_through_typed_rejected"] = register(str(socket_path), request, expect_failure=True)
            return checks
        finally:
            if process is not None:
                if os.path.ismount(mount):
                    subprocess.run(["fusermount", "-u", str(mount)], timeout=30, check=False,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate(); process.wait(timeout=10)

    def execute(self):
        from h08_mount_smoke import sha256
        manifest = json.loads((self.prepared / "manifest.json").read_text())
        require(manifest["protocol"] == "iotdbfs-hdf5-h08-fixture/1", "unknown prepared fixture protocol")
        for name in ("fixture", "public"):
            require(sha256(manifest[name]) == manifest[name + "_sha256"], f"prepared {name} changed")
        raw = Path(manifest["fixture"])
        plain = self.root / "plain.txt"
        plain.write_text("H09 ordinary byte file\n")
        historical = self.seed("/h09-small.hdf5", raw)
        sources = {"/h09-small.hdf5": self.seed("/h09-small.hdf5", raw),
                   "/h09-public.hdf5": self.seed("/h09-public.hdf5", manifest["public"])}
        markers = {}
        for name, data, mode in (("plain", plain, "pass"), ("indexed", raw, "indexed"),
                                 ("old1", raw, "layout1"), ("old2", raw, "layout2"), ("other", raw, "other")):
            markers[name] = self.seed(f"/h09-{name}.hdf5", data, mode)
        report = self.root / "primary"
        inventory = self.migration("inventory", report)
        kinds = {(item["path"], item["source_version"]): item["kind"] for item in inventory}
        require(len(inventory) == 8, "inventory lost a declared fixture version")
        require(kinds[("/h09-small.hdf5", historical["version"])] == "historical_version", "history was selected")
        for path, source in sources.items():
            require(kinds[(path, source["version"])] == "candidate", "latest HDF pass-through is not a candidate")
        for name, kind in (("plain", "not_format"), ("indexed", "already_indexed"),
                           ("old1", "requires_reingestion"), ("old2", "requires_reingestion"), ("other", "other_format")):
            require(kinds[(f"/h09-{name}.hdf5", markers[name]["version"])] == kind, f"bad {name} classification")
        migrated = self.migration("run", report)
        require(len(migrated) == 2, "only latest pass-through candidates may migrate")
        results = {item["path"]: item for item in migrated}
        for path, source in sources.items():
            self.check_result(results[path], source)
            original = raw if path.endswith("small.hdf5") else manifest["public"]
            self.verify(path, source["version"], original, None)
            target = self.verify(path, results[path]["destination_version"], original, "hdf5", "IOTDBFS-HDF5-LAYOUT/3")
            require(target["layout_sha256"] == results[path]["layout_sha256"], "published layout hash mismatch")
        self.verify("/h09-small.hdf5", historical["version"], raw, None)
        repeated = self.migration("resume", report)
        require(repeated == migrated, "repeat successful resume changed destinations")
        # This is a simulation of losing final events, not a real power-loss test.
        journal = report / "journal.log"
        lines = journal.read_text().splitlines(keepends=True)
        started = [line for line in lines if len(line.split()) == 13 and line.split()[6] == "started"]
        require(len(started) == 2, "journal did not contain two start events")
        (report / "journal-before-simulation.log").write_text("".join(lines))
        with journal.open("w") as out:
            out.writelines(started); out.flush(); os.fsync(out.fileno())
        reconciled = self.migration("resume", report)
        require(reconciled == migrated, "published destinations were not reconciled identically")
        require(self.migration("resume", report) == migrated, "post-reconcile resume changed destinations")
        self.summary["checks"].update({"inventory": inventory, "migrated": migrated,
            "synthetic_old_layout_markers": "only magic/version changed; classification fixtures, not historical encodings",
            "latest_only_and_historical_bytes_preserved": True, "repeat_resume_identical": True,
            "after_publish_simulated_lost_final_events": {"reconciled_existing_destinations": True}})
        self.summary["checks"]["before_write_interruption"] = self.before_write_interruption(raw)
        self.summary["checks"]["latest_conflict"] = self.conflict(raw)
        self.summary["checks"]["live_mount"] = self.live_mount(manifest, sources, results)
        self.summary["ok"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--reports-parent", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    args = parser.parse_args()
    require(sys.platform == "linux", "live migration proof requires Linux FUSE")
    args.reports_parent.mkdir(parents=True, exist_ok=True)
    experiment = Experiment(args)
    try:
        experiment.execute()
    except Exception as error:
        experiment.summary.update(ok=False, error=str(error))
        raise
    finally:
        write_json(experiment.root / "summary.json", experiment.summary)
        print(json.dumps({"report": str(experiment.root), "database": experiment.database,
                          "ok": experiment.summary.get("ok", False)}), flush=True)


if __name__ == "__main__":
    main()
