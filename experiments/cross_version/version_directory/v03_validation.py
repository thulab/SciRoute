#!/usr/bin/env python3
"""V03 baseline correctness on a fresh schema2 owner plus read-only V02 audit.

Reuses the existing V02 corpus and permission oracle. This script does not
claim deterministic publication-race coverage; the corresponding unit/native
barrier tests must be attached separately. Never opens the V02 owner journal.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import uuid

from experiments.cross_version.pilot import corpus
from experiments.cross_version.pilot import publish
from experiments.cross_version.version_directory import v00_permissions as permissions
from experiments.cross_version.version_directory.v00_head_probe import sha, public_failure, save_json
from experiments.cross_version.version_directory.v02_validation import mount_owner, process_memory
from experiments.cross_version.version_directory.v03_n128 import sync_window, checkpoint, DATABASE


def require(value, message):
    if not value:
        raise RuntimeError(message)


def files_under(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob("*")) if p.is_file()}


def run(args):
    for name, value in vars(args).items():
        if isinstance(value, Path): setattr(args, name, value.resolve())
    require(not args.work.exists() and not args.report.exists(), "use fresh work/report paths")
    original = json.loads(args.v02_report.read_text())
    require(original["ok"] and original["table_prefix"].startswith("vd_v02_"), "V02 evidence incomplete")
    old_prefix = original["table_prefix"]
    old_journal = Path(original["journal"])
    require(old_journal.is_dir(), "preserved V02 journal is missing")
    old_journal_files = files_under(old_journal)
    args.work.mkdir(parents=True, mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    _, _, env = publish.runtime(args.environment, args.source_pid, DATABASE)
    prefix = args.owner_prefix or "vd_v03_correctness_" + uuid.uuid4().hex[:16]
    require(re.fullmatch(r"vd_v03_[a-z0-9_]+", prefix) and prefix != old_prefix,
            "new correctness prefix required")
    journal = args.work / "owner"
    report = {"protocol": "iotdbfs-v03-baseline-correctness/1", "complete": False,
        "database": DATABASE, "table_prefix": prefix, "journal": str(journal),
        "daemon_sha256": sha(args.daemon), "store_probe_sha256": sha(args.probe),
        "content_probe_sha256": sha(args.content_probe), "script_sha256": sha(__file__),
        "v02_report": str(args.v02_report), "v02_report_sha256": sha(args.v02_report),
        "credentials_recorded": False, "bindings": [], "checks": {},
        "scope": "correctness, read-only V02 graph, fresh owner, restart, permission oracle; deterministic V03 admission races are separate evidence"}
    checkpoint(args, report)
    def probe(command, selected=prefix):
        path = args.work / "control" / ("native-" + uuid.uuid4().hex[:16])
        executable = args.content_probe if command == "inspect" else args.probe
        with path.with_suffix(".stderr").open("xb") as log:
            result = subprocess.run([str(executable), command, "--prefix", selected], env=env,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=log, text=True, timeout=900)
        path.with_suffix(".stdout").write_text(result.stdout)
        require(result.returncode == 0, "native read-only probe failed; inspect private logs")
        objects = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        require(len(objects) == 1 and (objects[0].get("completed") or command == "head"), "native probe JSON missing")
        return objects[0]
    def audit(context, selected, templates):
        checked = 0
        for binding in report["bindings"]:
            entry = next(e for e in selected if (e["format"], e["scenario"], e["ordinal"]) ==
                (binding["format"], binding["scenario"], binding["ordinal"]))
            require(sha(publish.history_path(context["mount"], binding["logical"], binding["version"])) ==
                binding["sha256"], "exact source byte hash differs")
            rows = publish.discovery(context, binding["logical"], binding["format"],
                templates[binding["format"]], binding["version"])
            require(len(rows) == 4 and all(r["version"] == binding["version"] for r in rows),
                "exact dataset/message inventory differs")
            checked += publish.validate_values(context, entry, binding["logical"], binding["version"])
        for logical in sorted({b["logical"] for b in report["bindings"]}):
            sample = next(b for b in report["bindings"] if b["logical"] == logical)
            rows = publish.all_versions(context, logical, sample["format"], templates[sample["format"]])
            wanted = {b["version"] for b in report["bindings"] if b["logical"] == logical}
            require(len(rows) == 32 and {r["version"] for r in rows} == wanted, "all-version scientific inventory differs")
        return checked
    try:
        with sync_window(args, report):
            old_head = probe("head", old_prefix)
            report["v02_readonly_inspection"] = probe("inspect", old_prefix)
            require(probe("head", old_prefix) == old_head, "read-only V02 inspection changed head")
            require(files_under(old_journal) == old_journal_files, "read-only V02 inspection changed journal")
            report["checks"]["v02_prefix_readonly_unchanged"] = True
            manifest = corpus.verify(args.input)
            manifest = corpus.load(args.input)
            selected = [e for e in manifest["entries"] if e["file_index"] == 1]
            require(len(selected) == 32, "expected original two-format/two-scenario/eight-version subset")
            templates = {fmt: publish.load_manifest(base["root"]) for fmt, base in manifest["bases"].items()}
            with mount_owner(args.daemon, env, args.work / "initialize", prefix, journal, True) as context:
                (context["mount"] / "science").mkdir(mode=0o755)
                report["initial_objects"] = probe("stats")
                for entry in selected:
                    logical = "/science/" + entry["scenario"] + "." + ("h5" if entry["format"] == "hdf5" else "grib")
                    elapsed = publish.write_version(corpus.resolve(args.input, manifest, entry),
                        context["mount"] / logical.lstrip("/"), entry["ordinal"] == 1)
                    rows = publish.discovery(context, logical, entry["format"], templates[entry["format"]])
                    require(len(rows) == 4, "latest inventory differs after publication")
                    version = rows[0]["version"]
                    require(all(r["version"] == version for r in rows), "mixed exact version publication")
                    report["bindings"].append({"logical": logical, "format": entry["format"],
                        "scenario": entry["scenario"], "ordinal": entry["ordinal"], "version": version,
                        "sha256": entry["sha256"], "publication_ns": elapsed})
                    checkpoint(args, report)
                report["checks"]["first_audit_typed_units"] = audit(context, selected, templates)
                report["published_head"] = probe("head")
                report["published_objects"] = probe("stats")
                report["populated_memory"] = process_memory(context["process"].pid)
                report["content_graph"] = probe("inspect")
            with mount_owner(args.daemon, env, args.work / "restart", prefix, journal) as context:
                require(probe("head") == report["published_head"], "owner restart changed committed head")
                report["checks"]["restarted_audit_typed_units"] = audit(context, selected, templates)
                report["restart_startup_ns"] = context["startup_ns"]
                report["restarted_memory"] = process_memory(context["process"].pid)
            factory = permissions.mount_runtime
            @contextmanager
            def permission_factory(daemon, current_env, work, ignored_lock):
                # The result/control tree is 0700. A non-root client must be
                # able to traverse the host path before reaching FUSE; keep
                # the test mount in an owned public-searchable temporary tree.
                surface = Path(tempfile.mkdtemp(prefix="iotdbfs-v03-permissions-", dir="/tmp"))
                surface.chmod(0o755)
                mount_work = surface / "owner-mount"
                evidence = work / ("mount-evidence-" + surface.name)
                report["permission_mount_surface"] = {"path": str(surface),
                    "mode": "0755", "private_evidence": str(evidence)}
                try:
                    with mount_owner(args.daemon, current_env, mount_work, prefix, journal) as context:
                        yield context["mount"], context["socket"]
                finally:
                    # Never recursively clean a still-mounted experiment tree.
                    if not os.path.ismount(mount_work / "mount"):
                        shutil.copytree(surface, evidence, ignore=shutil.ignore_patterns("mount"))
                        shutil.rmtree(surface)
            permissions.mount_runtime = permission_factory
            try:
                permissions.run(SimpleNamespace(work=args.work / "permission-oracle", report=args.work / "permissions.json",
                    environment=args.environment, source_pid=args.source_pid, database=DATABASE,
                    writer_lock=args.work / "unused.lock"))
            finally:
                permissions.mount_runtime = factory
            report["permissions"] = json.loads((args.work / "permissions.json").read_text())
            report["permissions"]["reference_runtime_daemon_sha256"] = report["permissions"]["daemon_sha256"]
            report["permissions"]["daemon_sha256"] = sha(args.daemon)
            report["permissions"]["table_prefix"] = prefix
            report["permissions"]["mount_factory"] = "v03_validation.permission_factory"
            save_json(args.work / "permissions.json", report["permissions"])
            report["final_content_graph"] = probe("inspect")
            require(probe("head", old_prefix) == old_head and files_under(old_journal) == old_journal_files,
                "V02 prefix/journal changed during V03 correctness run")
            report["complete"] = True
    except BaseException as error:
        report["failure"] = public_failure(error)
        report["complete"] = False
        raise
    finally:
        checkpoint(args, report)
    print(json.dumps({"complete": True, "scientific_versions": len(report["bindings"]), "report": str(args.report)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("input", "environment", "daemon", "probe", "content-probe", "runtime", "release", "work", "report", "v02-report"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--datanode-pid", type=int, required=True)
    parser.add_argument("--owner-prefix")
    run(parser.parse_args())
