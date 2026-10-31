#!/usr/bin/env python3
"""Verify the existing 256-version pilot with the new binary's default strict mode.

Copies the publication report and preserves the original report and bindings.
The legacy runtime descriptor is checked before substituting the explicit new
binary. No owner-mode properties or new publication are used.
"""
import argparse
import json
from pathlib import Path

from experiments.cross_version.pilot import publish
from environment import atomic_json


def run(args):
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    publish.require(not args.report.exists() and not args.work.exists(), "use new report/work paths")
    original_runtime = publish.runtime
    _, old_daemon, _ = original_runtime(args.environment, args.source_pid, args.database)
    old_sha = publish.sha_file(old_daemon)
    new_sha = publish.sha_file(args.daemon)
    source = json.loads(args.source_report.read_text())
    publish.require(source["complete"] and source["daemon_sha256"] == old_sha,
                    "original pilot report identity differs")
    source["daemon_sha256"] = new_sha
    source["strict_validation"] = {"protocol": "iotdbfs-v01-default-strict-regression/1",
        "source_report": str(args.source_report), "source_report_sha256": publish.sha_file(args.source_report),
        "runtime_reference_daemon_sha256": old_sha, "actual_daemon_sha256": new_sha,
        "mode": "default-strict-with-no-version-directory-properties", "ok": False}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(args.report, source)
    def checked_runtime(environment, pid, database):
        descriptor, old, env = original_runtime(environment, pid, database)
        publish.require(publish.sha_file(old) == old_sha and publish.sha_file(args.daemon) == new_sha,
                        "daemon changed during strict validation")
        return descriptor, args.daemon, env
    publish.runtime = checked_runtime
    try:
        args.command = "verify"
        publish.run(args)
    finally:
        publish.runtime = original_runtime
    result = json.loads(args.report.read_text())
    publish.require(result["complete"], "strict pilot verification incomplete")
    result["strict_validation"]["ok"] = True
    atomic_json(args.report, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("input", "source-report", "report", "work", "environment", "writer-lock", "daemon"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--database", default=publish.DATABASE)
    run(parser.parse_args())
