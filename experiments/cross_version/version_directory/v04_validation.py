#!/usr/bin/env python3
"""V04 correctness and physical-reference evidence on the existing instance.

Uses a fresh namespace and the original raw corpus. Keeps performance clocks
out of this driver, preserves old V03 data, and restores service in finally.
No execution occurs on import; the primary agent runs only a frozen snapshot.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import uuid

from experiments.cross_version.pilot import corpus
from experiments.cross_version.pilot import publish
from experiments.cross_version.version_directory import v00_permissions as permissions
from common import load_manifest
from experiments.cross_version.version_directory.v00_head_probe import public_failure, save_json, sha
from experiments.cross_version.version_directory.v02_validation import mount_owner, process_memory
from experiments.cross_version.version_directory.v03_n128 import DATABASE, checkpoint, sync_window

BLOCK = 1024 * 1024
MULTIBLOCK_SIZE = 3 * BLOCK + 137


def require(value, message):
    if not value:
        raise RuntimeError(message)


def block_id(value):
    kind, protocol = b"payload-block", b"iotdbfs-payload-block/1"
    framing = (b"iotdbfs-content-id/1\0" + len(kind).to_bytes(4, "big") + kind +
        len(protocol).to_bytes(4, "big") + protocol + len(value).to_bytes(8, "big") + value)
    return hashlib.sha256(framing).hexdigest()


def fixture(scenario, ordinal):
    value = bytearray(MULTIBLOCK_SIZE)
    for block in range(4):
        begin, end = block * BLOCK, min((block + 1) * BLOCK, len(value))
        value[begin:end] = bytes([23 + block]) * (end - begin)
    if scenario == "single_unit":
        for version in range(2, ordinal + 1):
            value[((version - 2) % 4) * BLOCK + 37] ^= version
    return bytes(value)


def expected_blocks(value):
    return [{"logical_offset": offset, "length": len(value[offset:offset + BLOCK]),
        "content_offset": 0, "content_size": len(value[offset:offset + BLOCK]),
        "content_id": block_id(value[offset:offset + BLOCK])}
        for offset in range(0, len(value), BLOCK)]


def journal_files(root):
    require(root.is_dir() and (root / "state").is_file(), "preserved owner journal absent")
    return {str(path.relative_to(root)): sha(path) for path in root.rglob("*") if path.is_file()}


def check_large(context, bindings, graph):
    for binding in bindings:
        value = fixture(binding["scenario"], binding["ordinal"])
        path = publish.history_path(context["mount"], binding["logical"], binding["version"])
        require(sha(path) == binding["sha256"], "large exact source full hash differs")
        with path.open("rb", buffering=0) as source:
            for offset, length in ((0, 37), (BLOCK - 5, 23), (2 * BLOCK - 7, 19),
                                   (len(value) - 17, 17), (len(value), 0)):
                source.seek(offset)
                require(source.read(length) == value[offset:offset + length], "large exact range differs")
        rows = [row for row in graph["files"] if row["path"] == binding["logical"] and
                row["version"] == binding["version"]]
        require(len(rows) == 1, "large exact payload graph missing or duplicated")
        row = rows[0]
        require(row["size"] == len(value) and row["sha256"] == binding["sha256"] and
            row["extents"] == expected_blocks(value), "payload graph differs from independent block oracle")
        require(row["payload_batch_queries"] == 1 and row["payload_block_cells"] == 4 and
                row["payload_returned_bytes"] == len(value), "four-block verification was not one native batch query")
    identical = [row for row in graph["files"] if row["path"] == "/payload/identical.bin" and row["size"]]
    changed = [row for row in graph["files"] if row["path"] == "/payload/single_unit.bin" and row["size"]]
    require(len(identical) == len(changed) == 8, "expected eight real large versions per stable path")
    require(len({row["payload_root"] for row in identical}) == 1, "identical maps did not share")
    require(len({row["payload_root"] for row in changed}) == 8, "changed maps were incorrectly merged")
    require(len({row["seal_id"] for row in identical + changed}) == 16, "versions lost independent seals")
    unique = {extent["content_id"]: extent["content_size"] for row in identical + changed for extent in row["extents"]}
    require(len(unique) == 11, "large fixture should have four initial plus seven newly changed blocks")
    return {"versions": 16, "blocks_per_version": 4, "logical_block_references": 64,
        "logical_payload_bytes": 16 * MULTIBLOCK_SIZE, "unique_payload_blocks": len(unique),
        "unique_referenced_payload_bytes": sum(unique.values()), "independent_seals": 16,
        "physical_scope": "referenced payload-block BLOB values only; excludes maps, seals, COW, staging, orphans and physical database encoding"}



def check_production_ranges(result, binding):
    """Independent fixture oracle for the production ReadHandle, including order and duplicates."""
    value = fixture(binding["scenario"], binding["ordinal"])
    require(result.get("completed") and result.get("command") == "v04_range" and
        result.get("production_read_handle") and result.get("existing_namespace_only"),
        "production range probe is incomplete or bypassed ReadHandle")
    require(result.get("path") == binding["logical"] and result.get("version") == binding["version"] and
        result.get("size") == len(value) and result.get("full_sha256") == binding["sha256"],
        "production range probe is bound to different source bytes")
    require(result.get("head_unchanged") and result.get("journal_unchanged") and
        result.get("head_sha256_before") == result.get("head_sha256_after") and
        result.get("journal_sha256_before") == result.get("journal_sha256_after"),
        "production range probe changed persistence")
    mixed = [(2 * BLOCK - 3, 13), (7, 19), (BLOCK - 2, 7), (len(value) - 17, 17),
             (7, 19), (0, 1), (9, 8)]
    expected = [("full_warmup", [(0, len(value))]), ("unordered_cross_block", mixed),
                ("warm_full_single_batch", [(0, len(value))])]
    observed = result.get("observations", [])
    require(len(observed) == len(expected), "production range observations missing")
    counters = {"sdk_query_calls", "version_store_query_calls", "actual_query_sum",
        "version_store_batch_query_calls", "store_returned_blob_bytes", "data_batches",
        "actual_block_bytes", "output_bytes", "logical_extents"}
    for observation, (name, ranges) in zip(observed, expected):
        require(observation.get("name") == name and counters.issubset(observation),
                "production range counter scope is unavailable")
        require(observation["actual_query_sum"] == observation["sdk_query_calls"] +
            observation["version_store_query_calls"], "actual SQL counter sum differs")
        wanted = [{"offset": offset, "length": length,
            "sha256": hashlib.sha256(value[offset:offset + length]).hexdigest()}
            for offset, length in ranges]
        require(observation.get("outputs") == wanted, "production range bytes/order differ from independent fixture")
    warm = observed[-1]
    require(warm["actual_query_sum"] == warm["version_store_query_calls"] ==
        warm["version_store_batch_query_calls"] == warm["data_batches"] == 1 and
        warm["sdk_query_calls"] == 0 and warm["logical_extents"] == 4 and
        warm["store_returned_blob_bytes"] == warm["actual_block_bytes"] == warm["output_bytes"] == len(value),
        "warm production four-block read did not use one exact native batch SQL")
    return result


def run(args):
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    require(not args.report.exists() and not args.work.exists(), "use fresh work and report paths")
    args.work.mkdir(mode=0o700)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    _, _, env = publish.runtime(args.environment, args.source_pid, DATABASE)
    old = json.loads(args.v03_report.read_text())
    require(old["complete"] and old["table_prefix"].startswith("vd_v03_"), "old V03 evidence is incomplete")
    require(sha(args.v03_content_probe) == old["content_probe_sha256"], "old inspector binary is not pinned V03")
    old_journal = Path(old["journal"])
    preserved_journal = journal_files(old_journal)
    prefix = "vd_v04_correctness_" + uuid.uuid4().hex[:16]
    journal = args.work / "owner"
    report = {"protocol": "iotdbfs-v04-correctness/1", "complete": False,
        "database": DATABASE, "table_prefix": prefix, "journal": str(journal),
        "daemon_sha256": sha(args.daemon), "probe_sha256": sha(args.probe),
        "payload_probe_sha256": sha(args.payload_probe), "range_probe_sha256": sha(args.range_probe),
        "script_sha256": sha(Path(__file__)),
        "old_v03_report_sha256": sha(args.v03_report), "old_v03_probe_sha256": sha(args.v03_content_probe),
        "credentials_recorded": False, "scientific_bindings": [], "large_bindings": [], "checks": {},
        "scope": "correctness, real payload block references, bounded range hashes and restart; not frozen N128 performance or physical disk compression"}
    ordinal = 0

    def probe(command="inspect", selected=prefix, binary=None):
        nonlocal ordinal
        ordinal += 1
        executable = binary or (args.payload_probe if command == "inspect" else args.probe)
        result = subprocess.run([str(executable), command, "--prefix", selected], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=600)
        log = args.work / ("probe-%04d.log" % ordinal)
        log.write_text(result.stdout + "\n" + result.stderr)
        log.chmod(0o600)
        require(result.returncode == 0, "native probe failed; inspect private probe log")
        objects = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        require(len(objects) == 1 and (objects[0].get("completed") or command == "head"), "native result is incomplete")
        return objects[0]

    def audit_science(context, selected, templates):
        checked = 0
        for binding in report["scientific_bindings"]:
            entry = next(item for item in selected if (item["format"], item["scenario"], item["ordinal"]) ==
                (binding["format"], binding["scenario"], binding["ordinal"]))
            require(sha(publish.history_path(context["mount"], binding["logical"], binding["version"])) ==
                    binding["sha256"], "scientific exact byte hash differs")
            rows = publish.discovery(context, binding["logical"], binding["format"],
                                     templates[binding["format"]], binding["version"])
            require(len(rows) == 4 and all(row["version"] == binding["version"] for row in rows),
                    "scientific exact inventory differs")
            checked += publish.validate_values(context, entry, binding["logical"], binding["version"])
        for logical in {binding["logical"] for binding in report["scientific_bindings"]}:
            sample = next(binding for binding in report["scientific_bindings"] if binding["logical"] == logical)
            rows = publish.all_versions(context, logical, sample["format"], templates[sample["format"]])
            wanted = {binding["version"] for binding in report["scientific_bindings"] if binding["logical"] == logical}
            require(len(rows) == 32 and {row["version"] for row in rows} == wanted, "scientific all-version inventory differs")
        return checked

    try:
        with sync_window(args, report):
            old_head = probe("head", old["table_prefix"])
            report["old_v03_graph"] = probe(selected=old["table_prefix"], binary=args.v03_content_probe)
            require(probe("head", old["table_prefix"]) == old_head and journal_files(old_journal) == preserved_journal,
                    "old V03 head or journal changed")
            try:
                with mount_owner(args.daemon, env, args.work / "reject-old-v03", old["table_prefix"], old_journal):
                    raise AssertionError("V04 silently accepted old V03 namespace")
            except RuntimeError as error:
                require(str(error).startswith("owner daemon exited"), "unexpected old-profile rejection failure")
                require(b"V04 content/payload profile missing or unsupported" in
                    (args.work / "reject-old-v03/mount.log").read_bytes(), "old-profile rejection lacks expected diagnostic")
            require(probe("head", old["table_prefix"]) == old_head and journal_files(old_journal) == preserved_journal,
                    "rejected V04 activation changed old V03 head or journal")
            report["checks"]["old_v03_implicit_upgrade_rejected"] = True
            corpus.verify(args.input)
            manifest = corpus.load(args.input)
            selected = [entry for entry in manifest["entries"] if entry["file_index"] == 1]
            require(len(selected) == 32, "expected the original 32 scientific versions")
            templates = {fmt: load_manifest(base["root"]) for fmt, base in manifest["bases"].items()}
            raw = args.work / "large-fixtures"
            raw.mkdir(mode=0o700)
            with mount_owner(args.daemon, env, args.work / "initialize", prefix, journal, True) as context:
                (context["mount"] / "science").mkdir(mode=0o755)
                (context["mount"] / "payload").mkdir(mode=0o755)
                report["initial_objects"] = probe("stats")
                for entry in selected:
                    logical = "/science/" + entry["scenario"] + (".h5" if entry["format"] == "hdf5" else ".grib")
                    elapsed = publish.write_version(corpus.resolve(args.input, manifest, entry),
                        context["mount"] / logical.lstrip("/"), entry["ordinal"] == 1)
                    rows = publish.discovery(context, logical, entry["format"], templates[entry["format"]])
                    require(len(rows) == 4 and len({row["version"] for row in rows}) == 1, "latest scientific inventory differs")
                    report["scientific_bindings"].append({"logical": logical, "format": entry["format"],
                        "scenario": entry["scenario"], "ordinal": entry["ordinal"], "version": rows[0]["version"],
                        "sha256": entry["sha256"], "publication_ns": elapsed})
                    checkpoint(args, report)
                for scenario in ("identical", "single_unit"):
                    logical = "/payload/" + scenario + ".bin"
                    for version in range(1, 9):
                        value = fixture(scenario, version)
                        source = raw / (scenario + ".bin")
                        source.write_bytes(value)
                        elapsed = publish.write_version(source, context["mount"] / logical.lstrip("/"), version == 1)
                        graph = probe()
                        rows = [row for row in graph["files"] if row["path"] == logical and row["size"] == len(value)]
                        require(len(rows) == version, "same-path rewrite did not create one new real version")
                        current = max(rows, key=lambda row: row["version"])
                        require(current["sha256"] == hashlib.sha256(value).hexdigest(), "large published bytes differ")
                        report["large_bindings"].append({"logical": logical, "scenario": scenario, "ordinal": version,
                            "version": current["version"], "sha256": current["sha256"], "publication_ns": elapsed})
                        checkpoint(args, report)
                report["checks"]["first_scientific_typed_units"] = audit_science(context, selected, templates)
                report["published_graph"] = probe()
                require(report["published_graph"]["staging_payload_rows"] == 0,
                        "committed payload is still duplicated in per-version staging table")
                report["large_sharing"] = check_large(context, report["large_bindings"], report["published_graph"])
                report["published_objects"] = probe("stats")
                report["published_head"] = probe("head")
                report["populated_memory"] = process_memory(context["process"].pid)
            # The mount must release the FileJournal/namespace locks before backend recovery.
            # Recovery can retry committed staging cleanup; this is not a pure SQL inspector.
            report["production_range_probes"] = []
            before_range_journal = journal_files(journal)
            for scenario in ("identical", "single_unit"):
                binding = next(item for item in report["large_bindings"]
                               if item["scenario"] == scenario and item["ordinal"] == 8)
                result = subprocess.run([str(args.range_probe), "--prefix", prefix,
                    "--journal", str(journal), "--path", binding["logical"],
                    "--version", str(binding["version"]), "--size", str(MULTIBLOCK_SIZE)],
                    env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=600)
                log = args.work / ("production-ranges-" + scenario + ".log")
                log.write_text(result.stdout + "\n" + result.stderr)
                log.chmod(0o600)
                require(result.returncode == 0, "production range probe failed; inspect private log")
                objects = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
                require(len(objects) == 1 and objects[0].get("prefix") == prefix,
                        "production range probe result missing or namespace differs")
                report["production_range_probes"].append(check_production_ranges(objects[0], binding))
                require(probe("head") == report["published_head"] and journal_files(journal) == before_range_journal,
                        "production range recovery changed committed head or journal")
                checkpoint(args, report)
            report["checks"]["production_four_blocks_one_native_batch_sql"] = True
            with mount_owner(args.daemon, env, args.work / "restart", prefix, journal) as context:
                require(probe("head") == report["published_head"], "restart changed committed head")
                report["restart_startup_ns"] = context["startup_ns"]
                report["checks"]["restarted_scientific_typed_units"] = audit_science(context, selected, templates)
                graph = probe()
                require(check_large(context, report["large_bindings"], graph) == report["large_sharing"], "restart changed sharing graph")
                report["restarted_memory"] = process_memory(context["process"].pid)
                deleted = next(binding for binding in report["large_bindings"] if binding["scenario"] == "single_unit" and binding["ordinal"] == 2)
                permissions.unlink_after_release(publish.history_path(context["mount"], deleted["logical"], deleted["version"]))
                survivor = next(binding for binding in report["large_bindings"] if binding["scenario"] == "identical" and binding["ordinal"] == 2)
                require(sha(publish.history_path(context["mount"], survivor["logical"], survivor["version"])) == survivor["sha256"], "deleting one version damaged shared payload")
                report["checks"]["delete_one_reference_keeps_other_version"] = True
            factory = permissions.mount_runtime
            @contextmanager
            def permission_factory(daemon, current_env, work, ignored_lock):
                surface = Path(tempfile.mkdtemp(prefix="iotdbfs-v04-permissions-", dir="/tmp"))
                surface.chmod(0o755)
                report["permission_mount_surface"] = {"path": str(surface), "mode": "0755"}
                try:
                    with mount_owner(args.daemon, current_env, surface / "owner-mount", prefix, journal) as context:
                        # mkdir(mode=0755) is filtered by the controller's 077
                        # umask. Only this public test parent must be searchable;
                        # controls, credentials and daemon logs stay private.
                        parent = surface / "owner-mount"
                        report["permission_mount_surface"]["owner_parent_before"] = oct(parent.stat().st_mode & 0o7777)
                        parent.chmod(0o755)
                        report["permission_mount_surface"]["owner_parent_after"] = oct(parent.stat().st_mode & 0o7777)
                        yield context["mount"], context["socket"]
                finally:
                    if not os.path.ismount(surface / "owner-mount/mount"):
                        shutil.copytree(surface, work / ("mount-evidence-" + surface.name), ignore=shutil.ignore_patterns("mount"))
                        shutil.rmtree(surface)
            permissions.mount_runtime = permission_factory
            try:
                permissions.run(SimpleNamespace(work=args.work / "permission-oracle", report=args.work / "permissions.json",
                    environment=args.environment, source_pid=args.source_pid, database=DATABASE, writer_lock=args.work / "unused.lock"))
            finally:
                permissions.mount_runtime = factory
            report["permissions"] = json.loads((args.work / "permissions.json").read_text())
            require(report["permissions"]["ok"], "permission oracle did not pass")
            report["permissions"]["reference_runtime_daemon_sha256"] = report["permissions"]["daemon_sha256"]
            report["permissions"]["daemon_sha256"] = sha(args.daemon)
            report["permissions"]["table_prefix"] = prefix
            report["permissions"]["mount_factory"] = "v04_validation.permission_factory"
            save_json(args.work / "permissions.json", report["permissions"])
            require(probe("head", old["table_prefix"]) == old_head and journal_files(old_journal) == preserved_journal,
                    "old V03 head or journal changed during acceptance")
            report["checks"]["old_v03_readonly_unchanged"] = True
            report["final_objects"] = probe("stats")
            corpus.verify(args.input)
            report["complete"] = True
    except BaseException as error:
        report["complete"] = False
        report["failure"] = public_failure(error)
        raise
    finally:
        checkpoint(args, report)
    print(json.dumps({"complete": True, "scientific_versions": len(report["scientific_bindings"]),
        "large_versions": len(report["large_bindings"]), "report": str(args.report)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("daemon", "probe", "payload-probe", "range-probe", "v03-content-probe", "v03-report", "input", "environment", "runtime", "release", "work", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--datanode-pid", type=int, required=True)
    run(parser.parse_args())
