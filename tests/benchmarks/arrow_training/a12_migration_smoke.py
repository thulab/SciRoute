"""Isolated live A12 smoke against an existing local IoTDB service.

Reads connection settings from a trusted local process without printing them.
The test creates its own database and preserves its report directory for review.
"""

import argparse
import json
import os
import pathlib
import subprocess
import tempfile
import time

from a11_mount_smoke import register


def connection_environment(pid: int, database: str) -> dict[str, str]:
    env = os.environ.copy()
    raw = pathlib.Path(f"/proc/{pid}/environ").read_bytes()
    peer = {}
    for item in raw.split(b"\0"):
        if b"=" in item:
            key, value = item.split(b"=", 1)
            peer[key.decode()] = value.decode()
    for name in ("IOTDB_HOST", "IOTDB_PORT", "IOTDB_USER", "IOTDB_PASS"):
        if name in peer:
            env[name] = peer[name]
    config = peer.get("IOTDB_CONFIG_FILE")
    if config and pathlib.Path(config).is_file():
        properties = {}
        for line in pathlib.Path(config).read_text().splitlines():
            line = line.strip()
            if not line or line.startswith(("#", ";", "//")) or "=" not in line:
                continue
            key, value = line.split("=", 1)
            properties[key.strip()] = value.strip()
        for prop, name in (("iotdb.host", "IOTDB_HOST"),
                           ("iotdb.port", "IOTDB_PORT"),
                           ("iotdb.user", "IOTDB_USER"),
                           ("iotdb.pass", "IOTDB_PASS"),
                           ("iotdb.password", "IOTDB_PASS")):
            if prop in properties:
                env[name] = properties[prop]
    env["IOTDB_DB"] = database
    env["IOTDB_CONFIG_FILE"] = "/nonexistent/a12-isolated-config.properties"
    return env


def run_json(command: list[str], env: dict[str, str]) -> list[dict]:
    completed = subprocess.run(command, env=env, text=True,
                               capture_output=True)
    if completed.returncode:
        raise RuntimeError(f"{command[0]} failed: {completed.stderr}")
    return [json.loads(line) for line in completed.stdout.splitlines()
            if line.startswith("{")]


def verify_fuse(cli: pathlib.Path, report: pathlib.Path,
                env: dict[str, str], version: int,
                source_file: pathlib.Path | None = None) -> None:
    import pyarrow as pa

    mount = report / "mount"
    mount.mkdir()
    # sockaddr_un.sun_path is only 108 bytes on Linux; the report path is long.
    socket_path = pathlib.Path(f"/tmp/a12-query-{os.getpid()}.sock")
    log = report / "mount.log"
    mount_env = env.copy()
    mount_env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    process = None
    try:
        with log.open("wb") as output:
            process = subprocess.Popen([str(cli), "-f", str(mount)],
                                       env=mount_env, stdout=output,
                                       stderr=subprocess.STDOUT)
        for _ in range(100):
            if socket_path.exists():
                break
            if process.poll() is not None:
                raise RuntimeError("FUSE mount exited before query socket was ready")
            time.sleep(0.1)
        else:
            raise RuntimeError("FUSE query socket was not ready")

        contested = subprocess.run(
            [str(cli), "arrow-migrate", "inventory", "--report",
             str(report / "contended"), "--path", "/legacy.arrow"],
            env=env, text=True, capture_output=True)
        assert contested.returncode == 3, contested.stderr
        assert "namespace writer lock" in contested.stderr

        sources = {"paths": [{"kind": "exact", "path": "/legacy.arrow"}],
                   "versions": {"kind": "exact", "exact": version}}
        if source_file:
            one_level = {"kind": "typed", "request": {
                "sources": sources,
                "subset": {"projection": ["index", "value"],
                           "physical_array": {"field_id": "target",
                                              "record_batch_index": 0,
                                              "row_index": 0,
                                              "values": [{"child_indices": []}]}}}}
            route = register(str(socket_path), one_level)
            with pa.memory_map(route, "r") as result_file:
                actual = pa.ipc.open_stream(result_file).read_all()
            with pa.memory_map(str(source_file), "r") as original_file:
                oracle = pa.ipc.open_stream(original_file).read_next_batch()
            assert actual.column("value").to_pylist() == \
                oracle.column("target")[0].as_py()
            return
        typed = {"kind": "typed", "request": {
            "sources": sources,
            "subset": {"projection": ["index", "value.0", "value.1"],
                       "physical_array": {"field_id": "target",
                                          "record_batch_index": 0,
                                          "row_index": 0,
                                          "values": [{"child_indices": [0]},
                                                     {"child_indices": [1]}]}}}}
        typed_route = register(str(socket_path), typed)
        with pa.memory_map(typed_route, "r") as source:
            table = pa.ipc.open_stream(source).read_all()
        assert table.column("value.0").to_pylist() == [1.0, 2.0, 3.0]
        assert table.column("value.1").to_pylist() == [10.0, 20.0, 30.0]

        window = {
            "kind": "window",
            "snapshot": {
                "dataset_id": "a12-migration",
                "selector": {"paths": [{"kind": "exact", "path": "/legacy.arrow"}],
                             "versions": {"kind": "exact", "exact": version},
                             "formats": {"adapters": "listed", "adapter_ids": ["arrow"],
                                         "include_byte_passthrough": False}},
                "shards": [{"filename": "legacy.arrow", "path": "/legacy.arrow",
                            "version": version}],
                "rows": [{"logical_row": 0, "shard_index": 0,
                          "batch_index": 0, "row_index": 0,
                          "series_id": "series", "steps": {"begin": 0, "end": 3},
                          "declared_n_channels": 2, "declared_length": 3,
                          "channels": [
                              {"field_id": "target", "original_ordinal": 0,
                               "physical_ordinal": 0, "label": "x", "length": 3},
                              {"field_id": "target", "original_ordinal": 1,
                               "physical_ordinal": 1, "label": "y", "length": 3}]}]},
            "view": {"view_id": "a12-view", "experiment_id": "smoke",
                     "input_channels": [{"field_id": "target", "ordinal": 0,
                                         "label": "x"}],
                     "target_channels": [{"field_id": "target", "ordinal": 1,
                                          "label": "y"}],
                     "input_length": 2, "gap": 0, "target_length": 1,
                     "stride": 1,
                     "split": {"split_id": "train",
                               "permitted_input": {"begin": 0, "end": 3},
                               "permitted_target": {"begin": 0, "end": 3}}},
            "request": {"coordinates": [{"series_id": "series", "anchor": 2}],
                        "max_windows": 1}}
        window_route = register(str(socket_path), window)
        with pa.memory_map(window_route, "r") as source:
            values = pa.ipc.open_stream(source).read_all()
        assert values.column("input.0").to_pylist() == [1.0, 2.0, None]
        assert values.column("target.0").to_pylist() == [None, None, 30.0]
    finally:
        if process is not None:
            subprocess.run(["fusermount", "-u", str(mount)], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pid", type=int, required=True)
    parser.add_argument("--build", type=pathlib.Path, required=True)
    parser.add_argument("--reports-parent", type=pathlib.Path, required=True)
    parser.add_argument("--source-file", type=pathlib.Path)
    args = parser.parse_args()
    database = f"a12probe{int(time.time())}"
    env = connection_environment(args.source_pid, database)
    report = pathlib.Path(tempfile.mkdtemp(prefix="a12-migration-",
                                           dir=args.reports_parent))
    seed = args.build / "bin/a12_legacy_seed"
    cli = args.build / "bin/iotdb_fs"
    seed_command = [str(seed), "/legacy.arrow"]
    if args.source_file:
        seed_command.append(str(args.source_file))
    old = run_json(seed_command, env)
    assert len(old) == 1, old
    inventory = run_json([str(cli), "arrow-migrate", "inventory", "--report",
                          str(report), "--path", "/legacy.arrow"], env)
    assert len(inventory) == 1 and inventory[0]["kind"] == "candidate", inventory
    assert inventory[0]["source_version"] == old[0]["version"]
    migrated = run_json([str(cli), "arrow-migrate", "run", "--report",
                         str(report)], env)
    assert len(migrated) == 1 and migrated[0]["state"] == "verified", migrated
    assert migrated[0]["source_version"] == old[0]["version"]
    assert migrated[0]["destination_version"] > old[0]["version"]
    assert migrated[0]["source_sha256"] == migrated[0]["destination_sha256"]
    # Simulate a crash after publish but before the final event became durable.
    # This is an isolated harness; operators must not edit production journals.
    journal = report / "journal.log"
    events = journal.read_text().splitlines()
    assert len(events) == 2 and " started " in events[0]
    journal.write_text(events[0] + "\n")
    resumed = run_json([str(cli), "arrow-migrate", "resume", "--report",
                        str(report)], env)
    assert len(resumed) == 1 and resumed[0]["state"] == "verified", resumed
    assert resumed[0]["destination_version"] == migrated[0]["destination_version"]
    after = report / "after"
    indexed = run_json([str(cli), "arrow-migrate", "inventory", "--report",
                        str(after), "--path", "/legacy.arrow"], env)
    assert len(indexed) == 2, indexed
    assert indexed[0]["kind"] == "historical_version", indexed
    assert indexed[0]["source_version"] == old[0]["version"]
    assert indexed[1]["kind"] == "already_indexed", indexed
    assert indexed[1]["source_version"] == migrated[0]["destination_version"]
    verify_fuse(cli, report, env, migrated[0]["destination_version"],
                args.source_file)
    print(json.dumps({"status": "ok", "database": database,
                      "report": str(report), "source_version": old[0]["version"],
                      "destination_version": migrated[0]["destination_version"],
                      "sha256": migrated[0]["source_sha256"]}))


if __name__ == "__main__":
    main()
