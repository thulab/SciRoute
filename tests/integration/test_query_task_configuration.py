"""Validate query task startup configuration without connecting or mounting."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


PREFIX = "query.tasks."


def configuration_cases():
    cases = [("defaults", {}, True)]
    # Accepted worker counts also exercise the omitted batch default: workers
    # 1 and 2 must not retain the otherwise invalid default batch size of 4.
    for workers in (1, 2, 4, 256):
        cases.append((f"workers-{workers}-default-batch",
                      {"worker_threads": str(workers)}, True))
        cases.append((f"workers-{workers}-equal-batch",
                      {"worker_threads": str(workers),
                       "max_batch_size": str(workers)}, True))
        cases.append((f"workers-{workers}-excess-batch",
                      {"worker_threads": str(workers),
                       "max_batch_size": str(workers + 1)}, False))
    cases += [
        ("one-queued-task", {"max_queued_tasks": "1"}, True),
        ("maximum-queued-tasks", {"max_queued_tasks": "65536"}, True),
        ("excess-queued-tasks", {"max_queued_tasks": "65537"}, False),
        ("excess-workers", {"worker_threads": "257"}, False),
        ("one-batch-task", {"max_batch_size": "1"}, True),
        ("default-worker-equal-batch", {"max_batch_size": "4"}, True),
        ("default-worker-excess-batch", {"max_batch_size": "5"}, False),
    ]
    invalid_integers = ("", "0", "-1", "1.5", "+1", "4x",
                        "999999999999999999999999999")
    for property_name in ("worker_threads", "max_queued_tasks", "max_batch_size"):
        for index, value in enumerate(invalid_integers):
            cases.append((f"{property_name}-invalid-{index}",
                          {property_name: value}, False))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    args = parser.parse_args()
    binary = args.bin.resolve(strict=True)
    cases = configuration_cases()
    with tempfile.TemporaryDirectory(prefix="iotdbfs-query-task-config-") as directory:
        root = Path(directory)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("IOTDB_", "IOTDBFS_"))}
        config_path = root / "config.properties"
        env["IOTDB_CONFIG_FILE"] = str(config_path)
        base_config = [
            "iotdb.host=127.0.0.1", "iotdb.port=1", "iotdb.db=config_validation",
            f"log.dir={root / 'logs'}", "storage.version_directory.mode=owner",
            "storage.version_directory.table_prefix=vd_config_validation",
            f"storage.version_directory.owner_directory={root / 'owner'}",
        ]
        for name, properties, valid in cases:
            config_path.write_text("\n".join(base_config + [
                f"{PREFIX}{key}={value}" for key, value in properties.items()
            ]) + "\n", encoding="utf-8")
            # Owner grep rejects accepted configuration before initializing
            # the connector or mounting; malformed config must reject earlier.
            result = subprocess.run([str(binary), "grep"], env=env, cwd=root,
                                    capture_output=True, text=True, timeout=10)
            output = result.stdout + result.stderr
            expected = "requires the owner socket" if valid else "Invalid configuration"
            unexpected = "Invalid configuration" if valid else "requires the owner socket"
            if result.returncode != 2 or expected not in output or unexpected in output:
                raise AssertionError(
                    f"startup config validation failed: {name}, properties={properties!r}, "
                    f"returncode={result.returncode}, output={output!r}")
    print(f"{len(cases)} query task startup configuration cases passed; "
          "no database connections or mounts")


if __name__ == "__main__":
    main()
