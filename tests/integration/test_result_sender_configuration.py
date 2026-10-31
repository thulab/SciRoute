"""Validate daemon result sender configuration without connecting or mounting."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


PROPERTY = "query.result_fetch.sender_threads"


def configuration_cases():
    cases = [("defaults", {}, True)]
    # Sender count and query execution count are independent. Omitting the
    # query batch size also exercises its worker-dependent default.
    for senders in (1, 2):
        cases.append((f"senders-{senders}", {PROPERTY: str(senders)}, True))
        for workers in (1, 4):
            cases.append((f"senders-{senders}-query-workers-{workers}",
                          {PROPERTY: str(senders),
                           "query.tasks.worker_threads": str(workers)}, True))
    invalid = ("", "0", "-1", "3", "256", "1.5", "+1", "2x",
               "999999999999999999999999999")
    for index, value in enumerate(invalid):
        cases.append((f"invalid-senders-{index}", {PROPERTY: value}, False))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    args = parser.parse_args()
    binary = args.bin.resolve(strict=True)
    cases = configuration_cases()
    with tempfile.TemporaryDirectory(prefix="iotdbfs-result-sender-config-") as directory:
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
                f"{key}={value}" for key, value in properties.items()
            ]) + "\n", encoding="utf-8")
            # Accepted configuration reaches the owner-client guard; malformed
            # configuration must fail before connector or mount initialization.
            result = subprocess.run([str(binary), "grep"], env=env, cwd=root,
                                    capture_output=True, text=True, timeout=10)
            output = result.stdout + result.stderr
            expected = "requires the owner socket" if valid else "Invalid configuration"
            unexpected = "Invalid configuration" if valid else "requires the owner socket"
            if result.returncode != 2 or expected not in output or unexpected in output:
                raise AssertionError(
                    f"startup config validation failed: {name}, properties={properties!r}, "
                    f"returncode={result.returncode}, output={output!r}")
    print(f"{len(cases)} result sender startup configuration cases passed; "
          "no database connections or mounts")


if __name__ == "__main__":
    main()
