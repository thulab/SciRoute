"""Reject the withdrawn Native HDF5 process property before connection or mount."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    binary = parser.parse_args().bin.resolve(strict=True)
    count = 0
    with tempfile.TemporaryDirectory(prefix="iotdbfs-removed-native-process-") as directory:
        root = Path(directory)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("IOTDB_", "IOTDBFS_"))}
        config_path = root / "config.properties"
        env["IOTDB_CONFIG_FILE"] = str(config_path)
        base = [
            "iotdb.host=127.0.0.1", "iotdb.port=1", "iotdb.db=config_validation",
            f"log.dir={root / 'logs'}", "storage.version_directory.mode=owner",
            "storage.version_directory.table_prefix=vd_config_validation",
            f"storage.version_directory.owner_directory={root / 'owner'}",
            f"storage.native_file_root={root / 'native-files'}",
        ]
        for mode in ("native-hdf5", "iotdb-indexed"):
            for value in (None, "", "0", "1", "4", "16", "-1", "invalid"):
                config = base + [f"query.hdf5.mode={mode}"]
                if value is not None:
                    config.append(f"query.hdf5.native_processes={value}")
                config_path.write_text("\n".join(config) + "\n", encoding="utf-8")
                result = subprocess.run([str(binary), "grep"], env=env, cwd=root,
                                        capture_output=True, text=True, timeout=10)
                output = result.stdout + result.stderr
                if value is None:
                    valid = "requires the owner socket" in output and "Invalid configuration" not in output
                else:
                    valid = ("Invalid configuration" in output
                             and "query.hdf5.native_processes was removed" in output
                             and "requires the owner socket" not in output)
                if result.returncode != 2 or not valid:
                    raise AssertionError(f"mode={mode}, value={value!r}, "
                                         f"returncode={result.returncode}, output={output!r}")
                count += 1
    print(f"{count} withdrawn Native process configuration cases passed; "
          "no database connections or mounts")


if __name__ == "__main__":
    main()
