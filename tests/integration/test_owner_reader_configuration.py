"""Validate product startup parsing without connecting to IoTDB or mounting."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin", required=True, type=Path)
    args = parser.parse_args()
    binary = args.bin.resolve(strict=True)
    cases = [(value, "owner", False) for value in ("", "0", "-1", "1.5", "257", "999999999999999999999999999")]
    cases += [(value, "owner", True) for value in (None, "1", "2", "4", "256")]
    cases += [("1", "strict", False)]
    with tempfile.TemporaryDirectory(prefix="iotdbfs-reader-config-") as directory:
        root = Path(directory)
        env = {k: v for k, v in os.environ.items() if not k.startswith("IOTDB_")}
        env["IOTDB_CONFIG_FILE"] = str(root / "config.properties")
        for value, mode, valid in cases:
            config = ["iotdb.host=127.0.0.1", "iotdb.port=1", "iotdb.db=config_validation",
                      f"log.dir={root / 'logs'}", f"storage.version_directory.mode={mode}"]
            if mode == "owner":
                config += ["storage.version_directory.table_prefix=vd_config_validation",
                           f"storage.version_directory.owner_directory={root / 'owner'}"]
            if value is not None:
                config.append(f"storage.version_directory.reader_pool_size={value}")
            (root / "config.properties").write_text("\n".join(config) + "\n")
            # This existing owner-only command rejection occurs before connector
            # initialization, and distinguishes accepted from rejected config.
            result = subprocess.run([str(binary), "grep"], env=env, cwd=root,
                                    capture_output=True, text=True, timeout=10)
            text = result.stdout + result.stderr
            expected = "requires the owner socket" if valid else "Invalid configuration"
            if result.returncode != 2 or expected not in text:
                raise AssertionError(f"startup config validation failed for mode={mode}, value={value!r}")
    print(f"{len(cases)} startup configuration cases passed; no database connections or mounts")


if __name__ == "__main__":
    main()
