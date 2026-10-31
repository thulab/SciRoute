#!/usr/bin/env python3
"""Repeat GEOS A/C reads against an already published source without reingestion."""
import json
import os
from pathlib import Path
import sys
import uuid

import geos_pilot as pilot


if __name__ == "__main__":
    root = Path(sys.argv[1])
    source = json.loads((root / "run-01/pilot.json").read_text())["sources"][0]
    work = root / ("read-" + uuid.uuid4().hex[:8])
    work.mkdir()
    env = pilot.connection_environment(1001546, "h09probe1790939889dbfc1f")
    env["IOTDBFS_NAMESPACE_LOCK"] = (
        "/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/"
        "hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock"
    )
    socket_path = Path("/tmp") / f"geos-read-{os.getpid()}.sock"
    env["IOTDBFS_QUERY_SOCKET"] = str(socket_path)
    env["IOTDB_CONFIG_FILE"] = str(root / "run-01/profile.properties")
    daemon = Path("/data_01/zzy/iotdb-fs-exp/public-arrow-baseline/"
                  "hdf5-sc-20261006/build/bin/iotdb_fs")
    process, log = pilot.mount(daemon, work / "mount", env, socket_path)
    result = {"protocol": "iotdbfs-geos-readonly/1", "source": source,
              "observations": []}
    try:
        mounted = work / "mount" / source["logical_path"].lstrip("/")
        for pass_num in range(6):
            for case, bounds in pilot.CASES.items():
                native = pilot.query_native(source["raw"], bounds)
                mounted_read = pilot.query_native(mounted, bounds)
                pilot.check((native["rows"], native["digest"]) ==
                            (mounted_read["rows"], mounted_read["digest"]),
                            f"mismatch {case}/{pass_num}")
                result["observations"].append({"pass": pass_num, "case": case,
                                               "native": native, "mounted": mounted_read})
                pilot.write(work / "read.json", result)
    finally:
        pilot.unmount(work / "mount", process, log)
    print(json.dumps({"report": str(work / "read.json"),
                      "observations": len(result["observations"])}))
