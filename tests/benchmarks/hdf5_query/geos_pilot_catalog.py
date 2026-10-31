#!/usr/bin/env python3
"""Time native dataset attribute discovery for the GEOS pilot source."""
import json
from pathlib import Path
import sys
import time

import h5py


if __name__ == "__main__":
    path = Path(sys.argv[1])
    observations = []
    for _ in range(11):
        start = time.perf_counter_ns()
        with h5py.File(path, "r") as source:
            selected = []
            for name, obj in source.items():
                if isinstance(obj, h5py.Dataset) and obj.attrs.get("standard_name") == b"air_temperature":
                    selected.append(name)
        observations.append({"ns": time.perf_counter_ns() - start, "selected": selected})
    print(json.dumps({"protocol": "iotdbfs-geos-native-catalog/1", "source": str(path),
                      "observations": observations}))
