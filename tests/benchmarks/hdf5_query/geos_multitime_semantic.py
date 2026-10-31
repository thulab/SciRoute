#!/usr/bin/env python3
"""Validate production GEOS semantic slices over exact published time files."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import h5py
import numpy as np
if os.environ.get("GEOS_PYARROW_SITE"):
    sys.path.append(os.environ["GEOS_PYARROW_SITE"])
import pyarrow as pa

import geos_pilot as pilot


SLICES = {
    "point": {"time": (0.0, 1.0), "pressure": (749.0, 751.0),
              "latitude": (-0.001, 0.001), "longitude": (-0.001, 0.001)},
    "roi": {"time": (0.0, 1.0), "pressure": (749.0, 751.0),
            "latitude": (-2.0, 2.0), "longitude": (-2.5, 2.5)},
}


def scalar(value):
    if isinstance(value, np.ndarray):
        pilot.check(value.size == 1, "non-scalar metadata")
        value = value.item()
    return value.decode() if isinstance(value, bytes) else str(value)


def native(source, selectors):
    begin = time.perf_counter_ns()
    with h5py.File(source["raw"], "r") as file:
        data = file["/T"]
        pilot.check(scalar(data.attrs["standard_name"]) == "air_temperature",
                    "wrong native temperature variable")
        axes = []
        coordinates = {}
        for dimension, role in enumerate(("time", "pressure", "latitude", "longitude")):
            links = list(data.dims[dimension].values())
            pilot.check(len(links) == 1, "ambiguous native scale")
            axis = links[0]
            coordinate = np.asarray(axis[...], dtype=np.float64)
            lo, hi = selectors[role]
            positions = np.flatnonzero((coordinate >= lo) & (coordinate < hi))
            pilot.check(positions.size and np.array_equal(
                positions, np.arange(positions[0], positions[-1] + 1)),
                "nonrectangular native coordinate selection")
            axes.append((int(positions[0]), int(positions[-1]) + 1))
            coordinates[role] = coordinate[positions]
        values = data[tuple(slice(*interval) for interval in axes)].ravel(order="C")
        pilot.check(values.dtype == np.dtype("float32"), "wrong native value type")
        time_units = scalar(file["/time"].attrs["units"])
        pressure_units = scalar(file["/lev"].attrs["units"])
    return {"ns": time.perf_counter_ns() - begin, "origin": [a for a, _ in axes],
            "shape": [b - a for a, b in axes], "rows": int(values.size),
            "sha256": hashlib.sha256(values.astype("<f4", copy=False).tobytes()).hexdigest(),
            "time_units": time_units, "pressure_units": pressure_units,
            "coordinates": {k: v.tolist() for k, v in coordinates.items()}}


def request(source, selectors):
    slices = []
    for role, (lower, upper) in selectors.items():
        slices.append({"dimension": "hdf5.coord." + role,
                       "lower": {"kind": "inclusive", "value":
                                 {"type": "float64", "value": lower}},
                       "upper": {"kind": "exclusive", "value":
                                 {"type": "float64", "value": upper}}})
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": source["logical_path"]}],
                    "versions": {"kind": "exact", "exact": source["version"]}},
        "subset": {"projection": ["hdf5.value:%2FT", "hdf5.coord.time",
                                  "hdf5.coord.pressure", "hdf5.coord.latitude",
                                  "hdf5.coord.longitude"],
                   "predicate": {"kind": "compare", "field": "hdf5.attr:standard_name",
                                 "operator": "eq", "value":
                                 {"type": "utf8", "value": "air_temperature"}},
                   "slices": slices}}}


def registered(socket, source, selectors):
    response, registration_ns = pilot.control(socket, request(source, selectors))
    begin = time.perf_counter_ns()
    with pa.memory_map(response["path"], "r") as stream:
        table = pa.ipc.open_stream(stream).read_all()
    read_ns = time.perf_counter_ns() - begin
    pilot.check(table.num_columns == 5 and table.column(0).null_count == 0,
                "wrong production GEOS IPC schema")
    values = table.column(0).to_numpy(zero_copy_only=False).astype("<f4", copy=False)
    prefix = b"iotdbfs.result_descriptor.0."
    metadata = table.schema.metadata or {}
    return {"registration_ns": registration_ns, "ipc_read_ns": read_ns,
            "rows": table.num_rows, "sha256": hashlib.sha256(values.tobytes()).hexdigest(),
            "profile": metadata.get(prefix + b"coordinate.profile", b"").decode(),
            "view": metadata.get(prefix + b"view", b"").decode(),
            "layout": metadata.get(prefix + b"layout.protocol", b"").decode(),
            "origin": [int(metadata[prefix + f"axis.{axis}.selection_origin".encode()])
                       for axis in range(4)],
            "shape": [int(metadata[prefix + f"axis.{axis}.selection_extent".encode()])
                      for axis in range(4)],
            "time_units": metadata.get(prefix + b"coordinate.time.units", b"").decode(),
            "pressure_units": metadata.get(prefix + b"coordinate.pressure.units", b"").decode(),
            "coordinates": {role: table.column(axis + 1).to_numpy(
                zero_copy_only=False).tolist()
                for axis, role in enumerate(("time", "pressure", "latitude", "longitude"))}}


def main(args):
    pilot_manifest = json.loads((args.pilot_dir / "pilot.json").read_text())
    pilot.check(pilot_manifest["complete"], "requires published GEOS pilot")
    sources = pilot_manifest["sources"]
    pilot.check(1 <= len(sources) <= 4, "requires bounded source list")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = pilot.connection_environment(args.source_pid, pilot_manifest["database"])
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock.resolve())
    env["IOTDBFS_QUERY_SOCKET"] = str(Path("/tmp") / f"geos-semantic-{os.getpid()}.sock")
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(output / "server-profile.jsonl")
    env["IOTDB_CONFIG_FILE"] = str(args.pilot_dir / "profile.properties")
    socket = Path(env["IOTDBFS_QUERY_SOCKET"])
    report = {"protocol": "iotdbfs-geos-semantic-multitime/1", "complete": False,
              "pilot": str(args.pilot_dir), "sources": sources,
              "observations": []}
    pilot.write(output / "report.json", report)
    process, log = pilot.mount(args.daemon, output / "mount", env, socket)
    try:
        prefix = pilot_manifest["source_prefix"]
        discovered, discovery_ns = pilot.control(socket, {
            "kind": "dataset_catalog", "request": {
                "sources": {"paths": [{"kind": "subtree", "path": prefix}]},
                "dataset_path": "/T",
                "predicate": {"kind": "compare",
                              "field": "hdf5.attr:standard_name",
                              "operator": "eq", "value":
                              {"type": "utf8", "value": "air_temperature"}}}})
        expected = {(source["logical_path"], source["version"]) for source in sources}
        actual = {(item["path"], item["version"]) for item in discovered["datasets"]}
        pilot.check(actual == expected, "cross-file dataset catalog mismatch")
        report["discovery"] = {"ns": discovery_ns, "candidate_datasets":
                               discovered["candidate_datasets"],
                               "selected_sources": discovered["selected_sources"],
                               "sql_queries": discovered["sql_queries"]}
        for repeat in range(args.repeats):
            for source in sources:
                for case, selectors in SLICES.items():
                    oracle = native(source, selectors)
                    result = registered(socket, source, selectors)
                    for key in ("rows", "sha256", "origin", "shape", "time_units",
                                "pressure_units"):
                        pilot.check(oracle[key] == result[key],
                                    f"{source['id']} {case} {key} mismatch")
                    for role, values in oracle["coordinates"].items():
                        # IPC has a coordinate for every C-order output row.
                        dimension = ("time", "pressure", "latitude",
                                     "longitude").index(role)
                        expected_values = np.asarray(values)[
                            np.indices(oracle["shape"])[dimension].ravel(order="C")]
                        pilot.check(np.array_equal(expected_values, result["coordinates"][role]),
                                    f"{source['id']} {case} {role} coordinate mismatch")
                    pilot.check(result["profile"] == "geos572-pressure-grid/1" and
                                result["view"] == "geos572-pressure-grid/1:physical" and
                                result["layout"] == "IOTDBFS-HDF5-LAYOUT/5",
                                "GEOS result lacks validated profile/layout evidence")
                    report["observations"].append({"repeat": repeat, "source": source["id"],
                                                    "case": case, "oracle": oracle,
                                                    "typed": result})
                    pilot.write(output / "report.json", report)
        if len(sources) > 1:
            time_units = {source["id"]: next(
                observation["typed"]["time_units"] for observation in
                report["observations"] if observation["source"] == source["id"])
                for source in sources}
            pilot.check(len(set(time_units.values())) == len(sources),
                        "time files must retain distinct native time origins")
    finally:
        pilot.unmount(output / "mount", process, log)
    profiles = [json.loads(line) for line in
                (output / "server-profile.jsonl").read_text().splitlines()]
    pilot.check(len(profiles) == 1 + len(report["observations"]),
                "incomplete server profile")
    for observation, profile in zip(report["observations"], profiles[1:]):
        observation["server"] = {
            "total_ns": profile["server_total_ns"],
            "metadata_sql": profile["iotdb_delta"]["metadata_query_calls"],
            "layout_sql": profile["iotdb_delta"]["layout_calls"],
            "payload_sql": profile["iotdb_delta"]["payload_calls"],
            "payload_bytes": profile["iotdb_delta"]["payload_returned_bytes"],
            "counters": {k: v for k, v in profile["counters"].items()
                         if k.startswith("hdf5.")}}
    report["complete"] = True
    pilot.write(output / "report.json", report)
    print(json.dumps({"complete": True, "sources": len(sources),
                      "observations": len(report["observations"]),
                      "report": str(output / "report.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--repeats", type=int, default=3)
    main(parser.parse_args())
