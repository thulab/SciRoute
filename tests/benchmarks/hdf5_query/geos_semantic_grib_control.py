#!/usr/bin/env python3
"""GEOS semantic-to-chunk pilot and same-socket GRIB/HDF5 SQL-call control."""

import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

import h5py
import numpy as np
if os.environ.get("GEOS_PYARROW_SITE"):
    sys.path.append(os.environ["GEOS_PYARROW_SITE"])
import pyarrow as pa

import geos_pilot as pilot


def scalar_text(value):
    if isinstance(value, np.ndarray):
        assert value.size == 1
        value = value.item()
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    assert isinstance(value, str)
    return value


def strict_axis(dataset, axis, path, units):
    links = list(dataset.dims[axis].values())
    assert len(links) == 1 and links[0].name == path, (axis, links)
    scale = dataset.file[path]
    assert scale.ndim == 1 and scale.shape[0] == dataset.shape[axis]
    assert scalar_text(scale.attrs["CLASS"]) == "DIMENSION_SCALE"
    assert scalar_text(scale.attrs["units"]) == units
    values = np.asarray(scale[...], dtype=np.float64)
    assert np.isfinite(values).all()
    delta = np.diff(values)
    assert (delta > 0).all() or (delta < 0).all()
    return values


def ordinal_interval(values, lower, upper):
    assert lower <= upper
    # GEOS stores the equatorial latitude as 6.36e-15 rather than exact zero.
    tolerance = 1e-8
    chosen = np.flatnonzero((values >= lower - tolerance) &
                            (values <= upper + tolerance))
    assert chosen.size and np.array_equal(chosen, np.arange(chosen[0], chosen[-1] + 1))
    return int(chosen[0]), int(chosen[-1] + 1)


def chunk_count(chunks, bounds):
    count = 1
    for step, (lower, upper) in zip(chunks, bounds):
        count *= (upper - 1) // step - lower // step + 1
    return count


def bind_geos(raw):
    with h5py.File(raw, "r") as f:
        variables = [item for item in f.values() if isinstance(item, h5py.Dataset)
                     and "standard_name" in item.attrs
                     and scalar_text(item.attrs["standard_name"]) == "air_temperature"]
        assert len(variables) == 1
        data = variables[0]
        assert data.name == "/T" and data.ndim == 4 and data.dtype == np.dtype("<f4")
        time = strict_axis(data, 0, "/time", "minutes since 2011-07-02 09:00:00")
        level = strict_axis(data, 1, "/lev", "hPa")
        latitude = strict_axis(data, 2, "/lat", "degrees_north")
        longitude = strict_axis(data, 3, "/lon", "degrees_east")
        assert scalar_text(f["/lev"].attrs["positive"]) == "down"
        assert scalar_text(data.attrs["units"]) == "K"
        assert time.size == 1 and time[0] == 0
        assert level[0] == 1000 and level[-1] < level[0]
        # The source's valid_range conflicts with real values. Restrict this
        # pilot to the physical Float32 view and report the conflict explicitly.
        valid_range = np.asarray(data.attrs["valid_range"], dtype=np.float64)
        assert valid_range.shape == (2,) and valid_range[0] > 1e14
        assert float(data[0, 10, 360, 576]) < valid_range[0]
        point = ((0, 1), ordinal_interval(level, 750, 750),
                 ordinal_interval(latitude, 0, 0), ordinal_interval(longitude, 0, 0))
        roi = ((0, 1), ordinal_interval(level, 750, 750),
               ordinal_interval(latitude, -2, 1.75),
               ordinal_interval(longitude, -2.5, 2.1875))
        assert point == pilot.CASES["point"] and roi == pilot.CASES["roi"]
        total = sum(obj.id.get_num_chunks() for obj in f.values()
                    if isinstance(obj, h5py.Dataset) and obj.chunks)
        return {
            "evidence": {"dataset": data.name, "standard_name": "air_temperature",
                         "dimension_scales": ["/time", "/lev", "/lat", "/lon"],
                         "time_units": scalar_text(f["/time"].attrs["units"]),
                         "level_units": "hPa", "level_positive": "down",
                         "latitude_units": "degrees_north", "longitude_units": "degrees_east",
                         "value_units": "K", "valid_range_conflict": True},
            "candidate_chunks": {"whole_file": total,
                                 "variable_only": data.id.get_num_chunks(),
                                 "variable_time_level": chunk_count(data.chunks,
                                     ((0, 1), point[1], (0, 721), (0, 1152))),
                                 "point": chunk_count(data.chunks, point),
                                 "roi": chunk_count(data.chunks, roi)},
            "bounds": {"point": point, "roi": roi},
        }


def grib_request(path, version=None):
    versions = {"kind": "latest"} if version is None else {"kind": "exact", "exact": version}
    return {"kind": "typed", "request": {
        "sources": {"paths": [{"kind": "exact", "path": path}], "versions": versions},
        "subset": {"projection": ["grib.message.bytes"],
                   "predicate": {"kind": "compare", "field": "grib.message.level",
                                 "operator": "eq", "value": {"type": "int64", "value": 500}},
                   "output": {"mode": "binary_view"}}}}


def profile_record(records, index):
    item = records[index]
    assert item["ok"] and item["operation"] == "typed"
    return {"server_total_ns": item["server_total_ns"],
            "metadata_sql": item["iotdb_delta"]["metadata_query_calls"],
            "layout_sql": item["iotdb_delta"]["layout_calls"],
            "payload_sql": item["iotdb_delta"]["payload_calls"],
            "layout_bytes": item["iotdb_delta"]["layout_returned_bytes"],
            "payload_bytes": item["iotdb_delta"]["payload_returned_bytes"],
            "stages_ns": {k: v for k, v in item["stages_ns"].items()
                          if k in ("registration_catalog", "registration_authorize",
                                   "typed_catalog_scan", "typed_source_open", "typed_prepare",
                                   "typed_cursor_execute", "query_authorize")},
            "counters": {k: v for k, v in item["counters"].items()
                         if "layout" in k or "catalog" in k or "authorization" in k}}


def main(args):
    report_dir = args.report_dir.resolve()
    report_dir.mkdir(parents=True, exist_ok=False)
    source = json.loads((args.pilot_dir / "run-01/pilot.json").read_text())["sources"][0]
    binding = bind_geos(source["raw"])
    env = pilot.connection_environment(args.source_pid, args.database)
    env["IOTDBFS_NAMESPACE_LOCK"] = str(args.writer_lock)
    env["IOTDBFS_QUERY_PROFILE_JSONL"] = str(report_dir / "profile.jsonl")
    env["IOTDBFS_QUERY_SOCKET"] = str(Path("/tmp") / f"geos-grib-{os.getpid()}.sock")
    env["IOTDB_CONFIG_FILE"] = str(args.pilot_dir / "run-01/profile.properties")
    process, log = pilot.mount(args.daemon, report_dir / "mount", env,
                               Path(env["IOTDBFS_QUERY_SOCKET"]))
    report = {"protocol": "iotdbfs-geos-grib-control/1", "binding": binding,
              "grib_raw": str(args.grib_raw), "observations": []}
    try:
        grib_path = source["logical_path"].rsplit("/", 1)[0] + "/grib-control.grib2"
        grib_dest = report_dir / "mount" / grib_path.lstrip("/")
        shutil.copyfile(args.grib_raw, grib_dest)
        assert grib_dest.stat().st_size == args.grib_raw.stat().st_size
        response, _ = pilot.control(Path(env["IOTDBFS_QUERY_SOCKET"]), grib_request(grib_path))
        report["grib_source"] = {"path": grib_path, "version_selector": "latest"}
        for repeat in range(args.repeats):
            for case in ("point", "roi"):
                bounds = binding["bounds"][case]
                expected = pilot.query_native(source["raw"], bounds)
                actual = pilot.query_iotdb(Path(env["IOTDBFS_QUERY_SOCKET"]), source, bounds)
                assert (expected["rows"], expected["digest"]) == (actual["rows"], actual["digest"])
                report["observations"].append({"format": "hdf5", "case": case,
                                                "repeat": repeat, "rows": actual["rows"]})
            response, _ = pilot.control(Path(env["IOTDBFS_QUERY_SOCKET"]),
                                        grib_request(grib_path))
            with pa.memory_map(response["path"], "r") as stream:
                table = pa.ipc.open_stream(stream).read_all()
            assert table.num_rows == 1 and table.num_columns == 1
            value = table.column(0)[0].as_py()
            assert value and value[:4] == b"GRIB"
            report["observations"].append({"format": "grib", "case": "level500",
                                            "repeat": repeat, "rows": table.num_rows,
                                            "message_sha256": hashlib.sha256(value).hexdigest()})
            del table, value, stream
            gc.collect()
    finally:
        pilot.unmount(report_dir / "mount", process, log)
    records = [json.loads(line) for line in (report_dir / "profile.jsonl").read_text().splitlines()]
    assert len(records) == 1 + len(report["observations"])
    for index, observation in enumerate(report["observations"], 1):
        observation["profile"] = profile_record(records, index)
    pilot.write(report_dir / "summary.json", report)
    print(json.dumps({"report": str(report_dir / "summary.json"),
                      "observations": len(report["observations"])}, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--grib-raw", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--writer-lock", type=Path, required=True)
    parser.add_argument("--source-pid", type=int, default=1001546)
    parser.add_argument("--database", default="h09probe1790939889dbfc1f")
    parser.add_argument("--repeats", type=int, default=6)
    main(parser.parse_args())
