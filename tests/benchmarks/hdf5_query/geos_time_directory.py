#!/usr/bin/env python3
"""Build and compare exact-version GEOS UTC time directories on FS and IoTDB-FS.

The FS side is a prebuilt immutable JSON index, not a per-query H5Dopen scan.
IoTDB-FS uses the same utc-point/1 semantics published by HDF5 ingestion.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

import h5py
import numpy as np

PROTOCOL = "iotdbfs-geos-time-directory-control/1"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def string(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    require(isinstance(value, str), "non-string GEOS evidence")
    return value


def native_utc_point(path):
    with h5py.File(path, "r") as file:
        root = file.attrs
        require(string(root["Source"]) == "GEOSadas-5_7_2" and
                string(root["VersionID"]) == "5.7.2", "wrong GEOS product profile")
        require("CF-" in string(root["Conventions"]), "missing CF declaration")
        data = file["/T"]
        require(data.ndim == 4 and data.dtype == np.dtype("<f4") and
                string(data.attrs["standard_name"]) == "air_temperature" and
                string(data.attrs["units"]) == "K", "wrong temperature dataset")
        expected = (("time", "/time"), ("pressure", "/lev"),
                    ("latitude", "/lat"), ("longitude", "/lon"))
        for axis, (role, name) in enumerate(expected):
            scales = list(data.dims[axis].values())
            require(len(scales) == 1 and scales[0].name == name and
                    scales[0].shape == (data.shape[axis],),
                    f"ambiguous {role} dimension-scale association")
            require(string(scales[0].attrs["CLASS"]) == "DIMENSION_SCALE",
                    f"invalid {role} dimension scale")
        pressure = file["/lev"]
        require(string(pressure.attrs["units"]) == "hPa" and
                string(pressure.attrs["positive"]) == "down" and
                string(pressure.attrs["standard_name"]) == "PLE_level",
                "invalid GEOS pressure scale")
        pressure_values = np.asarray(pressure[...], dtype=np.float64)
        require(np.isfinite(pressure_values).all() and
                np.all(pressure_values > 0) and
                (len(pressure_values) == 1 or
                 np.all(np.diff(pressure_values) > 0) or
                 np.all(np.diff(pressure_values) < 0)),
                "invalid pressure coordinate")
        for name, units, bounds in (("/lat", "degrees_north", (-90, 90)),
                                    ("/lon", "degrees_east", None)):
            axis = file[name]
            require(string(axis.attrs["units"]) == units, "wrong spatial unit")
            values = np.asarray(axis[...], dtype=np.float64)
            require(np.isfinite(values).all() and
                    (len(values) == 1 or np.all(np.diff(values) > 0) or
                     np.all(np.diff(values) < 0)), "invalid spatial coordinate")
            if bounds is not None:
                require(np.all((values >= bounds[0]) & (values <= bounds[1])),
                        "latitude out of bounds")
        time_axis = file["/time"]
        require(time_axis.shape == (1,), "GEOS time coverage is not one point")
        units = string(time_axis.attrs["units"])
        prefix = "minutes since "
        require(units.startswith(prefix), "unsupported time units")
        epoch = datetime.strptime(units[len(prefix):], "%Y-%m-%d %H:%M:%S")
        require(epoch.strftime("%Y-%m-%d %H:%M:%S") == units[len(prefix):] and
                2000 <= epoch.year <= 2099, "noncanonical time reference")
        calendar = string(time_axis.attrs.get("calendar", "standard"))
        require(calendar == "standard", "unsupported GEOS calendar")
        minute = float(time_axis[0])
        require(np.isfinite(minute) and minute.is_integer() and
                abs(minute) <= 100000000, "nonintegral GEOS time coordinate")
        # GEOS-5.7.2 file specification declares these instants in UTC.
        point = epoch.replace(tzinfo=timezone.utc) + timedelta(minutes=int(minute))
        return int(point.timestamp()), units, calendar


def build(manifest_path, output):
    manifest = json.loads(manifest_path.read_text())
    require(manifest["complete"] and manifest["sources"], "incomplete source manifest")
    start = time.perf_counter_ns()
    entries = []
    seen = set()
    for source in manifest["sources"]:
        key = (source["logical_path"], source["version"])
        require(key not in seen, "duplicate exact source version")
        seen.add(key)
        raw = Path(source["raw"])
        stat = raw.stat()
        require(stat.st_size == source["bytes"] and
                sha256_file(raw) == source["sha256"],
                f"raw source identity mismatch: {raw}")
        point, units, calendar = native_utc_point(raw)
        entries.append({"path": key[0], "version": key[1], "raw": str(raw),
                        "size": stat.st_size, "mtime_ns": stat.st_mtime_ns,
                        "sha256": source["sha256"], "utc_second": point,
                        "native_units": units, "calendar": calendar})
    entries.sort(key=lambda item: (item["utc_second"], item["path"], item["version"]))
    index = {"protocol": PROTOCOL, "coverage_protocol": "utc-point/1",
             "database": manifest["database"], "entries": entries,
             "build_ns": time.perf_counter_ns() - start}
    output.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    return index


def search(index, lower, upper):
    require(index["protocol"] == PROTOCOL and
            index["coverage_protocol"] == "utc-point/1" and lower < upper,
            "invalid time directory or interval")
    begin = time.perf_counter_ns()
    # The same exact point-in-half-open-range predicate as IoTDB-FS.
    selected = [entry for entry in index["entries"]
                if lower <= entry["utc_second"] < upper]
    for entry in selected:
        stat = Path(entry["raw"]).stat()
        require(stat.st_size == entry["size"] and
                stat.st_mtime_ns == entry["mtime_ns"],
                "FS source changed since index build")
    return selected, time.perf_counter_ns() - begin


def compare(index, socket, lower, upper):
    from geos_pilot import control
    fs, fs_ns = search(index, lower, upper)
    request = {"kind": "dataset_catalog", "request": {
        "sources": {"paths": [{"kind": "exact", "path": item["path"]}
                               for item in index["entries"]]},
        "dataset_path": "/T",
        "predicate": {"kind": "compare", "field": "hdf5.attr:standard_name",
                      "operator": "eq", "value":
                      {"type": "utf8", "value": "air_temperature"}},
        "time_range": {"lower_unix_s": lower, "upper_unix_s": upper}}}
    response, iotdb_ns = control(socket, request)
    expected = sorted((item["path"], item["version"]) for item in fs)
    actual = sorted((item["path"], item["version"])
                    for item in response["datasets"])
    require(expected == actual, "FS and IoTDB time candidate identities differ")
    require(response["time_candidate_sources"] == len(expected),
            "IoTDB time candidate count disagrees")
    return {"lower_unix_s": lower, "upper_unix_s": upper,
            "fs_lookup_ns": fs_ns, "iotdb_client_ns": iotdb_ns,
            "matched": len(expected), "selected_sources": response["selected_sources"],
            "time_candidate_sources": response["time_candidate_sources"],
            "catalog_read_bytes": response["catalog_read_bytes"],
            "sql_queries": response["sql_queries"], "identities": expected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build")
    b.add_argument("--manifest", type=Path, required=True)
    b.add_argument("--output", type=Path, required=True)
    q = sub.add_parser("compare")
    q.add_argument("--index", type=Path, required=True)
    q.add_argument("--socket", type=Path, required=True)
    q.add_argument("--lower-unix-s", type=int, required=True)
    q.add_argument("--upper-unix-s", type=int, required=True)
    args = parser.parse_args()
    if args.command == "build":
        print(json.dumps({"entries": len(build(args.manifest, args.output)["entries"]),
                          "index": str(args.output)}))
    else:
        print(json.dumps(compare(json.loads(args.index.read_text()), args.socket,
                                 args.lower_unix_s, args.upper_unix_s)))


if __name__ == "__main__":
    main()
