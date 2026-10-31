import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import h5py
import numpy as np

import geos_time_directory as directory


def fixture(path, reference):
    with h5py.File(path, "w") as file:
        file.attrs["Source"] = "GEOSadas-5_7_2"
        file.attrs["VersionID"] = "5.7.2"
        file.attrs["Conventions"] = "CF-1.6"
        axes = []
        for name, values, units in (
                ("time", [0], "minutes since " + reference),
                ("lev", [750], "hPa"),
                ("lat", [-.25, 0, .25], "degrees_north"),
                ("lon", [0, .3125], "degrees_east")):
            axis = file.create_dataset(name, data=np.asarray(values, dtype="<f8"))
            axis.make_scale(name)
            axis.attrs["units"] = units
            if name == "lev":
                axis.attrs["positive"] = "down"
                axis.attrs["standard_name"] = "PLE_level"
            axes.append(axis)
        data = file.create_dataset("T", shape=(1, 1, 3, 2), dtype="<f4")
        data.attrs["standard_name"] = "air_temperature"
        data.attrs["units"] = "K"
        for dimension, axis in enumerate(axes):
            data.dims[dimension].attach_scale(axis)


class GeosTimeDirectoryTest(unittest.TestCase):
    def test_build_and_half_open_lookup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sources = []
            for version, reference in ((10, "2011-07-02 09:00:00"),
                                       (11, "2011-07-02 12:00:00")):
                raw = root / f"{version}.nc4"
                fixture(raw, reference)
                sources.append({"raw": str(raw), "logical_path": f"/geos/{version}.nc4",
                                "version": version, "bytes": raw.stat().st_size,
                                "sha256": hashlib.sha256(raw.read_bytes()).hexdigest()})
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"complete": True, "database": "test",
                                            "sources": sources}))
            index = directory.build(manifest, root / "index.json")
            first = index["entries"][0]["utc_second"]
            self.assertEqual(index["entries"][1]["utc_second"] - first, 10800)
            self.assertEqual(len(directory.search(index, first, first + 1)[0]), 1)
            self.assertEqual(len(directory.search(index, first + 1, first + 10800)[0]), 0)
            self.assertEqual(len(directory.search(index, first, first + 10801)[0]), 2)
            sources[0]["sha256"] = "0" * 64
            manifest.write_text(json.dumps({"complete": True, "database": "test",
                                            "sources": sources}))
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                directory.build(manifest, root / "bad-index.json")


if __name__ == "__main__":
    unittest.main()
