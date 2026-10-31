"""Contract checks for ERA5 split boundaries and stable sample identity."""

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import tempfile
import unittest

from sample_contract import CHANNELS, Catalog, VIEWS


class SampleContractTest(unittest.TestCase):
    def make_catalog(self, directory: Path, tier: str) -> Catalog:
        manifests = {}
        count = 8 if tier == "d0" else 124
        start = datetime(2018, 1, 1, tzinfo=timezone.utc)
        for kind in ("single-levels", "pressure-levels"):
            path = directory / (kind + ".grib")
            path.write_bytes(kind.encode())
            rows = []
            for index in range(count):
                when = start + timedelta(hours=6 * index)
                for channel in CHANNELS:
                    if channel.source != kind:
                        continue
                    rows.append({
                        "paramId": channel.param_id,
                        "level": channel.level,
                        "typeOfLevel": channel.type_of_level,
                        "units": channel.unit,
                        "Ni": 1440, "Nj": 721, "gridType": "regular_ll",
                        "step": 0, "numberOfMissing": 0,
                        "validityDate": int(when.strftime("%Y%m%d")),
                        "validityTime": int(when.strftime("%H%M")),
                        "offset": len(rows), "length": 1,
                    })
            manifests[kind] = {
                "path": str(path), "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "grid": [721, 1440], "complete_decode": True,
                "messages_with_missing": 0,
                "message_count": len(rows), "messages": rows,
            }
        return Catalog(manifests, tier)

    def test_d0_sample_identity_and_both_regions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            first = self.make_catalog(Path(temp), "d0")
            second = self.make_catalog(Path(temp), "d0")
            self.assertEqual(first.fingerprint, second.fingerprint)
            self.assertEqual(first.sample_rows("V0"), second.sample_rows("V0"))
            self.assertEqual(len(first.sample_rows("V0")), 96)
            self.assertEqual(len(first.sample_rows("V1")), 96)
            self.assertEqual(len(first.sample_rows("V2")), 96)
            self.assertEqual(len(first.sample_rows("V3")), 48)
            first_sample = first.sample_rows("V0")[0]
            self.assertEqual(first_sample["input_times"],
                             ["2018-01-01T00:00:00Z", "2018-01-01T06:00:00Z"])
            self.assertEqual(first_sample["target_time"], "2018-01-01T12:00:00Z")
            self.assertEqual((first_sample["row"], first_sample["col"]), (156, 400))
            self.assertEqual({s["split"] for s in first.sample_rows("V0")}, {"train"})

    def test_d1_split_windows_do_not_cross_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            catalog = self.make_catalog(Path(temp), "d1")
            for view_id in ("V0", "V1", "V2"):
                self.assertEqual(catalog.freeze()["views"][view_id]["counts"],
                                 {"train": 1312, "validation": 288, "test": 288})
            self.assertEqual(catalog.freeze()["views"]["V3"]["counts"],
                             {"train": 1264, "validation": 240, "test": 240})
            self.assertEqual(VIEWS["V2"]["channels"], [0, 1, 2, 3, 7, 9, 13, 17])
            rows = catalog.sample_rows("V0")
            last_train = [row for row in rows if row["split"] == "train"][-1]
            first_validation = next(row for row in rows
                                    if row["split"] == "validation")
            self.assertEqual(last_train["target_time"], "2018-01-21T18:00:00Z")
            self.assertEqual(first_validation["input_times"][0],
                             "2018-01-22T00:00:00Z")

    def test_missing_channel_is_rejected_before_sampling(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            catalog = self.make_catalog(Path(temp), "d0")
            manifests = {}
            for kind in ("single-levels", "pressure-levels"):
                rows = [row for slot in catalog.rows.values()
                        for ordinal, row in slot.items()
                        if CHANNELS[ordinal].source == kind]
                path = catalog.paths[kind]
                manifests[kind] = {
                    "path": str(path), "bytes": path.stat().st_size,
                    "sha256": next(x["sha256"] for x in catalog.sources
                                   if x["kind"] == kind),
                    "grid": [721, 1440], "complete_decode": True,
                    "messages_with_missing": 0, "message_count": len(rows),
                    "messages": rows,
                }
            manifests["single-levels"]["messages"].pop()
            manifests["single-levels"]["message_count"] -= 1
            with self.assertRaisesRegex(ValueError, "lack a channel"):
                Catalog(manifests, "d0")


if __name__ == "__main__":
    unittest.main()
