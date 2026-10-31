"""Independent boundary/oracle checks for the large-file experiment harness."""
import copy
import itertools
from pathlib import Path
import tempfile
import unittest

from experiments.cross_version.large_file import large_corpus as data
from experiments.cross_version.large_file import large_matrix as matrix
from common import TARGET
import grib_data
import hdf5_data
import h5py
import numpy as np


class LargeMatrixTest(unittest.TestCase):
    def test_preparation_continuation_rejects_partial_or_post_query_runs(self):
        manifest = {"manifest_sha256": "frozen"}
        namespaces = [{"system": system, "scenario": "identical", "V": 1, "head": {"seq": 54},
            "bindings": [{"format": fmt, "file_index": index, "ordinal": 1}
                for fmt in data.FORMATS for index in range(8)]} for system in ("V03", "V04")]
        old = {"original_service_restored": True, "complete": False, "query_records": 0,
            "corpus_sha256": "frozen", "failure": {"type": "ValueError"},
            "binaries": matrix.matrix.DAEMON_SHAS, "namespaces": namespaces}
        self.assertEqual(len(matrix.validate_preparation(old, manifest)), 2)
        for field, value in (("query_records", 1), ("original_service_restored", False), ("corpus_sha256", "changed")):
            wrong = copy.deepcopy(old)
            wrong[field] = value
            with self.assertRaises(RuntimeError):
                matrix.validate_preparation(wrong, manifest)
        wrong = copy.deepcopy(old)
        wrong["namespaces"][1]["bindings"].pop()
        with self.assertRaises(RuntimeError):
            matrix.validate_preparation(wrong, manifest)

    def records(self):
        result = []
        for scenario, versions, fmt, selection, system, campaign, repeat in itertools.product(
                data.SCENARIOS, (1, 8), data.FORMATS, matrix.SELECTIONS, matrix.SYSTEMS, range(4), range(5)):
            cost = (10 if system == "V04" else 20) * versions
            result.append(dict(scenario=scenario, V=versions, format=fmt, selection=selection,
                system=system, campaign=campaign, repeat=repeat, total_ns=cost * 1000000,
                oracle_verified=True))
        return result

    def test_growth_is_endpoint_ratio_and_joint_gate_requires_relative_gain(self):
        result = matrix.summarize(self.records())
        self.assertEqual(len(result["cells"]), 64)
        self.assertEqual(len(result["comparisons"]), 24)
        for comparison in result["comparisons"]:
            self.assertEqual(comparison["V04_growth"], 8)
            self.assertEqual(comparison["speedup"], 2)
            self.assertEqual(comparison["relative_gain"], 1)
            self.assertFalse(comparison["all_campaigns_joint_pass"])

    def test_explicit_grib_only_matrix_has_32_cells_and_640_queries(self):
        previous = matrix.FORMATS
        try:
            matrix.FORMATS = ("grib",)
            rows = [r for r in self.records() if r["format"] == "grib"]
            result = matrix.summarize(rows)
            self.assertEqual(len(rows), 640)
            self.assertEqual(len(result["cells"]), 32)
            self.assertEqual(len(result["comparisons"]), 12)
        finally:
            matrix.FORMATS = previous

    def test_first_request_excluded_but_retained(self):
        records = self.records()
        for row in records:
            if row["repeat"] == 0:
                row["total_ns"] = 999 * 1000000
        for cell in matrix.summarize(records)["cells"]:
            self.assertEqual(cell["first_by_campaign_ms"], [999] * 4)
            self.assertEqual(cell["warm_observations"], 16)
            self.assertNotEqual(cell["warm_p50_ms"], 999)

    def test_missing_duplicate_or_unverified_observation_rejected(self):
        records = self.records()
        for wrong in (records[:-1], records + [copy.copy(records[-1])]):
            with self.assertRaises(RuntimeError):
                matrix.summarize(wrong)
        records[-1]["oracle_verified"] = False
        with self.assertRaises(RuntimeError):
            matrix.summarize(records)

    def test_cyclic_arm_order_balances_every_position(self):
        for group in range(4):
            positions = {system: [] for system in matrix.SYSTEMS}
            for campaign in range(4):
                shift = (campaign + group) % 4
                arms = matrix.SYSTEMS[shift:] + matrix.SYSTEMS[:shift]
                for position, system in enumerate(arms):
                    positions[system].append(position)
            self.assertTrue(all(sorted(p) == [0, 1, 2, 3] for p in positions.values()))

    def test_fixed_block_tail_and_local_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "raw"
            path.write_bytes(b"a" * data.BLOCK + b"b" * 17)
            old = data.blocks(path)
            with path.open("r+b") as stream:
                stream.seek(7)
                stream.write(b"c")
            new = data.blocks(path)
            self.assertNotEqual(old[0], new[0])
            self.assertEqual(old[1], new[1])
            self.assertEqual([b[1] for b in new], [data.BLOCK, 17])

    def mutation(self, fmt):
        spec = {"query": TARGET, "D": 4, "K": 2, "seed": 20261013,
            "payload": ({"shape": [1, 1, 64, 64], "chunks": [1, 1, 32, 32], "gzip_level": 1,
                         "shuffle": False} if fmt == "hdf5" else
                        {"Ni": 64, "Nj": 64, "bits_per_value": 16, "packing": "grid_simple"})}
        backend = hdf5_data if fmt == "hdf5" else grib_data
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "v1"
            old = backend.create_file(source, spec, 0)["units"]
            digest = data.sha_file(source)
            for ordinal in range(2, 9):
                target = root / ("v%d" % ordinal)
                fresh = data.mutate(source, target, fmt, old, ordinal)
                changed = (ordinal - 2) % 4
                if fmt == "hdf5":
                    with h5py.File(source) as a, h5py.File(target) as b:
                        count = sum(np.count_nonzero(a[u["dataset"]][...] != b[u["dataset"]][...]) for u in old)
                else:
                    codes = grib_data.Codes()
                    count = 0
                    for u in old:
                        def values(path):
                            with path.open("rb") as stream:
                                stream.seek(u["offset"])
                                encoded = stream.read(u["length"])
                            with codes.handle(encoded) as handle:
                                return codes.array(handle, "values")
                        count += np.count_nonzero(values(source) != values(target))
                self.assertEqual(count, 1)
                self.assertEqual(data.sha_file(root / "v1"), digest)
                self.assertNotEqual(old[changed]["unit_id"], fresh[changed]["unit_id"])
                source, old = target, fresh

    def test_hdf5_mutations_change_exactly_one_value(self):
        self.mutation("hdf5")

    def test_grib_mutations_change_exactly_one_value_and_preserve_offsets(self):
        self.mutation("grib")


if __name__ == "__main__":
    unittest.main()
