import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from experiments.cross_version.hdf5_route3 import hdf5_two_modes as run


class TwoModesRunnerTest(unittest.TestCase):
    def test_bounded_matrix_contains_only_formal_typed_and_native_fs(self):
        self.assertEqual(run.plan()["expected_queries"], 36)
        self.assertEqual(run.plan(True)["expected_queries"], 54)
        self.assertEqual(run.plan()["arms"], ["FS-index", "typed"])
        self.assertEqual(run.plan()["candidate_sources"], 128)
        self.assertEqual(run.plan()["K"], 2)

    def test_candidate_launcher_replaces_instead_of_aliases_old_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "binary"
            binary.write_bytes(b"explicit candidate")
            def launcher(path, binary, prefix, journal, initialize):
                self.assertFalse(initialize)
                settings = {"query.hdf5.catalog_mode":"substructure-index",
                            "storage.version_directory.initialize":"false"}
                path.write_text("SETTINGS="+repr(settings)+"\n")
                return dict(binary_sha256=run.ab.sha(binary), settings=settings, path=str(path))
            prior = SimpleNamespace(launcher=launcher)
            for mode in run.MODES:
                result = run.create_launcher(prior, root / mode, binary, run.ab.sha(binary), "p", root / "owner", mode)
                settings = result["settings"]
                if mode == "baseline-r3":
                    self.assertEqual(settings["query.hdf5.catalog_mode"], "substructure-index")
                    self.assertNotIn("query.hdf5.mode", settings)
                else:
                    self.assertNotIn("query.hdf5.catalog_mode", settings)
                    self.assertEqual(settings["query.hdf5.mode"], mode)
                    self.assertEqual(result["sha256"], run.ab.sha(root / mode))
                self.assertEqual(settings["query.frontend.max_registrations"], "8192")

    def test_actual_profile_mode_must_match_for_every_candidate_call(self):
        call = {"server_profile":{"server_hdf5_execution":{"mode":"native-hdf5"},
            "counters":{"hdf5.native.slabs":1, "hdf5.native.source.calls":2}}}
        row = dict(arm="typed", discovery_calls=[call], value_calls=[call])
        run.verify_mode(row, "native-hdf5")
        with self.assertRaises(RuntimeError):
            run.verify_mode(row, "iotdb-indexed")
        run.verify_mode(dict(arm="FS-index", discovery_calls=[], value_calls=[]), "native-hdf5")
        with self.assertRaises(RuntimeError):
            run.verify_mode(dict(arm="typed", discovery_calls=[], value_calls=[]), "native-hdf5")

    def test_engine_gate_rejects_mode_label_without_actual_engine_execution(self):
        def row(mode, counters):
            call = {"server_profile":{"server_hdf5_execution":{"mode":mode}, "counters":counters}}
            return dict(arm="typed", discovery_calls=[call], value_calls=[call])
        for counters in ({}, {"hdf5.native.slabs":1}, {"hdf5.native.source.calls":1},
                         {"hdf5.native.slabs":1,"hdf5.native.source.calls":1,"hdf5.selection.decoder_compiles":1}):
            with self.assertRaises(RuntimeError):
                run.verify_mode(row("native-hdf5", counters), "native-hdf5")
        run.verify_mode(row("iotdb-indexed", {}), "iotdb-indexed")
        for key in ("hdf5.native.slabs", "hdf5.native.source.calls"):
            with self.assertRaises(RuntimeError):
                run.verify_mode(row("iotdb-indexed", {key:1}), "iotdb-indexed")
        both = row("native-hdf5", {"hdf5.native.slabs":1,"hdf5.native.source.calls":1})
        both["value_calls"].append({"server_profile":{"server_hdf5_execution":{"mode":"native-hdf5"},"counters":{}}})
        with self.assertRaises(RuntimeError):
            run.verify_mode(both, "native-hdf5")

    def test_summary_never_reuses_or_pools_old_samples(self):
        rows = []
        for mode in run.MODES:
            for arm in run.plan()["arms"]:
                for campaign in range(2):
                    for repeat in range(3):
                        rows.append(dict(mode=mode, arm=arm, campaign=campaign, repeat=repeat,
                            total_ns=(1000 if repeat == 0 else 10)*1000000,
                            discovery_client_ns=3000000, selected_value_client_ns=7000000))
        result = run.summarize(rows, run.plan())
        self.assertEqual(len(result["cells"]), 6)
        self.assertTrue(all(r["warm_p50_ms"] == 10 and r["warm_samples"] == 4 for r in result["cells"]))
        with self.assertRaises(RuntimeError):
            run.summarize(rows[:-1], run.plan())

    def test_final_source_and_binary_binding_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            header = source / "include/iotdbfs/formats/hdf5/Hdf5QueryMode.h"
            header.parent.mkdir(parents=True)
            header.write_text("final mode contract")
            paths = {}
            for name in ("iotdb_fs", "v03_context_probe", "global_hot_cache_probe"):
                paths[name] = root / name
                paths[name].write_text(name)
            binding = root / "binding.json"
            run.ab.save(binding, dict(complete=True, binaries={n:run.ab.sha(p) for n,p in paths.items()},
                source_files={str(header.relative_to(source)):run.ab.sha(header)}))
            args = SimpleNamespace(build_binding=binding, candidate_source=source,
                candidate=paths["iotdb_fs"], context_probe=paths["v03_context_probe"], global_probe=paths["global_hot_cache_probe"])
            self.assertEqual(run.verify_candidate_binding(args), run.ab.sha(paths["iotdb_fs"]))
            header.write_text("changed after build")
            with self.assertRaises(RuntimeError):
                run.verify_candidate_binding(args)


if __name__ == "__main__":
    unittest.main()
