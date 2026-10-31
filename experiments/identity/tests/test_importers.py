"""SDK-free importer tests. Fixtures are evidence documents, not generated data."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from experiments.identity.importers import import_multi_file, import_route3


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def sealed(value):
    result = deepcopy(value)
    result.pop("manifest_sha256", None)
    result["manifest_sha256"] = hashlib.sha256(canonical(result)).hexdigest()
    return result


def multi(format_id="grib"):
    sources = []
    for file_id in range(2):
        units = []
        for slot in range(2):
            unit = {"attributes":{"level":850 if file_id == slot == 0 else 851},
                    "oracle":{"elements":4, "value_sha256":"a"*64, "physical_dtype":"<f4"}}
            if format_id == "grib":
                unit.update(offset=slot*4, length=4, schema={"Ni":2,"Nj":2})
            else:
                unit.update(dataset="/fields/d%d"%slot, schema={"shape":[1,1,2,2], "chunks":[1,1,1,1]})
            units.append(unit)
        sources.append({"file":"raw/f%d.%s"%(file_id, "grib" if format_id == "grib" else "h5"),
                        "bytes":8,"sha256":str(file_id+1)*64,"units":units})
    query = {"level":850}
    return sealed({"protocol":"iotdbfs-multifile-data/1", "format":format_id, "kind":"generated",
                   "sources":sources,"raw_bytes":16,"query":query,"query_template":{"predicate":query},
                   "actual":{"N":2,"D_per_file":[2,2],"K":1,"K_unit":1},
                   "spec":{"protocol":"iotdbfs-multifile-spec/1","format":format_id,"N":2,"D":2,
                           "seed":99,"query":query,"payload":{"Ni":2,"Nj":2} if format_id == "grib"
                           else {"shape":[1,1,2,2],"chunks":[1,1,1,1]}}})


def route3():
    origins = [[0,0,0,0],[0,0,1,1]]
    entries = []
    for ordinal in (1,2):
        for file_id in (0,1):
            logical = "b%02d/f%d.h5"%(file_id,file_id)
            entries.append({"file_id":file_id,"logical_file":logical,"ordinal":ordinal,
                "file":"v%03d/%s"%(ordinal,logical),"bytes":16,"sha256":str(file_id+ordinal)*64,
                "units":[{"dataset":"/fields/d00","attributes":{"level":850+file_id},"oracles":[
                    {"origin":origin,"shape":[1,1,2,2],"elements":4,"value_sha256":"b"*64,"mask_sha256":"c"*64}
                    for origin in origins]}]})
    return {"protocol":"iotdbfs-route3-hdf5-data/1","N":2,"D":1,"K":1,"rounds":2,"seed":99,
            "shape":[1,1,4,4],"chunks":[1,1,2,2],"roi":[1,1,2,2],"origins":origins,
            "storage":"synthetic evidence document only","physical_bytes":64,"predicate":{"level":850},"entries":entries}


def publication(manifest):
    entries = manifest.get("entries", manifest.get("sources"))
    return {"receipt_sha256":"d"*64,"namespace":"explicit-test-namespace","bindings":[
        {"logical_file":entry.get("logical_file",entry.get("file")),"ordinal":entry.get("ordinal",1),
         "path":"/published/"+entry.get("logical_file",entry.get("file")),
         "version":10000+index,"bytes":entry["bytes"],"sha256":entry.get("sha256","e"*64)}
        for index,entry in enumerate(entries)]}


class Importers(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)/"manifest.json"

    def write(self, value):
        self.path.write_bytes(canonical(value))
        return hashlib.sha256(self.path.read_bytes()).hexdigest()

    def test_multifile_all_sources_have_only_explicit_database_versions(self):
        manifest = multi(); evidence = publication(manifest); original = deepcopy(evidence)
        result = import_multi_file(self.path, expected_sha256=self.write(manifest), publication_evidence=evidence)
        self.assertTrue(result["bound"])
        self.assertEqual(result["N_total"],2)
        self.assertEqual([r["ordinal"] for r in result["inventory"]],[1,1])
        self.assertEqual([r["binding"]["version"] for r in result["inventory"]],[10000,10001])
        self.assertEqual(result["inventory"][0]["units"],manifest["sources"][0]["units"])
        self.assertEqual(evidence,original)
        self.assertFalse(result["source_evidence"]["raw_source_hashes_reverified"])
        self.assertEqual(result["source_evidence"]["raw_files"][0],
            {"logical_file":"raw/f0.grib","ordinal":1,"file":"raw/f0.grib"})

    def test_hdf_oracle_without_mask_is_preserved_without_synthesis(self):
        manifest = multi("hdf5")
        result = import_multi_file(self.path, expected_sha256=self.write(manifest), publication_evidence=publication(manifest))
        self.assertTrue(result["bound"])
        self.assertNotIn("mask_sha256",result["inventory"][0]["units"][0]["oracle"])
        self.assertTrue(any("mask" in warning for warning in result["warnings"]))

    def test_no_publication_means_unbound_not_ordinal_as_database_version(self):
        result = import_multi_file(self.path, expected_sha256=self.write(multi()))
        self.assertFalse(result["bound"])
        self.assertIsNone(result["publication_evidence"])
        self.assertTrue(all("binding" not in record for record in result["inventory"]))

    def test_partial_publication_does_not_shrink_inventory(self):
        manifest = multi(); evidence = publication(manifest); evidence["bindings"].pop()
        result = import_multi_file(self.path, expected_sha256=self.write(manifest),publication_evidence=evidence)
        self.assertFalse(result["bound"])
        self.assertEqual(len(result["inventory"]),2)
        self.assertEqual(sum("binding" in r for r in result["inventory"]),1)

    def test_external_byte_digest_and_internal_fingerprint_are_separate_gates(self):
        manifest = multi(); self.write(manifest)
        with self.assertRaisesRegex(ValueError,"byte SHA"):
            import_multi_file(self.path,expected_sha256="0"*64)
        manifest["sources"][0]["bytes"] += 1
        with self.assertRaisesRegex(ValueError,"internal fingerprint"):
            import_multi_file(self.path,expected_sha256=self.write(manifest))

    def test_missing_raw_hash_stays_unbound_even_with_published_hash(self):
        manifest = multi(); evidence = publication(manifest); manifest["sources"][0].pop("sha256")
        result = import_multi_file(self.path,expected_sha256=self.write(sealed(manifest)),publication_evidence=evidence)
        self.assertFalse(result["bound"])
        self.assertEqual(result["inventory"][0]["sha256"],"")
        self.assertNotIn("binding",result["inventory"][0])

    def test_missing_generation_seed_never_becomes_new_generation_evidence(self):
        manifest = multi();manifest["spec"].pop("seed")
        result = import_multi_file(self.path,expected_sha256=self.write(sealed(manifest)),publication_evidence=publication(manifest))
        self.assertFalse(result["bound"])
        self.assertNotIn("seed",result["generation"])

    def test_missing_native_oracle_does_not_gain_synthetic_hashes(self):
        manifest=multi();manifest["sources"][0]["units"][0].pop("oracle")
        result=import_multi_file(self.path,expected_sha256=self.write(sealed(manifest)),publication_evidence=publication(manifest))
        self.assertFalse(result["bound"])
        self.assertNotIn("oracle",result["inventory"][0]["units"][0])

    def test_duplicate_extraneous_and_conflicting_publication_evidence_fail(self):
        manifest = multi(); digest = self.write(manifest)
        for mode in ("duplicate","extraneous","hash","size","published_identity"):
            with self.subTest(mode=mode):
                evidence = publication(manifest)
                if mode == "duplicate": evidence["bindings"].append(deepcopy(evidence["bindings"][0]))
                if mode == "extraneous": evidence["bindings"][0]["logical_file"]="raw/outside.grib"
                if mode == "hash": evidence["bindings"][0]["sha256"]="f"*64
                if mode == "size": evidence["bindings"][0]["bytes"] += 1
                if mode == "published_identity":
                    evidence["bindings"][1]["path"]=evidence["bindings"][0]["path"]
                    evidence["bindings"][1]["version"]=evidence["bindings"][0]["version"]
                with self.assertRaises(ValueError):
                    import_multi_file(self.path,expected_sha256=digest,publication_evidence=evidence)

    def test_publication_requires_explicit_receipt_namespace_and_version(self):
        manifest = multi();digest = self.write(manifest)
        for field in ("receipt_sha256","namespace","version"):
            evidence = publication(manifest)
            if field == "version": evidence["bindings"][0].pop(field)
            else: evidence.pop(field)
            with self.subTest(field=field),self.assertRaises(ValueError):
                import_multi_file(self.path,expected_sha256=digest,publication_evidence=evidence)

    def test_manifest_relabelling_counts_and_unsafe_paths_are_rejected(self):
        for mode in ("N","D","bytes","path","geometry"):
            manifest = multi()
            if mode == "N": manifest["spec"]["N"]=1
            if mode == "D": manifest["actual"]["D_per_file"]=[1,1]
            if mode == "bytes": manifest["raw_bytes"]=15
            if mode == "path": manifest["sources"][0]["file"]="raw/../escape.grib"
            if mode == "geometry": manifest["sources"][0]["units"][0]["schema"]["Ni"]=9
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                import_multi_file(self.path,expected_sha256=self.write(sealed(manifest)))

    def test_route3_retains_complete_history_and_explicit_timestamp_versions(self):
        manifest=route3();evidence=publication(manifest)
        result=import_route3(self.path,expected_sha256=self.write(manifest),publication_evidence=evidence)
        self.assertTrue(result["bound"])
        self.assertEqual(result["N_total"],2)
        self.assertEqual(len(result["inventory"]),4)
        self.assertEqual({r["ordinal"] for r in result["inventory"]},{1,2})
        self.assertTrue(all(r["binding"]["version"] >= 10000 for r in result["inventory"]))
        self.assertEqual(result["generation"]["rounds"],2)
        self.assertEqual(result["query_oracles"]["origins"],manifest["origins"])
        self.assertEqual(result["source_evidence"]["raw_files"][1],
            {"logical_file":"b00/f0.h5","ordinal":2,"file":"v002/b00/f0.h5"})

    def test_route3_partial_window_cannot_be_imported_as_complete_history(self):
        manifest=route3();manifest["entries"].pop()
        with self.assertRaisesRegex(ValueError,"partial or relabelled"):
            import_route3(self.path,expected_sha256=self.write(manifest))

    def test_route3_rejects_ordinal_fileid_or_raw_path_rebinding(self):
        for mode in ("ordinal","file_id","physical_file","duplicate","N"):
            manifest=route3()
            if mode == "ordinal": manifest["entries"][0]["ordinal"]=3
            if mode == "file_id": manifest["entries"][2]["file_id"]=1
            if mode == "physical_file": manifest["entries"][0]["file"]="v002/"+manifest["entries"][0]["logical_file"]
            if mode == "duplicate": manifest["entries"][1]=deepcopy(manifest["entries"][0])
            if mode == "N": manifest["N"]=1
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                import_route3(self.path,expected_sha256=self.write(manifest))

    def test_route3_wrong_roi_oracle_or_query_count_fail(self):
        for mode in ("origin","shape","elements","K"):
            manifest=route3();oracle=manifest["entries"][0]["units"][0]["oracles"][0]
            if mode == "origin": oracle["origin"]=[0,0,9,9]
            if mode == "shape": oracle["shape"]=[1,1,1,4]
            if mode == "elements": oracle["elements"]=9
            if mode == "K": manifest["K"]=2
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                import_route3(self.path,expected_sha256=self.write(manifest))

    def test_route3_missing_geometry_stays_unbound(self):
        manifest=route3();manifest.pop("seed")
        result=import_route3(self.path,expected_sha256=self.write(manifest),publication_evidence=publication(manifest))
        self.assertFalse(result["bound"])

    def test_route3_publication_cannot_rebind_logical_path_or_reverse_history(self):
        manifest=route3();digest=self.write(manifest)
        for mode in ("path","version"):
            evidence=publication(manifest)
            if mode == "path": evidence["bindings"][2]["path"]="/different/file.h5"
            else: evidence["bindings"][2]["version"]=1
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                import_route3(self.path,expected_sha256=digest,publication_evidence=evidence)

    def test_duplicate_json_keys_and_symlink_manifests_are_rejected(self):
        self.path.write_text('{"protocol":1,"protocol":2}')
        with self.assertRaisesRegex(ValueError,"duplicate JSON"):
            import_route3(self.path,expected_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest())
        target=self.path.with_name("link.json");target.symlink_to(self.path)
        with self.assertRaisesRegex(ValueError,"nonsymlink"):
            import_route3(target,expected_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
