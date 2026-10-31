"""File-only CLI integration; no SDK, decoder, service or scientific workload."""
from contextlib import redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from experiments.identity.__main__ import main
from experiments.identity.tests.test_importers import multi, publication, route3, sealed


class Cli(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name)
        self.registry=self.root/"registry"

    def write(self, name, value):
        path=self.root/name
        path.write_text(json.dumps(value,sort_keys=True)+"\n")
        return path

    @staticmethod
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def call(self, command, *arguments, code=0):
        output=io.StringIO()
        with redirect_stdout(output):
            actual=main([command,"--registry",str(self.registry),*map(str,arguments)])
        document=json.loads(output.getvalue())
        self.assertEqual(actual,code,document)
        return document

    def corpus(self, fmt="grib"):
        manifest=multi(fmt)
        raw=self.root/("raw-"+fmt)
        for index,source in enumerate(manifest["sources"]):
            path=raw/source["file"]
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(bytes([index+1])*source["bytes"])
            source["sha256"]=self.sha(path)
        manifest=sealed(manifest)
        path=self.write(fmt+"-manifest.json",manifest)
        pub=self.write(fmt+"-publication.json",publication(manifest))
        document=self.call("import-corpus","--format","multi-file","--manifest",path,
            "--expected-sha256",self.sha(path),"--publication",pub)
        return document,raw,path

    def case(self, corpus, values=False, mask=False):
        shape=[2,2] if corpus["parameters"]["format"]=="grib" else [1,1,2,2]
        request={"scope":{"kind":"all","logical_files":sorted({r["logical_file"] for r in corpus["inventory"]}),
                  "buckets":[],"selection_basis":"complete synthetic unit-test population"},
            "selection":{"kind":"latest"},"predicate":{"level":850},
            "operation":"discover-values" if values else "discover",
            "roi":{"kind":"whole","origin":[0]*len(shape),"shape":shape},
            "output":{"projection":["value","mask"] if mask else ["value"] if values else ["locators"],
                      "dtype":"float32" if values else "none"},
            "expected_K":{"K_files":1,"K_file_versions":1,"K_units":1}}
        return self.call("case","--corpus-id",corpus["corpus_id"],"--request",self.write("request.json",request),
                         code=2 if mask else 0)

    def register_run(self, case, name="baseline", clock=None):
        payload={"case_id":case["case_id"],"execution":name,"arm":"typed",
            "implementation":{"binary_sha256":("a" if name=="baseline" else "b")*64,
                              "runner_sha256":"c"*64,"engine":"unit-test-engine"},
            "clock":clock or {"boundary":"client owned buffers"},"cache":{"policy":"warm"},
            "environment":{"scope":"unit-test-only"},"replication":{"campaigns":1,"repeats":5}}
        return self.call("run","--document",self.write(name+"-run.json",payload))

    def test_import_case_run_show_and_equivalent_comparison(self):
        corpus,_,path=self.corpus();original=self.sha(path)
        case=self.case(corpus);left=self.register_run(case);right=self.register_run(case,"candidate")
        result=self.call("compare","--left",left["run_id"],"--right",right["run_id"])
        self.assertTrue(result["comparable"])
        for kind,document in (("corpus",corpus),("case",case),("run",left)):
            self.assertEqual(self.call("show","--kind",kind,"--id",document[kind+"_id"]),document)
        self.assertEqual(self.sha(path),original)

    def test_sealed_run_document_registers_without_rebuilding_another_execution(self):
        corpus,_,_=self.corpus();run=self.register_run(self.case(corpus))
        repeated=self.call("run","--document",self.write("sealed-run.json",run))
        self.assertEqual(run,repeated)

    def test_compare_rejects_clock_mismatch(self):
        corpus,_,_=self.corpus();case=self.case(corpus)
        left=self.register_run(case);right=self.register_run(case,"candidate",{"boundary":"native internal buffers"})
        error=self.call("compare","--left",left["run_id"],"--right",right["run_id"],code=2)
        self.assertIn("clock",error["error"])

    def test_missing_publication_import_registers_unbound_not_corpus(self):
        manifest=multi();path=self.write("manifest.json",manifest)
        result=self.call("import-corpus","--format","multi-file","--manifest",path,
                         "--expected-sha256",self.sha(path))
        self.assertIn("unbound_id",result)
        self.assertFalse(result["bound"])
        self.assertNotIn("corpus_id",result)
        self.assertFalse((self.registry/"corpora").exists())

    def test_route3_dispatch_preserves_full_population_and_stored_history(self):
        manifest=route3();path=self.write("route3.json",manifest)
        pub=self.write("route3-publication.json",publication(manifest))
        result=self.call("import-corpus","--format","route3","--manifest",path,
                         "--expected-sha256",self.sha(path),"--publication",pub)
        self.assertEqual(result["parameters"]["N"],2)
        self.assertEqual(result["parameters"]["V_stored"],2)
        self.assertEqual(len(result["inventory"]),4)

    def test_old_hdf_native_mask_not_synthesized_by_case_command(self):
        corpus,_,_=self.corpus("hdf5")
        self.assertIn("case_id",self.case(corpus))
        self.assertIn("case_id",self.case(corpus,values=True))
        error=self.case(corpus,values=True,mask=True)
        self.assertIn("SHA",error["error"])

    def test_verify_inputs_reads_declared_bytes_and_rejects_changed_source(self):
        corpus,raw,_=self.corpus()
        result=self.call("verify-inputs","--corpus-id",corpus["corpus_id"],"--raw-root",raw)
        self.assertIn("verification_id",result)
        target=raw/corpus["inventory"][0]["logical_file"]
        target.write_bytes(b"changed!")
        error=self.call("verify-inputs","--corpus-id",corpus["corpus_id"],"--raw-root",raw,code=2)
        self.assertIn("changed",error["error"])

    def test_history_mapping_hashes_original_artifact_and_never_rewrites_it(self):
        corpus,_,_=self.corpus();case=self.case(corpus)
        artifact=self.write("historical.json",{"rows":[{"query_id":"0","old_label":"legacy"}]})
        before=artifact.read_bytes()
        payload={"artifact":{"path":str(artifact),"sha256":self.sha(artifact)},
            "row_selector":{"query_id":"0"},"case_id":case["case_id"],
            "evidence":[],"interpretation":"unit-test mapping; no new scientific execution"}
        path=self.write("mapping.json",payload)
        result=self.call("map-history","--document",path)
        self.assertIn("mapping_id",result)
        self.assertEqual(artifact.read_bytes(),before)
        artifact.write_text("changed")
        self.call("map-history","--document",path,code=2)

    def test_explicit_unbound_document_retains_reason_and_provenance(self):
        payload={"reason":"publication unavailable","provenance":{"legacy_label":"N128"}}
        result=self.call("unbound","--document",self.write("unbound.json",payload))
        self.assertEqual(result["reason"],payload["reason"])
        self.assertEqual(result["provenance"],payload["provenance"])

    def test_invalid_arguments_and_json_have_machine_readable_errors(self):
        output=io.StringIO()
        with redirect_stdout(output):
            code=main(["compare","--registry",str(self.registry)])
        self.assertEqual(code,2)
        self.assertFalse(json.loads(output.getvalue())["complete"])
        path=self.root/"invalid.json";path.write_text('{"reason":"a","reason":"b"}')
        error=self.call("unbound","--document",path,code=2)
        self.assertIn("duplicate JSON",error["error"])

    def test_wrong_manifest_sha_and_unknown_run_fields_fail_before_registration(self):
        path=self.write("manifest.json",multi())
        self.call("import-corpus","--format","multi-file","--manifest",path,"--expected-sha256","0"*64,code=2)
        self.assertFalse((self.registry/"corpora").exists())
        error=self.call("run","--document",self.write("unknown-run.json",{"case_id":"a"*64,"unexpected":True}),code=2)
        self.assertIn("exact",error["error"])


if __name__=="__main__":
    unittest.main()
