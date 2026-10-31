import unittest
from unittest.mock import Mock, patch
from experiments.cross_version.resource_limited import hdf_scene as h


class HdfSceneTests(unittest.TestCase):
    def test_missing_physical_evidence_fails_not_rebuilds(self):
        with self.assertRaises((OSError,RuntimeError)):
            h.inspect_hdf(dict(corpus_id=h.CORPUS_ID,case_id=h.CASE_ID,
                frozen_inputs={'/missing/never-created':'0'*64},native_artifacts={},
                native_library='/missing/library',native_sha256='0'*64,
                preparation_evidence={'path':'/missing/prepared','sha256':'0'*64}))

    def test_wrong_case_rejected_before_any_io(self):
        with patch.object(h,'_sha',side_effect=AssertionError('must not read')):
            with self.assertRaises(RuntimeError):
                h.inspect_hdf({'corpus_id':h.CORPUS_ID,'case_id':'0'*64})

    def test_owned_clock_result_requires_actual_oracle_and_bytes(self):
        scene={'expected_matches':[{'binding':{'path':'/a','version':123}}]}
        for row in ({'oracle_verified':False,'owned_output_bytes':40960},
                    {'oracle_verified':True,'owned_output_bytes':0}):
            with self.assertRaises(RuntimeError): h._validated(row,scene)
        row=h._validated({'oracle_verified':True,'owned_output_bytes':40960},scene)
        self.assertEqual(row['actual_matches'],scene['expected_matches'])
        self.assertIsNot(row['actual_matches'],scene['expected_matches'])
        self.assertEqual(row['backing_N'],512)
        self.assertEqual(row['candidate_sources'],128)

    def test_native_open_explicitly_disables_index_build(self):
        ctor=Mock()
        modules={'route3_native':Mock(NativeContext=ctor)}
        scene={'native_artifacts':{'/old/catalog.json':'a','/old/catalog.sqlite':'b'},
               'native_library':'/old/native.so'}
        with patch.object(h,'_case',return_value={}):
            handle=h._Native(scene,Mock(),modules)
        self.assertFalse(ctor.call_args.args[1]['build_index'])
        self.assertEqual(ctor.call_args.args[1]['cache_files'],8)
        handle.close();ctor.return_value.close.assert_called_once()

    def test_iotdb_offset_and_frozen_full_query_are_preserved(self):
        shared=Mock(); shared.timed_query.side_effect=lambda *args:dict(oracle_verified=True,owned_output_bytes=40960)
        context={};scene={'selector':{'paths':['four buckets']},'expected_matches':[]}
        with patch.object(h,'_modules',return_value=(shared,{})),patch.object(h,'_case',return_value={'case':1}):
            row,offset=h.query_hdf_iotdb(object(),context,scene,41)
        self.assertEqual(offset,41)
        self.assertEqual(shared.timed_query.call_args.args[0],'typed')
        self.assertIs(shared.timed_query.call_args.args[2],context)
        self.assertEqual(row['actual_matches'],[])


if __name__=='__main__': unittest.main()
