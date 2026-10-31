import copy
import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

from experiments.cross_version.resource_limited import native_hdf_run as run
from experiments.identity.registry import file_sha


def matrix():
    return [dict(scene_id=s,arm=a,repeat=r,phase='warmup' if r<2 else 'measurement',
        ok=True,oracle_verified=True,native_mode_verified=True,owned_output_bytes=40960,
        total_ns=300 if a=='IoTDB-FS' else 100,
        discovery_client_ns=100 if a=='IoTDB-FS' else 50,
        selected_value_client_ns=200 if a=='IoTDB-FS' else 50,
        run_id=s+a,case_id=s,corpus_id=s) for s in run.SCENES for a in run.ARMS for r in range(12)]


class NativeRetestTests(unittest.TestCase):
    def test_typed_api_must_prove_native_execution_and_reject_chunk_fallback(self):
        from experiments.cross_version.resource_limited.native_hdf_scene import assert_native_mode
        profile=dict(server_hdf5_execution={'mode':'native-hdf5'},
            stages_ns={'hdf5.native.setup':1,'hdf5.native.read':1},
            counters={'hdf5.native.slabs':1,'hdf5.native.source.calls':1},runtime_delta={})
        row={'value_calls':[{'server_profile':copy.deepcopy(profile)} for _ in range(2)]}
        self.assertTrue(assert_native_mode(copy.deepcopy(row))['native_mode_verified'])
        for section,key,value in [('server_hdf5_execution','mode','iotdb-indexed'),
                ('counters','hdf5.native.slabs',0),('counters','hdf5.native.source.calls',0),
                ('counters','hdf5.selection.decoder_compiles',1),('runtime_delta','decode_count',1)]:
            invalid=copy.deepcopy(row);invalid['value_calls'][0]['server_profile'][section][key]=value
            with self.assertRaises(ValueError):assert_native_mode(invalid)

    def test_all_48_queries_are_required_and_warm_ratios_are_phase_specific(self):
        result=run.summarize(matrix())
        self.assertEqual(result['new_queries'],48)
        for cell in result['cells']:
            for value, expected in zip((cell['R'],cell['Rd'],cell['Rq']),(3,2,4)):
                self.assertAlmostEqual(value,expected)
        for mutate in (lambda rows: rows.pop(),
                       lambda rows: rows.__setitem__(0,copy.deepcopy(rows[1])),
                       lambda rows: rows[0].update(oracle_verified=False),
                       lambda rows: rows[12].update(native_mode_verified=False),
                       lambda rows: rows[0].update(total_ns=101),
                       lambda rows: rows[2].update(run_id='mixed-run')):
            rows=matrix();mutate(rows)
            with self.assertRaises(RuntimeError): run.summarize(rows)

    def test_launcher_executes_only_explicit_native_engine_with_frozen_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);binary=root/'binary';binary.write_text('unchanged binary')
            config=root/'profile.conf';config.write_text('original=preserved\n')
            launcher=root/'launcher'
            receipt=run.launcher(launcher,binary,file_sha(binary),'trusted_prefix',root/'owner')
            with patch.dict(os.environ,{'IOTDB_CONFIG_FILE':str(config)}),patch('os.execv') as execute:
                runpy.run_path(str(launcher),run_name='__main__')
                execute.assert_called_once()
            self.assertEqual(receipt['settings']['query.hdf5.mode'],'native-hdf5')
            self.assertIn('\nquery.hdf5.mode=native-hdf5\n',config.read_text())
            self.assertNotIn('iotdb-indexed',config.read_text())
            self.assertNotIn('catalog_mode',config.read_text())


if __name__=='__main__':unittest.main()
