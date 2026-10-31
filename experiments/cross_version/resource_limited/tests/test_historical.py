import copy
from pathlib import Path
import statistics
import tempfile
import unittest

from experiments.cross_version.resource_limited import historical as h
from experiments.cross_version.resource_limited.analysis import historical_minima
from experiments.cross_version.resource_limited.large_scene import prepare_large_scene
from experiments.identity.registry import Registry

REPO=Path(__file__).resolve().parents[4]


class HistoricalTests(unittest.TestCase):
    def test_real_frozen_samples_reproduce_three_known_same_block_ratios(self):
        if not (REPO/h.SOURCES[0][1]).is_file():
            self.skipTest('historical local public receipts not present')
        with tempfile.TemporaryDirectory() as root:
            large=prepare_large_scene(REPO,Registry(root))
        scenes=[dict(scene_id='grib-n128',case_id=h.GRIB_CASE,
                     corpus_id='fcfcc777268da6cb86c6724d3ef114803f2b4e880e718b72381454cd2feb2ffc'),
                dict(scene_id='hdf5-c128',case_id=h.HDF_CASE,
                     corpus_id='69afe940d4184232679e46266e4f56e6089b363b6b8d19fe19f00bc119c5cfae'),large]
        result=h.build_history(REPO,scenes)
        self.assertEqual(len(result['blocks']),11)
        ratios={b['block_id']:statistics.median(b['samples_ns']['IoTDB-FS'])/
                              statistics.median(b['samples_ns']['FS-index']) for b in result['blocks']}
        self.assertAlmostEqual(next(v for k,v in ratios.items() if 'public-ab/' in k and k.endswith('#candidate')),
                               7.453800220245239)
        self.assertAlmostEqual(next(v for k,v in ratios.items() if 'reverse-final/' in k and k.endswith('#baseline-r3')),
                               10.795937126188328)
        self.assertAlmostEqual(min(v for k,v in ratios.items() if 'large-file-' in k),3.5953676694308174)
        summaries=historical_minima(result['blocks'],arms={'fs':'FS-index','iotdb':'IoTDB-FS'})
        self.assertTrue(summaries)
        for block in result['blocks']:
            self.assertNotIn('run_id',block)
            self.assertEqual(len(block['source_evidence']['sha256']),64)
            for rows in block['source_evidence']['original_rows'].values():
                self.assertTrue(any(row['warmup'] for row in rows))

    def test_duplicate_missing_failed_original_rows_rejected(self):
        scene=dict(case_id='a'*64,corpus_id='b'*64)
        artifact={'path':'frozen.jsonl','sha256':'c'*64}
        rows=[(1,dict(arm='FS',repeat=0,total_ns=2,oracle_verified=True)),
              (2,dict(arm='IO',repeat=0,total_ns=3,oracle_verified=True)),
              (3,dict(arm='FS',repeat=1,total_ns=2,oracle_verified=True)),
              (4,dict(arm='IO',repeat=1,total_ns=3,oracle_verified=True))]
        args=(scene,artifact,artifact,'block')
        self.assertEqual(h._block(*args,rows,'FS','IO',{})['samples_ns'],{'FS-index':[2],'IoTDB-FS':[3]})
        for bad in (rows+[rows[-1]],rows[:-1],copy.deepcopy(rows)):
            if len(bad)==4:bad[-1][1]['oracle_verified']=False
            with self.assertRaises(ValueError):h._block(*args,bad,'FS','IO',{})

    def test_unknown_scene_is_not_invented(self):
        with self.assertRaises(ValueError):h._scenes([])
        with self.assertRaises(ValueError):h._scenes([{'case_id':h.GRIB_CASE,'corpus_id':'missing'}])


if __name__=='__main__':unittest.main()
