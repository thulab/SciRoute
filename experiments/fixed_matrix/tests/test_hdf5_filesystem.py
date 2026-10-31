"""Offline coverage/aggregation checks for the sealed filesystem-native suite."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import hdf5_filesystem as subject


class MatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(__file__).resolve().parents[3]
        cls.selected = subject.groups(cls.repo)

    def fixture(self):
        rows, bindings = [], {}
        for index, group in enumerate(self.selected):
            gid = group['group_id']
            b = dict(corpus_id='corpus'+str(index), case_id='case'+str(index), oracle_sha256='oracle'+str(index),
                     run_ids={str(c):{a:str(c)+a+gid for a in subject.run.ARMS} for c in subject.CAMPAIGNS})
            bindings[gid] = b
            for c in subject.CAMPAIGNS:
                for a in subject.run.ARMS:
                    for n in range(12):
                        factor = 2 if a == 'IoTDB-FS' else 1
                        profile = dict(server_hdf5_execution=dict(mode='native-hdf5',payload_backend='filesystem'),
                            counters={'hdf5.native.filesystem.opens':1,'hdf5.native.filesystem.calls':18,
                                'hdf5.native.source.calls':18,'hdf5.native.filesystem.bytes':19156,'hdf5.native.source.bytes':19156},
                            iotdb_delta=dict(query_calls=0,payload_calls=0,layout_calls=0,version_store_query_calls=0))
                        row = dict(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                            campaign=c,arm=a,repeat=n,run_id=b['run_ids'][str(c)][a],phase='warmup' if n<2 else 'measurement',
                            ok=True,mode_verified=True,oracle_verified=True,execution_mode='native-hdf5',
                            total_ns=3_000_000*factor,discovery_ns=1_000_000*factor,value_ns=2_000_000*factor,
                            resource_before={},resource_after={})
                        if a == 'IoTDB-FS':row['value_calls']=[dict(server_profile=deepcopy(profile)) for _ in range(group['K_units'])]
                        rows.append(row)
        return rows, bindings

    def analyze(self, rows, bindings):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'queries.jsonl';path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            with patch.object(subject.run,'validate_sample'):
                return subject.analyze(path,bindings,self.selected)

    def test_fixed_primary_only(self):
        self.assertEqual(len(self.selected),16)
        self.assertTrue(all(g['format']=='hdf5' and g['collection_tier']=='primary' for g in self.selected))
        self.assertEqual(max(g['K_units'] for g in self.selected),16)

    def test_complete_matrix_and_same_campaign_ratios(self):
        rows, bindings=self.fixture();result=self.analyze(rows,bindings)
        self.assertEqual(result['query_rows'],1728)
        self.assertEqual(len(result['cells']),16)
        self.assertTrue(all(c['R']==c['Rd']==c['Rq']==2 for c in result['cells']))

    def test_incomplete_or_duplicate_cells_rejected(self):
        rows, bindings=self.fixture()
        with self.assertRaises(RuntimeError):self.analyze(rows[:-1],bindings)
        rows[-1]=rows[0]
        with self.assertRaises(RuntimeError):self.analyze(rows,bindings)

    def test_database_fallback_rejected(self):
        rows, bindings=self.fixture();r=next(r for r in rows if r['arm']=='IoTDB-FS')
        r['value_calls'][0]['server_profile']['iotdb_delta']['payload_calls']=1
        with self.assertRaises(RuntimeError):self.analyze(rows,bindings)

    def test_metadata_sql_is_allowed_and_reported(self):
        rows, bindings=self.fixture();r=next(r for r in rows if r['arm']=='IoTDB-FS')
        r['value_calls'][0]['server_profile']['iotdb_delta']['query_calls']=1
        result=self.analyze(rows,bindings)
        self.assertEqual(result['value_database_calls']['query_calls'],1)
        self.assertEqual(result['value_database_calls']['payload_calls'],0)

    def test_history_delivery_count_rejected(self):
        rows, bindings=self.fixture();r=next(r for r in rows if r['arm']=='IoTDB-FS' and len(r['value_calls'])==16)
        r['value_calls'].pop()
        with self.assertRaises(RuntimeError):self.analyze(rows,bindings)

    def test_no_latency_based_exclusion(self):
        rows, bindings=self.fixture()
        for r in rows:
            if r['arm']=='IoTDB-FS' and r['campaign']==1:
                for k in ('total_ns','discovery_ns','value_ns'):r[k]*=100
        result=self.analyze(rows,bindings)
        self.assertTrue(all(c['R']==2 and c['R_range']==[2,200] and len(c['blocks'])==3 for c in result['cells']))


if __name__=='__main__':unittest.main()
