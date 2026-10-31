import importlib.util
from pathlib import Path
import tempfile
import unittest

from experiments.cross_version.hdf5_route3 import route3_data as data


class Route3DataTests(unittest.TestCase):
    def test_plan_has_two_permuted_dataset_matches(self):
        for n in (8,128,512,1024):
            rows=data.plan(n)
            selected=[(r['file_id'],u['dataset']) for r in rows for u in r['units'] if u['attributes']==data.PREDICATE]
            self.assertEqual(len(selected),2)
            self.assertNotEqual({r[0] for r in selected},{0,1})
            self.assertEqual(rows,data.plan(n))

    def test_full_independent_history_and_fixed_roi_oracle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'raw'
            first=data.prepare_round(root,8,1)
            hashes={e['file']:e['sha256'] for e in first['entries']}
            second=data.prepare_round(root,8,2)
            self.assertEqual(second['rounds'],2)
            self.assertEqual(len(second['entries']),16)
            self.assertEqual(len({(e['device'],e['inode']) for e in second['entries']}),16)
            self.assertTrue(all(e['nlink']==1 and e['allocated_bytes']>0 for e in second['entries']))
            self.assertTrue(all(data.sha_file(root/name)==digest for name,digest in hashes.items()))
            for origin in data.ORIGINS:
                expected=data.expected(second,[1,2],origin)
                self.assertEqual(len(expected),4)
                self.assertTrue(all(o['elements']==4096 for o in expected.values()))
            with self.assertRaisesRegex(ValueError,'append-only'):
                data.prepare_round(root,8,2)

    def test_controller_source_budget_and_semantic_windows(self):
        from experiments.cross_version.hdf5_route3 import route3_run
        self.assertEqual([s['name'] for s in route3_run.selections(1,512)],['latest'])
        for n in (128,512):
            for v in (8,32):
                selections=route3_run.selections(v,n)
                self.assertEqual(len(selections),3)
                self.assertLessEqual(n*max(len(s['rounds']) for s in selections),1024)
                self.assertNotEqual(selections[-1]['rounds'],list(range(1,v+1)))
        self.assertEqual(len(route3_run.selections(32,1024)),2)


if __name__=='__main__':unittest.main()
