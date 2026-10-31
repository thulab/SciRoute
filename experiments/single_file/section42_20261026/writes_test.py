import hashlib
import copy
import json
from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import writes
from experiments.single_file.section42_20261026 import lifecycle_collection as lifecycle, read_driver


class WriteTests(unittest.TestCase):
    def test_full_copy_does_not_modify_source_and_rejects_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "input.h5"
            data = bytes(range(256)) * 8193
            raw.write_bytes(data)
            target = root / "output.h5"
            row = writes.checked_copy(raw, target, expected_bytes=len(data))
            self.assertEqual(raw.read_bytes(), data)
            self.assertEqual(target.read_bytes(), data)
            self.assertEqual(row["bytes"], len(data))
            self.assertTrue(row["file_fsync_acknowledged"] and row["closed"])
            self.assertEqual(row["total_ns"], sum(row[k] for k in
                ("open_ns", "copy_ns", "file_fsync_ns", "close_ns")))
            with self.assertRaisesRegex(ValueError, "already exists"):
                writes.checked_copy(raw, target, expected_bytes=len(data))

    def test_visibility_confirms_exact_version(self):
        seen = []
        def control(request):
            seen.append(request["request"]["sources"]["versions"].copy())
            return {"datasets": [{"path": "/x/a.h5", "version": 19}]}, 1
        identity = writes.exact_visibility_locator(control=control,
            logical_path="/x/a.h5", format_id="hdf5")
        self.assertEqual(seen, [{"kind": "latest"}, {"kind": "exact", "exact": 19}])
        self.assertTrue(identity["exact_visible"])

    def test_publish_partial_receipt_and_frozen_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mount = root / "mount"
            mount.mkdir()
            raw = root / "a.h5"
            raw.write_bytes(b"frozen")
            digest = hashlib.sha256(b"frozen").hexdigest()
            row = writes.publish_sources(context={"mount": mount},
                sources=[{"id": "H-F6-T2", "raw": str(raw), "bytes": 6,
                          "sha256": digest, "format_id": "hdf5"}], prefix="/private-new",
                receipt_path=root / "receipt.json",
                locator=lambda path, source: {"logical_path": path, "version": 9,
                                               "exact_visible": True})
            self.assertTrue(row["complete"])
            self.assertEqual(row["publication_bytes"], 6)
            self.assertTrue(row["sources"][0]["verification_complete"])


class IndependentLifecycleCampaignTests(unittest.TestCase):
    @staticmethod
    def registry():
        cases, sources = [], {}
        for ident in lifecycle.BASE_CASES:
            source_id = ident.replace('-F1', '')
            source = dict(source_id=source_id, raw='/frozen/'+source_id+'.h5', bytes=16,
                          sha256='f'*64, format_id='grib' if ident.startswith('G') else 'hdf5')
            sources[source_id] = source
            cases.append(dict(source, case_id=ident))
        return dict(cases=cases, sources=sources)

    def test_single_campaign_keeps_global_method_and_q_rotation(self):
        for campaigns in ((0,), (1,), (2,), (0, 1, 2)):
            with self.subTest(campaigns=campaigns), tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                emitted, mounts = [], []
                @contextmanager
                def factory(campaign):
                    mount = work/('mount%d'%campaign)
                    mount.mkdir()
                    mounts.append(campaign)
                    yield dict(mount=mount)
                def emit(**item):
                    emitted.append((item['campaign'],item['case']['case_id'],item['method'],item['q']))
                with patch.object(lifecycle.rm,'main_cases',return_value=self.registry()), \
                     patch.object(lifecycle,'NativeHdf5'), patch.object(lifecycle,'NativeGrib'):
                    count = lifecycle.collect_lifecycles(work,work,factory,emit,campaigns=campaigns)
                self.assertEqual(count,30*len(campaigns))
                self.assertEqual(sum(item[3] for item in emitted),1110*len(campaigns))
                self.assertEqual(mounts,list(campaigns))
                self.assertEqual(len(set(emitted)),count)
                for campaign in campaigns:
                    selected = [item for item in emitted if item[0] == campaign]
                    self.assertEqual(selected[0][2], 'IoTDB-FS' if campaign == 1 else 'native FS')
                    self.assertEqual([item[3] for item in selected[:3]],
                        list(lifecycle.Q_VALUES[campaign:]+lifecycle.Q_VALUES[:campaign]))

    def test_namespace_is_run_specific_bounded_and_stage_scope_is_explicit(self):
        base = 'vd_'+'a'*48
        first = lifecycle.lifecycle_namespace(base,Path('/runs/Retry-A_20261027'),1)
        self.assertRegex(first,r'^vd_[a-z0-9_]{1,48}$')
        self.assertEqual(first,lifecycle.lifecycle_namespace(base,Path('/runs/Retry-A_20261027'),1))
        self.assertNotEqual(first,lifecycle.lifecycle_namespace(base,Path('/runs/Retry-B_20261027'),1))
        self.assertNotEqual(first,lifecycle.lifecycle_namespace(base,Path('/runs/Retry-A_20261027'),2))
        self.assertEqual(lifecycle.campaigns_for_stage('lifecycle-c1'),(1,))
        self.assertEqual(lifecycle.campaigns_for_stage('lifecycle'),(0,1,2))
        for bad in ((), (1,1), (3,), (True,), [1]):
            with self.assertRaises((ValueError,RuntimeError)):
                lifecycle.normalize_campaigns(bad)
        with self.assertRaises(ValueError):
            lifecycle.campaigns_for_stage('lifecycle-c3')

    @staticmethod
    def records(campaigns):
        return [dict(case_id=ident.replace('-F1','-F6'),method=method,campaign=campaign,q=q,
                oracle_verified=True,lifecycle_wall_ns=1000000+campaign*100,
                write=dict(total_ns=100),source_locator_ns=10,
                session=dict(q=q,session_wall_ns=1000,observations=[{} for _ in range(q)]))
            for campaign in campaigns for ident in lifecycle.BASE_CASES
            for method in ('native FS','IoTDB-FS') for q in (1,10,100)]

    def analyze_fixture(self,stage,records):
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory)
            (work/'preflight.json').write_text(json.dumps(dict(stage=stage,
                lifecycle_campaigns=list(lifecycle.campaigns_for_stage(stage)))))
            (work/'lifecycles.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in records))
            with patch.object(read_driver.run,'validate_coverage',return_value=dict(complete=True)):
                return read_driver.analyze(work)

    def test_analysis_reports_one_observation_per_declared_single_campaign_cell(self):
        for campaign in (0,1,2):
            result=self.analyze_fixture('lifecycle-c%d'%campaign,self.records((campaign,)))
            self.assertEqual(result['lifecycles'],30)
            self.assertEqual(result['lifecycle_queries'],1110)
            self.assertEqual(result['lifecycle_campaigns'],[campaign])
            self.assertEqual(len(result['lifecycle_cells']),30)
            for cell in result['lifecycle_cells']:
                self.assertEqual(cell['count'],1)
                self.assertEqual(cell['campaigns'],[campaign])
                self.assertEqual(cell['aggregation'],'single-campaign observation')
                self.assertEqual(cell['raw_lifecycle_ms'],[(1000000+campaign*100)/1e6])
        result=self.analyze_fixture('lifecycle',self.records((0,1,2)))
        self.assertEqual(result['lifecycles'],90)
        self.assertEqual(result['lifecycle_queries'],3330)
        self.assertTrue(all(cell['count']==3 for cell in result['lifecycle_cells']))

    def test_analysis_rejects_partial_duplicate_wrong_campaign_or_incomplete_session(self):
        valid=self.records((1,))
        short=copy.deepcopy(valid)
        short[0]['session']['observations']=[]
        for bad in (valid[:-1],valid+[valid[0]],self.records((0,)),short):
            with self.assertRaises((ValueError,RuntimeError)):
                self.analyze_fixture('lifecycle-c1',bad)


if __name__ == "__main__":
    unittest.main()
