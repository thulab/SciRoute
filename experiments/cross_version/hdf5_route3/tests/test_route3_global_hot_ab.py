import unittest
from types import SimpleNamespace
from unittest.mock import patch
from experiments.cross_version.hdf5_route3 import route3_global_hot_ab as run


class GlobalHotRunnerTest(unittest.TestCase):
    def test_pilot_covers_all_matches_with_exact_128_sources(self):
        predicate={'value':'target'}
        entries=[dict(logical_file='b%02d/f%06d.h5'%(i%16,i),
            units=[dict(attributes=predicate if i in (0,511) else {'value':'other'})]) for i in range(512)]
        buckets=run.pilot_buckets(dict(entries=entries),predicate)
        self.assertEqual(len(buckets),4)
        self.assertTrue({'b00','b15'}.issubset(buckets))
        selected=[e for e in entries if e['logical_file'].split('/')[0] in buckets]
        self.assertEqual(len(selected),128)
        self.assertEqual(sum(e['units'][0]['attributes']==predicate for e in selected),2)

    def test_bad_bucket_topology_is_rejected(self):
        with self.assertRaises(RuntimeError):
            run.pilot_buckets({'entries':[dict(logical_file='other/a.h5',units=[])]},{})

    def test_scoped_discovery_transmits_real_selector_and_rejects_escape(self):
        observed=[]
        def call(context,request,*args):
            observed.append(request)
            return {'datasets':[dict(path='/root/b00/a.h5',version=1,dataset_path='/data')]},[],{'total_ns':7},99
        remote=SimpleNamespace(discovery_request=lambda *a:{'request':{'sources':{'paths':[]}}},
            _call=call,_logical=lambda value:value)
        paths=[dict(kind='subtree',path='/root/b00')]
        rows,stats=run.scoped_discover(remote,{},'/root',None,paths)
        self.assertEqual(observed[0]['request']['sources']['paths'],paths)
        self.assertEqual(rows[0]['version'],1)
        self.assertEqual(stats['total_ns'],7)
        with self.assertRaises(RuntimeError):run.scoped_discover(remote,{},'/root',None,[dict(kind='subtree',path='/root/b01')])

    def test_owned_clock_contains_reads_and_excludes_only_profile_overhead(self):
        native=SimpleNamespace(stats=lambda:{},read=lambda *args:('owned-values','owned-mask'))
        remote=SimpleNamespace(equality_predicate=lambda p:p)
        data=SimpleNamespace(PREDICATE={},ROI=[1,1,64,64])
        def validate(units,*args):
            self.assertEqual(units[0]['array'],'owned-values')
            self.assertEqual(units[0]['mask'],'owned-mask')
            return {'oracle_verified':True}
        frozen=SimpleNamespace(validate_units=validate)
        modules=dict(route3_iotdb=remote,route3_data=data,route3_run=frozen,
            route3_native=SimpleNamespace(counter_delta=lambda *a:{}))
        case=dict(report={'logical_prefix':'/r'},bindings={('/r/b/a',7):{'source':'/raw/a'}},
                  expected={('latest',(0,0,0,0)): {}})
        stats=dict(instrumentation_ns=10)
        with patch.object(run,'scoped_discover',return_value=([dict(path='/r/b/a',version=7,dataset='/d')],stats)),\
             patch.object(run.time,'perf_counter_ns',side_effect=[100,140,200]):
            row=run.timed_query('catalog-native-local',native,{},case,dict(paths=[]),modules)
        self.assertEqual(row['total_ns'],90)
        self.assertEqual(row['discovery_client_ns'],30)
        self.assertEqual(row['selected_value_client_ns'],60)
        self.assertTrue(row['oracle_verified'])

    def test_summary_uses_only_fresh_scope_cells(self):
        rows=[]
        for binary,ms in [('baseline',10),('candidate',2)]:
            for scope,n in [('probe128',128),('full512',512)]:
                for repeat in range(3):
                    rows.append(dict(binary=binary,scope=scope,arm='typed',N=n,campaign=0,repeat=repeat,
                                     total_ns=ms*1000000,discovery_client_ns=1000000,selected_value_client_ns=1000000))
        result=run.summary(rows)
        self.assertEqual(len(result['cells']),4)
        self.assertEqual(len(result['comparisons']),2)
        self.assertTrue(all(c['warm_samples']==2 for c in result['cells']))
        self.assertTrue(all(c['candidate_over_baseline']==.2 for c in result['comparisons']))


if __name__=='__main__':unittest.main()
