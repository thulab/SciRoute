from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest

from experiments.fixed_matrix.analysis import summarize


def sha(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def fixture():
    catalog = json.loads((Path(__file__).parents[1] / 'catalog.json').read_text())
    groups = [g for g in catalog['groups'] if g['collection_tier'] == 'primary']
    bindings, preparation, rows = {}, {}, []
    resource_id = sha('resource')
    before = {'monotonic_ns': 1000, 'cgroups': {role: {
        'path': '/sys/fs/cgroup/test/' + role,
        'files': {'cpu.stat': 'usage_usec 123\nnr_throttled 0',
                  'memory.current': '123456'}} for role in ('parent', 'query', 'database')}}
    after = deepcopy(before); after['monotonic_ns'] = 2000
    for g in groups:
        gid = g['group_id']
        binding = {'corpus_id': sha(gid + '-corpus'), 'case_id': sha(gid),
                   'oracle_sha256': sha(gid + '-oracle'), 'run_ids': {}}
        bindings[gid] = binding
        preparation[binding['corpus_id']] = {'artifact': gid + '/prepare.json',
                                             'sha256': sha(gid + '-prepare')}
        for campaign in range(3):
            binding['run_ids'][str(campaign)] = {}
            for arm in ('FS-scan', 'FS-index', 'IoTDB-FS'):
                run_id = sha((gid, campaign, arm))
                binding['run_ids'][str(campaign)][arm] = run_id
                for repeat in range(12):
                    factor = {'FS-scan': 10, 'FS-index': 1, 'IoTDB-FS': 2}[arm]
                    d = (10 + repeat) * factor
                    q = (30 + repeat) * factor
                    rows.append({'group_id': gid, **{k: binding[k] for k in
                        ('corpus_id', 'case_id', 'oracle_sha256')},
                        'campaign': campaign, 'arm': arm, 'repeat': repeat,
                        'phase': 'warmup' if repeat < 2 else 'measurement',
                        'run_id': run_id, 'resource_id': resource_id,
                        'ok': True, 'oracle_verified': True, 'mode_verified': True,
                        'execution_mode': 'native-hdf5' if g['format'] == 'hdf5' else
                            'iotdb-indexed' if arm == 'IoTDB-FS' else 'native-grib',
                        'total_ns': d + q, 'discovery_ns': d, 'value_ns': q,
                        'resource_before': deepcopy(before), 'resource_after': deepcopy(after)})
    return rows, catalog, bindings, {'resource_ids': {c: resource_id for c in range(3)},
        'preparation_refs': preparation,
        'completion': {'complete': True, 'original_service_restored': True,
                       'resources_restored': True}}


class FixedAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = fixture()

    def test_complete_ratios_are_campaign_median_quotients(self):
        rows, catalog, bindings, kwargs = deepcopy(self.base)
        report = summarize(rows, catalog, bindings, **kwargs)
        self.assertEqual(report['query_records'], 3456)
        self.assertEqual(report['warmup_records'], 576)
        self.assertEqual(report['measured_records'], 2880)
        case = report['cases'][0]
        self.assertEqual(case['ratios']['FS-index']['R']['campaign_values'], [2, 2, 2])
        self.assertEqual(case['ratios']['FS-index']['Rd']['median'], 2)
        self.assertEqual(case['ratios']['FS-index']['Rq']['median'], 2)
        self.assertEqual(case['ratios']['FS-scan']['R']['median'], .2)
        self.assertIsNone(case['x']); self.assertIsNone(case['y'])
        self.assertEqual(len(report['format_minima']['grib']['group_ids']), 16)
        self.assertEqual(report['cases'][0]['campaigns'][0]['arms']['FS-index']['measured']['total_ns']['count'], 10)

    def reject(self, mutate, expected):
        rows, catalog, bindings, kwargs = deepcopy(self.base)
        mutate(rows, catalog, bindings, kwargs)
        with self.assertRaisesRegex(ValueError, expected):
            summarize(rows, catalog, bindings, **kwargs)

    def test_missing_and_duplicate_are_rejected(self):
        self.reject(lambda r,c,b,k:r.pop(), '3,456')
        self.reject(lambda r,c,b,k:r.__setitem__(1,deepcopy(r[0])), 'duplicate')

    def test_identity_run_and_oracle_mismatch_rejected(self):
        for field in ('corpus_id', 'case_id', 'oracle_sha256'):
            self.reject(lambda r,c,b,k,f=field:r[0].__setitem__(f,sha('wrong')), field)
        self.reject(lambda r,c,b,k:r[0].__setitem__('run_id',sha('wrong')), 'run ID')

    def test_modes_flags_and_clocks_fail_closed(self):
        self.reject(lambda r,c,b,k:r[0].__setitem__('execution_mode','native-hdf5'), 'mode')
        for flag in ('ok', 'oracle_verified', 'mode_verified'):
            self.reject(lambda r,c,b,k,f=flag:r[0].__setitem__(f,False), 'failed/unverified')
        self.reject(lambda r,c,b,k:r[0].__setitem__('total_ns',999), 'T = d')
        self.reject(lambda r,c,b,k:r[0].__setitem__('discovery_ns',True), 'positive integer')
        self.reject(lambda r,c,b,k:r[0].__setitem__('phase','measurement'), 'phase')

    def test_resource_receipts_and_restoration_required(self):
        self.reject(lambda r,c,b,k:r[0].__setitem__('resource_before',{}), 'timestamp')
        self.reject(lambda r,c,b,k:r[0]['resource_after'].__setitem__('monotonic_ns',1), 'increase')
        self.reject(lambda r,c,b,k:r[0]['resource_after']['cgroups']['query']['files'].__setitem__('cpu.stat',{'unavailable':'missing'}), 'CPU/memory')
        self.reject(lambda r,c,b,k:k['completion'].__setitem__('resources_restored',False), 'restoration')
        self.reject(lambda r,c,b,k:k['preparation_refs'].clear(), 'preparation receipt')
        self.reject(lambda r,c,b,k:r[0].__setitem__('resource_id',sha('wrong')), 'resource identity')

    def test_cannot_silently_analyze_extensions_or_select_partial_best(self):
        self.reject(lambda r,c,b,k:c['groups'][0].__setitem__('collection_tier','extension'), '32 groups')

    def test_no_input_mutation_and_allowed_set_supported(self):
        rows, catalog, bindings, kwargs = deepcopy(self.base)
        original = json.dumps([rows, catalog, bindings, kwargs], sort_keys=True)
        kwargs2 = dict(kwargs, resource_ids=set(kwargs['resource_ids'].values()))
        summarize(rows, catalog, bindings, **kwargs2)
        self.assertEqual(original, json.dumps([rows, catalog, bindings, kwargs], sort_keys=True))


if __name__ == '__main__':
    unittest.main()
