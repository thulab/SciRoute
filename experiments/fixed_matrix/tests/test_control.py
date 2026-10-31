"""Failure seams for historical version fencing and actual resource admission."""
from copy import deepcopy
import unittest
import os
from unittest import mock
import json
from pathlib import Path
import tempfile
from experiments.fixed_matrix import data
from experiments.identity.model import build_corpus
from experiments.identity.registry import Registry
from experiments.fixed_matrix.run import bind_cases
from experiments.fixed_matrix.run import version_forms,validate_sample,worker_environment

class ControlTests(unittest.TestCase):
    def test_client_does_not_inherit_product_arrow_library_overrides(self):
        with mock.patch.dict(os.environ,dict(LD_LIBRARY_PATH='/product/arrow',LD_PRELOAD='/product/preload',
                IOTDBFS_EXPERIMENT_EXCLUSIVE_TIMING='1'),clear=True):
            env=worker_environment()
            self.assertNotIn('LD_LIBRARY_PATH',env);self.assertNotIn('LD_PRELOAD',env)
            self.assertNotIn('IOTDBFS_EXPERIMENT_EXCLUSIVE_TIMING',env)
            self.assertEqual(env['OMP_NUM_THREADS'],'1')
            self.assertEqual(os.environ['LD_LIBRARY_PATH'],'/product/arrow')

    def test_generation_fences_use_all_paths_and_exact_published_bindings(self):
        entries=[dict(ordinal=v,binding=dict(version=v*100+n)) for v in range(1,9) for n in range(8)]
        group=dict(V_stored=8,selection='historical-first')
        model,native,physical,selected=version_forms(group,entries)
        self.assertEqual(selected,[1]);self.assertEqual(native,dict(kind='ordinal',ordinals=[1]))
        self.assertEqual(physical,dict(kind='between',lower_inclusive=100,upper_exclusive=108))
        group['selection']='last-two'
        self.assertEqual(version_forms(group,entries)[2],dict(kind='between',lower_inclusive=700,upper_exclusive=808))
        bad=deepcopy(entries);bad[-1]['binding']['version']=105
        with self.assertRaisesRegex(RuntimeError,'fences overlap'):version_forms(dict(V_stored=8,selection='historical-first'),bad)

    def test_native_manifest_to_all_four_history_case_bindings(self):
        try:import h5py
        except ImportError:self.skipTest('native h5py required')
        catalog=json.loads((Path(__file__).parents[1]/'catalog.json').read_text())
        spec=next(s for s in data.specifications(catalog) if s['format']=='hdf5' and s['N']==8 and
                  s['V_stored']==8 and s['history_scenario']=='identical')
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);manifest=data.prepare(spec,root/'raw')
            inventory=[];published=[]
            for e in manifest['entries']:
                b=dict(path='/test/'+e['logical_file'],version=e['ordinal']*1000+e['file_id'])
                inventory.append(dict(logical_file=e['logical_file'],ordinal=e['ordinal'],bytes=e['bytes'],
                    sha256=e['sha256'],units=e['units'],binding=b))
                published.append(dict(logical_file=e['logical_file'],ordinal=e['ordinal'],bytes=e['bytes'],sha256=e['sha256'],**b))
            corpus=build_corpus(dict(bound=True,format='hdf5',N_total=8,generation=dict(specification=spec),inventory=inventory,
                source_evidence=dict(manifest_sha256='a'*64),publication_evidence=dict(namespace='functional-only',receipt_sha256='b'*64,bindings=published)))
            registry=Registry(root/'registry');corpus=registry.register_corpus(corpus)
            groups=[g for g in catalog['groups'] if g['group_id'] in spec['group_ids']]
            bindings=bind_cases(registry,corpus,manifest,groups)
            self.assertEqual(len(bindings),4)
            for group in groups:
                b=bindings[group['group_id']];self.assertEqual(len(b['expected_matches']),group['K_units'])
                self.assertEqual(len(b['scope']),8)
                self.assertEqual(registry.get_case(b['case_id'])['parameters']['C_versions'],group['C_versions'])

    def test_actual_resource_policy_and_oom_are_mandatory(self):
        def sample():
            groups={}
            for role in ('parent','query','database'):
                groups[role]=dict(recursive_pids=[],files={'cpuset.cpus.effective':'2-3',
                    'cpuset.cpus.partition':'root' if role=='parent' else 'member',
                    'cpuset.cpus.exclusive.effective':'2-3' if role=='parent' else '',
                    'cpu.max':'200000 100000' if role=='parent' else 'max 100000',
                    'memory.max':str(2*1024**3) if role=='query' else 'max',
                    'memory.events':'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0'})
            return dict(cgroups=groups,processes={})
        validate_sample(sample())
        for role,key,value in [('parent','cpuset.cpus.effective','4-5'),('parent','cpuset.cpus.partition','isolated'),
            ('parent','cpuset.cpus.exclusive.effective','2'),('query','cpuset.cpus.partition','root'),
            ('database','cpu.max','200000 100000'),('parent','cpu.max','max 100000'),
            ('query','memory.max','max'),('database','memory.events','oom 1\noom_kill 1')]:
            s=sample();s['cgroups'][role]['files'][key]=value
            with self.assertRaises(RuntimeError):validate_sample(s)

if __name__=='__main__':unittest.main()
