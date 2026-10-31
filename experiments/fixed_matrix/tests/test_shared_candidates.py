"""Offline seams only; never mount, contact a database, or create cgroups."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from experiments.fixed_matrix import shared_candidates as m

REPO=Path(__file__).resolve().parents[3]


def row(group,config):
    common=dict(selected_units=group['K_units'],oracle_verified=True,mode_verified=True)
    if group['format']=='grib':
        common.update(execution_mode='iotdb-indexed',discovery_protocol='format_catalog',
            discovery_call=dict(ipc_bytes=0,response_path=None,server_profile=dict(operation='format_catalog',
                counters={'registration_source_refs_borrowed':group['C_paths']} if config=='B' else {})),
            value_calls=[dict(server_profile=dict(counters={'registration_source_refs_borrowed':1} if config=='B' else {}))
                for _ in range(group['K_units'])])
    else:
        c={'hdf5.native.slabs':2,'hdf5.native.session.cache.enabled':1,'hdf5.native.filesystem.calls':1,
           'hdf5.native.source.calls':1,'hdf5.native.filesystem.bytes':10,'hdf5.native.source.bytes':10}
        if config=='B':c.update(registration_source_refs_borrowed=group['K_units'],typed_batch_items=group['K_units'])
        common.update(execution_mode='native-hdf5',batch_enabled=True,value_calls=[dict(batch_items=group['K_units'],
            server_profile=dict(operation='typed_batch',counters=c,iotdb_delta={'payload_calls':0},
                server_hdf5_execution=dict(mode='native-hdf5',payload_backend='filesystem',native_cache_enabled=True)))])
    return common


def resource(t):
    return dict(monotonic_ns=t,processes={},cgroups={name:dict(recursive_pids=[],files={
        'cpuset.cpus.effective':'2-3','cpuset.cpus.partition':'root' if name=='parent' else 'member',
        'cpuset.cpus.exclusive.effective':'2-3' if name=='parent' else '',
        'cpu.max':'200000 100000' if name=='parent' else 'max 100000',
        'memory.max':str(2*1024**3) if name=='query' else 'max','memory.events':'oom 0\noom_kill 0'})
        for name in ('parent','query','database')})


def science(tier):
    selected=m.groups(REPO,tier);bindings={};rows=[]
    for group in selected:
        gid=group['group_id'];b=dict(corpus_id='corpus-'+gid,case_id='case-'+gid,oracle_sha256='oracle-'+gid,run_ids={});bindings[gid]=b
        for campaign in m.campaigns(group):
            for config in m.CONFIGS:
                b['run_ids'].setdefault(str(campaign),{})[config]={a:gid+str(campaign)+config+a for a in m.ARMS}
                for arm in m.ARMS:
                    for repeat in range(12):
                        r=row(group,config) if arm=='IoTDB-FS' else dict(oracle_verified=True,mode_verified=True,selected_units=group['K_units'])
                        d,q=(40,60) if arm=='FS-index' else ((200,200) if config=='A' else (100,200))
                        r.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                            campaign=campaign,configuration=config,arm=arm,repeat=repeat,run_id=b['run_ids'][str(campaign)][config][arm],
                            phase='warmup' if repeat<2 else 'measurement',ok=True,resource_id='current-resource',
                            owned_output_bytes=group['owned_output_bytes'],product_sha256=m.BASELINE_SHA if config=='A' else m.CANDIDATE_SHA,
                            discovery_ns=d,value_ns=q,total_ns=d+q,resource_before=resource(1),resource_after=resource(2))
                        rows.append(r)
    return selected,bindings,rows


class SharedCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def test_fixed_four_groups_and_480_query_schedule(self):
        self.assertEqual(m.expected_rows(SimpleNamespace(repo=REPO,tier='n8')),192)
        self.assertEqual(m.expected_rows(SimpleNamespace(repo=REPO,tier='n128')),288)
        self.assertEqual([m.configuration_order(c) for c in range(3)],[('A','B'),('B','A'),('A','B')])
        self.assertEqual([len(m.campaigns(g)) for g in m.groups(REPO,'n8')],[1,3])
        for tier in ('n8','n128'):
            for g in m.groups(REPO,tier):self.assertEqual((g['V_stored'],g['D'],g['selection']),(1,4,'latest'))

    def test_both_format_mode_and_borrow_mechanisms(self):
        for g in m.groups(REPO,'n8'):
            for c in m.CONFIGS:m.validate_row(row(g,c),g,c)
            broken=row(g,'B');target=broken['value_calls'][0]['server_profile']['counters']
            target['registration_authorization_membership_builds']=1
            with self.assertRaises(RuntimeError):m.validate_row(broken,g,'B')
            broken=row(g,'B');broken['value_calls'][0]['server_profile']['counters'].pop('registration_source_refs_borrowed')
            with self.assertRaises(RuntimeError):m.validate_row(broken,g,'B')
        g=m.groups(REPO,'n8')[1];broken=row(g,'B');broken['batch_enabled']=False
        with self.assertRaises(RuntimeError):m.validate_row(broken,g,'B')

    def test_full_coverage_480_and_paired_ratio_recomputation(self):
        count=0
        for tier in ('n8','n128'):
            selected,b,rows=science(tier);count+=len(rows);p=self.root/(tier+'.jsonl')
            p.write_text(''.join(json.dumps(v)+'\n' for v in rows));result=m.analyze(p,b,selected,'current-resource')
            self.assertEqual(result['query_rows'],len(rows))
            for e in result['effects']:
                self.assertAlmostEqual(e['R_B_over_A'],.75);self.assertAlmostEqual(e['raw_IoTDB_T_B_over_A'],.75)
                self.assertAlmostEqual(e['Rd_B_over_A'],.5);self.assertAlmostEqual(e['Rq_B_over_A'],1.)
        self.assertEqual(count,480)

    def test_coverage_identity_resources_and_slow_campaign_not_silently_dropped(self):
        selected,b,rows=science('n128');p=self.root/'rows.jsonl'
        changes=[lambda x:x.pop(),lambda x:x.append(deepcopy(x[0])),
            lambda x:x[0].update(oracle_sha256='wrong'),lambda x:x[0].update(resource_id='wrong'),
            lambda x:x[0].update(total_ns=1),lambda x:x[0]['resource_before']['cgroups']['parent']['files'].update({'cpuset.cpus.partition':'isolated'})]
        for change in changes:
            copy=deepcopy(rows);change(copy);p.write_text(''.join(json.dumps(v)+'\n' for v in copy))
            with self.assertRaises(RuntimeError):m.analyze(p,b,selected,'current-resource')
        p.write_text(''.join(json.dumps(v)+'\n' for v in rows))
        with self.assertRaisesRegex(RuntimeError,'resource'):m.analyze(p,b,selected,'some-other-run')
        for v in rows:
            if v['campaign']==2 and v['configuration']=='B' and v['arm']=='IoTDB-FS':
                v['discovery_ns']*=100;v['value_ns']*=100;v['total_ns']*=100
        p.write_text(''.join(json.dumps(v)+'\n' for v in rows));result=m.analyze(p,b,selected,'current-resource')
        self.assertEqual(result['query_rows'],288)
        self.assertTrue(all(e['blocks'][-1]['R_B_over_A']>1 for e in result['effects']))

    def test_actual_1833_unit_gate_and_eight_new_tests(self):
        path=REPO/'experiments/cross_version/work/shared-candidate-reuse-20261022/all-unit-tests.xml'
        m.validate_unit_gate(path)
        tree=ET.parse(path);tree.getroot().append(ET.Element('skipped'));bad=self.root/'skipped.xml';tree.write(bad)
        with self.assertRaisesRegex(RuntimeError,'skips'):m.validate_unit_gate(bad)
        tree=ET.parse(path);test=next(n for n in tree.getroot().iter('testcase') if (n.get('classname'),n.get('name')) in m.NEW_TESTS)
        test.set('name','missing-new-test');bad=self.root/'missing.xml';tree.write(bad)
        with self.assertRaisesRegex(RuntimeError,'missing or duplicated'):m.validate_unit_gate(bad)

    def test_build_cache_compares_actual_keys_and_explicit_optional_absence(self):
        compiler=self.root/'c++';compiler.write_bytes(b'test compiler');binaries=[]
        for label in ('A','B'):
            build=self.root/label/'build';(build/'bin').mkdir(parents=True);binaries.append(build/'bin/iotdb_fs')
            (build/'CMakeCache.txt').write_text('CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_CXX_COMPILER:FILEPATH='+str(compiler)+'\nArrow_DIR:PATH=/same/arrow\nHDF5_DIR:PATH=/same/hdf5\n')
        env=m.compare_build_environment(*binaries);self.assertIsNone(env['A']['configuration']['CMAKE_PREFIX_PATH'])
        cache=binaries[1].parents[1]/'CMakeCache.txt';cache.write_text(cache.read_text()+'CMAKE_CXX_FLAGS:STRING=-O0\n')
        with self.assertRaisesRegex(RuntimeError,'configuration differs'):m.compare_build_environment(*binaries)

    def test_native_binding_checks_managed_bytes_and_immutable_receipt(self):
        current=self.root/'current';current.mkdir();native=self.root/'native';native.mkdir();journal=self.root/'owner';journal.mkdir()
        (journal/'state').write_text('sealed');(journal/'owner.lock').write_text('')
        data=b'native source';sha=hashlib.sha256(data).hexdigest();blob=native/(sha+'.blob');blob.write_bytes(data)
        jd=m.run.infrastructure.journal_snapshot(journal);head={'seq':1};receipt=self.root/'location.json'
        receipt.write_text(json.dumps(dict(complete=True,scientific_identities_unchanged=True,head_after=head,active_journal_digest=jd)))
        item=dict(corpus_id='cid',namespace='namespace',active_owner_journal=str(journal),native_file_root=str(native),
            file_binding_receipt=str(receipt),file_binding_receipt_sha256=m.file_sha(receipt),head=head,journal_digest=jd)
        (current/'report.json').write_text(json.dumps(dict(complete=True,original_service_restored=True,resources_restored=True,corpora=[item])))
        (current/'current-locations.json').write_text(json.dumps(dict(corpora=[dict(corpus_id='cid',namespace='namespace',active_owner_journal=str(journal),native_file_root=str(native),location_receipt=str(receipt))])))
        args=SimpleNamespace(current_collection=current);manifest=dict(entries=[dict(sha256=sha,bytes=len(data))])
        binding=m.native_binding(args,'cid',manifest);self.assertEqual(binding['head'],head)
        blob.write_bytes(b'corruption')
        with self.assertRaisesRegex(RuntimeError,'native HDF5 bytes'):m.native_binding(args,'cid',manifest)

    def test_hdf_launcher_explicit_cache_and_native_mode_without_render_switch(self):
        binary=self.root/'product';binary.write_text('test');native=self.root/'native';native.mkdir()
        for g in m.groups(REPO,'n8'):
            path=self.root/g['format'];p=dict(group=g,native_root=native)
            receipt=m.launcher(path,binary,m.file_sha(binary),'test_namespace',self.root/'owner',p)
            self.assertNotIn('query.grib.geometry_cache.enabled',receipt['settings'])
            if g['format']=='hdf5':
                self.assertEqual(receipt['settings']['query.hdf5.native_cache'],'true')
                self.assertEqual(receipt['settings']['query.hdf5.mode'],'native-hdf5')

    def test_failed_json_proof_cannot_start_resources(self):
        with patch.object(m,'CgroupV2Resources') as resource,patch.object(m.run,'bootstrap') as boot:
            with self.assertRaises(RuntimeError):m.supervise(SimpleNamespace(),{'bad':('tuple',)})
            resource.assert_not_called();boot.assert_not_called()


if __name__=='__main__':unittest.main()
