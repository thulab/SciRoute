"""Offline fixtures only: no SSH, database, mount, compilation, or cgroups."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from experiments.fixed_matrix import batch_parse as m
from experiments.fixed_matrix.tests.test_shared_candidates import resource, row as native_row

REPO=Path(__file__).resolve().parents[3]
TEST_CANDIDATE_SHA=hashlib.sha256(b'offline candidate identity').hexdigest()


def row(group):
    value=native_row(group,'A')
    value['discovery_call']=dict(server_profile=dict(counters={'registration_attempts':1}))
    value['value_calls'][0]['server_profile']['counters'].update(registration_attempts=3,typed_batch_items=2)
    return value


def science(tier):
    selected=m.groups(REPO,tier);group=selected[0];gid=group['group_id'];rows=[]
    bound=dict(corpus_id='corpus-'+gid,case_id='case-'+gid,oracle_sha256='oracle-'+gid,run_ids={})
    for campaign in m.CAMPAIGNS:
        for config in m.CONFIGS:
            bound['run_ids'].setdefault(str(campaign),{})[config]={a:gid+str(campaign)+config+a for a in m.ARMS}
            for arm in m.ARMS:
                for repeat in range(m.REPEATS):
                    value=row(group) if arm=='IoTDB-FS' else dict(oracle_verified=True,mode_verified=True,selected_units=group['K_units'])
                    d,q=(40,60) if arm=='FS-index' else ((200,200) if config=='A' else (100,200))
                    value.update(group_id=gid,corpus_id=bound['corpus_id'],case_id=bound['case_id'],oracle_sha256=bound['oracle_sha256'],
                        campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                        run_id=bound['run_ids'][str(campaign)][config][arm],phase='warmup' if repeat<5 else 'measurement',
                        ok=True,resource_id='current-resource',owned_output_bytes=group['owned_output_bytes'],
                        product_sha256=m.BASELINE_SHA if config=='A' else TEST_CANDIDATE_SHA,
                        discovery_ns=d,value_ns=q,total_ns=d+q,resource_before=resource(1),resource_after=resource(2))
                    rows.append(value)
    return selected,{gid:bound},rows


class BatchParseTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def analyze(self,tier,change=None,resource_id='current-resource',candidate_sha=TEST_CANDIDATE_SHA):
        groups,bound,rows=science(tier)
        if change:change(rows)
        path=self.root/'rows.jsonl';path.write_text(''.join(json.dumps(v)+'\n' for v in rows))
        return m.analyze(path,bound,groups,resource_id,candidate_sha)

    def test_exact_hdf_cases_four_campaigns_and_480_protocol(self):
        total=warm=measured=0
        self.assertEqual(m.PROTOCOL,'iotdbfs-fixed-batch-parse/1')
        self.assertEqual([m.configuration_order(c) for c in m.CAMPAIGNS],
            [('A','B'),('B','A'),('B','A'),('A','B')])
        for tier,n in (('n8',8),('n128',128)):
            group=m.groups(REPO,tier)[0]
            self.assertEqual((group['format'],group['N'],group['C_paths'],group['V_stored'],group['D'],group['K_units'],group['size_profile'],group['selection']),
                ('hdf5',n,n,1,4,2,'S0','latest'))
            self.assertEqual(m.expected_rows(SimpleNamespace(repo=REPO,tier=tier)),240)
            _,_,rows=science(tier);total+=len(rows)
            warm+=sum(r['phase']=='warmup' for r in rows);measured+=sum(r['phase']=='measurement' for r in rows)
        self.assertEqual((total,warm,measured),(480,160,320))

    def test_original_native_cache_batch_three_and_full_four_registration_attempts(self):
        group=m.groups(REPO,'n8')[0]
        value=row(group)
        d=value['discovery_call']['server_profile']['counters']['registration_attempts']
        q=value['value_calls'][0]['server_profile']['counters']['registration_attempts']
        self.assertEqual((d,q,d+q),(1,3,4))
        for config in m.CONFIGS:m.validate_row(row(group),group,config)
        changes=[lambda r:r.update(batch_enabled=False),
            lambda r:r['discovery_call']['server_profile']['counters'].update(registration_attempts=2),
            lambda r:r['value_calls'][0]['server_profile']['counters'].update(registration_attempts=1),
            lambda r:r['value_calls'][0]['server_profile']['counters'].update(registration_attempts=2),
            lambda r:r['value_calls'][0]['server_profile']['counters'].update(typed_batch_items=1)]
        for change in changes:
            value=row(group);change(value)
            with self.assertRaises(RuntimeError):m.validate_row(value,group,'B')
        self.assertNotIn('registration_source_refs_borrowed',row(group)['value_calls'][0]['server_profile']['counters'])

    def test_exact_four_paired_campaign_statistics(self):
        for tier in ('n8','n128'):
            result=self.analyze(tier);effect=result['effects'][0]
            self.assertEqual(result['query_rows'],240);self.assertEqual(len(effect['blocks']),4)
            self.assertAlmostEqual(effect['R_B_over_A'],.75)
            self.assertAlmostEqual(effect['raw_IoTDB_T_B_over_A'],.75)
            self.assertAlmostEqual(effect['Rd_B_over_A'],.5);self.assertAlmostEqual(effect['Rq_B_over_A'],1)

    def test_five_warmups_excluded_without_removing_slow_measured_campaign(self):
        def warm_spikes(rows):
            for r in rows:
                if r['repeat']<5:
                    r['discovery_ns']*=10000;r['value_ns']*=10000;r['total_ns']*=10000
        self.assertAlmostEqual(self.analyze('n8',warm_spikes)['effects'][0]['R_B_over_A'],.75)
        def slow_last(rows):
            for r in rows:
                if r['campaign']==3 and r['configuration']=='B' and r['arm']=='IoTDB-FS':
                    r['discovery_ns']*=100;r['value_ns']*=100;r['total_ns']*=100
        result=self.analyze('n8',slow_last)
        self.assertEqual(result['query_rows'],240);self.assertGreater(result['effects'][0]['blocks'][-1]['R_B_over_A'],1)

    def test_coverage_science_clock_resource_and_dynamic_candidate_are_mandatory(self):
        changes=[lambda r:r.pop(),lambda r:r.append(deepcopy(r[0])),lambda r:r[0].update(oracle_sha256='bad'),
            lambda r:r[0].update(product_sha256='wrong'),lambda r:r[0].update(total_ns=1),
            lambda r:r[0]['resource_after']['cgroups']['parent']['files'].update({'cpuset.cpus.partition':'isolated'}),
            lambda r:r[4].update(phase='measurement')]
        for change in changes:
            with self.assertRaises(RuntimeError):self.analyze('n128',change)
        with self.assertRaises(RuntimeError):self.analyze('n8',resource_id='unrelated')
        with self.assertRaises(RuntimeError):self.analyze('n8',candidate_sha='different-build')

    def test_n8_gate_requires_same_ab_and_all_controller_restoration(self):
        report=dict(protocol=m.PROTOCOL,tier='n8',query_records=240,product_sha256=TEST_CANDIDATE_SHA,baseline_sha256=m.BASELINE_SHA,
            **{k:True for k in ('complete','scientific_complete','ready_for_analysis','original_service_restored','resources_restored','controller_restoration_verified')})
        m.validate_n8_gate(report,m.BASELINE_SHA,TEST_CANDIDATE_SHA)
        for key,value in [('protocol','old-protocol'),('tier','n128'),('query_records',192),('product_sha256','other'),
                          ('baseline_sha256','other'),('ready_for_analysis',False),('resources_restored',False),('controller_restoration_verified',False)]:
            changed=dict(report);changed[key]=value
            with self.assertRaises(RuntimeError):m.validate_n8_gate(changed,m.BASELINE_SHA,TEST_CANDIDATE_SHA)

    def test_standard_build_receipt_dynamic_candidate_and_single_product_diff(self):
        xml=self.root/'tests.xml'
        root=ET.Element('testsuites',tests=str(len(m.REQUIRED_REGRESSION_TESTS)))
        suite=ET.SubElement(root,'testsuite',tests=str(len(m.REQUIRED_REGRESSION_TESTS)))
        for name in m.REQUIRED_REGRESSION_TESTS:
            ET.SubElement(suite,'testcase',classname='SocketFixture',name=name,status='run',result='completed')
        ET.ElementTree(root).write(xml)
        a=self.root/'A';a.write_bytes(b'A binary');b=self.root/'B';b.write_bytes(b'B current binary')
        sources={'src/query/frontend/QueryRegistrationSocket.cpp':'A source','src/formats/grib/GribAdapter.cpp':'unchanged'}
        receipts=[]
        for label,source in [('A',sources),('B',dict(sources,**{'src/query/frontend/QueryRegistrationSocket.cpp':'B source'}))]:
            p=self.root/(label+'.json');p.write_text(json.dumps(dict(source_files=source,focused_tests=str(xml),all_unit_tests=str(xml))))
            receipts.append(p)
        args=SimpleNamespace(baseline_binary=a,binary=b,baseline_build_binding=receipts[0],build_binding=receipts[1])
        real_sha=m.file_sha
        def sha(path):return m.BASELINE_SHA if Path(path)==a else real_sha(path)
        with (patch.object(m,'file_sha',side_effect=sha),
              patch.object(m,'verify_build_binding',return_value={'complete':True}) as gate,
              patch.object(m,'compare_build_environment',return_value={'identical':True})):
            proof=m.verify_candidate(args);self.assertEqual(proof['candidate_sha256'],real_sha(b));self.assertEqual(gate.call_count,2)
            valid_xml=xml.read_text();tree=ET.parse(xml);tree.getroot().append(ET.Element('skipped'));tree.write(xml)
            with self.assertRaisesRegex(RuntimeError,'skipped or incomplete'):m.verify_candidate(args)
            xml.write_text(valid_xml)
            data=json.loads(receipts[1].read_text());data['source_files']['src/formats/grib/GribAdapter.cpp']='render C';receipts[1].write_text(json.dumps(data))
            with self.assertRaisesRegex(RuntimeError,'outside approved'):m.verify_candidate(args)

    def test_candidate_requires_four_unique_completed_safety_tests_in_both_xml(self):
        a=self.root/'A';a.write_bytes(b'A binary');b=self.root/'B';b.write_bytes(b'B current binary')
        sources={'src/query/frontend/QueryRegistrationSocket.cpp':'A source'}
        receipt_a=self.root/'A.json';receipt_b=self.root/'B.json'
        baseline_xml=self.root/'baseline.xml'
        baseline_xml.write_text('<testsuites tests="1"><testsuite tests="1"><testcase classname="Old" name="Baseline" status="run" result="completed"/></testsuite></testsuites>')
        paths={key:self.root/(key+'.xml') for key in ('focused_tests','all_unit_tests')}
        def valid_xml():
            root=ET.Element('testsuites',tests='4');suite=ET.SubElement(root,'testsuite',tests='4')
            for name in m.REQUIRED_REGRESSION_TESTS:
                ET.SubElement(suite,'testcase',classname='SocketFixture',name=name,status='run',result='completed')
            return root
        for path in paths.values():ET.ElementTree(valid_xml()).write(path)
        receipt_a.write_text(json.dumps(dict(source_files=sources,focused_tests=str(baseline_xml),all_unit_tests=str(baseline_xml))))
        receipt_b.write_text(json.dumps(dict(source_files=dict(sources,**{'src/query/frontend/QueryRegistrationSocket.cpp':'B source'}),
                                            **{key:str(path) for key,path in paths.items()})))
        args=SimpleNamespace(baseline_binary=a,binary=b,baseline_build_binding=receipt_a,build_binding=receipt_b)
        real_sha=m.file_sha
        def sha(path):return m.BASELINE_SHA if Path(path)==a else real_sha(path)
        with (patch.object(m,'file_sha',side_effect=sha),
              patch.object(m,'verify_build_binding',return_value={'complete':True}),
              patch.object(m,'compare_build_environment',return_value={'identical':True})):
            m.verify_candidate(args)
            for key,path in paths.items():
                for name in m.REQUIRED_REGRESSION_TESTS:
                    for change in ('missing','duplicate','wrong_suite','not_run','not_completed'):
                        root=valid_xml();suite=root.find('testsuite')
                        case=next(n for n in suite if n.get('name')==name)
                        if change=='missing':suite.remove(case)
                        elif change=='duplicate':suite.append(deepcopy(case))
                        elif change=='wrong_suite':case.set('classname','OtherFixture')
                        elif change=='not_run':case.set('status','notrun')
                        else:case.set('result','suppressed')
                        root.set('tests',str(len(suite)));suite.set('tests',str(len(suite)))
                        ET.ElementTree(root).write(path)
                        with self.subTest(xml=key,name=name,change=change):
                            with self.assertRaises(RuntimeError):m.verify_candidate(args)
                        ET.ElementTree(valid_xml()).write(path)

    def test_failed_json_proof_cannot_start_resource_or_bootstrap(self):
        with patch.object(m,'CgroupV2Resources') as resource_manager,patch.object(m.run,'bootstrap') as bootstrap:
            with self.assertRaises(RuntimeError):m.supervise(SimpleNamespace(),{'invalid':('tuple',)})
            resource_manager.assert_not_called();bootstrap.assert_not_called()

    def test_actual_launcher_enables_native_cache_without_render_or_new_borrow_switch(self):
        binary=self.root/'binary';binary.write_bytes(b'product');native=self.root/'native';native.mkdir()
        plan=dict(group=m.groups(REPO,'n8')[0],native_root=native)
        r=m.launcher(self.root/'launcher',binary,m.file_sha(binary),'test_namespace',self.root/'owner',plan)
        self.assertEqual(r['settings']['query.hdf5.mode'],'native-hdf5')
        self.assertEqual(r['settings']['query.hdf5.native_cache'],'true')
        self.assertNotIn('query.grib.geometry_cache.enabled',r['settings'])


if __name__=='__main__':unittest.main()
