from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import grib_catalog as subject
from experiments.fixed_matrix import grib_catalog_client as consumer


class CatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo=Path(__file__).resolve().parents[3]
        cls.group=subject.groups(cls.repo)[0]

    def locator(self):
        return dict(path='/sealed/file.grib',version=123,format_id='grib',revision=1,
                    unit_id='message:8:12',offset=8,length=12)

    def fixture(self,campaigns=(0,1,2)):
        g=self.group;gid=g['group_id'];binding=dict(corpus_id='c',case_id='case',oracle_sha256='oracle',run_ids={});rows=[]
        for campaign in campaigns:
            binding['run_ids'][str(campaign)]={}
            for config in subject.CONFIGS:
                binding['run_ids'][str(campaign)][config]={a:config+a+str(campaign) for a in subject.ARMS}
                for arm in subject.ARMS:
                    for repeat in range(12):
                        row=dict(group_id=gid,corpus_id='c',case_id='case',oracle_sha256='oracle',campaign=campaign,configuration=config,
                            arm=arm,repeat=repeat,phase='warmup' if repeat<2 else 'measurement',ok=True,oracle_verified=True,
                            mode_verified=True,selected_units=g['K_units'],owned_output_bytes=g['owned_output_bytes'],
                            run_id=binding['run_ids'][str(campaign)][config][arm],total_ns=3,discovery_ns=1,value_ns=2,
                            resource_id='resource',product_sha256=subject.BASELINE_SHA if config=='A' else 'f'*64,
                            resource_before=dict(monotonic_ns=1),resource_after=dict(monotonic_ns=2),execution_mode='iotdb-indexed' if arm=='IoTDB-FS' else 'native-grib')
                        if arm=='IoTDB-FS':
                            row.update(discovery_protocol='format_catalog' if config=='C' else 'typed',
                                discovery_call=dict(ipc_bytes=0 if config=='C' else 99,response_path=None if config=='C' else '/owned/route',
                                    server_profile=dict(operation='format_catalog' if config=='C' else 'typed')),
                                value_calls=[{} for _ in range(g['K_units'])])
                        rows.append(row)
        return rows,{gid:binding}

    def analyze(self,rows,bindings):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'queries.jsonl';path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            with patch.object(subject.run,'validate_sample'):
                return subject.analyze(path,bindings,[self.group],(0,1,2))

    def test_approved_matrix_counts_and_case_identity(self):
        self.assertEqual(subject.groups(self.repo,True)[0]['N'],8)
        self.assertEqual(subject.groups(self.repo)[0]['N'],128)
        self.assertTrue(all(g['format']=='grib' and g['C_paths']==g['N'] and g['V_stored']==1 and g['K_units']==2
            for g in subject.groups(self.repo)+subject.groups(self.repo,True)))
        self.assertEqual(subject.expected_rows(SimpleNamespace(smoke=True)),72)
        self.assertEqual(subject.expected_rows(SimpleNamespace(smoke=False)),216)

    def test_balanced_preregistered_order(self):
        orders=[subject.configuration_order(c) for c in range(3)]
        self.assertEqual(orders,[('A','B','C'),('B','C','A'),('C','A','B')])
        for config in subject.CONFIGS:self.assertEqual({o.index(config) for o in orders},{0,1,2})

    def test_complete_paired_analysis(self):
        rows,b=self.fixture();out=self.analyze(rows,b)
        self.assertEqual(out['query_rows'],216);self.assertEqual(len(out['cells']),3);self.assertEqual(len(out['effects']),3)
        self.assertTrue(all(c['R']==c['Rd']==c['Rq']==1 for c in out['cells']))
        self.assertEqual([(e['numerator'],e['denominator']) for e in out['effects']],[('B','A'),('C','B'),('C','A')])

    def test_slow_campaign_never_excluded(self):
        rows,b=self.fixture()
        for row in rows:
            if row['campaign']==1 and row['configuration']=='C' and row['arm']=='IoTDB-FS':
                for k in ('total_ns','discovery_ns','value_ns'):row[k]*=100
        out=self.analyze(rows,b)
        cell=next(c for c in out['cells'] if c['configuration']=='C')
        self.assertAlmostEqual(cell['R_range'][0],1);self.assertAlmostEqual(cell['R_range'][1],100);self.assertEqual(cell['R'],1)
        self.assertEqual(len(cell['blocks']),3)

    def test_missing_duplicate_bad_clock_identity_and_phase_rejected(self):
        for kind in ('missing','duplicate','clock','case','phase','resource','output','resource_id','binary'):
            with self.subTest(kind=kind):
                rows,b=self.fixture()
                if kind=='missing':rows.pop()
                elif kind=='duplicate':rows[-1]=deepcopy(rows[0])
                elif kind=='clock':rows[-1]['total_ns']=4
                elif kind=='case':rows[-1]['case_id']='other'
                elif kind=='phase':rows[-1]['phase']='warmup'
                elif kind=='resource':rows[-1]['resource_after']['monotonic_ns']=0
                elif kind=='output':rows[-1]['owned_output_bytes']=1
                elif kind=='resource_id':rows[-1]['resource_id']='other'
                elif kind=='binary':rows[-1]['product_sha256']='g'*64
                with self.assertRaises(RuntimeError):self.analyze(rows,b)

    def test_full_source_proof_is_json_roundtrip_stable(self):
        source=Path('/virtual/collection-r2');repo=Path('/virtual/repo');base=source/'corpora'/'independent'
        expected=[{'unit':'message:8:12'}];oracle=subject.digest(expected)
        fixed=self.group;catalog={'groups':[fixed,subject.groups(self.repo,True)[0]]}
        for smoke in (True,False):
            with self.subTest(smoke=smoke):
                gid=subject.GROUPS[0 if smoke else 1]
                args=SimpleNamespace(source_collection=source,repo=repo,library=Path('/virtual/native.so'),
                    inspector=Path('/virtual/inspector'),baseline_binary=Path('/virtual/old'),binary=Path('/virtual/new'),
                    build_binding=Path('/virtual/build.json'),smoke=smoke,smoke_report=None if smoke else Path('/virtual/smoke/report.json'))
                corpus={'corpus_id':'corpus','source_evidence':{'manifest_sha256':'m'*64},'publication_evidence':{'receipt_sha256':'p'*64}}
                case={'case_id':'case','corpus_id':'corpus','expected_matches':expected}
                binding={'corpus_id':'corpus','case_id':'case','expected_matches':expected,'oracle_sha256':oracle}
                journal={'state':'s'*64,'owner.lock':'o'*64}
                item={'corpus_id':'corpus','corpus_key':'independent','index_sha256':'i'*64,'raw_manifest_sha256':'m'*64,'head':{'epoch':1},'journal_digest':journal}
                pinned={'experiments/fixed_matrix/client.py':'a'*64,'experiments/fixed_matrix/native.py':'b'*64,
                        'experiments/fixed_matrix/catalog.json':subject.run.CATALOG_SHA}
                documents={source/'prepared.json':{'native_library_sha256':subject.run.NATIVE_SHA,'sources':pinned},
                    source/'bindings.json':{gid:binding},source/'report.json':{'corpora':[item]},
                    repo/'experiments/fixed_matrix/catalog.json':catalog,
                    base/'preparation.json':{'complete':True,'index_sha256':'i'*64,'publication_receipt_sha256':'p'*64},
                    base/'input/manifest.json':{'entries':[]},base/'publication.json':{'head':{'epoch':1}}}
                hashes={source/name:sha for name,sha in subject.SOURCE_PINS.items()}
                hashes.update({args.library:subject.run.NATIVE_SHA,args.inspector:subject.run.infrastructure.INSPECTOR_SHA,
                    args.baseline_binary:subject.BASELINE_SHA,args.binary:'f'*64,base/'index.sqlite':'i'*64,
                    base/'input/manifest.json':'m'*64,base/'publication.json':'p'*64,base/'catalog.json':'c'*64,
                    base/'preparation.json':'e'*64})
                for name,sha in pinned.items():hashes[repo/name]=sha;hashes[source/'source'/name]=sha
                hashes[repo/'fs_index/native.py']=pinned['experiments/fixed_matrix/native.py']
                if not smoke:
                    documents[args.smoke_report]=dict(complete=True,smoke=True,query_records=72,
                        original_service_restored=True,controller_restoration_verified=True,resources_restored=True,
                        product_sha256='f'*64,baseline_sha256=subject.BASELINE_SHA)
                    hashes[args.smoke_report]='g'*64
                fake_registry=SimpleNamespace(get_corpus=lambda identity:corpus,get_case=lambda identity:case)
                gate=dict(complete=True,receipt_sha256='r'*64,source_root='/virtual/snapshot',source_files=7,
                    tests={'focused_tests':{'tests':2,'path':'/virtual/focused.xml','sha256':'t'*64}})
                raw=dict(complete=True,source_versions=8 if smoke else 128,hashes_sha256='h'*64)
                with patch.object(subject,'read_json',side_effect=lambda path:deepcopy(documents[Path(path)])), \
                     patch.object(subject,'file_sha',side_effect=lambda path:hashes[Path(path)]), \
                     patch.object(subject,'Registry',return_value=fake_registry), \
                     patch.object(subject,'verify_build_binding',return_value=gate), \
                     patch.object(subject.run.infrastructure,'journal_snapshot',return_value=journal), \
                     patch.object(subject.run,'verify_raw',return_value=raw):
                    proof=subject.source_proof(args)
                self.assertEqual(proof,json.loads(json.dumps(proof,allow_nan=False)))
                self.assertEqual(proof['expected_queries'],72 if smoke else 216)
                self.assertEqual(set(proof),{'protocol','complete','cases','source_collection','source_pins','configurations',
                    'configuration_meaning','campaigns','orders','arms','expected_queries','warmup_per_arm','measured_per_arm',
                    'build_gate','baseline','product','smoke','smoke_report_sha256','scientific_publication','index_rebuilt','new_cases','mutation'})
                subject.require_json_roundtrip(proof,'complete proof')

    def test_unstable_proof_fails_before_any_supervisor_work(self):
        bad={'cases':[{'nested':{'order':('A','B','C')}}]}
        with patch.object(subject.run,'bootstrap') as bootstrap,patch.object(subject,'CgroupV2Resources') as resources:
            with self.assertRaisesRegex(RuntimeError,'JSON round-trip stable'):
                subject.supervise(SimpleNamespace(),bad)
        bootstrap.assert_not_called();resources.assert_not_called()
        with self.assertRaises(RuntimeError):subject.require_json_roundtrip({1:'integer key'},'source closure')

    def test_catalog_cannot_route_through_typed_or_fuse(self):
        for alteration in ('protocol','profile','path','ipc'):
            rows,_=self.fixture();row=next(r for r in rows if r['configuration']=='C' and r['arm']=='IoTDB-FS')
            if alteration=='protocol':row['discovery_protocol']='typed'
            elif alteration=='profile':row['discovery_call']['server_profile']['operation']='typed'
            elif alteration=='path':row['discovery_call']['response_path']='/somewhere'
            elif alteration=='ipc':row['discovery_call']['ipc_bytes']=1
            with self.assertRaises(RuntimeError):subject.validate_row(row,'C',self.group['K_units'])

    def test_typed_client_is_the_unchanged_baseline(self):
        sentinel={'verified':'original'}
        with patch.object(consumer.client,'query_iotdb',return_value=sentinel) as original:
            result=consumer.query({},self.group,['/a'],{'kind':'latest'},[],lightweight=False)
        original.assert_called_once_with({},self.group,['/a'],{'kind':'latest'},[])
        self.assertIs(result,sentinel);self.assertEqual(result['discovery_protocol'],'typed')

    def test_explicit_catalog_request_keeps_exact_query_body(self):
        scope=['/a','/b'];versions=dict(kind='exact',exact=17)
        request=consumer.catalog_request(scope,versions)
        self.assertEqual(request['kind'],'format_catalog')
        self.assertEqual(request['request'],consumer.client.discover_request('grib',scope,versions)['request'])
        self.assertEqual(request['request']['subset']['projection'],['grib.message.offset'])

    def test_valid_locators_preserve_source_message_order(self):
        a=self.locator();b=dict(a,offset=40,unit_id='message:40:12')
        rows=consumer.catalog_rows(dict(kind='format_catalog',units=[b,a]))
        self.assertEqual([r['offset'] for r in rows],[40,8])
        self.assertEqual(rows[0]['revision'],1)
        zero=dict(a,revision=0);self.assertEqual(consumer.catalog_rows(dict(kind='format_catalog',units=[zero]))[0]['revision'],0)

    def test_invalid_catalog_inputs_fail_without_fallback(self):
        invalid=[]
        for key,value in [('version',True),('version',0),('revision','1'),('revision',-1),('format_id','hdf5'),
                          ('offset',-1),('length',0),('length',2**64),('unit_id','message:wrong'),('path','relative')]:
            unit=self.locator();unit[key]=value;invalid.append(dict(kind='format_catalog',units=[unit]))
        unit=self.locator();del unit['revision'];invalid.append(dict(kind='format_catalog',units=[unit]))
        invalid.extend([dict(kind='published_route',units=[]),dict(kind='format_catalog',units=None),
                        dict(kind='format_catalog',path='/route',units=[]),dict(kind='format_catalog',units=[self.locator(),self.locator()])])
        for response in invalid:
            with self.subTest(response=response):
                with self.assertRaises(RuntimeError):consumer.catalog_rows(response)

    def test_catalog_lengths_bound_to_full_native_oracle(self):
        row=self.locator();expected=[dict(binding=dict(path=row['path'],version=row['version']),unit='message:8:12')]
        consumer.verify_locators([row],expected)
        for changed in (dict(row,length=13),dict(row,version=124)):
            with self.assertRaises(RuntimeError):consumer.verify_locators([changed],expected)

    def test_lightweight_full_clock_and_value_calls_keep_baseline_contract(self):
        locator=self.locator();group=dict(format='grib',owned_output_bytes=10)
        expected=[dict(binding=dict(path=locator['path'],version=locator['version']),unit='message:8:12')]
        part=dict(meta={b'iotdbfs.sources':b'1',b'iotdbfs.source.0.path':locator['path'].encode(),
                        b'iotdbfs.source.0.version':b'123'},array=object(),mask=object())
        with patch.dict('sys.modules',{'numpy':SimpleNamespace()}), \
             patch.object(consumer.client,'control',return_value=(dict(kind='format_catalog',units=[locator]),7)) as control, \
             patch.object(consumer.client,'call',return_value=({},[part],dict(control_ns=5,ipc_bytes=100,response_path='/owned/value'))) as call, \
             patch.object(consumer.client,'profiles',return_value=[dict(operation='format_catalog'),dict(path='/owned/value')]) as profiles, \
             patch.object(consumer.client,'validate',return_value=10) as validate, \
             patch.object(consumer.time,'perf_counter_ns',side_effect=[10,20,35]):
            result=consumer.query({},group,[locator['path']],dict(kind='latest'),expected,lightweight=True)
        self.assertEqual((result['total_ns'],result['discovery_ns'],result['value_ns']),(25,10,15))
        control.assert_called_once_with({},consumer.catalog_request([locator['path']],dict(kind='latest')))
        call.assert_called_once_with({},consumer.client.value_request('grib',locator),True)
        profiles.assert_called_once_with({},2)
        self.assertEqual(result['discovery_call']['ipc_bytes'],0)
        self.assertEqual(result['catalog_units'],[locator]);self.assertEqual(len(result['value_calls']),1)
        validate.assert_called_once();self.assertTrue(result['oracle_verified'])

    def test_empty_locator_set_valid(self):
        self.assertEqual(consumer.catalog_rows(dict(kind='format_catalog',units=[])),[])
        consumer.verify_locators([],[])


if __name__=='__main__':unittest.main()
