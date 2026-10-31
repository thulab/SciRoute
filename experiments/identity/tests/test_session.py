"""Runtime identity seam tests using tiny JSON/byte fixtures, never a database."""
import ast
import copy
from collections import defaultdict
import json
from pathlib import Path
import statistics
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock

from experiments.identity.model import IdentityError, build_case, build_corpus, digest
from experiments.identity.session import (MeasurementSession, file_sha, measurement_session,
                                         output_contract, result_group_key, registered_result_group_key)
from experiments.identity.tests.test_model import imported_population, request_for, fingerprint


class RegistryDouble:
    def __init__(self):
        self.cases, self.corpora, self.runs = {}, {}, {}
    def get_case(self, key): return copy.deepcopy(self.cases[key])
    def get_corpus(self, key): return copy.deepcopy(self.corpora[key])
    def get_run(self, key): return copy.deepcopy(self.runs[key])
    def register_run(self, run): self.runs[run['run_id']] = copy.deepcopy(run)


def runner_function(name):
    # The real runner entry/aggregator can be checked without loading native libraries.
    path = Path(__file__).resolve().parents[2] / 'multi_file/small_runner.py'
    tree = ast.parse(path.read_text())
    definition = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace = dict(measurement_session=measurement_session, result_group_key=result_group_key, registered_result_group_key=registered_result_group_key,
        defaultdict=defaultdict, statistics=statistics,
        sf=SimpleNamespace(require=lambda ok, reason: None if ok else (_ for _ in ()).throw(ValueError(reason))))
    exec(compile(ast.Module(body=[definition], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name], namespace


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.registry = RegistryDouble()
        self.manifests, self.specs, self.versions, self.raw, self.fs = {}, {}, {}, {}, {}
        imported = {}
        for fmt in ('grib', 'hdf5'):
            value = imported_population(n=3, versions=1, format_id=fmt)
            spec = value['generation']; self.specs[fmt] = spec
            self.raw[fmt] = self.root / (fmt + '-data'); self.raw[fmt].mkdir()
            self.fs[fmt] = self.root / (fmt + '-fs'); self.fs[fmt].mkdir()
            sources = []
            for record in value['inventory']:
                content = ('raw-' + record['logical_file']).encode()
                for base in (self.raw[fmt], self.fs[fmt]):
                    path = base / record['logical_file']; path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(content)
                record['bytes'] = len(content); record['sha256'] = file_sha(self.raw[fmt] / record['logical_file'])
                sources.append(dict(file=record['logical_file'], bytes=record['bytes'],
                                    sha256=record['sha256'], units=copy.deepcopy(record['units'])))
            for binding, record in zip(value['publication_evidence']['bindings'], value['inventory']):
                binding.update(bytes=record['bytes'], sha256=record['sha256'])
            self.versions[fmt] = {r['binding']['path']: r['binding']['version'] for r in value['inventory']}
            manifest = dict(spec=spec, actual={'N': 3}, query={'kind': 'target'}, sources=sources)
            path = self.raw[fmt] / 'manifest.json'; path.write_text(json.dumps(manifest, sort_keys=True))
            value['source_evidence']['manifest_sha256'] = file_sha(path)
            self.manifests[fmt] = manifest; imported[fmt] = value
        self.preparation = self.root / 'preparation.json'
        self.preparation.write_text(json.dumps({'prepared': True, 'versions': self.versions}))
        self.bundle = {}
        for fmt, value in imported.items():
            value['publication_evidence']['receipt_sha256'] = file_sha(self.preparation)
            corpus = build_corpus(value); self.registry.corpora[corpus['corpus_id']] = corpus
            self.bundle[fmt] = {}
            for phase in ('discover', 'discover-values'):
                request = request_for(corpus); request.update(operation=phase, output=output_contract(phase))
                case = build_case(corpus, request)
                self.registry.cases[case['case_id']] = case; self.bundle[fmt][phase] = case['case_id']
        self.bundle_path = self.root / 'cases.json'; self.save_bundle()

    def save_bundle(self): self.bundle_path.write_text(json.dumps(self.bundle))
    def session(self): return MeasurementSession(self.root, self.bundle_path, registry=self.registry)
    def verify(self, session, fmt='grib', **changes):
        record = session.corpora[fmt]['inventory'][0]
        args = dict(spec=self.specs[fmt], manifest=self.manifests[fmt],
            manifest_path=self.raw[fmt]/'manifest.json', raw_root=self.raw[fmt], fs_root=self.fs[fmt],
            logical_prefix=record['binding']['path'][:-len(record['logical_file'])-1],
            versions=self.versions[fmt], preparation_sha256=file_sha(self.preparation))
        args.update(changes); session.verify_inputs(fmt, **args)

    def metadata(self, session, campaign=0, **changes):
        arguments = dict(binary_sha256=fingerprint('binary'), runner_sha256=fingerprint('runner'),
            engine='iotdb-indexed', clock={'protocol':'client-owned/1'}, cache={'warmup':1},
            environment={'host':'synthetic'}, campaigns=2, repeats=6)
        arguments.update(changes)
        return session.run_metadata('grib','discover-values','iotdb',campaign,**arguments)

    def test_missing_identity_and_reuse_fail_before_real_runner_connection_seam(self):
        run, globals_ = runner_function('run')
        globals_['connection_environment'] = Mock(side_effect=AssertionError('must not connect'))
        globals_['sf'].private_mount = Mock(side_effect=AssertionError('must not mount'))
        args = SimpleNamespace(prepare_only=False, identity_registry=None, identity_cases=None,
                               reuse_preparation=None,reuse_work=None)
        with self.assertRaisesRegex(IdentityError, 'identity-registry'): run(args)
        globals_['connection_environment'].assert_not_called(); globals_['sf'].private_mount.assert_not_called()
        args.identity_registry=self.root; args.identity_cases=self.bundle_path
        with self.assertRaisesRegex(IdentityError, 'reuse-preparation'):
            measurement_session(args, registry=self.registry)
        args.prepare_only=True
        self.assertIsNone(measurement_session(args))

    def test_valid_cold_verified_publication_creates_bound_run_and_same_campaign_reuses_id(self):
        session=self.session(); self.verify(session)
        with self.assertRaisesRegex(IdentityError,'live exact publication'): self.metadata(session)
        session.verify_publication('grib',self.versions['grib'])
        a=self.metadata(session); self.assertEqual(a,self.metadata(session))
        b=self.metadata(session,1); self.assertNotEqual(a['run_id'],b['run_id'])
        self.assertEqual(a['measurement_config_id'],b['measurement_config_id'])
        self.assertEqual(len(self.registry.runs),2)
        self.assertEqual(a['case_id'],self.bundle['grib']['discover-values'])

    def test_spec_manifest_prefix_receipt_or_publication_change_rejects(self):
        for change in ('spec','manifest','prefix','receipt','versions','extra-version'):
            with self.subTest(change=change):
                session=self.session(); kwargs={}
                if change=='spec': kwargs['spec']={**self.specs['grib'],'N':128}
                if change=='manifest': kwargs['manifest']={**self.manifests['grib'],'query':{'kind':'other'}}
                if change=='prefix': kwargs['logical_prefix']='/different'
                if change=='receipt': kwargs['preparation_sha256']=fingerprint('different')
                if change in ('versions','extra-version'):
                    versions=dict(self.versions['grib']); key=next(iter(versions))
                    if change=='versions': versions[key]+=1
                    else: versions['/extraneous/source']=1
                    kwargs['versions']=versions
                with self.assertRaises(IdentityError): self.verify(session,**kwargs)

    def test_fresh_hash_checks_both_raw_and_native_publication_copy(self):
        for root in (self.raw['grib'],self.fs['grib']):
            with self.subTest(root=root.name):
                path=root/self.manifests['grib']['sources'][0]['file']; before=path.read_bytes()
                path.write_bytes(b'x'*len(before))
                with self.assertRaisesRegex(IdentityError,'bytes differ'): self.verify(self.session())
                path.write_bytes(before)
        path=self.raw['grib']/'manifest.json'; path.write_text(path.read_text()+'\n')
        with self.assertRaisesRegex(IdentityError,'manifest bytes'): self.verify(self.session())

    def test_live_new_version_or_phantom_rejects_after_local_verification(self):
        session=self.session();self.verify(session)
        for key in (next(iter(self.versions['grib'])),'/new/path'):
            versions=dict(self.versions['grib']);versions[key]=999
            with self.assertRaisesRegex(IdentityError,'live publication'):session.verify_publication('grib',versions)

    def test_slot_case_output_operation_and_two_phase_corpus_must_match(self):
        self.bundle['grib']['discover']=self.bundle['grib']['discover-values'];self.save_bundle()
        with self.assertRaisesRegex(IdentityError,'operation'):self.session()

    def test_n512_b4_c128_cannot_enter_official_independent_n_runner(self):
        corpus=build_corpus(imported_population(n=512,versions=1))
        paths=sorted({r['logical_file'] for r in corpus['inventory']})[:128]
        scope=dict(kind='buckets',logical_files=paths,buckets=['bucket%04d'%i for i in range(4)],selection_basis='synthetic')
        self.registry.corpora[corpus['corpus_id']]=corpus
        for phase in ('discover','discover-values'):
            request=request_for(corpus,scope=scope);request.update(operation=phase,output=output_contract(phase))
            case=build_case(corpus,request);self.registry.cases[case['case_id']]=case;self.bundle['grib'][phase]=case['case_id']
        self.save_bundle()
        with self.assertRaisesRegex(IdentityError,'full independent N'):self.session()

    def test_summary_rejects_unbound_rows_and_splits_case_binary_and_protocol(self):
        summary,_=runner_function('summarize')
        with self.assertRaises(IdentityError):summary([dict(format='grib',phase='discover',arm='fs-index',repeat=1,total_ns=10)], self.registry)
        session=self.session();self.verify(session);session.verify_publication('grib',self.versions['grib'])
        binding=self.metadata(session)
        base=dict(format='grib',phase='discover-values',arm='iotdb',campaign=0,repeat=1,total_ns=10,iotdb_sql_queries=0,**binding)
        rows=[base]
        for field in ('case','binary','clock'):
            if field == 'case':
                old=self.registry.cases[self.bundle['grib']['discover-values']]
                corpus=self.registry.corpora[old['corpus_id']]
                request={k:copy.deepcopy(old['parameters'][k]) for k in
                    ('scope','selection','predicate','operation','roi','output','expected_K')}
                request['scope']['selection_basis']='another prospective case protocol'
                case=build_case(corpus,request);self.registry.cases[case['case_id']]=case
                self.bundle['grib']['discover-values']=case['case_id'];self.save_bundle()
            another=self.session();self.verify(another);another.verify_publication('grib',self.versions['grib'])
            changes={}
            if field=='binary':changes['binary_sha256']=fingerprint('another-binary')
            if field=='clock':changes['clock']={'protocol':'different'}
            rows.append(dict(base,**self.metadata(another,**changes)))
        self.assertEqual(len(summary(rows,self.registry)),4)
        for field in ('case_id','format','phase','campaign'):
            bad=copy.deepcopy(base);bad[field]={'case_id':fingerprint('unknown'),'format':'hdf5',
                'phase':'discover','campaign':1}[field]
            with self.subTest(field=field),self.assertRaises(IdentityError):summary([bad],self.registry)
        bad=copy.deepcopy(base);bad['implementation']['engine']='unbound'
        with self.assertRaises(IdentityError):result_group_key(bad)

    def test_real_registry_roundtrip_binds_run_and_rejects_unknown_result(self):
        from experiments.identity.registry import Registry
        registry=Registry(self.root/'real-registry')
        for corpus in self.registry.corpora.values():registry.register_corpus(corpus)
        for case in self.registry.cases.values():registry.register_case(case)
        session=MeasurementSession(registry.root,self.bundle_path)
        self.verify(session);session.verify_publication('grib',self.versions['grib'])
        metadata=self.metadata(session)
        run=registry.get_run(metadata['run_id'])
        self.assertEqual(run['case_id'],self.bundle['grib']['discover-values'])
        row=dict(format='grib',phase='discover-values',arm='iotdb',campaign=0,**metadata)
        self.assertEqual(registered_result_group_key(row,registry)[3],run['case_id'])
        row['run_id']=fingerprint('not registered')
        with self.assertRaisesRegex(IdentityError,'unknown run'):registered_result_group_key(row,registry)

    def test_missing_native_mask_evidence_fails_before_connection(self):
        # A historical value-only oracle must not become a value+mask case.
        from experiments.identity.model import seal
        case=copy.deepcopy(self.registry.cases[self.bundle['hdf5']['discover-values']])
        case.pop('case_id');case.pop('protocol')
        case['expected_matches'][0]['native_oracle'].pop('mask_sha256')
        case=seal('case',case);self.registry.cases[case['case_id']]=case
        self.bundle['hdf5']['discover-values']=case['case_id'];self.save_bundle()
        run, globals_=runner_function('run');globals_['connection_environment']=Mock()
        args=SimpleNamespace(prepare_only=False,identity_registry=self.root,identity_cases=self.bundle_path,
            reuse_preparation=self.preparation,reuse_work=self.root)
        globals_['measurement_session']=lambda a:measurement_session(a,registry=self.registry)
        with self.assertRaises(IdentityError):run(args)
        globals_['connection_environment'].assert_not_called()



if __name__=='__main__': unittest.main()
