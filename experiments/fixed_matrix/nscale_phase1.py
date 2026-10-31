"""Phase-1 N scaling A/B on six sealed FM1 cases; 1,440 rows, query only."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil

from . import run, telemetry, client, grib_catalog_client, hdf5_batch_client
from .hdf5_native_ab import NativeABRunner, ProtocolSpec, CONFIGS, ARMS, CAMPAIGNS, WARMUPS, MEASURED, REPEATS, PATH_ARGS
from .hdf5_cache_batch import validate_row as validate_hdf5
from .grib_catalog import validate_row as validate_grib, require_json_roundtrip
from fs_index.native import NativeContext
from experiments.identity.model import build_run, digest
from experiments.identity.registry import Registry, file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

need = run.need
PROTOCOL = 'iotdbfs-fixed-nscale-phase1/1'
BASELINE_SHA = 'f1b896244f2a7cbb1abc43b7f08c1386178e2cd9f59755b4ee06c11bbd843087'
TIERS = ('n8', 'n128', 'n512')
PRODUCT_DIFF = frozenset({
    'include/iotdbfs/IoTDBPublishedCatalogSql.h',
    'include/iotdbfs/query/core/QueryPlanning.h',
    'include/iotdbfs/query/frontend/TypedQueryService.h',
    'src/IoTDBStructuredBlobBackend.cpp',
    'src/main.cpp',
    'src/query/core/DatasetCatalogQuery.cpp',
    'src/query/core/QueryModule.cpp',
    'src/query/core/QueryPlanning.cpp',
    'src/query/frontend/FuseTypedQueryBridge.cpp',
    'src/query/frontend/QueryRegistrationSocket.cpp',
    'src/query/frontend/TypedQueryService.cpp',
    'src/query/training/arrow/DatasetSnapshot.cpp',
    'src/storage/versioning/NamespaceSelection.cpp',
})
REQUIRED_REGRESSION_TESTS = (
    ('CatalogSelection', 'CopiesShareFrozenCandidatesWithoutCopyingManifests'),
    ('CatalogSelection', 'ReplacingCandidatesDoesNotRetargetPinnedSelection'),
    ('CatalogSelection', 'CopiedSelectionSurvivesOriginalAndCallerVector'),
    ('CatalogSelection', 'ClearingOneSelectionPreservesOtherFrozenCandidates'),
    ('PathScopeSet', 'CopiesShareCompiledIdentityAndReplacementCannotMutateFrozenCopy'),
    ('PathScopeSet', 'CompiledLifetimeAndIdentityPreserveNormalizationAndScopeKinds'),
    ('PathScopeSet', 'ReplacementRecompilesValidationAndVersionErrorsRemainFirst'),
    ('OwnerQueryAdmission', 'MaterializationBorrowsAllFrozenSourcesAndKeepsThemAlive'),
    ('OwnerQueryAdmission', 'StrictMaterializationKeepsOwnedSourceReferences'),
    ('SocketFixture', 'OwnerRegistrationBorrowsTheCompleteFrozenCandidateArray'),
)


class NScaleRunner(NativeABRunner):
    def groups(self, repo, tier):
        need(tier in TIERS, 'unknown fixed tier')
        size = int(tier[1:])
        by = {g['group_id']: g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
        return [by[f'FM1-{fmt}-N{size}-V1-C{size}-S0-base-latest'] for fmt in ('grib', 'hdf5')]

    def validate_tier_gate(self, gate, tier, baseline_sha, candidate_sha):
        need(gate.get('protocol') == self.spec.protocol and gate.get('tier') == tier and
             gate.get('query_records') == 480 and
             all(gate.get(k) is True for k in ('complete', 'scientific_complete', 'ready_for_analysis',
                 'original_service_restored', 'resources_restored', 'controller_restoration_verified')) and
             gate.get('product_sha256') == candidate_sha and gate.get('baseline_sha256') == baseline_sha,
             'preceding tier incomplete, not restored or different A/B')

    def validate_n8_gate(self, gate, baseline_sha, candidate_sha):
        self.validate_tier_gate(gate, 'n8', baseline_sha, candidate_sha)

    def validate_gate_artifacts(self, args, proof):
        if args.tier == 'n8':
            need(args.gate_report is None, 'N8 does not accept a preceding gate')
            return
        need(args.gate_report is not None, 'next tier requires preceding completed gate')
        gate = read_json(args.gate_report)
        preceding = TIERS[TIERS.index(args.tier)-1]
        self.validate_tier_gate(gate, preceding, self.spec.baseline_sha, proof['product']['sha256'])
        prior = args.gate_report.parent
        frozen = read_json(prior/'prepared.json')
        previous = read_json(prior/'reuse-source.json')
        need(frozen['sources'] == self.closure(args.repo), 'preceding tier execution closure differs')
        for key in ('source_collection', 'source_pins', 'current_collection', 'current_location_files', 'build_gate',
                    'resource_identity', 'product', 'baseline'):
            need(previous[key] == proof[key], 'preceding tier source/build/resource binding differs: '+key)
        need(frozen['product'] == proof['product'] and frozen['baseline'] == proof['baseline'],
             'preceding tier prepared A/B differs')
        for artifact, key in (('queries.jsonl', 'queries_sha256'), ('bindings.json', 'bindings_sha256'),
                              ('analysis.json', 'analysis_sha256')):
            need(file_sha(prior/artifact) == gate.get(key), 'preceding gate artifact changed: '+artifact)

    def source_proof(self, args):
        # Check free space before any bootstrap, mount, or resource mutation.
        # The changing free-space observation is excluded from the frozen proof.
        target = args.work
        while not target.exists():
            target = target.parent
        space = shutil.disk_usage(target)
        need(space.total > 0 and space.free/space.total >= .01, 'less than approved 1% disk space remains')
        proof = super().source_proof(args)
        self.validate_gate_artifacts(args, proof)
        proof.update(gate_report_sha256=file_sha(args.gate_report) if args.gate_report else None,
                     minimum_disk_free_fraction=.01)
        return proof

    def validate_row(self, row, group, configuration):
        need(configuration in CONFIGS, 'unknown configuration')
        if group['format'] == 'hdf5':
            super().validate_row(row, group, configuration)
        else:
            need(group['format'] == 'grib', 'unknown format')
            validate_grib(row, 'C', group['K_units'])
        # Ownership/selector mechanisms have unit gates and source audit; no
        # absent baseline counter is interpreted as zero work.
        return row

    def plans(self,args,registry,report,proof):
        original=read_json(args.source_collection/'report.json');oldbind=read_json(args.source_collection/'bindings.json')
        source_registry=Registry(args.source_collection/'registry');selected={g['group_id']:g for g in self.groups(args.repo,args.tier)}
        result=[];bindings={}
        for entry in proof['cases']:
            gid=entry['group_id'];cid=entry['corpus_id'];base=Path(entry['source_root'])
            corpus=registry.register_corpus(source_registry.get_corpus(cid));registry.register_case(source_registry.get_case(entry['case_id']))
            bindings[gid]=deepcopy(oldbind[gid]);bindings[gid]['run_ids']={}
            execution=args.work/'corpora'/entry['corpus_key'];execution.mkdir(parents=True,mode=0o700)
            item=deepcopy(next(v for v in original['corpora'] if v['corpus_id']==cid))
            item.update(complete=False,mounts=[],source_root=str(base),head=entry['head'],journal_digest=entry['journal_digest'])
            report['corpora'].append(item)
            result.append(dict(root=base,execution_root=execution,corpus=corpus,group=selected[gid],item=item,
                journal=Path(entry['journal']),journal_digest=entry['journal_digest'],scientific_head=entry['head'],
                native_root=Path(entry['native_root']) if entry['format']=='hdf5' else None,
                prep=read_json(base/'preparation.json'),manifest=read_json(base/'input/manifest.json'),
                cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),
                    cache_files=8,cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)))
        return result,bindings

    def launcher(self,path,binary,sha,namespace,journal,plan):
        if plan['group']['format']=='grib':
            return run.infrastructure.launcher(path,binary,sha,namespace,journal)
        return super().launcher(path,binary,sha,namespace,journal,plan)

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        paths=read_json(args.work/'resource-paths.json');count=0
        writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        for campaign in CAMPAIGNS:
            for ordinal,plan in enumerate(execution):
                group=plan['group'];gid=group['group_id'];b=bindings[gid]
                if campaign not in self.campaigns(group):continue
                for config in self.configuration_order(campaign):
                    block=plan['execution_root']/('campaign-'+str(campaign))/config;block.mkdir(parents=True)
                    journal=block/'owner';shutil.copytree(plan['journal'],journal)
                    need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'private journal copy differs')
                    binary=args.baseline_binary if config=='A' else args.binary;sha=file_sha(binary)
                    launch=self.launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal,plan)
                    atomic_json(block/'launcher.receipt.json',launch)
                    before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                    need(before==plan['scientific_head'],'head changed before mount')
                    print(json.dumps(dict(event='nscale_phase1_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
                    with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                        need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha,'running binary differs')
                        actual=read_json(context['root']/'profile.launch.json');need(actual['settings']==launch['settings'],'actual settings differ')
                        native=stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                        environment=telemetry.environment(args.repo,artifact_paths=[args.library,binary],process_roles=dict(client=os.getpid(),
                            daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                        atomic_json(block/'environment.json',environment)
                        mount=dict(campaign=campaign,configuration=config,daemon_pid=context['pid'],startup_ns=context['startup_ns'],
                            product_sha256=sha,environment_sha256=file_sha(block/'environment.json'));plan['item']['mounts'].append(mount)
                        atomic_json(block/'query-plan.json',native.explain('index',b['native_selection'],client.PREDICATE,b['scope']))
                        for arm in ARMS:
                            model=build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                                implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                    runner_sha256=digest(frozen['sources']),engine=('SQLite index + native owned values' if arm=='FS-index' else
                                        'native-hdf5 cache+batch' if group['format']=='hdf5' else 'format_catalog + unchanged GRIB typed values')),
                                clock=dict(kind='client-owned-full-query',d='entry through discovery delivery',q='discovery through owned Float32/mask',profile_oracle='outside'),
                                cache=dict(protocol=self.spec.protocol,configuration=config,native_helper=plan['cfg'] if arm=='FS-index' else None,
                                    daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=group['format']=='hdf5'),
                                environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                                replication=dict(campaign=campaign,warmup=WARMUPS,measured=MEASURED,schedule='AB/BA/BA/AB; paired alternating arms; parity repeat+campaign+format+configuration'))
                            bound=registry.register_run(model);b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                        atomic_json(args.work/'bindings.json',bindings)
                        for repeat in range(REPEATS):
                            order=ARMS if (repeat+campaign+ordinal+CONFIGS.index(config))%2==0 else tuple(reversed(ARMS))
                            for arm in order:
                                rb=telemetry.sample(paths);run.validate_sample(rb)
                                if arm=='IoTDB-FS':
                                    if group['format']=='hdf5':row=hdf5_batch_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],batch=True)
                                    else:row=grib_catalog_client.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],lightweight=True)
                                    self.validate_row(row,group,config)
                                else:row=client.query_native(native,arm,group,b['scope'],b['native_selection'],b['expected_matches'])
                                ra=telemetry.sample(paths);run.validate_sample(ra)
                                row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                    run_id=b['run_ids'][str(campaign)][config][arm],campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                    phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,resource_id=frozen['resource_id'],resource_before=rb,
                                    resource_after=ra,environment_sha256=mount['environment_sha256'],product_sha256=sha)
                                run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                            atomic_json(args.work/'report.json',report)
                    after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                    need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'query changed head/journal')
                    need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed FS-index')
                    mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True);atomic_json(args.work/'report.json',report)
        return count

    def main(self):
        parser = argparse.ArgumentParser(description=self.spec.description)
        for key in PATH_ARGS:
            parser.add_argument('--'+key.replace('_', '-'), type=Path, required=True)
        parser.add_argument('--tier', choices=TIERS, required=True)
        parser.add_argument('--gate-report', type=Path)
        modes = parser.add_mutually_exclusive_group()
        modes.add_argument('--worker', action='store_true')
        modes.add_argument('--prepare-only', action='store_true')
        modes.add_argument('--verify-prepared', action='store_true')
        args = parser.parse_args()
        for key in PATH_ARGS:
            if key != 'work':
                setattr(args, key, getattr(args, key).resolve(strict=True))
        args.work = args.work.resolve()
        if args.gate_report:
            args.gate_report = args.gate_report.resolve(strict=True)
        for source in (args.source_collection, args.current_collection):
            need(not args.work.is_relative_to(source) and not source.is_relative_to(args.work), 'run/source paths overlap')
        if args.worker:
            self.worker(args)
        else:
            proof = self.source_proof(args)
            require_json_roundtrip(proof, 'preflight proof')
            if args.prepare_only:
                self.prepare_only(args, proof)
            elif args.verify_prepared:
                self.verify_prepared(args, proof)
            else:
                self.supervise(args, proof)


SPEC = ProtocolSpec(
    protocol=PROTOCOL, module='experiments.fixed_matrix.nscale_phase1', description=__doc__,
    baseline_sha=BASELINE_SHA, product_diff=PRODUCT_DIFF,
    required_regression_tests=REQUIRED_REGRESSION_TESTS,
    extra_closure=(
        'experiments/fixed_matrix/tests/test_nscale_phase1.py',
        'experiments/cross_version/hdf5_route3/native_filesystem/fixed_probe.py',
        'experiments/cross_version/nscale_phase1/README.md',
        'experiments/cross_version/nscale_phase1/run.sh',
    ),
)
runner = NScaleRunner(SPEC)

if __name__ == '__main__':
    runner.main()
