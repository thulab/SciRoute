"""True old/new GRIB fee-summary comparison on identical prepared queries."""
from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import shutil

from . import client,run,telemetry
from .hdf5_native_ab import NativeABRunner,ProtocolSpec,ARMS,CONFIGS,CAMPAIGNS,REPEATS,WARMUPS
from .catalog_selector_pair import CatalogSelectorPairRunner,clock_stamp,effects
from fs_index.native import NativeContext
from experiments.identity.model import build_run,digest
from experiments.identity.registry import read_json,file_sha
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL='iotdbfs-fixed-grib-fee-summary/1'
WORK='experiments/cross_version/work/grib-fee-summary-20261024'
BASELINE_SHA='50f24526ed71ccb9da8e6b2c06e5de4fd0582e3c851a3e5bc3574916150a296a'
FLAGS=('complete','scientific_complete','ready_for_analysis','original_service_restored',
       'resources_restored','controller_restoration_verified')

def load_module(repo,name):
    spec=importlib.util.spec_from_file_location('grib_fee_'+name,repo/WORK/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def protocol_spec(repo):
    binding=read_json(repo/WORK/'protocol-binding.json')
    run.need(binding.get('complete') is True and binding.get('baseline_sha256')==BASELINE_SHA,
             'true baseline/product freeze required')
    return ProtocolSpec(PROTOCOL,'experiments.fixed_matrix.grib_fee_summary',__doc__,BASELINE_SHA,
        frozenset(binding['product_diff']),tuple(tuple(v) for v in binding['required_regression_tests']),
        tuple('experiments/fixed_matrix/tests/test_grib_fee_summary.py' if name=='TEST' else WORK+'/'+name for name in
            ('TEST','protocol-binding.json','baseline-build-binding.json','baseline-source-manifest.json',
             'baseline-composite-provenance.json','baseline-probe-revision-r2.json','baseline-probe-source-r2.cpp',
             'grib_handle_client.py','run_control.py','pid_cpu_sampler.py','execution-paths.json',
             'experiment-plan.json','README.md','live_gate.py','audit_fee_summary.py','derive_fee_summary.py','derive_load.py',
             'test_offline.py')))

class GribFeeSummaryRunner(CatalogSelectorPairRunner):
    def __init__(self,spec):super().__init__(spec,'grib')

    # Keep the original restoration-owning supervisor and scientific worker.
    worker=NativeABRunner.worker

    def groups(self,repo,tier):
        run.need(tier in ('n8','n128'),'unknown qualification/joint tier')
        by={g['group_id']:g for g in read_json(repo/'experiments/fixed_matrix/catalog.json')['groups']}
        return [by[f'FM1-grib-N{N}-V1-C{N}-S0-base-latest'] for N in ((8,) if tier=='n8' else (8,128))]

    def validate_n8_gate(self,gate,baseline_sha,candidate_sha):
        NativeABRunner.validate_n8_gate(self,gate,baseline_sha,candidate_sha)
        run.need(gate.get('phase')=='qualification' and gate.get('format')=='grib','new independent N8 qualification required')

    def source_proof(self,args):
        proof=NativeABRunner.source_proof(self,args)
        composite=read_json(args.repo/WORK/'baseline-composite-provenance.json')
        run.need(composite['product_binary_sha256']==BASELINE_SHA and
            composite['original_build_binding_sha256']==file_sha(args.baseline_build_binding) and
            composite['original_source_manifest_sha256']==file_sha(args.repo/WORK/'baseline-source-manifest.json') and
            composite['probe_only_revision']['receipt_sha256']==file_sha(args.repo/WORK/'baseline-probe-revision-r2.json') and
            composite['probe_only_revision']['corrected_sha256']==file_sha(args.repo/WORK/'baseline-probe-source-r2.cpp') and
            composite['probe_only_revision']['corrected_sha256']=='c05b6af4732fb42f8ff530c82791064ab1aee2fe9b4804b3f325406c6f952bd4',
            'baseline composite provenance differs')
        if args.tier=='n128':
            previous=args.gate_report.parent;gate=read_json(args.gate_report)
            run.need(read_json(previous/'prepared.json')['sources']==self.closure(args.repo),'qualification closure changed')
            for name,key in (('queries.jsonl','queries_sha256'),('bindings.json','bindings_sha256'),
                ('analysis.json','analysis_sha256'),('selector-lifecycle.jsonl','lifecycle_sha256')):
                run.need(file_sha(previous/name)==gate[key],'qualification artifact changed: '+name)
        proof.update(phase='qualification' if args.tier=='n8' else 'joint-formal',format='grib',
            query_endpoint='format_catalog_by_selector in both binary configurations',
            scientific_qualification_excluded_from_main=True,baseline_composite_sha256=file_sha(args.repo/WORK/'baseline-composite-provenance.json'),
            tier_order=[[8] if args.tier=='n8' else ([8,128] if c in (0,3) else [128,8]) for c in CAMPAIGNS])
        return proof

    def validate_row(self,row,group,configuration):
        run.need(configuration in CONFIGS and group['format']=='grib' and
            all(row.get(k) is True for k in ('oracle_verified','mode_verified')) and
            row.get('execution_mode')=='iotdb-indexed' and row.get('selected_units')==2 and
            row.get('owned_output_bytes')==40960 and row.get('batch_enabled') is False and
            len(row.get('value_calls',[]))==2,'full prepared GRIB query differs')
        profile=row['discovery_call']['server_profile'];c=profile['counters']
        run.need(profile.get('operation')=='format_catalog_by_selector' and 'path' not in profile and
            all(v['server_profile'].get('operation')=='typed' for v in row['value_calls']), 'same endpoint/serial q required')
        if row.get('phase')!='measurement':return row
        expected={'selector.compiles':0,'selector.input_path_items':0,'selector.handle_queries':1,
            'hot_query.selector_short_ref_hits':1,'hot_query.selector_long_key_bytes':0,
            'hot_query.proof_hits':3,'read_context.admissions':3,'version_context.admissions':3,
            'request_metadata.collector_exits':0,'request_metadata.fallbacks':0}
        run.need(all(type(c.get(k)) is int and c[k]==v for k,v in expected.items()),'warm selector/admission evidence differs')
        run.need(type(c.get('request_metadata.consumed_bytes')) is int and c['request_metadata.consumed_bytes']>0,
                 'actual request charge missing')
        for prefix in ('candidate','adapter'):
            key='hot_query.'+prefix+'_fee_';logical=c.get(key+'logical_entries')
            run.need(type(logical) is int and logical>0 and c.get('hot_query.'+prefix+'_domain_hits')==1,
                     'qualified resident GRIB domain required')
            wanted=dict(entries_visited=logical,entries_skipped=0,summary_stages=0) if configuration=='A' else \
                   dict(entries_visited=0,entries_skipped=logical,summary_stages=1)
            run.need(all(type(c.get(key+k)) is int and c[key+k]==v for k,v in wanted.items()),
                     'old/new prepared fee traversal mechanism differs')
        if configuration=='B':
            run.need(c.get('request_metadata.summary_stages')==3 and c.get('request_metadata.fee_entries_visited')==0 and
                type(c.get('request_metadata.memo_hits')) is int and c['request_metadata.memo_hits']>=2,
                'warm candidate requires three summaries and memoized later admissions')
        return row

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        helper=load_module(args.repo,'grib_handle_client');paths=read_json(args.work/'resource-paths.json');count=0
        writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        report.update(format='grib',phase='qualification' if args.tier=='n8' else 'joint-formal',same_prepared_endpoint=True)
        order=[(c,p,f) for c in CAMPAIGNS for p in sorted(execution,key=lambda p:p['group']['N'],reverse=c in (1,2))
               for f in self.configuration_order(c)]
        atomic_json(args.work/'execution-order.json',dict(phase=report['phase'],mounts=[dict(campaign=c,
            group=p['group']['group_id'],configuration=f) for c,p,f in order],fresh_mount_and_FS_context=True))
        for campaign,plan,config in order:
            group=plan['group'];gid=group['group_id'];b=bindings[gid]
            block=plan['execution_root']/('campaign-'+str(campaign))/config;block.mkdir(parents=True)
            journal=block/'owner';shutil.copytree(plan['journal'],journal)
            run.need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],'journal copy differs')
            binary=args.baseline_binary if config=='A' else args.binary;sha=file_sha(binary)
            launch=self.launcher(block/'launcher',binary,sha,plan['item']['namespace'],journal,plan)
            atomic_json(block/'launcher.receipt.json',launch)
            before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
            run.need(before==plan['scientific_head'],'head changed before mount')
            print(json.dumps(dict(event='grib_fee_mount',campaign=campaign,group=gid,configuration=config)),flush=True)
            with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context,ExitStack() as stack:
                run.need(file_sha(Path('/proc')/str(context['pid'])/'exe')==sha,'actual binary differs')
                run.need(read_json(context['root']/'profile.launch.json')['settings']==launch['settings'],'actual config differs')
                native=stack.enter_context(NativeContext(args.library,dict(plan['cfg'],build_index=False)))
                environment=telemetry.environment(args.repo,artifact_paths=[args.library,binary],process_roles=dict(
                    client=os.getpid(),daemon=context['pid'],datanode=int((args.repo.parent/'runtime/run/datanode.pid').read_text())),data_path=plan['root'])
                atomic_json(block/'environment.json',environment)
                who={k:environment['processes']['client'][k] for k in ('pid','start_ticks')}
                native_id=f'{who["pid"]}:{who["start_ticks"]}:{gid}:{campaign}:{config}'
                mount=dict(campaign=campaign,configuration=config,daemon_pid=context['pid'],startup_ns=context['startup_ns'],
                    product_sha256=sha,environment_sha256=file_sha(block/'environment.json'),client_identity=who,
                    native_context_id=native_id,native_context_closed=False)
                plan['item']['mounts'].append(mount)
                atomic_json(block/'query-plan.json',native.explain('index',b['native_selection'],client.PREDICATE,b['scope']))
                for arm in ARMS:
                    bound=registry.register_run(build_run(registry.get_case(b['case_id']),execution=str(block),arm=arm,
                        implementation=dict(binary_sha256=sha if arm=='IoTDB-FS' else run.NATIVE_SHA,
                            runner_sha256=digest(frozen['sources']),engine='GRIB same prepared endpoint; '+config),
                        clock=dict(kind='client-owned-full-query',d='entry through discovery delivery',
                            q='discovery through owned Float32/mask',profile_oracle='outside'),
                        cache=dict(protocol=PROTOCOL,configuration=config,prepared_endpoint=True,
                            native_helper=plan['cfg'] if arm=='FS-index' else None,daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None),
                        environment=dict(resource_id=frozen['resource_id'],process_receipt_sha256=mount['environment_sha256']),
                        replication=dict(campaign=campaign,warmup=5,measured=10,binary_order='AB/BA/BA/AB',
                            tier_order='8,128/128,8/128,8/8,128' if args.tier=='n128' else 'independent N8 qualification')))
                    b['run_ids'].setdefault(str(campaign),{}).setdefault(config,{})[arm]=bound['run_id']
                atomic_json(args.work/'bindings.json',bindings)
                response,prep=helper.lifecycle(context,helper.prepare_request('grib',b['scope'],b['physical_versions']))
                run.need(response.get('kind')=='prepared_catalog_selector' and isinstance(response.get('selector_id'),str) and
                    bool(response['selector_id']) and type(response.get('idle_ttl_ms')) is int and response['idle_ttl_ms']>0,'prepare contract differs')
                selector=response['selector_id'];prep.update(group_id=gid,campaign=campaign,configuration=config,
                    selector_id=selector,profile_offset_before=0)
                run.append(args.work/'selector-lifecycle.jsonl',prep)
                try:
                    for repeat in range(REPEATS):
                        arms=ARMS if (repeat+campaign+CONFIGS.index(config))%2==0 else ARMS[::-1]
                        for arm in arms:
                            rb=telemetry.sample(paths);run.validate_sample(rb);pb=context.get('profile_offset',0);cb=clock_stamp()
                            row=(helper.query(context,group,b['scope'],b['physical_versions'],b['expected_matches'],batch=False,selector_id=selector)
                                 if arm=='IoTDB-FS' else client.query_native(native,arm,group,b['scope'],b['native_selection'],b['expected_matches']))
                            ce=clock_stamp();ra=telemetry.sample(paths);run.validate_sample(ra);pa=context.get('profile_offset',0)
                            run.need(pa>pb if arm=='IoTDB-FS' else pa==pb,'shared profile cursor differs')
                            row.update(group_id=gid,corpus_id=b['corpus_id'],case_id=b['case_id'],oracle_sha256=b['oracle_sha256'],
                                run_id=b['run_ids'][str(campaign)][config][arm],campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,resource_id=frozen['resource_id'],
                                resource_before=rb,resource_after=ra,environment_sha256=mount['environment_sha256'],product_sha256=sha,
                                client_identity=who,native_context_id=native_id,client_call_begin=cb,client_call_end=ce,
                                profile_offset_before=pb,profile_offset_after=pa)
                            if arm=='IoTDB-FS':self.validate_row(row,group,config)
                            run.append(args.work/'queries.jsonl',row);count+=1;report['query_records']=count
                        atomic_json(args.work/'report.json',report)
                finally:
                    pb=context.get('profile_offset',0)
                    response,release=helper.lifecycle(context,dict(kind='catalog_selector_release',selector_id=selector))
                    run.need(response.get('kind')=='released_catalog_selector' and response.get('released') is True,'release contract differs')
                    release.update(group_id=gid,campaign=campaign,configuration=config,selector_id=selector,profile_offset_before=pb)
                    run.append(args.work/'selector-lifecycle.jsonl',release);mount['selector_released']=True
            after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
            run.need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'] and
                file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],'query changed sealed inputs')
            mount.update(complete=True,native_context_closed=True,scientific_head_unchanged=True,journal_unchanged=True)
            atomic_json(args.work/'report.json',report)
        report['lifecycle_sha256']=file_sha(args.work/'selector-lifecycle.jsonl')
        return count

    def analyze(self,path,bindings,selected,resource_id,candidate_sha):
        # Independent post-run script adds numeric decomposition; the producer
        # verifies identities/full clocks/oracles before a report can complete.
        result=NativeABRunner.analyze(self,path,bindings,selected,resource_id,candidate_sha)
        rows=[json.loads(s) for s in Path(path).read_text().splitlines()]
        for group in selected:
            for campaign in CAMPAIGNS:
                for repeat in range(WARMUPS,REPEATS):
                    pair={r['configuration']:r for r in rows if r['group_id']==group['group_id'] and r['campaign']==campaign and
                          r['repeat']==repeat and r['arm']=='IoTDB-FS'}
                    charges={config:[v['server_profile']['counters']['request_metadata.consumed_bytes']
                        for v in [row['discovery_call']]+row['value_calls']] for config,row in pair.items()}
                    run.need(charges['A']==charges['B'],'baseline/candidate exact d/q charges differ')
        result.update(same_prepared_endpoint=True,exact_formal_charges_equal=True,qualification_excluded=True)
        return result

if __name__=='__main__':GribFeeSummaryRunner(protocol_spec(Path(__file__).resolve().parents[2])).main()
