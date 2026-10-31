"""Approved FM1 campaign-2 replacement using sealed r2 corpora, never publication.

Keep complete campaigns 0/1 byte-for-byte. Replace the *whole* partial third
campaign, without consulting timing outcomes. Evidence for the two execution
segments stays separate; the stopped r2 report and its 72 third-campaign rows
remain immutable. Product, native helper, client clocks and caches are unchanged.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import signal

from . import run
from experiments.identity.model import digest
from experiments.identity.registry import Registry,read_json,file_sha
from experiments.cross_version.resource_limited.resources import atomic_json

need=run.need
SOURCE_PINS={
 'prepared.json':'256c584dd0c34db67214ee4083752a42e0869c0d3026570d7bf6dc5aa5579156',
 'report.json':'b8dc4fc8ddaff6b02e3517cc9afe354b335faae7f07deb2a2ca8ac65516ccb6c',
 'bindings.json':'b281c095101206479da1734756736b50fcccf82811208db4f26a27bdbeada03f',
 'preparation-refs.json':'79dabaa755b9530daf38336dc41849c11d62daa6278f21272768ddfe75db3ca6',
 'queries.jsonl':'731fb64157dfd688e1159461ee74cc324f32612458a815816ccc3299d5cad0d6',
 'resource-samples.jsonl':'82263fb03b8484753f2065d12e9bcf9ec47ceb18a46122f92b46cbe2d181bc0e',
 'final-audit-filesystem/result.json':'bbadd39773105b976c3342d7e73b91250579d86062dbf1a0501d33d72be7d394',
 'recovery-transport-disconnect/result.json':'56983fa296b88feedd38124b83060c44c698b4d6db47155144d24de277c7b784',
}
RETAIN=(0,1)
REPLACE=2


def selected_rows(path,campaigns):
    """Yield unchanged raw lines; no latency filter, sorting, or reserialization."""
    with Path(path).open('rb') as stream:
        for line in stream:
            row=json.loads(line)
            if row['campaign'] in campaigns:yield line,row


def validate_rows(path,catalog,bindings,campaigns,resource_id):
    groups={g['group_id']:g for g in catalog['groups'] if g['collection_tier']=='primary'}
    seen=set()
    for _,r in selected_rows(path,campaigns):
        key=(r['group_id'],r['campaign'],r['arm'],r['repeat'])
        need(key not in seen,'duplicate query cell');seen.add(key)
        need(r['group_id'] in groups and r['arm'] in run.ARMS and 0<=r['repeat']<12,'unregistered row')
        b=bindings[r['group_id']]
        for name in ('case_id','corpus_id','oracle_sha256'):need(r[name]==b[name],'row identity differs')
        need(r['run_id']==b['run_ids'][str(r['campaign'])][r['arm']],'run identity differs')
        need(r['phase']==('warmup' if r['repeat']<2 else 'measurement'),'phase differs')
        need(all(r.get(k) is True for k in ('ok','oracle_verified','mode_verified')),'unverified row')
        need(all(type(r[k]) is int and r[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
             r['total_ns']==r['discovery_ns']+r['value_ns'],'invalid full-query clock')
        fmt=groups[r['group_id']]['format']
        mode='native-hdf5' if fmt=='hdf5' else 'iotdb-indexed' if r['arm']=='IoTDB-FS' else 'native-grib'
        need(r['execution_mode']==mode and r['resource_id']==resource_id,'mode/resource differs')
        for side in ('resource_before','resource_after'):run.validate_sample(r[side])
        need(r['resource_after']['monotonic_ns']>r['resource_before']['monotonic_ns'],'resource clock reversed')
    expected={(g,c,a,r) for g in groups for c in campaigns for a in run.ARMS for r in range(12)}
    need(seen==expected,'whole campaign incomplete; no partial replacement allowed')
    return dict(complete=True,campaigns=list(campaigns),records=len(seen))


def check_source(args,*,verify_bytes=True):
    source=args.source_collection
    need(source==args.registration.parent/'collection-r2','only approved sealed r2 publication may be replayed')
    for relative,sha in SOURCE_PINS.items():need(file_sha(source/relative)==sha,'sealed source changed: '+relative)
    prepared=read_json(source/'prepared.json');report=read_json(source/'report.json')
    need(prepared['product']['sha256']==run.PRODUCT_SHA and prepared['native_library_sha256']==run.NATIVE_SHA,
         'source implementation differs')
    # Only orchestration/maintenance logging may differ; science remains pinned.
    allowed={'experiments/fixed_matrix/run.py','experiments/cross_version/hdf5_route3/route3_control.py'}
    for relative,sha in prepared['sources'].items():
        need(file_sha(source/'source'/relative)==sha,'original executed source snapshot changed')
        if relative not in allowed:need(file_sha(args.repo/relative)==sha,'scientific dependency changed: '+relative)
    need(file_sha(args.repo/'experiments/fixed_matrix/catalog.json')==run.CATALOG_SHA,'catalog changed')
    audit=read_json(source/'final-audit-filesystem/result.json')
    recovery=read_json(source/'recovery-transport-disconnect/result.json')
    need(audit['complete'] and audit['source_closure_unchanged'] and not audit['performance_ratios_computed'] and
         recovery['complete'] and recovery['original_service_restored'] and recovery['resources_restored'],
         'prior audit/recovery evidence incomplete')
    need(report['query_records']==2376 and len(report['corpora'])==14,'source collection differs')
    catalog=read_json(args.repo/'experiments/fixed_matrix/catalog.json');bindings=read_json(source/'bindings.json')
    need(len(bindings)==32 and len({b['corpus_id'] for b in bindings.values()})==14,'sealed bindings incomplete')
    retained=validate_rows(source/'queries.jsonl',catalog,bindings,RETAIN,prepared['resource_id'])
    registry=Registry(source/'registry');refs=read_json(source/'preparation-refs.json');corpora=[]
    for item in report['corpora']:
        base=source/'corpora'/item['corpus_key'];manifest=read_json(base/'input/manifest.json')
        corpus=registry.get_corpus(item['corpus_id']);prep=read_json(base/'preparation.json')
        need(file_sha(base/'input/manifest.json')==item['raw_manifest_sha256']==corpus['source_evidence']['manifest_sha256'],
             'manifest binding changed')
        need(refs[item['corpus_id']]==dict(artifact=str(base/'preparation.json'),sha256=file_sha(base/'preparation.json')),
             'preparation receipt changed')
        need(prep['complete'] and file_sha(base/'index.sqlite')==item['index_sha256']==prep['index_sha256'],'sealed FS-index changed')
        need(run.infrastructure.journal_snapshot(base/'owner')==item['journal_digest'],'owner journal changed')
        need(file_sha(base/'publication.json')==prep['publication_receipt_sha256']==corpus['publication_evidence']['receipt_sha256'],
             'publication receipt changed')
        need(read_json(base/'publication.json')['head']==item['head'],'published head binding changed')
        for c in RETAIN:
            mounts=[m for m in item['mounts'] if m['campaign']==c]
            need(len(mounts)==1 and all(mounts[0].get(k) is True for k in ('complete','scientific_head_unchanged','journal_unchanged')),
                 'retained mount validation missing')
            need(file_sha(base/('campaign-'+str(c))/'environment.json')==mounts[0]['environment_sha256'],'retained environment changed')
        validation=run.verify_raw(manifest,base/'input',base/'fs') if verify_bytes else prep['raw_validation']
        corpora.append(dict(corpus_key=item['corpus_key'],corpus_id=item['corpus_id'],raw_validation=validation,
            artifacts={name:file_sha(base/name) for name in ('catalog.json','preparation.json','publication.json')}))
    for b in bindings.values():
        case=registry.get_case(b['case_id']);need(case['corpus_id']==b['corpus_id'] and digest(case['expected_matches'])==b['oracle_sha256'] and
            case['expected_matches']==b['expected_matches'],'case oracle/binding changed')
        for c in RETAIN:
            for arm in run.ARMS:
                r=registry.get_run(b['run_ids'][str(c)][arm]);need(r['case_id']==b['case_id'] and r['arm']==arm,'retained run differs')
    return dict(protocol='iotdbfs-fixed-query-only-source/1',complete=True,source_collection=str(source),
        source_pins=SOURCE_PINS,corpora=corpora,retained=retained,replacement_campaign=REPLACE,
        excluded_source_campaigns=[2],excluded_source_rows=72,expected_new_rows=1152,
        scientific_queries_executed=0,byte_validation=verify_bytes,
        note='Entire campaign replacement approved before R analysis. Never choose rows by speed.')


def build_plans(args,registry,report,bindings):
    source=args.source_collection;original=read_json(source/'report.json')
    catalog=read_json(args.repo/'experiments/fixed_matrix/catalog.json');plans=[]
    for old in original['corpora']:
        base=source/'corpora'/old['corpus_key'];corpus=Registry(source/'registry').get_corpus(old['corpus_id'])
        registry.register_corpus(corpus)
        manifest=read_json(base/'input/manifest.json');spec=corpus['parameters']['generation']['specification']
        item=deepcopy(old);item.update(complete=False,mounts=[],source_root=str(base));report['corpora'].append(item)
        cfg=dict(catalog_path=str(base/'catalog.json'),index_path=str(base/'index.sqlite'),cache_files=8,
            cache_datasets=8,metadata_cache_bytes=2097152,raw_chunk_cache_bytes=262144)
        plans.append(dict(spec=spec,root=base,execution_root=args.work/'corpora'/old['corpus_key'],manifest=manifest,
            fsroot=base/'fs',journal=base/'owner',item=item,scientific_head=old['head'],journal_digest=old['journal_digest'],
            corpus=corpus,groups=[g for g in catalog['groups'] if g['group_id'] in spec['group_ids']],cfg=cfg,
            prep=read_json(base/'preparation.json')))
    for b in bindings.values():
        registry.register_case(Registry(source/'registry').get_case(b['case_id']))
        b['run_ids']={}
    plans.sort(key=lambda p:(p['spec']['N'],p['spec']['V_stored'],p['spec']['size_profile'],p['spec']['history_scenario'],p['spec']['format']))
    return plans


def worker(args):
    need(os.read(args.gate_fd,1)==b'1','worker not admitted');os.close(args.gate_fd)
    def interrupted(signum,frame):raise RuntimeError('query-only worker cancelled; preserve failed segment and restore')
    for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
    paired,_,env,startup=run.dependencies(args.repo)
    startup.adjust_mount(paired.sf,1800,args.work/'startup-receipts.jsonl')
    frozen=read_json(args.work/'prepared.json');need(run.closure(args.repo)==frozen['sources'],'source closure changed')
    proof=read_json(args.work/'reuse-source.json');need(proof['complete'] and proof['byte_validation'],'source preflight missing')
    report=dict(protocol='iotdbfs-fixed-query-only-segment/1',complete=False,expected_queries=1152,query_records=0,
        campaign=REPLACE,corpora=[],catalog_sha256=run.CATALOG_SHA,resource_id=frozen['resource_id'],
        product_sha256=run.PRODUCT_SHA,source_collection=str(args.source_collection),credentials_recorded=False)
    atomic_json(args.work/'report.json',report)
    try:
        bindings=read_json(args.source_collection/'bindings.json');registry=Registry(args.work/'registry')
        plans=build_plans(args,registry,report,bindings)
        # Revalidate ALL frozen heads in the legal SYNC maintenance window
        # before the first formal query. Per-mount checks still run as before.
        (args.work/'head-readmission').mkdir(mode=0o700)
        for plan in plans:
            actual,_=run.head(Path(frozen['inspector']),plan['item']['namespace'],env,
                args.work/'head-readmission'/plan['spec']['corpus_key'])
            need(actual==plan['scientific_head'],'frozen head differs before query-only admission')
        atomic_json(args.work/'heads-readmitted.json',dict(complete=True,corpora=14,head_sha256=digest([p['scientific_head'] for p in plans])))
        atomic_json(args.work/'campaign-order.json',dict(corpora=[p['spec']['corpus_key'] for p in plans],campaign=2,
            rotation=10,case_order='catalog order rotated by campaign within corpus',arm_rotation=4))
        atomic_json(args.work/'bindings.json',bindings)
        atomic_json(args.work/'preparation-refs.json',read_json(args.source_collection/'preparation-refs.json'))
        rows=run.collect_campaigns(args,plans,bindings,registry,frozen,report,env,paired,campaigns=[REPLACE])
        for p in plans:
            p['item'].update(complete=True,final_raw_validation=run.verify_raw(p['manifest'],p['root']/'input',p['fsroot']))
        need(rows==1152,'replacement campaign incomplete')
        validate_rows(args.work/'queries.jsonl',read_json(args.repo/'experiments/fixed_matrix/catalog.json'),bindings,[REPLACE],frozen['resource_id'])
        need(run.closure(args.repo)==frozen['sources'],'source changed during replay')
        report.update(complete=True,queries_sha256=file_sha(args.work/'queries.jsonl'),bindings_sha256=file_sha(args.work/'bindings.json'))
    except BaseException as e:
        report.update(failure=dict(type=type(e).__name__,message=str(e)[:500]));raise
    finally:atomic_json(args.work/'report.json',report)


def assemble(args,report,prepared,sampler):
    """Called only after independent service/cgroup restoration of new segment."""
    from .analysis import summarize
    need(report['ready_for_analysis'] and sampler.returncode==0,'new segment restoration/sampler incomplete')
    source=args.source_collection
    # Original raw streams/report must remain byte-identical after shared reads.
    for relative,sha in SOURCE_PINS.items():need(file_sha(source/relative)==sha,'original source changed during replay: '+relative)
    current=read_json(args.work/'bindings.json');prior=read_json(source/'bindings.json')
    catalog=read_json(args.repo/'experiments/fixed_matrix/catalog.json')
    validate_rows(source/'queries.jsonl',catalog,prior,RETAIN,read_json(source/'prepared.json')['resource_id'])
    validate_rows(args.work/'queries.jsonl',catalog,current,[REPLACE],prepared['resource_id'])
    rows=[json.loads(line) for line in (args.work/'queries.jsonl').read_text().splitlines()]
    coverage=run.validate_coverage(args.work,rows);del rows
    merged=args.work/'accepted';merged.mkdir(mode=0o700)
    with (merged/'queries.jsonl').open('xb') as out:
        for path,campaigns in ((source/'queries.jsonl',RETAIN),(args.work/'queries.jsonl',[REPLACE])):
            for line,_ in selected_rows(path,campaigns):out.write(line)
        out.flush();os.fsync(out.fileno())
    for gid,b in current.items():
        for key,value in prior[gid].items():
            if key!='run_ids':need(b[key]==value,'replacement case identity differs')
        b['run_ids']={**{str(c):prior[gid]['run_ids'][str(c)] for c in RETAIN},**b['run_ids']}
    atomic_json(merged/'bindings.json',current)
    refs=read_json(source/'preparation-refs.json');atomic_json(merged/'preparation-refs.json',refs)
    oldreport=read_json(source/'report.json');olditems={c['corpus_id']:c for c in oldreport['corpora']}
    corpora=deepcopy(report['corpora'])
    for item in corpora:
        original=olditems[item['corpus_id']]
        for k in ('head','journal_digest','index_sha256','raw_manifest_sha256','namespace'):need(item[k]==original[k],'corpus changed')
        item['mounts']=[dict(m,execution_root=str(source/'corpora'/item['corpus_key'])) for m in original['mounts'] if m['campaign'] in RETAIN]+[
            dict(m,execution_root=str(args.work/'corpora'/item['corpus_key'])) for m in item['mounts']]
        need(len(item['mounts'])==3 and all(m.get('complete') is True for m in item['mounts']),'complete mount receipts missing')
    oldprepared=read_json(source/'prepared.json')
    segments=[dict(root=str(source),retained_campaigns=[0,1],retained_queries=2304,excluded_campaigns=[2],excluded_queries=72,
        source_pins=SOURCE_PINS,resource_id=oldprepared['resource_id'],coverage=read_json(source/'final-audit-filesystem/result.json')['coverage'],
        restoration=str(source/'recovery-transport-disconnect/result.json')),
        dict(root=str(args.work),retained_campaigns=[2],retained_queries=1152,resource_id=prepared['resource_id'],coverage=coverage,
            restoration=str(args.work/'service-report.json'))]
    completion=dict(complete=True,original_service_restored=True,resources_restored=True,
        controller_restoration_verified=True,segments=segments,
        limitation='Two separately observed execution segments; intervening service restart/approval gap is not continuously sampled.')
    accepted=dict(protocol='iotdbfs-fixed-matrix-composite/1',**completion,query_records=3456,expected_queries=3456,
        corpora=corpora,catalog_sha256=run.CATALOG_SHA,product_sha256=run.PRODUCT_SHA,
        queries_sha256=file_sha(merged/'queries.jsonl'),bindings_sha256=file_sha(merged/'bindings.json'))
    rows=[json.loads(line) for line in (merged/'queries.jsonl').read_text().splitlines()]
    result=summarize(rows,catalog,current,resource_ids={'0':oldprepared['resource_id'],'1':oldprepared['resource_id'],'2':prepared['resource_id']},
        preparation_refs=refs,completion=completion)
    atomic_json(merged/'analysis.json',result);accepted['analysis_sha256']=file_sha(merged/'analysis.json')
    atomic_json(merged/'report.json',accepted)
    report.update(complete=True,resource_coverage=coverage,accepted_matrix=str(merged),
        accepted_report_sha256=file_sha(merged/'report.json'),accepted_query_records=3456)
    atomic_json(args.work/'report.json',report)
    print(json.dumps(dict(complete=True,segment_queries=1152,accepted_queries=3456,accepted=str(merged))),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('repo','work','library','native-binding','registration','source-collection'):
        parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--gate-fd',type=int,help=argparse.SUPPRESS)
    args=parser.parse_args()
    for key in ('repo','work','library','native_binding','registration','source_collection'):
        setattr(args,key,getattr(args,key).resolve())
    if args.worker:worker(args)
    else:
        proof=check_source(args)
        # The anchor rejects future invocation of the initial publication entry.
        anchor=args.registration.parent/'published-cases.json'
        value=dict(protocol='iotdbfs-fixed-published-anchor/1',catalog_sha256=run.CATALOG_SHA,
            source_collection=str(args.source_collection),source_pins=SOURCE_PINS)
        if anchor.exists():need(read_json(anchor)==value,'existing publication anchor differs')
        else:atomic_json(anchor,value)
        # supervise creates the new directory exclusively; snapshot preflight
        # via a wrapper without placing files into the sealed source tree.
        original_bootstrap=run.bootstrap
        def bootstrap_with_proof(a):
            atomic_json(a.work/'reuse-source.json',proof)
            return original_bootstrap(a)
        run.bootstrap=bootstrap_with_proof
        run.supervise(args,worker_module='experiments.fixed_matrix.replay',
            worker_extra=['--source-collection',str(args.source_collection)],finalizer=assemble)

if __name__=='__main__':main()
