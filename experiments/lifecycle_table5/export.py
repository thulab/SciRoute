"""Independently validate and export a completed Table 5 campaign (no writes to service)."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

ARMS=('FS-scan','FS-index','IoTDB-FS')
SOURCES=('G-M22','G-M4598','H-T2','H-T160','H-T1024')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path):return json.loads(path.read_text())
def need(value,message):
    if not value:raise ValueError(message)


def no_oom(resource, label):
    events=dict(line.split() for line in resource['memory.events'].splitlines())
    need(all(events.get(key)=='0' for key in ('oom','oom_kill')),label+': OOM event')
    need(events.get('oom_group_kill','0')=='0',label+': group OOM event')


def audit_snapshot(snapshot, live, expected_paths):
    need(snapshot['phase']=='active' and snapshot['cpus']==[2,3] and
         snapshot['partition']=='root' and snapshot['scheduler_load_balancing'] is True,
         'resource sample is outside the active balanced two-core envelope')
    need(snapshot['paths']==expected_paths,'resource sample cgroup paths changed')
    groups=snapshot['groups'];need(set(groups)=={'parent','query','database'},'resource groups missing')
    for name,g in groups.items():
        need(g['cgroup.type']=='domain' and g['cpuset.cpus']=='2-3' and
             g['cpuset.cpus.effective']=='2-3',name+': CPU/domain policy differs')
        no_oom(g,name)
    parent=groups['parent'];query=groups['query'];database=groups['database']
    need(parent['cpu.max']=='200000 100000' and parent['memory.max']=='max' and
         parent['cpuset.cpus.partition']=='root' and parent['cpuset.cpus.exclusive.effective']=='2-3' and
         not parent['cgroup.procs'].strip(),'shared parent quota differs')
    for name,g in (('query',query),('database',database)):
        need(g['cpu.max']=='max 100000' and g['cpuset.cpus.partition']=='member' and
             g['cpuset.cpus.exclusive.effective']=='',name+': unexpected leaf CPU restriction')
    need(query['memory.max']=='2147483648' and query['memory.swap.max']=='0','query memory/swap policy differs')
    # Database heap/direct limits are recorded separately; do not invent a 2GiB
    # database limit or require swap.max=0 when the admitted service uses max.
    for key in ('memory.max','memory.swap.max'):
        need(database[key]==live['resources'][key],'database '+key+' changed')
    need(str(live['pid']) in database['cgroup.procs'].split(),'DataNode left the shared quota')


def audit_quiescence(path, live):
    r=read(path)
    need(r['complete'] and r['flush_acknowledged'] and
         r['protocol']=='iotdbfs-table5-quiescence/1','incomplete quiescence receipt')
    need(r['pid']==live['pid'] and r['process_start_ticks']==live['process_start_ticks'],
         'quiescence DataNode identity differs')
    need(r['sample_seconds']==2 and r['required_consecutive_windows']==3 and
         r['cpu_core_threshold']==0.10 and r['write_bytes_per_window_threshold']==1048576 and
         r['deadline_seconds']==180,'quiescence thresholds changed')
    need(0<r['elapsed_ns']<=180*10**9 and 0<=r['flush_wall_ns']<=r['elapsed_ns'],
         'quiescence deadline/flush clock differs')
    hz=r['clock_ticks_per_second'];need(type(hz) is int and hz>0,'invalid CPU tick rate')
    previous=r['post_flush_baseline'];consecutive=0;cpu_ticks=previous['cpu_ticks']
    writes=previous['write_bytes'];reads=previous['read_bytes']
    need(previous['pid']==live['pid'] and previous['start_ticks']==live['process_start_ticks'],
         'quiescence baseline identity differs')
    endpoint=previous['sampled_monotonic_ns']
    for w in r['windows']:
        need(w['start_ns']==endpoint and w['elapsed_ns']==w['end_ns']-w['start_ns'] and
             w['elapsed_ns']>=2*10**9,'quiescence window is discontinuous or too short')
        need(all(type(w[key]) is int and w[key]>=0 for key in
                 ('cpu_ticks_delta','write_bytes_delta','read_bytes_delta')),'resource counter regressed')
        cores=w['cpu_ticks_delta']/hz/(w['elapsed_ns']/1e9)
        need(math.isclose(cores,w['cpu_cores'],rel_tol=1e-12,abs_tol=1e-12),'quiescence CPU arithmetic differs')
        quiet=cores<=0.10 and w['write_bytes_delta']<=1048576
        consecutive=consecutive+1 if quiet else 0
        need(w['quiet']==quiet and w['consecutive_quiet_windows']==consecutive,'quiet-window classification differs')
        endpoint=w['end_ns'];cpu_ticks+=w['cpu_ticks_delta'];writes+=w['write_bytes_delta'];reads+=w['read_bytes_delta']
    need(consecutive>=3,'three final quiet windows missing')
    final=r['final_sample']
    need(final['pid']==live['pid'] and final['start_ticks']==live['process_start_ticks'] and
         final['sampled_monotonic_ns']==endpoint and final['cpu_ticks']==cpu_ticks and
         final['write_bytes']==writes and final['read_bytes']==reads,'quiescence terminal counters differ')
    return dict(path=str(path),windows=len(r['windows']),elapsed_ns=r['elapsed_ns'],
                final_quiet_windows=consecutive,flush_wall_ns=r['flush_wall_ns'])


def audit_environment(root, source_root):
    collection=root/'collection';live=read(collection/'environment.json')['live_database']
    restored=read(root/'resources-restored.json');journal=read(root/'resources.json')
    for state in (restored,journal):
        need(state['phase']=='closed' and state['restoration_complete'] and state['database_restored'] and
             state['keeper'] is None and not state.get('cleanup_error'),'resource restoration not confirmed')
        identity=dict(pid=live['pid'],start_ticks=live['process_start_ticks'])
        need(state['database_original']['identity']==identity and state['restored_database_identity']==identity,
             'restored DataNode identity differs')
    need(restored['paths']==journal['paths'] and restored['name']==journal['name'],'restoration journals disagree')
    paths=restored['paths']
    need(live['cgroup']=='/'+str(Path(paths['database']).relative_to('/sys/fs/cgroup')),
         'live DataNode cgroup differs from admission')
    samples=[read(root/'resource-before.json')]
    with (root/'resource-samples.jsonl').open() as stream:
        samples.extend(json.loads(line) for line in stream if line.strip())
    need(len(samples)>1,'periodic resource samples missing')
    samples.append(read(root/'resource-after.json'))
    for sample in samples:audit_snapshot(sample,live,paths)
    need(all(a['time_ns']<=b['time_ns'] for a,b in zip(samples,samples[1:])),
         'resource sample timestamps regress')
    quiescence=[]
    for campaign in range(3):
        for arm in ARMS:
            block=collection/f'c{campaign}-{arm}'
            closed=read(block/'closed.json');need(closed['complete'] and closed['unmounted'],'arm did not close')
            for phase in ('before','after'):
                quiescence.append(audit_quiescence(block/f'quiescence-{phase}.json',live))
    need(len(quiescence)==18,'quiescence coverage differs')
    need(source_root is not None,'--source-root must identify the unchanged frozen runner source')
    source_root=Path(source_root).resolve(strict=True)
    closure=read(collection/'source-closure.json');need(bool(closure),'source closure empty')
    for relative,digest in closure.items():
        source=(source_root/relative).resolve(strict=True)
        need(source_root in source.parents,'source closure escapes frozen tree')
        need(sha(source)==digest,'final frozen source differs: '+relative)
    return dict(resource_samples=len(samples),quiescence=quiescence,
                source_root=str(source_root),source_files_verified=len(closure),
                source_closure_sha256=sha(collection/'source-closure.json'),
                restoration_verified=True,database_identity=dict(pid=live['pid'],start_ticks=live['process_start_ticks']),
                database_memory_policy=live['resources'],jvm_memory_flags=live['jvm_memory_flags'])


def export(root, destination, source_root=None):
    collection=root/'collection'
    supervisor=read(root/'supervisor-report.json')
    need(supervisor['complete'] and supervisor['worker_returncode']==0 and not supervisor.get('cancelled'),
         'supervisor incomplete/cancelled')
    need(supervisor['resources_restored'],'resources not restored')
    report=read(collection/'report.json');need(report['complete'] and not report['pilot'],'not a complete formal run')
    rows=[json.loads(line) for line in (collection/'lifecycles.jsonl').read_text().splitlines()]
    expected={(c,s,q,a) for c in range(3) for s in SOURCES for q in (1,10,100) for a in ARMS}
    keys=[(r['campaign'],r['source_id'],r['q'],r['arm']) for r in rows]
    need(len(keys)==len(set(keys))==135 and set(keys)==expected,'matrix incomplete or duplicated')
    frozen=read(collection/'frozen.json');units={'G-M22':22,'G-M4598':4598,'H-T2':1,'H-T160':1,'H-T1024':1}
    audit=audit_environment(root,source_root)
    cases={c['source_id']:c for c in frozen['cases']}
    need(set(cases)==set(SOURCES),'frozen oracle sources differ')
    query_count=0;destinations=set();flat=[]
    for r in rows:
        need(r['complete'] and r['oracle_verified'] and r['lifecycle_wall_measured_directly'],'unverified lifecycle')
        need(r['destination'] not in destinations,'reused destination');destinations.add(r['destination'])
        need(r['source_sha256']==frozen['sources'][r['source_id']]['sha256'],'source hash differs')
        need(r['write']['file_fsync_acknowledged'] and r['write']['closed'],'publication acknowledgment missing')
        need(r['exact_source']['exact_visible'],'exact source not visible')
        need(r['lifecycle_wall_ns']==r['lifecycle_end_ns']-r['lifecycle_begin_ns'],'outer clock differs')
        need(not r['includes_query_callback_audit_overhead'],'oracle inside lifecycle clock')
        observations=r['session']['observations'];need(len(observations)==r['q'],'Q mismatch')
        need(r['session']['complete'] and r['session']['q']==r['q'] and
             r['session']['session_wall_measured_directly'],'session clock not verified')
        need(r['session']['session_wall_ns']==r['session']['session_end_ns']-r['session']['session_begin_ns'] and
             r['session']['query_sum_ns']==sum(q['total_ns'] for q in observations) and
             r['session']['session_wall_ns']>=r['session']['query_sum_ns'],'session timing arithmetic differs')
        for q in observations:
            need(q['oracle_verified'] and q['total_ns']>0,'unverified query')
            validation=q['validation'];oracle=cases[r['source_id']]['oracle']
            hashes=('value_sha256','mask_sha256')+(('latitude_sha256','longitude_sha256') if r['source_id'].startswith('G-') else ())
            need(validation['oracle_verified'] and validation['rows']==oracle['rows'] and
                 all(validation[key]==oracle[key] for key in hashes),'recorded output differs from frozen oracle')
            need(q['total_ns']==q['query_end_ns']-q['query_begin_ns'] and
                 q['total_ns']==q['discovery_ns']+q['value_and_delivery_ns'],'query clock arithmetic differs')
            if r['arm']=='IoTDB-FS':
                p=q['server_profile'];need(q['mode_verified'] and p['ok'],'unverified managed execution')
                if r['source_id'].startswith('H-'):need(p['server_hdf5_execution']['mode']=='iotdb-indexed','wrong HDF5 path')
        query_count+=len(observations)
        for resource in (r['resource_before'],r['resource_after']):
            need(resource['parent_cpu_max']=='200000 100000' and resource['memory.max']=='2147483648' and
                 resource['cpuset.cpus.effective']=='2-3' and resource['memory.swap.max']=='0','resource policy differs')
            no_oom(resource,'lifecycle query')
        if r['arm']=='FS-index':
            m=r['index_registration']['metrics'];need(m['extracted_files']==1 and m['extracted_units']==units[r['source_id']],'index extraction differs')
        if r['arm']=='IoTDB-FS':need(len(r['source_visibility_profiles'])==2,'missing visibility evidence')
        flat.append(dict(campaign=r['campaign'],source=r['source_id'],q=r['q'],arm=r['arm'],
            total_s=r['lifecycle_wall_ns']/1e9,copy_s=r['write']['total_ns']/1e9,
            publication_s=(r['write']['total_ns']+r['source_locator_ns'])/1e9,
            locator_s=r['source_locator_ns']/1e9,query_session_s=r['session']['session_wall_ns']/1e9,
            index_extract_s=r['index_registration']['metrics']['extract_ns']/1e9 if r['arm']=='FS-index' else '',
            index_commit_s=r['index_registration']['metrics']['commit_ns']/1e9 if r['arm']=='FS-index' else ''))
    need(query_count==4995,'query coverage differs')
    cells=[]
    for s in SOURCES:
        for q in (1,10,100):
            for arm in ARMS:
                group=sorted((r for r in flat if (r['source'],r['q'],r['arm'])==(s,q,arm)),key=lambda r:r['campaign'])
                need(len(group)==3,'three repetitions required')
                cell=dict(source=s,q=q,arm=arm)
                for field in ('total_s','publication_s','copy_s','locator_s','query_session_s'):
                    values=[r[field] for r in group]
                    cell[field]=dict(mean=statistics.mean(values),median=statistics.median(values),min=min(values),max=max(values),raw=values)
                cells.append(cell)
    stored=read(collection/'summary.json')
    need(stored['complete'] and stored['records']==135 and stored['queries']==4995 and len(stored['cells'])==45,
         'stored summary coverage differs')
    for cell in cells:
        old=next(r for r in stored['cells'] if (r['source_id'],r['q'],r['arm'])==(cell['source'],cell['q'],cell['arm']))
        need(old['count']==3 and set(old['campaigns'])=={0,1,2},'stored repetitions differ')
        for field in ('total_s','publication_s','copy_s','locator_s','query_session_s'):
            for aggregate in ('mean','median','min','max'):
                need(math.isclose(old[field][aggregate],cell[field][aggregate],rel_tol=1e-12,abs_tol=1e-12),
                     'exported '+field+' '+aggregate+' mismatch')
            need(sorted(old[field]['raw'])==sorted(cell[field]['raw']),'stored raw timings differ')
    catalogs=[]
    for campaign in range(3):
        block=collection/f'c{campaign}-FS-index'
        reopened=read(block/'catalog-reopened.json');need(reopened['reverified']==15 and reopened['sources']==15,'catalog reopen incomplete')
        catalogs.append(dict(campaign=campaign,setup=read(block/'catalog-setup.json'),
                             before_close=read(block/'catalog-status.json'),reopened=reopened))
    destination.mkdir(parents=True,exist_ok=False)
    with (destination/'lifecycles.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(flat[0]));w.writeheader();w.writerows(flat)
    summary=dict(complete=True,records=len(rows),queries=query_count,aggregation='arithmetic mean of three lifecycle walls',cells=cells,catalogs=catalogs,audit=audit)
    (destination/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    # Read evidence directories only, never recurse through retained source
    # copies, owner stores or database payload trees during an export.
    evidence_directories=[root,collection]
    evidence_directories.extend(collection/f'c{c}-{arm}' for c in range(3) for arm in ARMS)
    evidence_directories.extend(collection/f'c{c}-IoTDB-FS'/'mount' for c in range(3))
    evidence_files={p for directory in evidence_directories for p in directory.iterdir()
                    if p.is_file() and p.suffix in ('.json','.jsonl') and 'private' not in p.name}
    provenance={str(p.relative_to(root)):sha(p) for p in sorted(evidence_files)}
    (destination/'manifest.json').write_text(json.dumps(dict(root=str(root),evidence_sha256=provenance,
        product_sha256=frozen['product_sha256'],sources=frozen['sources'],export_sha256=sha(Path(__file__))),indent=2)+'\n')
    print(json.dumps(dict(complete=True,records=len(rows),queries=query_count)))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('destination',type=Path)
    p.add_argument('--source-root',type=Path,required=True,help='unchanged frozen runner tree, not the current working tree')
    a=p.parse_args();export(a.root,a.destination,a.source_root)
