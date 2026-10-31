"""Independent resource, clock and source audits for resident queries (no service writes)."""
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


def export(root,destination,source_root):
    root=root.resolve(strict=True);destination=destination.resolve();source_root=source_root.resolve(strict=True)
    c=root/'collection';report=read(c/'report.json');supervisor=read(root/'supervisor-report.json')
    need(report['complete'] and report['protocol']=='iotdbfs-resident-query/1' and report['data_publications']==0,'wrong or incomplete run')
    need(supervisor['complete'] and supervisor['resources_restored'] and not supervisor['cancelled'],'supervisor/restoration failed')
    audit=audit_environment(root,source_root)
    init=[json.loads(x) for x in (root/'publication-log-initialization.jsonl').read_text().splitlines()]
    need(len(init)==3 and {x['campaign'] for x in init}=={0,1,2},'publication log initialization not audited')
    frozen=read(c/'frozen.json');cases={x['source_id']:x for x in frozen['cases']}
    rows=[json.loads(x) for x in (c/'sessions.jsonl').read_text().splitlines()]
    warm=[json.loads(x) for x in (c/'warmups.jsonl').read_text().splitlines()]
    keys=[(r['campaign'],r['arm'],r['source_id'],r['q']) for r in rows]
    need(len(keys)==len(set(keys))==135 and set(keys)=={(i,a,s,q) for i in range(3) for a in ARMS for s in SOURCES for q in (1,10,100)},'matrix differs')
    need(len(warm)==135 and all(r['q']==1 for r in warm),'warmups differ')
    for r in rows+warm:
        need(r['complete'] and r['session_wall_measured_directly'] and r['audit_deferred_to_caller'],'session boundary differs')
        need(r['session_wall_ns']==r['session_end_ns']-r['session_begin_ns'] and r['session_wall_ns']>=r['query_sum_ns'],'session arithmetic differs')
        need(len(r['observations'])==r['q'] and r['source_sha256']==cases[r['source_id']]['sha256'],'source/query count differs')
        oracle=cases[r['source_id']]['oracle']
        for o in r['observations']:
            v=o['validation'];hashes=('value_sha256','mask_sha256')+(('latitude_sha256','longitude_sha256') if r['source_id'].startswith('G-') else ())
            need(o['oracle_verified'] and v['oracle_verified'] and v['rows']==oracle['rows'] and all(v[k]==oracle[k] for k in hashes),'oracle mismatch')
            need(o['total_ns']==o['query_end_ns']-o['query_begin_ns']==o['discovery_ns']+o['value_and_delivery_ns'],'query arithmetic differs')
            if r['arm']=='FS-index':
                need(o['persistent_index'] and o['metadata_reused'] and not o['handles_reused'] and o['catalog_lookup_ns']>=0,'index path missing')
            need(v['owned_output_bytes']<=21,'output retention exceeds bounded point profile')
            if r['arm']=='IoTDB-FS':
                need(o['mode_verified'] and o['server_profile']['ok'],'managed profile missing')
                if r['source_id'].startswith('H-'):need(o['server_profile']['server_hdf5_execution']['mode']=='iotdb-indexed','HDF5 mode differs')
        for v in (r['resource_before'],r['resource_after']):
            no_oom(v,'query');need(v['parent_cpu_max']=='200000 100000' and v['memory.max']=='2147483648' and v['memory.swap.max']=='0' and v['cpuset.cpus.effective']=='2-3','resources differ')
    for campaign in range(3):
        block=c/f'c{campaign}-IoTDB-FS'
        need(read(block/'journal-check.json')['unchanged'] and read(block/'journal-check.json')['head_unchanged'],'owner changed')
        need(read(block/'head-before.json')==read(block/'head-after.json'),'database head changed')
        initial=next(x for x in init if x['campaign']==campaign)
        need(initial['bytes']==0 and initial['path']==str(block/'mount/publication.jsonl') and initial['monotonic_ns']<min(r['session_begin_ns'] for r in warm if r['campaign']==campaign and r['arm']=='IoTDB-FS'),'log initialization was not before managed reads')
        p=block/'mount/publication.jsonl';need(p.is_file() and p.stat().st_size==0,'publication log missing or nonempty')
        need(read(block/'settings.json')['storage.version_directory.initialize']=='false','initialized instead of reopened')
        catalog=read(c/f'c{campaign}-FS-index/catalog-status.json');need(catalog['sources']==15 and catalog['units']==13869,'catalog counts differ')
        for sid in SOURCES:
            sr=[r for r in rows if r['campaign']==campaign and r['source_id']==sid]
            need(len({r['source_binding'] for r in sr if r['arm'] in ('FS-scan','FS-index')})==1,'FS arms read different files')
            for arm in ARMS:
                ww=[r for r in warm if r['campaign']==campaign and r['source_id']==sid and r['arm']==arm]
                need([r['role'] for r in ww]==['first','warmup','warmup'],'warmup roles differ')
                need(max(r['session_end_ns'] for r in ww)<min(r['session_begin_ns'] for r in sr if r['arm']==arm),'warmups not before measurement')
    cells=[]
    for s in SOURCES:
        for q in (1,10,100):
            for a in ARMS:
                rs=sorted((r for r in rows if r['source_id']==s and r['q']==q and r['arm']==a),key=lambda r:r['campaign'])
                values=[r['session_wall_ns']/1e6 for r in rs]
                cells.append(dict(source=s,q=q,arm=a,session_ms=dict(mean=statistics.mean(values),median=statistics.median(values),min=min(values),max=max(values),raw=values),per_query_ms=dict(mean=statistics.mean(values)/q,median=statistics.median(values)/q,min=min(values)/q,max=max(values)/q,raw=[v/q for v in values])))
    first_cells=[]
    for s in SOURCES:
        for a in ARMS:
            values=[r['session_wall_ns']/1e6 for r in sorted(warm,key=lambda r:r['campaign']) if r['source_id']==s and r['arm']==a and r['role']=='first']
            need(len(values)==3,'first-query count differs')
            first_cells.append(dict(source=s,arm=a,first_ms=dict(mean=statistics.mean(values),median=statistics.median(values),min=min(values),max=max(values),raw=values)))
    destination.mkdir(parents=True,exist_ok=False)
    reopens=[read(c/f'c{i}-IoTDB-FS/reopen.json') for i in range(3)]
    counters=[]
    for r in rows:
        if r['arm']=='IoTDB-FS':
            counters.append(dict(campaign=r['campaign'],source=r['source_id'],q=r['q'],iotdb_delta=[o['server_profile'].get('iotdb_delta',{}) for o in r['observations']],counters=[o['server_profile'].get('counters',{}) for o in r['observations']]))
    (destination/'managed-counters.json').write_text(json.dumps(counters,indent=2)+'\n')
    result=dict(service_reopens=reopens,complete=True,records=len(rows),queries=sum(r['q'] for r in rows),first_and_warmup_queries=len(warm),data_publications=0,aggregation='arithmetic mean of three independent measured session walls',scope='stored data and persisted indexes; retained state after uniform warmup',cells=cells,first_cells=first_cells,audit=audit)
    (destination/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    flat=[dict(campaign=r['campaign'],source=r['source_id'],q=r['q'],arm=r['arm'],session_ms=r['session_wall_ns']/1e6,source_binding=r['source_binding'],source_version=r['source_version']) for r in rows]
    with (destination/'sessions.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=list(flat[0]));w.writeheader();w.writerows(flat)
    manifest=dict(protocol=report['protocol'],root=str(root),prior=frozen['prior'],source_root=str(source_root),product_sha256=frozen['product_sha256'],sources=frozen['sources'],artifacts={str(p.relative_to(root)):sha(p) for p in [c/'sessions.jsonl',c/'warmups.jsonl',c/'frozen.json',c/'environment.json',c/'source-closure.json',root/'supervisor-report.json',root/'resources-restored.json']})
    (destination/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('destination',type=Path);p.add_argument('--source-root',type=Path,required=True);a=p.parse_args()
    r=export(a.root,a.destination,a.source_root);print(json.dumps({k:r[k] for k in ('complete','records','queries','first_and_warmup_queries','data_publications')}))
