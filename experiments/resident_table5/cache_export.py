"""Audit and export HDF5 follow-up from recorded clocks and native read receipts."""
import argparse
import csv
import json
import os
from pathlib import Path
import statistics

# Standalone audit: accepted frozen collectors can contain older exporters.
import hashlib
import math

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

ARMS = ('FS-scan', 'FS-index-reopen', 'FS-index-handles')
SOURCES = ('H-T2', 'H-T160', 'H-T1024')


def stats(values):
    return dict(mean=statistics.mean(values), median=statistics.median(values),
                min=min(values), max=max(values), raw=values)


def export(root, destination, source_root):
    root, source_root = root.resolve(strict=True), source_root.resolve(strict=True)
    c = root/'collection'
    report, sup = read(c/'report.json'), read(root/'supervisor-report.json')
    need(report['complete'] and report['protocol']=='iotdbfs-resident-hdf5-handles/1' and report['data_publications']==0, 'incomplete run')
    need(sup['complete'] and sup['resources_restored'] and not sup['cancelled'], 'supervisor failed')
    env = read(c/'environment.json'); live = env['live_database']
    restore = read(root/'resources-restored.json')
    need(restore==read(root/'resources.json'), 'resource journals differ')
    identity = dict(pid=live['pid'], start_ticks=live['process_start_ticks'])
    need(restore['phase']=='closed' and restore['restoration_complete'] and restore['database_restored'] and
         restore['keeper'] is None and not restore.get('cleanup_error') and
         restore['restored_database_identity']==restore['database_original']['identity']==identity, 'restoration differs')
    samples = [read(root/'resource-before.json')]
    samples += [json.loads(x) for x in (root/'resource-samples.jsonl').read_text().splitlines()]
    samples += [read(root/'resource-after.json')]
    need(len(samples)>2, 'periodic samples missing')
    for sample in samples:
        audit_snapshot(sample, live, restore['paths'])
    quiescence = []
    for i in range(3):
        for arm in ARMS:
            block = c/f'c{i}-{arm}'
            closed = read(block/'closed.json')
            need(closed['complete'] and closed['cache_closed'], 'native block not closed')
            for phase in ('before','after'):
                quiescence.append(audit_quiescence(block/f'quiescence-{phase}.json', live))
    closure = read(c/'source-closure.json')
    for relative, digest in closure.items():
        path = (source_root/relative).resolve(strict=True)
        need(source_root in path.parents and sha(path)==digest, 'frozen source changed: '+relative)
    # Independently check current process placement and every extant thread.
    pid = live['pid']; proc = Path('/proc')/str(pid)
    need(int((proc/'stat').read_text().rsplit(') ',1)[1].split()[19])==identity['start_ticks'], 'DataNode restarted')
    original = restore['database_original']
    membership = (proc/'cgroup').read_text().strip().split('::',1)[1]
    need('/sys/fs/cgroup'+membership==original['cgroup'], 'DataNode cgroup not restored')
    affinities = {}
    for path in (proc/'task').iterdir():
        try:
            mask = sorted(os.sched_getaffinity(int(path.name)))
        except ProcessLookupError:
            continue
        need(mask==original['thread_affinities'].get(path.name, original['affinity']), 'thread affinity not restored')
        affinities[path.name] = mask
    need(bool(affinities) and all(not Path(p).exists() for p in restore['paths'].values()), 'resource cleanup incomplete')
    from experiments.lifecycle_table5.qualification import live_environment
    audit_dir = root/'independent-audit'
    audit_dir.mkdir(exist_ok=False)
    after = live_environment(pid, live['rpc_port'], audit_dir)
    for key in ('pid','process_start_ticks','config_sha256','jar_sha256','jvm_memory_flags'):
        need(after[key]==live[key], 'DataNode environment changed: '+key)
    restoration = dict(complete=True, identity=identity, cgroup=membership,
                       thread_count=len(affinities), affinities=affinities, environment_unchanged=True)
    (root/'independent-restoration.json').write_text(json.dumps(restoration,indent=2)+'\n')
    frozen = read(c/'frozen.json'); cases = {x['source_id']:x for x in frozen['cases']}
    rows = [json.loads(x) for x in (c/'sessions.jsonl').read_text().splitlines()]
    warm = [json.loads(x) for x in (c/'warmups.jsonl').read_text().splitlines()]
    keys = [(r['campaign'],r['arm'],r['source_id'],r['q']) for r in rows]
    need(len(keys)==len(set(keys))==81 and set(keys)=={(i,a,s,q) for i in range(3) for a in ARMS for s in SOURCES for q in (1,10,100)}, 'matrix differs')
    need(len(warm)==81 and all(r['q']==1 for r in warm), 'warmup matrix differs')
    for r in rows+warm:
        need(r['complete'] and r['session_wall_measured_directly'] and r['audit_deferred_to_caller'] and
             r['session_wall_ns']==r['session_end_ns']-r['session_begin_ns'] and
             r['query_sum_ns']==sum(o['total_ns'] for o in r['observations']) and
             r['session_wall_ns']>=r['query_sum_ns'], 'session arithmetic differs')
        need(len(r['observations'])==r['q'] and r['source_version']==1 and
             r['source_sha256']==cases[r['source_id']]['sha256'], 'source contract differs')
        oracle = cases[r['source_id']]['oracle']
        for o in r['observations']:
            v = o['validation']
            need(o['oracle_verified'] and v['oracle_verified'] and v['rows']==oracle['rows'] and
                 all(v[k]==oracle[k] for k in ('value_sha256','mask_sha256')), 'oracle mismatch')
            need(o['native_value_read_calls']==1 and o['total_ns']==o['query_end_ns']-o['query_begin_ns']==o['discovery_ns']+o['value_and_delivery_ns'], 'native read or clocks differ')
            if r['arm']!='FS-scan':
                need(o['persistent_index'] and o['metadata_reused'] and o['catalog_lookup_ns']>=0, 'index bypassed')
            need(o['handles_reused']==(r['arm']=='FS-index-handles'), 'handle mode differs')
            if r['arm']=='FS-index-handles':
                need(o['handle_cache_hit']==(r['role']!='first') and 1<=o['handle_cache_entries']<=3, 'cache state differs')
        if r['arm']=='FS-index-handles':
            before, after_cache = r['cache_before'], r['cache_after']
            need(after_cache['opens']-before['opens']==int(r['role']=='first') and
                 after_cache['hits']-before['hits']==r['q']-int(r['role']=='first') and
                 after_cache['evictions']==0 and after_cache['closes']==0, 'cache counter delta differs')
        for resource in (r['resource_before'],r['resource_after']):
            no_oom(resource,'query')
            need(resource['parent_cpu_max']=='200000 100000' and resource['memory.max']=='2147483648' and
                 resource['memory.swap.max']=='0' and resource['cpuset.cpus.effective']=='2-3', 'query resources differ')
    handles = []
    resident_rows = [json.loads(x) for x in (Path(frozen['resident'])/'collection/sessions.jsonl').read_text().splitlines()]
    for i in range(3):
        h = read(c/f'c{i}-FS-index-handles/handles.json')
        need(h['before']==dict(opens=3,hits=339,evictions=0,closes=0,entries=3,capacity=8,closed=False), 'unexpected cache lifetime')
        need(h['after']==dict(opens=3,hits=339,evictions=0,closes=3,entries=0,capacity=8,closed=True) and h['close_ns']>0, 'cache release failed')
        handles.append(h)
        for sid in SOURCES:
            subset = [r for r in rows if r['campaign']==i and r['source_id']==sid]
            paths = {r['source_binding'] for r in subset}
            need(len(paths)==1 and paths=={r['source_binding'] for r in resident_rows if r['campaign']==i and r['source_id']==sid and r['arm']=='FS-index'}, 'native arms/campaigns differ in source')
            # Bounded-memory SHA from the original collector helper, outside measurement.
            from experiments.single_file.section42_20261026.writes import sha_file
            need(sha_file(Path(next(iter(paths))))==cases[sid]['sha256'], 'source content changed')
            for arm in ARMS:
                ww = [r for r in warm if r['campaign']==i and r['source_id']==sid and r['arm']==arm]
                need([r['role'] for r in ww]==['first','warmup','warmup'], 'warmup sequence differs')
                need(max(r['session_end_ns'] for r in ww)<min(r['session_begin_ns'] for r in subset if r['arm']==arm), 'warmup boundary differs')
        for arm in ARMS[1:]:
            status = read(c/f'c{i}-{arm}/catalog-status.json')
            need(status['sources']==15 and status['units']==13869, 'catalog differs')
    cells = []
    for sid in SOURCES:
        for q in (1,10,100):
            for arm in ARMS:
                rr = sorted((r for r in rows if r['source_id']==sid and r['arm']==arm and r['q']==q), key=lambda r:r['campaign'])
                cells.append(dict(source=sid,q=q,arm=arm,
                    per_query_ms=stats([r['session_wall_ns']/1e6/q for r in rr]),
                    session_ms=stats([r['session_wall_ns']/1e6 for r in rr])))
    first = [dict(source=sid,arm=arm,first_ms=stats([r['session_wall_ns']/1e6 for r in warm if r['source_id']==sid and r['arm']==arm and r['role']=='first'])) for sid in SOURCES for arm in ARMS]
    ratios = [dict(source=sid,q=q,reopen_over_handles=stats([
        next(r['session_wall_ns'] for r in rows if (r['campaign'],r['source_id'],r['q'],r['arm'])==(i,sid,q,'FS-index-reopen')) /
        next(r['session_wall_ns'] for r in rows if (r['campaign'],r['source_id'],r['q'],r['arm'])==(i,sid,q,'FS-index-handles'))
        for i in range(3)])) for sid in SOURCES for q in (1,10,100)]
    result = dict(complete=True,records=len(rows),queries=sum(r['q'] for r in rows),first_and_warmup_queries=len(warm),
        cells=cells,first_cells=first,paired_ratios=ratios,handles=handles,
        audit=dict(resource_samples=len(samples),quiescence=quiescence,source_files_verified=len(closure),
                   restoration_verified=True,thread_count=len(affinities)),
        scope='Native-only HDF5 follow-up with paired reopen control; original IoTDB-FS results remain unchanged')
    destination.mkdir(parents=True,exist_ok=False)
    (destination/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    flat = [dict(campaign=r['campaign'],source=r['source_id'],q=r['q'],arm=r['arm'],role=r['role'],session_ms=r['session_wall_ns']/1e6,per_query_ms=r['session_wall_ns']/1e6/r['q']) for r in rows+warm]
    with (destination/'sessions.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(flat[0]));writer.writeheader();writer.writerows(flat)
    manifest=dict(protocol=report['protocol'],root=str(root),source_root=str(source_root),frozen=frozen,
        artifacts={str(p.relative_to(root)):sha(p) for p in [c/'sessions.jsonl',c/'warmups.jsonl',c/'source-closure.json',c/'environment.json',root/'supervisor-report.json',root/'resources-restored.json',root/'independent-restoration.json']})
    (destination/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('destination',type=Path);p.add_argument('--source-root',type=Path,required=True)
    a=p.parse_args();r=export(a.root,a.destination,a.source_root)
    print(json.dumps({k:r[k] for k in ('complete','records','queries','first_and_warmup_queries')}))
