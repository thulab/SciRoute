"""Paired persistent/fresh query clients inside the same eight accepted-A mounts.

The two lifetimes execute the identical child loop. Only process reuse differs.
Driver IPC, startup and diagnostics are outside the original query clocks.
"""
from __future__ import annotations
from contextlib import ExitStack, redirect_stdout
import gc
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import time

from . import client, hdf5_batch_client, run, telemetry
from .hdf5_native_ab import ARMS, CONFIGS, ProtocolSpec, REPEATS, WARMUPS
from fs_index.native import NativeContext
from .tail_controls import TailControlRunner, BASELINE_SHA
from experiments.identity.model import build_run, digest
from experiments.identity.registry import file_sha, read_json
from experiments.cross_version.resource_limited.resources import atomic_json

PROTOCOL = 'iotdbfs-fixed-client-lifetime-pair/1'
MODES = ('persistent', 'fresh')
WORK = 'experiments/cross_version/work/fs-client-lifetime-paired-20261023'
SPEC = ProtocolSpec(protocol=PROTOCOL, module='experiments.fixed_matrix.client_lifetime_pair',
    description=__doc__, baseline_sha=BASELINE_SHA, product_diff=frozenset(),
    required_regression_tests=(), extra_closure=(
        'experiments/fixed_matrix/tests/test_client_lifetime_pair.py',
        WORK+'/run_control.py', WORK+'/experiment-plan.json',
        WORK+'/instrumentation/pid_cpu_sampler.py',
        WORK+'/instrumentation/service_gate_480.py',
        WORK+'/instrumentation/matched-load-plan.json'))


def identity(pid=None):
    pid = os.getpid() if pid is None else pid
    data = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, start_ticks=int(data[19]))


def stamp():
    return dict(monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns())


def query_order(campaign, configuration, block, repeat):
    arms = ARMS if (repeat+campaign+CONFIGS.index(configuration)) % 2 == 0 else ARMS[::-1]
    return [(arm, mode) for arm in arms for mode in
        (MODES if (repeat+block+ARMS.index(arm)) % 2 == 0 else MODES[::-1])]


def child_loop():
    """No driver-only setup in either mode; no data arrays cross driver IPC."""
    output = sys.stdout
    native = context = state = None
    me = identity()
    def reply(value):
        output.write(json.dumps(dict(ok=True, **value), separators=(',', ':'),
                                allow_nan=False)+'\n')
        output.flush()
    def cancel(number, frame):
        raise SystemExit('query child cancelled')
    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(sig, cancel)
    # Library/import diagnostics cannot corrupt the private driver protocol.
    with redirect_stdout(sys.stderr):
        reply(dict(event='ready', client_identity=me, **stamp()))
        try:
            for line in sys.stdin:
                request = json.loads(line)
                op = request['op']
                if op == 'begin':
                    run.need(native is None and state is None, 'previous client block still open')
                    state = request
                    context = {k:Path(v) if k in ('root','mount','socket') else v
                               for k,v in request['context'].items()}
                    native = NativeContext(Path(request['library']), dict(request['cfg'], build_index=False))
                    plan = native.explain('index', request['binding']['native_selection'],
                                          client.PREDICATE, request['binding']['scope'])
                    environment = telemetry.environment(Path(request['repo']),
                        artifact_paths=[Path(request['library']),Path(request['binary'])],
                        process_roles=dict(client=me['pid'],daemon=context['pid'],
                                           datanode=request['datanode_pid']),
                        data_path=Path(request['data_root']))
                    reply(dict(event='block_ready', client_identity=me,
                        native_context_id=f"{me['pid']}:{me['start_ticks']}:{request['block_ordinal']}",
                        query_plan=plan, environment=environment, **stamp()))
                elif op == 'query':
                    run.need(native is not None and state is not None, 'client block not open')
                    arm = request['arm']; run.need(arm in ARMS, 'unknown arm')
                    group, binding = state['group'], state['binding']
                    before = telemetry.sample(state['resource_paths']); run.validate_sample(before)
                    events = []
                    def callback(phase, info):
                        events.append((phase, info.get('generation'), info.get('collected'),
                            info.get('uncollectable'), time.monotonic_ns()))
                    context['profile_offset'] = request['profile_offset']
                    gc_before = list(gc.get_count())
                    call_begin = stamp()
                    gc.callbacks.append(callback)
                    try:
                        if arm == 'FS-index':
                            row = client.query_native(native, arm, group, binding['scope'],
                                binding['native_selection'], binding['expected_matches'])
                        else:
                            # The daemon has one profile stream shared by the
                            # sequential clients. The driver owns its cursor.
                            row = hdf5_batch_client.query(context, group, binding['scope'],
                                binding['physical_versions'], binding['expected_matches'], batch=True)
                    finally:
                        gc.callbacks.remove(callback)
                    call_end = stamp()
                    gc_after = list(gc.get_count())
                    after = telemetry.sample(state['resource_paths']); run.validate_sample(after)
                    row.update(resource_before=before, resource_after=after,
                        client_identity=me, client_mode=state['client_mode'],
                        block_ordinal=state['block_ordinal'],
                        native_context_id=f"{me['pid']}:{me['start_ticks']}:{state['block_ordinal']}",
                        client_call_begin=call_begin, client_call_end=call_end,
                        gc_count_before=gc_before, gc_count_after=gc_after,
                        gc_events=[dict(phase=p,generation=g,collected=c,uncollectable=u,
                                        monotonic_ns=t) for p,g,c,u,t in events],
                        gc_scope='original query-call envelope incl untimed validation/profile/stats')
                    reply(dict(event='query', row=row,
                        profile_offset=context.get('profile_offset', request['profile_offset'])))
                elif op == 'end':
                    run.need(native is not None, 'client block not open')
                    native.close(); native = context = state = None
                    reply(dict(event='block_closed', client_identity=me, **stamp()))
                elif op == 'stop':
                    run.need(native is None, 'cannot stop with open NativeContext')
                    reply(dict(event='stopped', client_identity=me, **stamp()))
                    return
                else:
                    raise RuntimeError('unknown child operation')
        finally:
            if native is not None:
                native.close()


class QueryChild:
    """One request at a time; cancellation closes only this owned query child."""
    def __init__(self, args, label, events):
        self.events = events; self.closed = False; self.blocks = 0
        self.pending = bytearray()
        self.error = (args.work/(label+'.private.stderr')).open('x')
        try:
            self.process = subprocess.Popen([sys.executable,'-B','-u','-m',SPEC.module,'--query-child'],
                cwd=args.repo, env=run.worker_environment(), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=self.error, bufsize=0)
        except BaseException:
            self.error.close()
            raise
        try:
            response = self.receive(120)
            self.identity = response['client_identity']
            run.need(response['event']=='ready' and self.identity==identity(self.process.pid),
                     'query child initial identity differs')
            self.events('client_started', label=label, client_identity=self.identity)
        except BaseException:
            self.close(False)
            raise

    def receive(self, timeout):
        deadline = time.monotonic()+timeout
        while b'\n' not in self.pending:
            remaining = deadline-time.monotonic()
            run.need(remaining>0, 'query child reply deadline; retain run without retry')
            ready,_,_ = select.select([self.process.stdout],[],[],remaining)
            run.need(ready, 'query child reply timeout; retain run without retry')
            chunk = os.read(self.process.stdout.fileno(),65536)
            run.need(chunk, 'query child exited before reply')
            self.pending.extend(chunk)
            run.need(len(self.pending)<=16*1024*1024, 'query child reply too large')
        line,_,rest = self.pending.partition(b'\n');self.pending=bytearray(rest)
        response = json.loads(line)
        run.need(response.get('ok') is True, 'query child returned failure')
        return response

    def request(self, value, timeout=420):
        run.need(self.process.poll() is None, 'query child already exited')
        payload=(json.dumps(value,separators=(',',':'),allow_nan=False)+'\n').encode()
        view=memoryview(payload)
        while view:
            written=self.process.stdin.write(view)
            run.need(written is not None and written>0, 'query child request pipe closed')
            view=view[written:]
        self.process.stdin.flush()
        return self.receive(timeout)

    def close(self, graceful=True):
        if self.closed:
            return
        try:
            if self.process.poll() is None:
                if graceful:
                    try:
                        response = self.request(dict(op='stop'),30)
                        run.need(response.get('event')=='stopped','query child stop reply differs')
                    except BaseException:
                        self.process.terminate()
                else:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    # This process owns no service restoration, mount or daemon.
                    self.process.kill(); self.process.wait(timeout=10)
            status = self.process.returncode
            self.events('client_exited', client_identity=getattr(self,'identity',
                dict(pid=self.process.pid)), returncode=status)
            if graceful:
                run.need(status==0, 'query child did not exit cleanly')
        finally:
            for stream in (self.process.stdin,self.process.stdout,self.error):
                stream.close()
            self.closed = self.process.poll() is not None


def close_children(children):
    """Attempt every owned child even if another child's cleanup/logging fails."""
    errors=[]
    for child in children:
        try:
            child.close(False)
        except BaseException as error:
            errors.append(error)
            try:
                child.close(False)
            except BaseException as retry_error:
                errors.append(retry_error)
    if errors:
        raise RuntimeError('owned query-child cleanup failed; inspect private evidence') from errors[0]


class LifetimePairRunner(TailControlRunner):
    def __init__(self):
        self.spec = SPEC

    def expected_rows(self, args):
        return 2*super().expected_rows(args)

    def source_proof(self,args):
        proof = super().source_proof(args)
        proof['diagnostic_control'].update(kind='paired-client-lifetimes',
            client_lifetimes=list(MODES), client_lifetime='persistent1/fresh8 query children',
            shared_mount_per_position=True, concurrent_timed_queries=False,
            schedule='AB/BA/BA/AB slots; per-arm adjacent lifetime pairs; balanced first mode',
            query_clock_changed=False)
        return proof

    def collect(self,args,execution,bindings,registry,frozen,report,env,paired):
        run.need(len(execution)==1, 'paired lifetime requires one sealed N128 case')
        paths=read_json(args.work/'resource-paths.json');count=0
        plan=execution[0];group=plan['group'];gid=group['group_id'];binding=bindings[gid]
        writer=args.repo.parent/'public-arrow-baseline/hdf5-h09-20261002/h09-migration-z7sqhzpa/namespace.lock'
        report.update(diagnostic_control='paired-client-lifetimes',query_clients=[])
        with (args.work/'client-events.jsonl').open('x') as stream:
            def event(kind, **data):
                stream.write(json.dumps(dict(event=kind,driver_identity=identity(),
                    **stamp(),**data),sort_keys=True)+'\n');stream.flush()
            event('collect_begin',query_clock_changed=False)
            persistent=QueryChild(args,'persistent-client',event)
            report['query_clients'].append(dict(client_mode='persistent',**persistent.identity))
            try:
                for campaign in range(4):
                    for config in self.configuration_order(campaign):
                        block_no=sum(len(v) for v in binding['run_ids'].values())
                        block=plan['execution_root']/('campaign-'+str(campaign))/config
                        block.mkdir(parents=True)
                        journal=block/'owner';shutil.copytree(plan['journal'],journal)
                        run.need(run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],
                                 'private journal copy differs')
                        launch=self.launcher(block/'launcher',args.binary,BASELINE_SHA,
                            plan['item']['namespace'],journal,plan)
                        atomic_json(block/'launcher.receipt.json',launch)
                        before,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-before')
                        run.need(before==plan['scientific_head'],'head changed before paired block')
                        fresh=QueryChild(args,'fresh-client-'+str(block_no),event)
                        try:
                            report['query_clients'].append(dict(client_mode='fresh',block_ordinal=block_no,**fresh.identity))
                            children=dict(persistent=persistent,fresh=fresh)
                            event('mount_begin',block_ordinal=block_no,campaign=campaign,configuration=config)
                            with paired.sf.private_mount(Path(launch['path']),block/'mount',env,writer,profile=True) as context:
                                run.need(file_sha(Path('/proc')/str(context['pid'])/'exe')==BASELINE_SHA,
                                         'paired daemon binary differs')
                                actual=read_json(context['root']/'profile.launch.json')
                                run.need(actual['settings']==launch['settings'],'paired actual daemon settings differ')
                                event('mount_ready',block_ordinal=block_no,daemon_pid=context['pid'],
                                      startup_ns=context['startup_ns'])
                                mount=dict(campaign=campaign,configuration=config,block_ordinal=block_no,
                                    daemon_pid=context['pid'],startup_ns=context['startup_ns'],
                                    product_sha256=BASELINE_SHA,client_modes={})
                                plan['item']['mounts'].append(mount)
                                profile_offset=context.get('profile_offset',0)
                                begin_order=MODES if block_no%2==0 else MODES[::-1]
                                opened=[]
                                try:
                                    for mode in begin_order:
                                        child=children[mode]
                                        response=child.request(dict(op='begin',client_mode=mode,
                                            block_ordinal=block_no,repo=str(args.repo),binary=str(args.binary),
                                            library=str(args.library),cfg=plan['cfg'],group=group,binding=binding,
                                            resource_paths=paths,data_root=str(plan['root']),
                                            datanode_pid=int((args.repo.parent/'runtime/run/datanode.pid').read_text()),
                                            context={k:str(context[k]) if k in ('root','mount','socket') else context[k]
                                                     for k in ('root','mount','socket','pid')}))
                                        run.need(response['event']=='block_ready' and response['client_identity']==child.identity,
                                                 'query child block identity differs')
                                        opened.append(mode);child.blocks+=1
                                        atomic_json(block/('environment-'+mode+'.json'),response['environment'])
                                        atomic_json(block/('query-plan-'+mode+'.json'),response['query_plan'])
                                        mount['client_modes'][mode]=dict(client_identity=child.identity,
                                            native_context_id=response['native_context_id'],
                                            environment_sha256=file_sha(block/('environment-'+mode+'.json')))
                                        event('client_block_ready',block_ordinal=block_no,client_mode=mode,
                                            client_identity=child.identity,native_context_id=response['native_context_id'])
                                    # Existing restoration verifier inspects the daemon identity here.
                                    shutil.copyfile(block/'environment-persistent.json',block/'environment.json')
                                    mount['environment_sha256']=file_sha(block/'environment.json')
                                    binding['run_ids'].setdefault(str(campaign),{})[config]={}
                                    for mode in MODES:
                                        binding['run_ids'][str(campaign)][config][mode]={}
                                        for arm in ARMS:
                                            model=build_run(registry.get_case(binding['case_id']),execution=str(block),arm=arm,
                                                implementation=dict(binary_sha256=BASELINE_SHA if arm=='IoTDB-FS' else run.NATIVE_SHA,
                                                    runner_sha256=digest(frozen['sources']),engine='paired client lifetime diagnostic'),
                                                clock=dict(kind='query-child-owned-full-query',d='entry through discovery delivery',
                                                    q='discovery through owned Float32/mask',driver_IPC='outside',profile_oracle='outside'),
                                                cache=dict(protocol=PROTOCOL,configuration=config,client_mode=mode,
                                                    native_helper=plan['cfg'] if arm=='FS-index' else None,
                                                    daemon_settings=launch['settings'] if arm=='IoTDB-FS' else None,batch=True),
                                                environment=dict(resource_id=frozen['resource_id'],
                                                    process_receipt_sha256=mount['client_modes'][mode]['environment_sha256']),
                                                replication=dict(campaign=campaign,block_ordinal=block_no,
                                                    warmup=WARMUPS,measured=10,client_mode=mode,
                                                    schedule='AB/BA/BA/AB; per-arm adjacent L/F pairs; balanced mode first'))
                                            bound=registry.register_run(model)
                                            binding['run_ids'][str(campaign)][config][mode][arm]=bound['run_id']
                                    atomic_json(args.work/'bindings.json',bindings)
                                    for repeat in range(REPEATS):
                                        for arm,mode in query_order(campaign,config,block_no,repeat):
                                            started=stamp()
                                            response=children[mode].request(dict(op='query',arm=arm,
                                                profile_offset=profile_offset,repeat=repeat))
                                            ended=stamp();row=response['row']
                                            run.need(response['event']=='query' and row['client_identity']==children[mode].identity,
                                                     'query reply identity differs')
                                            next_offset=response['profile_offset']
                                            run.need(type(next_offset) is int and
                                                (next_offset>profile_offset if arm=='IoTDB-FS' else next_offset==profile_offset),
                                                'shared profile cursor did not advance exactly for IoTDB call')
                                            profile_offset=next_offset
                                            if arm=='IoTDB-FS':self.validate_row(row,group,config)
                                            row.update(group_id=gid,corpus_id=binding['corpus_id'],case_id=binding['case_id'],
                                                oracle_sha256=binding['oracle_sha256'],
                                                run_id=binding['run_ids'][str(campaign)][config][mode][arm],
                                                campaign=campaign,configuration=config,arm=arm,repeat=repeat,
                                                phase='warmup' if repeat<WARMUPS else 'measurement',ok=True,
                                                resource_id=frozen['resource_id'],product_sha256=BASELINE_SHA,
                                                environment_sha256=mount['client_modes'][mode]['environment_sha256'],
                                                driver_ipc_begin=started,driver_ipc_end=ended,
                                                driver_identity=identity(),profile_offset_after=profile_offset)
                                            run.append(args.work/'queries.jsonl',row);count+=1
                                            report['query_records']=count
                                        atomic_json(args.work/'report.json',report)
                                except BaseException:
                                    # Children own no mounts or service. Reap
                                    # both before the shared mount unwinds.
                                    close_children(children.values())
                                    raise
                                else:
                                    # Both client NativeContexts close before the shared mount.
                                    try:
                                        for mode in reversed(opened):
                                            response=children[mode].request(dict(op='end'),30)
                                            run.need(response['event']=='block_closed','client NativeContext close differs')
                                            event('client_block_closed',block_ordinal=block_no,
                                                client_mode=mode,client_identity=children[mode].identity)
                                        fresh.close()
                                    except BaseException:
                                        close_children(children.values())
                                        raise
                                    event('mount_stop_begin',block_ordinal=block_no)
                            event('mount_stopped',block_ordinal=block_no)
                        finally:
                            fresh.close(False)
                        after,_=run.head(args.inspector,plan['item']['namespace'],env,block/'head-after')
                        run.need(after==before and run.infrastructure.journal_snapshot(journal)==plan['journal_digest'],
                                 'paired block changed scientific head/journal')
                        run.need(file_sha(plan['root']/'index.sqlite')==plan['prep']['index_sha256'],
                                 'paired block changed FS-index')
                        mount.update(complete=True,scientific_head_unchanged=True,journal_unchanged=True)
                        atomic_json(args.work/'report.json',report)
            finally:
                persistent.close(count==self.expected_rows(args))
                event('collect_end',queries_observed=count)
        return count

    def analyze(self,path,bindings,selected,resource_id,candidate_sha):
        rows=[json.loads(s) for s in Path(path).read_text().splitlines()]
        wanted=[(c,f,m,a,r) for block,(c,f) in enumerate((c,f) for c in range(4)
            for f in self.configuration_order(c)) for r in range(REPEATS)
            for a,m in query_order(c,f,block,r)]
        run.need([(r['campaign'],r['configuration'],r['client_mode'],r['arm'],r['repeat']) for r in rows]==wanted,
                 'paired lifetime complete480 exact order required; no exclusions')
        run.need(all(r['resource_id']==resource_id and r['product_sha256']==BASELINE_SHA and
                     r['total_ns']==r['discovery_ns']+r['value_ns'] and
                     all(type(r[k]) is int and r[k]>0 for k in ('total_ns','discovery_ns','value_ns')) and
                     r['selected_units']==2 and r['owned_output_bytes']==40960 and
                     all(r.get(k) is True for k in ('ok','oracle_verified','mode_verified'))
                     for r in rows),'paired lifetime row gate failed')
        binding=next(iter(bindings.values()))
        run.need(all(all(r[k]==binding[k] for k in ('corpus_id','case_id','oracle_sha256')) and
                     r['run_id']==binding['run_ids'][str(r['campaign'])][r['configuration']][r['client_mode']][r['arm']]
                     for r in rows),'paired lifetime science/run binding differs')
        for row in rows:
            run.need(row['phase']==('warmup' if row['repeat']<WARMUPS else 'measurement'),
                     'paired phase label differs')
            for side in ('resource_before','resource_after'):
                run.validate_sample(row[side])
                run.need(row[side]['sampler']['pid']==row['client_identity']['pid'],
                         'resource snapshot not owned by actual query child')
            run.need(row['resource_after']['monotonic_ns']>row['resource_before']['monotonic_ns'] and
                     row['client_call_end']['monotonic_ns']-row['client_call_begin']['monotonic_ns']>=row['total_ns'],
                     'paired child clock envelopes differ')
            if row['arm']=='IoTDB-FS':self.validate_row(row,selected[0],row['configuration'])
        return dict(complete=True,protocol=PROTOCOL,diagnostic_only=True,rows=len(rows),
            statistic='All paired rows retained. Independent offline audit compares per-mode tail ratios.',
            optimization_claim=False)


runner=LifetimePairRunner()
if __name__=='__main__':
    if sys.argv[1:]==['--query-child']:
        child_loop()
    else:
        runner.main()
