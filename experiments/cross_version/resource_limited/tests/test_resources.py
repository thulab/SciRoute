"""Synthetic kernel/process model only: never writes a real cgroup or affinity."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.cross_version.resource_limited.resources import (
    CgroupV2Resources, ResourceError, LinuxOps, cpus, cpu_text, enter_leaf_exec,
)


class FakeKernel:
    platform = 'linux'
    euid = staticmethod(lambda: 0)

    def __init__(self, root):
        self.root = root
        self.files = {}; self.directories = set(); self.processes = {}; self.writes=[];self.removed=[]
        self.current_pid=10; self.keep=900; self.fail_write=None;self.invalid_partition=False
        self.root.mkdir(); self.add_group(self.root, root=True)
        self.files.pop(self.root/'cgroup.type')
        self.files.pop(self.root/'cpuset.cpus.partition')
        self.add_group(self.root/'original.scope')
        self.add_process(10,'/',database=False)
        self.add_process(100,'/original.scope',database=True,threads=[100,101,102])
        self.add_process(200,'/',database=False,threads=[200,201])
        self.files[self.root/'cgroup.subtree_control']=''
        self.recompute()

    def manager_pid(self): return self.current_pid
    def is_cgroup2_mount(self, root): return root==self.root
    def exists(self,path): return Path(path) in self.files or Path(path) in self.directories
    def groups(self,root): return sorted(self.directories)
    def read(self,path):
        return self.files[Path(path)] if Path(path) in self.files else Path(path).read_text().strip()
    def add_group(self,path,root=False):
        path=Path(path);self.directories.add(path)
        fields={'cgroup.type':'domain','cgroup.procs':'','cgroup.threads':'','cgroup.events':'populated 0\nfrozen 0',
            'cgroup.controllers':'cpu cpuset memory','cgroup.subtree_control':'','cpuset.cpus':'','cpuset.mems':'',
            'cpuset.cpus.effective':'0-31','cpuset.mems.effective':'0','cpuset.cpus.exclusive':'',
            'cpuset.cpus.exclusive.effective':'','cpuset.cpus.partition':'member','cpu.max':'max 100000',
            'memory.max':'max','memory.current':'0','memory.peak':'0','memory.events':'oom 0\noom_kill 0',
            'memory.swap.current':'0','memory.swap.max':'max','cpu.stat':'usage_usec 10\nnr_throttled 0',
            'cpu.pressure':'some avg10=0.00 avg60=0.00 avg300=0.00 total=0',
            'memory.pressure':'some avg10=0.00 avg60=0.00 avg300=0.00 total=0'}
        if root:fields['cpuset.cpus.isolated']=''
        self.files.update({path/k:v for k,v in fields.items()})
    def mkdir(self,path):
        if path in self.directories:raise FileExistsError(path)
        self.add_group(path);self.recompute()
    def rmdir(self,path):
        if self.read(path/'cgroup.procs') or any(x.parent==path for x in self.directories):
            raise OSError('not empty')
        self.removed.append(path);self.directories.remove(path)
        self.files={k:v for k,v in self.files.items() if k.parent!=path};self.recompute()
    def write(self,path,value):
        path=Path(path);value=str(value);self.writes.append((path,value))
        if self.fail_write and self.fail_write(path,value):raise OSError('synthetic write failure')
        if path.name=='cgroup.subtree_control':
            controls=set(self.files[path].split())
            for cmd in value.split():
                if cmd[0]=='+':controls.add(cmd[1:])
                else:controls.discard(cmd[1:])
            self.files[path]=' '.join(sorted(controls))
        elif path.name=='cgroup.procs':
            pid=int(value);self.processes[pid]['group']='/'+str(path.parent.relative_to(self.root))
            if path.parent==self.root:self.processes[pid]['group']='/'
        else:self.files[path]=value
        if path.name=='cpuset.cpus.partition' and value in ('root','isolated') and self.invalid_partition:
            self.files[path]=value+' invalid (synthetic)'
        self.recompute()
    def recompute(self):
        isolated=set();reserved=set()
        for group in self.directories:
            partition=self.files.get(group/'cpuset.cpus.partition')
            if partition in ('root','isolated'):reserved|=cpus(self.files[group/'cpuset.cpus'])
            if partition=='isolated':isolated|=cpus(self.files[group/'cpuset.cpus'])
        self.files[self.root/'cpuset.cpus.isolated']=cpu_text(isolated)
        for group in sorted(self.directories,key=lambda p:len(p.parts)):
            if group==self.root:effective=set(range(32))-reserved
            elif self.files[group/'cpuset.cpus.partition'] in ('root','isolated'):effective=cpus(self.files[group/'cpuset.cpus'])
            else:
                parent=cpus(self.files[group.parent/'cpuset.cpus.effective'])
                requested=cpus(self.files[group/'cpuset.cpus']);effective=parent & requested if requested else parent
            self.files[group/'cpuset.cpus.effective']=cpu_text(effective)
            self.files[group/'cpuset.cpus.exclusive.effective']=self.files[group/'cpuset.cpus.exclusive'] or (
                self.files[group/'cpuset.cpus'] if self.files.get(group/'cpuset.cpus.partition') in ('root','isolated') else '')
            members=[p for p,v in self.processes.items() if self.group_path(v['group'])==group]
            self.files[group/'cgroup.procs']='\n'.join(map(str,sorted(members)))
            self.files[group/'cgroup.threads']='\n'.join(str(t) for p in members for t in self.processes[p]['threads'])
            occupied=any(self.group_path(v['group']).is_relative_to(group) for v in self.processes.values())
            self.files[group/'cgroup.events']='populated %d\nfrozen 0'%occupied
    def group_path(self,name):return self.root/name.lstrip('/')
    def add_process(self,pid,group,database=False,threads=None):
        self.processes[pid]=dict(group=group,database=database,start=pid*11,threads=threads or [pid],
            masks={t:set(range(32)) for t in (threads or [pid])})
        self.recompute()
    def exit(self,pid):self.processes.pop(pid);self.recompute()
    def identity(self,pid):
        if pid not in self.processes:raise ProcessLookupError(pid)
        return {'pid':pid,'start_ticks':self.processes[pid]['start']}
    def cgroup(self,pid):return self.processes[pid]['group']
    def is_database(self,pid):return self.processes[pid]['database']
    def task_ids(self,pid):return list(self.processes[pid]['threads'])
    def owner(self,tid):return next(v for v in self.processes.values() if tid in v['threads'])
    def affinity(self,tid):
        p=self.owner(tid);return p['masks'][tid] & cpus(self.files[self.group_path(p['group'])/'cpuset.cpus.effective'])
    def set_affinity(self,tid,values):self.owner(tid)['masks'][tid]=set(values)
    def spawn_keeper(self,seconds):
        self.keep+=1;self.add_process(self.keep,self.cgroup(self.current_pid));return self.keep
    def stop_keeper(self,identity):
        if self.identity(identity['pid'])!=identity:raise ResourceError('keeper identity mismatch')
        self.exit(identity['pid'])


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.root=self.base/'cgroup';self.kernel=FakeKernel(self.root)
        self.cpu=self.base/'cpu';self.cpu.mkdir();(self.cpu/'online').write_text('0-31')
        self.pidfile=self.base/'datanode.pid';self.pidfile.write_text('100')
        self.resource=CgroupV2Resources([30,31],self.base/'resources.json',cgroup_root=self.root,
            cpu_root=self.cpu,database_pid_file=self.pidfile,ops=self.kernel)

    def test_create_proves_one_shared_cpu_budget_and_distinct_memory_leaves(self):
        snap=self.resource.create()
        self.assertEqual(self.resource.state['protocol'],'iotdbfs-cgroup-ratio/2')
        self.assertEqual(snap['groups']['parent']['cpuset.cpus.partition'],'root')
        self.assertEqual(cpus(snap['groups']['parent']['cpuset.cpus.exclusive.effective']),{30,31})
        self.assertEqual(self.kernel.read(self.root/'cpuset.cpus.isolated'),'')
        self.assertEqual(self.kernel.affinity(10),set(range(30)))
        self.assertEqual(snap['groups']['parent']['cpu.max'],'200000 100000')
        self.assertEqual(snap['groups']['query']['memory.max'],str(2*1024**3))
        self.assertEqual(snap['groups']['database']['memory.max'],'max')
        self.assertEqual(self.kernel.cgroup(100),'/'+self.resource.name+'/database')
        self.assertEqual(self.kernel.cgroup(10),'/')
        for tid in (100,101,102):self.assertEqual(self.kernel.affinity(tid),{30,31})
        # Exclusive partition CPUs leave the host but retain balancing inside it.
        self.assertEqual(self.resource.state['database_original']['affinity'],list(range(32)))
        keeper=self.resource.state['keeper']['pid']
        self.assertEqual(self.kernel.cgroup(keeper),'/original.scope')
        self.resource.close()
        self.assertEqual(self.kernel.cgroup(100),'/original.scope')
        self.assertTrue(all(self.kernel.affinity(t)==set(range(32)) for t in (100,101,102)))
        self.assertEqual(self.kernel.read(self.root/'cgroup.subtree_control'),'')
        self.assertEqual(self.kernel.read(self.root/'cpuset.cpus.isolated'),'')
        self.assertTrue(json.loads(self.resource.journal.read_text())['restoration_complete'])

    def test_restarted_database_pid_from_runtime_pidfile_returns_to_preserved_original(self):
        self.resource.create();keeper=self.resource.state['keeper']['pid'];self.kernel.exit(100)
        # This is a synthetic replacement JVM already born in DB leaf by trampoline.
        self.kernel.add_process(110,'/'+self.resource.name+'/database',True,[110,111])
        self.pidfile.write_text('110');self.resource.attach_database(110)
        self.assertIn(keeper,self.kernel.processes)
        self.resource.close()
        self.assertEqual(self.kernel.cgroup(110),'/original.scope')
        self.assertEqual(self.kernel.affinity(111),set(range(32)))
        self.assertNotIn(keeper,self.kernel.processes)

    def test_never_remove_live_query_or_kill_it_cleanup_can_retry_after_exit(self):
        self.resource.create();self.resource.attach_query(200)
        with self.assertRaisesRegex(ResourceError,'live processes'):self.resource.close()
        self.assertIn(200,self.kernel.processes);self.assertNotIn(self.resource.query,self.kernel.removed)
        self.assertEqual(self.kernel.cgroup(100),'/original.scope')
        self.assertIn('cleanup_blocked',self.resource.events.read_text())
        self.kernel.exit(200);self.resource.close()
        self.assertFalse(self.kernel.exists(self.resource.parent));self.resource.close()

    def test_preflight_rejects_overlap_invalid_partition_missing_isolation_or_unavailable_cpu(self):
        for kind in ('exclusive','invalid','interface','offline'):
            with self.subTest(kind=kind):
                other=self.root/'other';self.kernel.add_group(other)
                self.kernel.files[other/'cpuset.cpus.exclusive']='30' if kind=='exclusive' else ''
                self.kernel.files[other/'cpuset.cpus.partition']='isolated invalid (test)' if kind=='invalid' else 'member'
                if kind=='interface':self.kernel.files.pop(self.root/'cpuset.cpus.isolated')
                if kind=='offline':(self.cpu/'online').write_text('0-29')
                before=len(self.kernel.writes)
                with self.assertRaises(ResourceError):self.resource.inspect()
                self.assertEqual(len(self.kernel.writes),before)
                self.kernel.files[self.root/'cpuset.cpus.isolated']='';(self.cpu/'online').write_text('0-31')
                self.kernel.files[other/'cpuset.cpus.partition']='member';self.kernel.files[other/'cpuset.cpus.exclusive']=''

    def test_partition_failure_restores_without_touching_foreign_group(self):
        self.kernel.fail_write=lambda path,value:path==self.resource.parent/'cpu.max'
        with self.assertRaises(OSError):self.resource.create()
        self.kernel.fail_write=None;self.resource.close()
        self.assertEqual(self.kernel.cgroup(100),'/original.scope')
        self.assertTrue(self.kernel.exists(self.root/'original.scope'))
        self.assertEqual(self.kernel.affinity(100),set(range(32)))

    def test_new_invalid_partition_or_changed_quota_blocks_measurement(self):
        self.resource.create();self.kernel.files[self.resource.parent/'cpuset.cpus.partition']='root invalid (hotplug)'
        with self.assertRaisesRegex(ResourceError,'became invalid'):self.resource.snapshot()
        self.kernel.files[self.resource.parent/'cpuset.cpus.partition']='root'
        self.kernel.files[self.resource.parent/'cpu.max']='100000 100000'
        with self.assertRaisesRegex(ResourceError,'quota changed'):self.resource.attach_query(200)
        self.resource.close()

    def test_other_balanced_root_reservation_is_rejected_before_writes(self):
        other=self.root/'balanced-job';self.kernel.add_group(other)
        self.kernel.files[other/'cpuset.cpus']='30'
        self.kernel.files[other/'cpuset.cpus.partition']='root';self.kernel.recompute()
        before=list(self.kernel.writes)
        with self.assertRaises(ResourceError):self.resource.inspect()
        self.assertEqual(self.kernel.writes,before)
        self.assertEqual(self.kernel.read(self.root/'cpuset.cpus.isolated'),'')

    def test_active_and_preexec_share_strict_balanced_policy_checks(self):
        self.resource.create();self.kernel.current_pid=200
        changes=[(self.resource.parent,'cpuset.cpus.partition','isolated'),
            (self.resource.parent,'cpuset.cpus.exclusive.effective','30'),
            (self.resource.parent,'cpu.max','100000 100000'),
            (self.resource.query,'memory.max','max'),
            (self.resource.database,'memory.max','1024'),
            (self.resource.query,'cpuset.cpus.partition','root'),
            (self.resource.database,'cpuset.cpus.partition','isolated'),
            (self.resource.query,'cpu.max','100000 100000')]
        for group,field,value in changes:
            with self.subTest(group=group.name,field=field):
                path=group/field;original=self.kernel.files[path];self.kernel.files[path]=value
                with self.assertRaises(ResourceError):self.resource.snapshot()
                before=list(self.kernel.writes)
                with self.assertRaises(ResourceError):enter_leaf_exec(self.resource.database,['true'],
                    journal_path=self.resource.journal,cgroup_root=self.root,ops=self.kernel)
                self.assertEqual(self.kernel.writes,before)
                self.kernel.files[path]=original
        self.kernel.current_pid=10;self.resource.close()

    def test_preexec_rejects_legacy_or_mislabelled_journal_before_admission(self):
        self.resource.create();self.kernel.current_pid=200
        original=json.loads(self.resource.journal.read_text())
        for field,value in [('protocol','iotdbfs-cgroup-ratio/1'),('partition','isolated'),
                            ('scheduler_load_balancing',False),('cpus',[True,31])]:
            changed=dict(original);changed[field]=value;self.resource.journal.write_text(json.dumps(changed))
            before=list(self.kernel.writes)
            with self.assertRaises(ResourceError):enter_leaf_exec(self.resource.database,['true'],
                journal_path=self.resource.journal,cgroup_root=self.root,ops=self.kernel)
            self.assertEqual(self.kernel.writes,before)
        self.resource.journal.write_text(json.dumps(original));self.kernel.current_pid=10;self.resource.close()

    def test_foreign_controller_change_is_not_rolled_back(self):
        self.resource.create();other=self.root/'new-job';self.kernel.add_group(other)
        self.kernel.files[other/'cpu.max']='50000 100000'
        with self.assertRaisesRegex(ResourceError,'foreign cgroup'):self.resource.close()
        self.assertEqual(self.kernel.read(other/'cpu.max'),'50000 100000')
        self.assertTrue({'cpu','cpuset','memory'} <= set(self.kernel.read(self.root/'cgroup.subtree_control').split()))

    def test_missing_restored_pidfile_retains_keeper_and_groups_for_supervisor(self):
        self.resource.create();keeper=self.resource.state['keeper']['pid'];self.kernel.exit(100)
        self.pidfile.write_text('')
        with self.assertRaisesRegex(ResourceError,'pidfile'):self.resource.close()
        self.assertIn(keeper,self.kernel.processes);self.assertTrue(self.kernel.exists(self.resource.parent))
        self.kernel.add_process(111,'/'+self.resource.name+'/database',True)
        self.pidfile.write_text('111');self.resource.close()

    def test_trampoline_moves_fresh_child_before_exec_and_does_not_move_supervisor(self):
        self.resource.create()
        with self.assertRaisesRegex(ResourceError,'supervisor'):enter_leaf_exec(self.resource.database,['bash','start-datanode.sh'],
            journal_path=self.resource.journal,cgroup_root=self.root,ops=self.kernel)
        self.kernel.current_pid=200
        receipt=enter_leaf_exec(self.resource.database,['bash','start-datanode.sh'],
            journal_path=self.resource.journal,cgroup_root=self.root,ops=self.kernel)
        self.assertEqual(receipt['leaf'],str(self.resource.database))
        self.assertEqual(self.kernel.cgroup(200),'/'+self.resource.name+'/database')
        self.assertEqual(self.kernel.affinity(200),{30,31})
        self.kernel.current_pid=10;self.kernel.exit(200);self.resource.close()

    def test_database_identity_requires_the_actual_timecho_class(self):
        proc=self.base/'proc';(proc/'300').mkdir(parents=True)
        command=proc/'300/cmdline';command.write_bytes(b'java\0com.timecho.iotdb.DataNode\0')
        operations=LinuxOps(proc)
        self.assertTrue(operations.is_database(300))
        command.write_bytes(b'java\0org.apache.iotdb.db.service.DataNode\0')
        self.assertFalse(operations.is_database(300))

    def test_cli_parses_options_before_separated_exec_argv(self):
        from experiments.cross_version.resource_limited import resources
        with patch.object(resources,'enter_leaf_exec') as execute:
            with patch('sys.argv',['resources','enter-leaf-exec','--leaf','/tmp/owned/database',
                                  '--journal','/tmp/receipt.json','--','bash','start-datanode.sh','-d']):
                resources.main()
            self.assertEqual(execute.call_args.args[1],['bash','start-datanode.sh','-d'])
            self.assertEqual(execute.call_args.kwargs['journal_path'],Path('/tmp/receipt.json'))

    def test_supervisor_placement_unowned_leaf_and_wrong_process_are_rejected(self):
        self.resource.create()
        with self.assertRaisesRegex(ResourceError,'supervisor'):self.resource.attach_query(10)
        with self.assertRaisesRegex(ResourceError,'DataNode'):self.resource.attach_database(200)
        with self.assertRaisesRegex(ResourceError,'unowned'):enter_leaf_exec(self.root/'original.scope',['true'],
            journal_path=self.resource.journal,cgroup_root=self.root,ops=self.kernel)
        self.resource.restore_database(100)
        with self.assertRaisesRegex(ResourceError,'not active'):self.resource.attach_query(200)
        self.resource.close()


if __name__=='__main__':unittest.main()
