from pathlib import Path
import json
import os
import runpy
import tempfile
import unittest
from unittest.mock import patch

from experiments.cross_version.resource_limited import run
from experiments.identity.registry import file_sha


class ControllerTests(unittest.TestCase):
    def test_database_trampoline_changes_only_exact_start_and_restores_module(self):
        from experiments.cross_version.version_directory import v00_head_probe
        original=v00_head_probe.subprocess
        with patch.object(original,'run',return_value='executed') as called:
            with run.database_start_in_leaf(Path('/owned/resources.py'),'/cg/owned/database','/owned/resources.json'):
                command=['bash','sbin/start-datanode.sh','-d','-p','/runtime/pid']
                self.assertEqual(v00_head_probe.subprocess.run(command,cwd='/release',timeout=60),'executed')
                self.assertEqual(called.call_args.args[0][-6:],['--',*command])
                self.assertEqual(called.call_args.kwargs,dict(cwd='/release',timeout=60))
                called.reset_mock()
                v00_head_probe.subprocess.run(['settings-worker','33667'])
                self.assertEqual(called.call_args.args[0],['settings-worker','33667'])
                with self.assertRaisesRegex(RuntimeError,'unexpected original'):
                    v00_head_probe.subprocess.run(['bash','sbin/start-datanode.sh','unsafe'])
            self.assertIs(v00_head_probe.subprocess,original)

    def test_trampoline_module_is_restored_after_exception(self):
        from experiments.cross_version.version_directory import v00_head_probe
        original=v00_head_probe.subprocess
        with self.assertRaises(ValueError):
            with run.database_start_in_leaf(Path('/resources.py'),'/database','/journal'):
                raise ValueError('test')
        self.assertIs(v00_head_probe.subprocess,original)

    def test_actual_launcher_uses_current_engine_and_preserves_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);binary=root/'binary';binary.write_text('frozen test binary')
            config=root/'profile.conf';config.write_text('original=preserved\n')
            script=root/'launcher'
            receipt=run.launcher(script,binary,file_sha(binary),'trusted_prefix',root/'owner')
            with patch.dict(os.environ,{'IOTDB_CONFIG_FILE':str(config),run.DIAGNOSTIC[0]:'1'}),patch('os.execv') as execute:
                runpy.run_path(str(script),run_name='__main__')
                execute.assert_called_once()
                self.assertNotIn(run.DIAGNOSTIC[0],os.environ)
            text=config.read_text()
            self.assertTrue(text.startswith('original=preserved\n'))
            self.assertIn('\nquery.hdf5.mode=iotdb-indexed\n',text)
            self.assertNotIn('catalog_mode',text)
            actual=json.loads(config.with_suffix('.launch.json').read_text())
            self.assertEqual(actual['settings'],receipt['settings'])
            self.assertEqual(actual['actual_binary_sha256'],file_sha(binary))
            binary.write_text('changed')
            with patch.dict(os.environ,{'IOTDB_CONFIG_FILE':str(config)}),self.assertRaisesRegex(RuntimeError,'pinned binary'):
                runpy.run_path(str(script),run_name='__main__')

    def test_grib_and_hdf_delivery_protocols_remain_explicit(self):
        self.assertTrue(run.protocol_for({'format':'grib'},'FS-index')['delivery'].startswith('C++'))
        self.assertTrue(run.protocol_for({'format':'grib'},'IoTDB-FS')['delivery'].startswith('NumPy'))
        self.assertTrue(run.protocol_for({'format':'hdf5'},'FS-index')['delivery'].startswith('NumPy'))

    def test_pending_journal_and_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'state').write_text('state');(root/'owner.lock').write_text('')
            self.assertEqual(set(run.journal_snapshot(root)),{'state','owner.lock'})
            (root/'pending').write_text('pending')
            with self.assertRaises(RuntimeError):run.journal_snapshot(root)
            (root/'pending').unlink();(root/'state').unlink();(root/'state').symlink_to(root/'owner.lock')
            with self.assertRaises(RuntimeError):run.journal_snapshot(root)

    def test_kernel_compact_cpu_range_is_accepted_by_observer(self):
        with tempfile.TemporaryDirectory() as directory:
            paths={}
            for name in ('parent','query','database'):
                path=Path(directory)/name;path.mkdir();paths[name]=str(path)
                for field,value in {'cgroup.procs':str(os.getpid()) if name=='query' else '',
                    'cpuset.cpus.effective':'2-3','cpuset.cpus.partition':'root' if name=='parent' else 'member',
                    'cpuset.cpus.exclusive.effective':'2-3' if name=='parent' else '',
                    'memory.max':str(2*1024**3) if name=='query' else 'max',
                    'cpu.max':'200000 100000' if name=='parent' else 'max 100000'}.items():
                    (path/field).write_text(value)
            proc=Path(directory)/'proc'/str(os.getpid());tid=proc/'task'/str(os.getpid());tid.mkdir(parents=True)
            (tid/'schedstat').write_text('10 20 30')
            (proc/'status').write_text('VmRSS: 1024 kB\nVmHWM: 2048 kB\n')
            with patch('os.sched_getaffinity',return_value={2,3},create=True):
                observed=run.observe(paths,proc_root=Path(directory)/'proc')
            self.assertEqual(observed['groups']['query']['processes'][0]['affinity'],[2,3])
            self.assertEqual(observed['protocol'],'iotdbfs-cgroup-ratio/2')
            for role,field,value in [('parent','cpuset.cpus.partition','isolated'),
                ('parent','cpuset.cpus.exclusive.effective','2'),('query','cpuset.cpus.partition','root'),
                ('database','cpu.max','100000 100000')]:
                target=Path(paths[role])/field;original=target.read_text();target.write_text(value)
                with patch('os.sched_getaffinity',return_value={2,3},create=True),self.assertRaises(RuntimeError):
                    run.observe(paths,proc_root=Path(directory)/'proc')
                target.write_text(original)

    def test_resource_identity_labels_balancing_and_new_journal_protocol(self):
        def read(path,*args,**kwargs):
            return 'model name : synthetic CPU' if str(path)=='/proc/cpuinfo' else '0'
        with patch.object(Path,'read_text',read):identity=run.resource_identity([2,3])
        self.assertEqual(identity['protocol'],'exclusive-balanced-shared-two-logical-cpu/2')
        self.assertEqual(identity['journal_protocol'],'iotdbfs-cgroup-ratio/2')
        self.assertEqual(identity['partition'],'root')
        self.assertIs(identity['scheduler_load_balancing'],True)
        self.assertEqual(identity['exclusive_cpus'],[2,3])
        self.assertEqual(identity['parent_cpu_max'],'200000 100000')


if __name__=='__main__':unittest.main()
