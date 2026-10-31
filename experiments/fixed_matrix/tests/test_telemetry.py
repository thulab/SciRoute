import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from experiments.fixed_matrix import telemetry as t


def proc_stat(pid=123, start=456):
    fields = ['0'] * 50
    fields[0] = 'S'; fields[11] = '12'; fields[12] = '3'
    fields[17] = '2'; fields[19] = str(start)
    fields[20] = '8192'; fields[21] = '1'
    return f'{pid} (name with spaces) ' + ' '.join(fields)


class TelemetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.proc = self.root / 'proc'; self.proc.mkdir()
        self.cg = self.root / 'cgroup'; self.cg.mkdir()
        self.p = self.proc / '123'; self.p.mkdir()
        (self.p / 'stat').write_text(proc_stat())
        (self.p / 'status').write_text('Name:\tprivate\nVmRSS:\t4 kB\nCpus_allowed_list:\t2-3\n')
        (self.p / 'cgroup').write_text('0::/test/query\n')
        (self.p / 'io').write_text('read_bytes: 4096\n')
        (self.p / 'cmdline').write_bytes(b'java\0-Xmx15232M\0-XX:MaxDirectMemorySize=2176M\0-Dpassword=TOP_SECRET\0--private\0secret\0')
        (self.p / 'maps').write_text('')
        (self.cg / 'cgroup.procs').write_text('123\n')
        (self.cg / 'cpu.stat').write_text('usage_usec 123\nnr_throttled 0\n')
        (self.cg / 'memory.current').write_text('4096\n')
        (self.proc / 'stat').write_text('cpu 1 2 3 4\ncpu0 1 2 3 4\n')
        (self.proc / 'loadavg').write_text('1.0 2.0 3.0 1/100 999\n')
        (self.proc / 'meminfo').write_text('MemTotal: 1048576 kB\n')
        (self.proc / 'diskstats').write_text('8 0 sda 0 0 0 0\n')
        self.addCleanup(patch.stopall)
        patch.object(t, 'PROC', self.proc).start()
        patch.object(t, 'SYS', self.root / 'sys').start()

    def test_pid_identity_rss_and_safe_jvm_flags_only(self):
        result = t.process(123, inventory=True)
        self.assertEqual(result['start_ticks'], 456)
        self.assertEqual(result['utime_ticks'], 12)
        self.assertEqual(result['jvm_safe_flags'], ['-XX:MaxDirectMemorySize=2176M','-Xmx15232M'])
        serialized = json.dumps(result)
        self.assertNotIn('TOP_SECRET', serialized)
        self.assertNotIn('password', serialized)
        self.assertNotIn('private', serialized)
        self.assertEqual(result['status']['Cpus_allowed_list'], '2-3')

    def test_raw_samples_preserve_missing_counter_and_process_identity(self):
        sample = t.sample({'query': str(self.cg)}, {'database':123})
        self.assertEqual(sample['cgroups']['query']['recursive_pids'], [123])
        self.assertEqual(sample['cgroups']['query']['files']['cpu.stat'], 'usage_usec 123\nnr_throttled 0')
        self.assertIn('unavailable', sample['cgroups']['query']['files']['memory.peak'])
        self.assertEqual(sample['processes']['123']['start_ticks'], 456)
        self.assertIn('cpu0', sample['host']['stat'])
        follow = t.sample({'query':str(self.cg)}, prev=sample)
        self.assertGreater(follow['sample_start_delta_ns'], 0)
        self.assertGreaterEqual(follow['sampler']['read_wall_ns'], 0)
        self.assertIn('not query-exclusive CPU', sample['scope'])

    def test_exited_pid_and_relative_group_marked_or_rejected(self):
        self.assertIn('unavailable', t.process(999))
        with self.assertRaisesRegex(ValueError, 'absolute'):
            t.sample({'query':'relative'})

    def test_replaced_or_deleted_mapping_does_not_hash_wrong_file(self):
        library = self.root / 'libfake.so'; library.write_bytes(b'new contents')
        (self.p / 'maps').write_text(
            f'1000-2000 r-xp 0000 00:01 99999 {library}\n'
            f'3000-4000 r-xp 0000 00:01 99999 {library} (deleted)\n')
        result = t.process(123, inventory=True)['mapped_binaries']
        self.assertTrue(all('sha256' not in item for item in result))
        self.assertEqual({v['unavailable'] for v in result},
                         {'mapped filename replaced','mapped file deleted'})

    def test_artifact_digest_not_contents_and_missing_not_zero(self):
        p = self.root / 'artifact'; p.write_text('private contents')
        result = t.digest_file(p)
        self.assertEqual(result['bytes'], 16)
        self.assertNotIn('private contents', json.dumps(result))
        self.assertEqual(len(result['sha256']),64)
        self.assertIn('unavailable', t.digest_file(self.root / 'missing'))

    def test_mount_options_do_not_export_credentials(self):
        (self.proc / 'self').mkdir()
        (self.proc / 'self/mountinfo').write_text('1 0 8:0 / /data rw,relatime,password=TOP_SECRET - ext4 /dev/sda rw,data=ordered\n')
        result = t._mount(Path('/data/run'))
        self.assertEqual(result['filesystem'],'ext4')
        self.assertEqual(result['safe_options'],['relatime','rw'])
        self.assertNotIn('TOP_SECRET', json.dumps(result))

    def test_environment_does_not_capture_env_or_config_and_inventory_missing_is_explicit(self):
        with patch.object(t,'_command', return_value={'unavailable':'test'}):
            result = t.environment(self.root, process_roles={'database':123})
        self.assertEqual(result['memory']['MemTotal']['value'],1048576)
        self.assertEqual(result['package_versions'].keys(), {'numpy','h5py','pyarrow','apache-iotdb','eccodes'})
        self.assertNotIn('TOP_SECRET',json.dumps(result))
        self.assertNotIn('environ',result)

    def test_sampler_cli_never_overwrites_and_stop_prevents_sampling(self):
        paths = self.root / 'paths.json'
        paths.write_text(json.dumps({'query':str(self.cg)}))
        stop = self.root / 'stop'; stop.touch()
        output = self.root / 'samples.jsonl'
        args = ['--paths',str(paths),'--stop-file',str(stop),'--output',str(output),
                '--max-seconds','1']
        t.main(args)
        self.assertEqual(output.read_text(),'')
        self.assertEqual(output.stat().st_mode & 0o777,0o600)
        with self.assertRaises(FileExistsError):
            t.main(args)


if __name__ == '__main__':
    unittest.main()
