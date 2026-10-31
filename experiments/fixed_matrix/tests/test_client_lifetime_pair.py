"""Offline schedule, shared-profile and pipe-protocol checks; no science I/O."""
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from experiments.fixed_matrix import client_lifetime_pair as definition


class LifetimePairTests(unittest.TestCase):
    def test_exact480_grid_and_formal_order_balance(self):
        keys=[]
        for block,(campaign,config) in enumerate((c,f) for c in range(4)
                for f in definition.runner.configuration_order(c)):
            first={arm:{mode:0 for mode in definition.MODES} for arm in definition.ARMS}
            for repeat in range(15):
                order=definition.query_order(campaign,config,block,repeat)
                self.assertEqual(len(set(order)),4)
                for arm in definition.ARMS:
                    pair=[mode for a,mode in order if a==arm]
                    self.assertEqual(set(pair),set(definition.MODES))
                    if repeat>=5:first[arm][pair[0]]+=1
                keys.extend((campaign,config,mode,arm,repeat) for arm,mode in order)
            self.assertTrue(all(value==5 for counts in first.values() for value in counts.values()))
        self.assertEqual(len(keys),480);self.assertEqual(len(set(keys)),480)
        self.assertEqual(definition.runner.expected_rows(SimpleNamespace(
            repo=Path(__file__).resolve().parents[3],tier='n128')),480)

    def test_shared_profile_cursor_overwrites_child_history_even_for_FS(self):
        contexts=[]
        class FakeNative:
            def __init__(self,*args):self.closed=False;contexts.append(self)
            def explain(self,*args):return {'plan':'same'}
            def close(self):self.closed=True
        begin=dict(op='begin',client_mode='persistent',block_ordinal=0,
            context={'root':'/r','mount':'/m','socket':'/s','pid':123},library='/lib',
            cfg={},binding={'native_selection':{},'scope':[], 'expected_matches':[],
                            'physical_versions':{}},repo='/repo',binary='/a',
            data_root='/data',datanode_pid=456,resource_paths={},group={})
        ops=[begin,dict(op='query',arm='IoTDB-FS',profile_offset=0),
             dict(op='query',arm='FS-index',profile_offset=250),
             dict(op='query',arm='IoTDB-FS',profile_offset=250),dict(op='end'),
             dict(begin,block_ordinal=1),dict(op='end'),dict(op='stop')]
        output=io.StringIO();incoming=io.StringIO(''.join(json.dumps(o)+'\n' for o in ops))
        row=dict(total_ns=3,discovery_ns=1,value_ns=2)
        def query_io(context,*args,**kw):
            context['profile_offset']+=100
            return dict(row)
        with patch.object(definition.sys,'stdin',incoming),patch.object(definition.sys,'stdout',output),\
             patch.object(definition,'identity',return_value={'pid':9,'start_ticks':10}),\
             patch.object(definition,'NativeContext',FakeNative),\
             patch.object(definition.telemetry,'environment',return_value={}),\
             patch.object(definition.telemetry,'sample',return_value={}),\
             patch.object(definition.run,'validate_sample'),\
             patch.object(definition.hdf5_batch_client,'query',side_effect=query_io),\
             patch.object(definition.client,'query_native',return_value=dict(row)),\
             patch.object(definition.signal,'signal'):
            definition.child_loop()
        replies=[json.loads(s) for s in output.getvalue().splitlines()]
        queries=[r for r in replies if r['event']=='query']
        self.assertEqual([q['profile_offset'] for q in queries],[100,250,350])
        self.assertEqual(len(contexts),2);self.assertTrue(all(c.closed for c in contexts))
        self.assertEqual(queries[0]['row']['native_context_id'],'9:10:0')

    def test_pipe_reply_handles_buffered_lines_and_partial_timeout(self):
        read,write=os.pipe()
        try:
            child=definition.QueryChild.__new__(definition.QueryChild)
            child.pending=bytearray(b'{"ok":true,"event":"first"}\n{"ok":true,"event":"second"}\n')
            child.process=SimpleNamespace(stdout=os.fdopen(read,'rb',buffering=0))
            self.assertEqual(child.receive(.05)['event'],'first')
            self.assertEqual(child.receive(.05)['event'],'second')
            os.write(write,b'{"ok":true')
            with self.assertRaisesRegex(RuntimeError,'timeout|deadline'):
                child.receive(.02)
        finally:
            os.close(write);child.process.stdout.close()


if __name__=='__main__':unittest.main()
