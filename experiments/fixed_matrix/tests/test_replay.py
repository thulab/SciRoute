"""Query-only recovery must replace complete blocks without result selection."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from experiments.fixed_matrix import replay,run
from experiments.fixed_matrix.tests.test_analysis import fixture

class ReplayTests(unittest.TestCase):
    def test_raw_line_selection_keeps_slow_rows_and_preserves_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'queries.jsonl'
            lines=[b'{"campaign":0,"total_ns":999999}\n',b'{ "campaign":2,"total_ns":1 }\n',
                   b'{"campaign":1,"total_ns":2}\n']
            p.write_bytes(b''.join(lines));before=p.read_bytes()
            self.assertEqual([line for line,_ in replay.selected_rows(p,replay.RETAIN)],[lines[0],lines[2]])
            self.assertEqual([line for line,_ in replay.selected_rows(p,[2])],[lines[1]])
            self.assertEqual(p.read_bytes(),before)

    def test_campaign_gate_rejects_partial_duplicate_identity_and_resource(self):
        rows,catalog,bindings,kwargs=fixture();rid=kwargs['resource_ids'][0]
        rows=[r for r in rows if r['campaign'] in (0,1)]
        with tempfile.TemporaryDirectory() as td,mock.patch.object(run,'validate_sample') as caps:
            p=Path(td)/'q'
            def write(rs):p.write_text(''.join(json.dumps(r)+'\n' for r in rs))
            write(rows);proof=replay.validate_rows(p,catalog,bindings,(0,1),rid)
            self.assertEqual(proof['records'],2304);self.assertEqual(caps.call_count,4608)
            for bad,reason in [(rows[:-1],'incomplete'),(rows+[rows[0]],'duplicate')]:
                write(bad)
                with self.assertRaisesRegex(RuntimeError,reason):replay.validate_rows(p,catalog,bindings,(0,1),rid)
            bad=deepcopy(rows);bad[0]['case_id']='f'*64;write(bad)
            with self.assertRaisesRegex(RuntimeError,'identity'):replay.validate_rows(p,catalog,bindings,(0,1),rid)
            bad=deepcopy(rows);bad[0]['resource_id']='f'*64;write(bad)
            with self.assertRaisesRegex(RuntimeError,'resource'):replay.validate_rows(p,catalog,bindings,(0,1),rid)

    def test_first_tampered_seal_rejected_before_database_or_data_actions(self):
        with tempfile.TemporaryDirectory() as td:
            base=Path(td);source=base/'collection-r2';source.mkdir();(source/'prepared.json').write_text('{}')
            args=SimpleNamespace(source_collection=source,registration=base/'registration-receipt.json',repo=base)
            with mock.patch.object(run,'dependencies') as service,mock.patch.object(run,'copy_file') as copy:
                with self.assertRaisesRegex(RuntimeError,'sealed source changed'):replay.check_source(args)
                service.assert_not_called();copy.assert_not_called()

    def test_replay_entry_has_no_data_preparation_or_index_creation(self):
        tree=ast.parse(Path(replay.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                self.assertNotIn(node.func.attr,('prepare','copy_file','bind_cases'))
            if isinstance(node,ast.keyword) and node.arg in ('build_index','initialize'):
                self.assertIsInstance(node.value,ast.Constant);self.assertIs(node.value.value,False)

    def test_extracted_scientific_loop_matches_executed_r2_ast(self):
        # The old executed source is a sealed fixture on the server. Local
        # environments without that snapshot still run the failure tests above.
        old=Path(__file__).parents[2]/'cross_version/work/fixed-matrix-20261021/collection-r2/source/experiments/fixed_matrix/run.py'
        if not old.exists():self.skipTest('sealed r2 source snapshot is on collection host')
        original=ast.parse(old.read_text());worker=next(n for n in original.body if isinstance(n,ast.FunctionDef) and n.name=='worker')
        loop=next(n for n in ast.walk(worker) if isinstance(n,ast.For) and isinstance(n.target,ast.Name) and n.target.id=='campaign')
        loop.iter=ast.Name(id='campaigns',ctx=ast.Load())
        # Only the output block directory changes to a new execution root.
        for n in ast.walk(loop):
            if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='block' for t in n.targets):
                n.value=ast.parse("plan.get('execution_root',root)/('campaign-'+str(campaign))",mode='eval').body
            if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='mkdir':
                n.keywords=[ast.keyword(arg='parents',value=ast.Constant(True))]
        current=ast.parse(Path(run.__file__).read_text());fn=next(n for n in current.body if isinstance(n,ast.FunctionDef) and n.name=='collect_campaigns')
        actual=next(n for n in fn.body if isinstance(n,ast.For))
        self.assertEqual(ast.dump(actual,include_attributes=False),ast.dump(loop,include_attributes=False))

if __name__=='__main__':unittest.main()
