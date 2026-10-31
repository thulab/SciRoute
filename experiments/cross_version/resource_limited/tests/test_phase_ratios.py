from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from experiments.identity.registry import file_sha
from experiments.cross_version.resource_limited.analysis import summarize
from experiments.cross_version.resource_limited.phase_ratios import derive, summarize_phases
from experiments.cross_version.resource_limited.tests.test_analysis import fixture


def phase_fixture():
    rows, design = fixture()
    for block in design['blocks']:
        hdf = block['scene_id']=='hdf-control'
        block['arm_protocols']['FS-index']['engine']='native-hdf5-SQLite-index' if hdf else 'native-ecCodes-SQLite-index'
        block['arm_protocols']['IoTDB-FS']['engine']='iotdb-indexed-HDF5' if hdf else 'iotdb-indexed-GRIB'
    for row in rows:
        block=next(b for b in design['blocks'] if b['block_id']==row['block_id'])
        row['engine']=block['arm_protocols'][row['arm']]['engine']
        if row['arm']=='FS-index' and row['scene_id']!='hdf-control':
            row.update(discovery_ns=row['total_ns']//4, value_read_ns=row['total_ns']-row['total_ns']//4)
        else:
            row.update(discovery_client_ns=row['total_ns']//2,
                       selected_value_client_ns=row['total_ns']-row['total_ns']//2)
    return rows, design


class PhaseRatios(unittest.TestCase):
    def test_primary_phase_ratios_are_same_block_ratios_of_medians(self):
        rows,design=phase_fixture();original=deepcopy(rows)
        result=summarize_phases(rows,design)
        for cell in result['cells']:
            self.assertAlmostEqual(cell['R'],100000/50500)
            if cell['scene_id']!='hdf-control':
                self.assertAlmostEqual(cell['Rd'],50000/12625)
                self.assertAlmostEqual(cell['Rq'],50000/37875)
                self.assertTrue(cell['arms']['fs']['discovery_clock_is_residual'])
            else:
                self.assertAlmostEqual(cell['Rd'],cell['R'])
                self.assertAlmostEqual(cell['Rq'],cell['R'])
                self.assertFalse(cell['arms']['fs']['discovery_clock_is_residual'])
            self.assertNotAlmostEqual(cell['R'],cell['Rd']+cell['Rq'])
            self.assertEqual(len(cell['pair_ratios']),12)
            self.assertEqual(cell['arms']['fs']['measurement']['Td']['count'],10)
            for pair in cell['pair_ratios']:
                w=pair['fs_discovery_weight']
                self.assertAlmostEqual(pair['R_pair'],w*pair['Rd_pair']+(1-w)*pair['Rq_pair'])
                self.assertEqual(pair['delta_T_ns'],pair['delta_Td_ns']+pair['delta_Tq_ns'])
        self.assertEqual(rows,original)

    def test_missing_invalid_or_nonpartitioning_phase_is_rejected_including_warmup(self):
        for mode in ('missing','zero','bool','float','negative','residual','unknown-engine'):
            rows,design=phase_fixture();row=next(r for r in rows if r['arm']=='FS-index')
            if mode=='missing':row.pop('discovery_ns')
            elif mode=='residual':row['value_read_ns']+=1
            elif mode=='unknown-engine':
                row['engine']='unknown'
                next(b for b in design['blocks'] if b['block_id']==row['block_id'])['arm_protocols'][row['arm']]['engine']='unknown'
                for item in rows:
                    if item['block_id']==row['block_id'] and item['arm']==row['arm']:item['engine']='unknown'
            else:row['discovery_ns']={'zero':0,'bool':True,'float':1.0,'negative':-1}[mode]
            with self.subTest(mode=mode),self.assertRaises(ValueError):summarize_phases(rows,design)

    def test_incomplete_identity_mixed_or_changed_design_still_fails_primary_gate(self):
        for mode in ('missing','identity','phase'):
            rows,design=phase_fixture()
            if mode=='missing':rows.pop()
            elif mode=='identity':rows[0]['case_id']='f'*64
            else:rows[0]['phase']='measurement'
            with self.subTest(mode=mode),self.assertRaises(ValueError):summarize_phases(rows,design)

    def test_stage_medians_are_not_relabelled_as_an_exact_total_decomposition(self):
        rows,design=phase_fixture()
        for row in rows:
            if row['phase']!='measurement' or row['arm']!='IoTDB-FS':continue
            index=row['repeat']-2
            d,q=([(1,100)]*4+[(100,1)]*4+[(100,100)]*2)[index]
            row.update(discovery_client_ns=d,selected_value_client_ns=q,total_ns=d+q)
        result=summarize_phases(rows,design)
        arm=result['cells'][0]['arms']['iotdb']['measurement']
        self.assertNotEqual(arm['Td']['median_ns']+arm['Tq']['median_ns'],arm['T']['median_ns'])

    def test_file_derivation_preserves_sources_and_rejects_changed_or_incomplete_evidence(self):
        rows,design=phase_fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);output=root/'phase-analysis.json'
            (root/'design.json').write_text(json.dumps(design))
            (root/'queries.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
            (root/'analysis.json').write_text(json.dumps(summarize(rows,design)))
            report=dict(complete=True,original_service_restored=True,resources_restored=True,query_records=144,
                **{name+'_sha256':file_sha(root/('queries.jsonl' if name=='queries' else name+'.json')) for name in ('design','queries','analysis')})
            (root/'report.json').write_text(json.dumps(report))
            hashes={p.name:file_sha(p) for p in root.iterdir()}
            result=derive(root,output)
            self.assertTrue(result['complete'])
            self.assertEqual({p.name:file_sha(p) for p in root.iterdir() if p.name in hashes},hashes)
            with self.assertRaisesRegex(ValueError,'already exists'):derive(root,output)
            with (root/'queries.jsonl').open('a') as stream:stream.write('\n')
            with self.assertRaisesRegex(ValueError,'digest differs'):derive(root,root/'changed.json')
            (root/'report.json').write_text(json.dumps(dict(report,resources_restored=False)))
            with self.assertRaisesRegex(ValueError,'complete restored'):derive(root,root/'incomplete.json')


if __name__=='__main__':unittest.main()
