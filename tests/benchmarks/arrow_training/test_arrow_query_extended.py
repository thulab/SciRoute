import unittest
import numpy as np
import pyarrow as pa
from run_arrow_query_extended import arrays_for, filtered_declaration, expected_results
from run_arrow_query_paths import single_client_records


class ExtendedOracleTests(unittest.TestCase):
    def test_single_client_counter_join_rejects_concurrency(self):
        records=[dict(type='query',case='x',repeat=0,clients=1,rows=5),
                 dict(type='wave',case='x',repeat=0,clients=1,rows=5,payload_read_calls=2)]
        self.assertEqual(list(single_client_records(records))[0]['payload_read_calls'],2)
        records[1]['clients']=4
        with self.assertRaisesRegex(ValueError,'concurrent'):
            list(single_client_records(records))

    def fixture(self):
        typ = pa.list_(pa.list_(pa.float32()))
        return [pa.record_batch([pa.array([[[10., 11., 12., 13., 14.],
                     [None, float('inf'), float('nan'), 3., 1.]]],type=typ)],names=['target']),
                pa.record_batch([pa.array([[[20., 21., 22., 23., 24.],
                     [0., 3., 1., 4., 2.]]],type=typ)],names=['target'])]

    def case(self):
        return dict(id='hidden',field='target',batch=0,row=0,children=[0],begin=0,length=5,
                    filter=dict(child=1,op='gt',threshold=2.))

    def test_hidden_filter_unknowns_and_sparse_index(self):
        values,index=arrays_for(self.fixture(),self.case(),0)
        self.assertEqual(index.tolist(),[3])
        self.assertEqual(values[0][0].tolist(),[13.])
        subset=filtered_declaration('/f',1,self.case(),0)['request']['subset']
        self.assertEqual(subset['projection'],['index','value.0'])
        self.assertEqual(subset['physical_array']['values'],[{'child_indices':[0]},{'child_indices':[1]}])
        self.assertEqual(subset['predicate']['field'],'value.1')

    def test_disjoint_batch_and_empty_result(self):
        case=self.case();case['client_batches']=[0,1,0,1]
        expected=expected_results(self.fixture(),[case])['hidden']
        self.assertEqual([e['rows'] for e in expected],[1,2,1,2])
        self.assertNotEqual(expected[0]['digest'],expected[1]['digest'])
        case['filter']['threshold']=10.
        columns,index=arrays_for(self.fixture(),case,1)
        self.assertEqual(index.size,0)
        self.assertEqual(len(columns),1)
        self.assertEqual(columns[0][0].dtype,np.dtype('<f4'))

if __name__=='__main__': unittest.main()
